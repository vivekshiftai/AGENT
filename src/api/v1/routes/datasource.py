"""Data source management endpoints."""
from fastapi import APIRouter, HTTPException, status, UploadFile, File, Form
from typing import List, Dict, Any
import logging
from pathlib import Path
import shutil
import os
from datetime import datetime
import psycopg2
from psycopg2 import errors
import pandas as pd

from application.dto.data_source_request import (
    DataSourceCreateRequest,
    DataSourceActivateRequest,
    DataSourceTestRequest,
    DataSourceResponse
)
from infrastructure.database.postgres_client_singleton import get_shared_postgres_client
from infrastructure.database.data_source_gateway import (
    DataSourceGateway,
    read_csv_with_encoding,
    read_excel_with_engine,
    get_excel_file_engine
)
from shared.exceptions import DatabaseException

router = APIRouter(prefix="/datasource", tags=["datasource"])
logger = logging.getLogger(__name__)


def get_postgres_client():
    """Get shared PostgreSQL client for data source config storage."""
    try:
        return get_shared_postgres_client(ensure_tables=False)
    except Exception as e: # FIXED INDENTATION AGAIN
        logger.error(f"Failed to get PostgreSQL client: {str(e)}")
        raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="PostgreSQL service unavailable. Data source configuration requires PostgreSQL connection."
            )


def _convert_datetime_fields(record: dict) -> dict:
    """Convert datetime fields to ISO format strings for JSON serialization."""
    converted = record.copy()
    if 'created_at' in converted and converted['created_at']:
        if isinstance(converted['created_at'], datetime):
            converted['created_at'] = converted['created_at'].isoformat()
    return converted


async def _activate_data_source_for_user(client, user_id: str, data_source_id: int):
    """
    Activate a data source for a user (deactivates all others).
    
    Args:
        client: PostgreSQL client
        user_id: User ID
        data_source_id: Data source ID to activate
    """
    try:
        # Deactivate all data sources for the user
        await client.execute_update_async(
            "UPDATE data_source_config SET is_active = FALSE WHERE user_id = %s",
            (user_id,)
        )
        
        # Activate the specified data source
        await client.execute_update_async(
            "UPDATE data_source_config SET is_active = TRUE WHERE id = %s AND user_id = %s",
            (data_source_id, user_id)
        )
        logger.info(f"Activated data source {data_source_id} for user {user_id}")
    except Exception as e:
        logger.error(f"Failed to activate data source {data_source_id} for user {user_id}: {str(e)}")
        # Don't raise - this is a best-effort operation


# NOTE:
# We no longer convert CSV -> Excel. CSV files are treated as first-class data sources and
# are queried directly (DuckDB-based CSV handling in DataSourceGateway).


@router.get("/list")
async def list_data_sources(user_id: str):
    """
    List all data sources for a user with tables and columns information.
    Returns ALL available data sources regardless of active status.
    If the user has no data sources, returns an empty list.
    
    Args:
        user_id: User ID
        
    Returns:
        List of all data sources (active and inactive) with tables and columns information.
        Each data source includes its is_active status.
    """
    try:
        client = get_postgres_client()
        results = await client.execute_query_async(
            "SELECT * FROM data_source_config WHERE user_id = %s ORDER BY created_at DESC",
            (user_id,)
        )
        
        if not results:
            logger.info(f"No data sources found for user {user_id}")
            return []
        
        logger.info(f"Found {len(results)} data source(s) for user {user_id} (returning all regardless of active status)")
        
        data_sources = []
        for row in results:
            source_data = _convert_datetime_fields(row)
            source_response = DataSourceResponse(**source_data)
            
            # Get tables/columns information for each data source
            tables_info = []
            try:
                # Create gateway instance to get table information
                data_source_config = {
                    'type': source_data['type'],
                    'host': source_data.get('host'),
                    'port': source_data.get('port'),
                    'username': source_data.get('username'),
                    'password': source_data.get('password'),
                    'database_name': source_data.get('database_name'),
                    'file_path': source_data.get('file_path'),
                }
                
                # For SAP Datasphere, add user_id to config (required for API calls)
                if source_data['type'].lower() in ('sap', 'sap_datasphere'):
                    data_source_config['user_id'] = user_id
                
                gateway = DataSourceGateway(data_source_config)
                
                # Get list of tables
                table_names = await gateway.list_tables()
                
                # For Excel/CSV, get column information
                if source_data['type'].lower() in ['excel', 'csv']:
                    file_path = source_data.get('file_path')
                    if file_path and Path(file_path).exists():
                        if source_data['type'].lower() == 'excel':
                            engine = get_excel_file_engine(file_path)
                            try:
                                xl_file = pd.ExcelFile(file_path, engine=engine)
                            except Exception:
                                alt_engine = 'xlrd' if engine == 'openpyxl' else 'openpyxl'
                                xl_file = pd.ExcelFile(file_path, engine=alt_engine)
                                engine = alt_engine
                            
                            for sheet_name in xl_file.sheet_names:
                                if sheet_name in table_names:
                                    try:
                                        df = read_excel_with_engine(file_path, sheet_name=sheet_name, engine=engine, nrows=0)
                                        tables_info.append({
                                            "name": sheet_name,
                                            "columns": list(df.columns),
                                            "column_count": len(df.columns)
                                        })
                                    except Exception as e:
                                        logger.warning(f"Failed to read columns for sheet '{sheet_name}': {str(e)}")
                                        tables_info.append({
                                            "name": sheet_name,
                                            "columns": [],
                                            "column_count": 0
                                        })
                        elif source_data['type'].lower() == 'csv':
                            try:
                                df = read_csv_with_encoding(file_path, nrows=0)
                                table_name = Path(file_path).stem
                                if table_name in table_names:
                                    tables_info.append({
                                        "name": table_name,
                                        "columns": list(df.columns),
                                        "column_count": len(df.columns)
                                    })
                            except Exception as e:
                                logger.warning(f"Failed to read columns for CSV: {str(e)}")
                                table_name = Path(file_path).stem
                                if table_name in table_names:
                                    tables_info.append({
                                        "name": table_name,
                                        "columns": [],
                                        "column_count": 0
                                    })
                else:
                    # For database sources, try to get column info for each table
                    for table_name in table_names:
                        try:
                            schema_str = await gateway.get_table_schema(table_name)
                            columns = []
                            for line in schema_str.split('\n'):
                                if ':' in line and not line.strip().startswith('Table:'):
                                    parts = line.strip().lstrip('- ').split(':', 1)
                                    if len(parts) >= 2:
                                        col_name = parts[0].strip()
                                        columns.append(col_name)
                            tables_info.append({
                                "name": table_name,
                                "columns": columns,
                                "column_count": len(columns)
                            })
                        except Exception as e:
                            logger.warning(f"Failed to get columns for table '{table_name}': {str(e)}")
                            tables_info.append({
                                "name": table_name,
                                "columns": [],
                                "column_count": 0
                            })
            except Exception as e:
                logger.warning(f"Failed to get table/column info for data source {source_data.get('name')}: {str(e)}")
                # Continue with empty tables_info if there's an error
            
            # Convert to dict and add tables info
            source_dict = source_response.model_dump()
            source_dict["tables"] = tables_info
            
            data_sources.append(source_dict)
        
        logger.info(f"Returning {len(data_sources)} data source(s) for user {user_id}")
        return data_sources
    except Exception as e:
        logger.error(f"Failed to list data sources: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list data sources: {str(e)}"
        )


@router.post("/create", response_model=DataSourceResponse, status_code=status.HTTP_201_CREATED)
async def create_data_source(request: DataSourceCreateRequest):
    """
    Create a new data source.
    
    Args:
        request: Data source creation request
        
    Returns:
        Created data source
    """
    try:
        client = get_postgres_client()
        
        # Validate required fields based on type
        if request.type.lower() != "excel":
            if not request.host or not request.database_name:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="host and database_name are required for database sources"
                )
        else:
            if not request.file_path:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="file_path is required for Excel sources"
                )
        
        # Insert new data source (set as active by default)
        try:
            result = await client.execute_update_async(
                """
                INSERT INTO data_source_config 
                (user_id, name, type, host, port, username, password, database_name, file_path, is_active)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE)
                RETURNING *
                """,
                (
                    request.user_id,
                    request.name,
                    request.type,
                    request.host,
                    request.port,
                    request.username,
                    request.password,
                    request.database_name,
                    request.file_path,
                )
            )
        except psycopg2.errors.UniqueViolation as e:
            # Handle duplicate name constraint
            logger.warning(f"Duplicate data source name: {request.name} for user {request.user_id}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"A data source with the name '{request.name}' already exists. Please choose a different name."
            )
        except psycopg2.errors.IntegrityError as e:
            # Handle other integrity errors
            logger.error(f"Integrity error creating data source: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Data validation error: {str(e)}"
            )
        
        # Fetch the created record
        created = await client.execute_query_async(
            "SELECT * FROM data_source_config WHERE user_id = %s AND name = %s ORDER BY id DESC LIMIT 1",
            (request.user_id, request.name)
        )
        
        if not created:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve created data source"
            )
        
        # Deactivate all other data sources for this user and ensure this one is active
        created_id = created[0]['id']
        await _activate_data_source_for_user(client, request.user_id, created_id)
        
        # Fetch the updated record to ensure is_active is TRUE
        updated = await client.execute_query_async(
            "SELECT * FROM data_source_config WHERE id = %s AND user_id = %s",
            (created_id, request.user_id)
        )
        
        if updated:
            return DataSourceResponse(**_convert_datetime_fields(updated[0]))
        else:
            return DataSourceResponse(**_convert_datetime_fields(created[0]))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create data source: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create data source: {str(e)}"
        )


@router.post("/upload_file", response_model=DataSourceResponse, status_code=status.HTTP_201_CREATED)
async def upload_file(
    user_id: str = Form(...),
    name: str = Form(...),
    file: UploadFile = File(...)
):
    """
    Upload an Excel or CSV file and create a data source.
    Supports multiple sheets for Excel - all sheets are available as separate tables.
    CSV files are treated as a single table.
    
    Args:
        user_id: User ID
        name: Data source name
        file: Excel (.xlsx, .xls) or CSV (.csv) file to upload
        
    Returns:
        Created data source with file information
    """
    try:
        # Validate file type
        if not file.filename:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="File is required"
            )
        
        is_excel = file.filename.endswith(('.xlsx', '.xls'))
        is_csv = file.filename.endswith('.csv')
        
        if not (is_excel or is_csv):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="File must be an Excel file (.xlsx, .xls) or CSV file (.csv)"
            )
        
        # Create uploads directory
        upload_dir = Path("uploads") / user_id
        upload_dir.mkdir(parents=True, exist_ok=True)
        
        # Save file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = upload_dir / f"{timestamp}_{file.filename}"
        
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        if is_csv:
            logger.info(f"CSV file uploaded; keeping as CSV (no CSV->Excel conversion): {file_path}")
            file_type = 'csv'
        else:
            file_type = 'excel'
        
        # Validate file can be read
        if is_excel:
            try:
                engine = get_excel_file_engine(file_path)
                try:
                    xl_file = pd.ExcelFile(file_path, engine=engine)
                except Exception:
                    # Try alternative engine
                    alt_engine = 'xlrd' if engine == 'openpyxl' else 'openpyxl'
                    xl_file = pd.ExcelFile(file_path, engine=alt_engine)
                sheet_names = xl_file.sheet_names
                logger.info(f"Excel file contains {len(sheet_names)} sheet(s): {', '.join(sheet_names)}")
                if not sheet_names:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Excel file contains no sheets"
                    )
            except HTTPException:
                raise
            except Exception as e:
                logger.warning(f"Failed to read Excel sheets: {str(e)}")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Failed to read Excel file: {str(e)}"
                )
        
        # Validate CSV can be read
        if is_csv:
            try:
                read_csv_with_encoding(str(file_path), nrows=5)
            except Exception as e:
                logger.warning(f"Failed to read CSV: {str(e)}")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Failed to read CSV file: {str(e)}"
                )

        # Create data source record (set as active by default)
        client = get_postgres_client()
        try:
            await client.execute_update_async(
                """
                INSERT INTO data_source_config 
                (user_id, name, type, file_path, is_active)
                VALUES (%s, %s, %s, %s, TRUE)
                """,
                (user_id, name, file_type, str(file_path))
            )
        except psycopg2.errors.UniqueViolation as e:
            # Handle duplicate name constraint
            logger.warning(f"Duplicate data source name: {name} for user {user_id}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"A data source with the name '{name}' already exists. Please choose a different name."
            )
        
        # Fetch the created record
        created = await client.execute_query_async(
            "SELECT * FROM data_source_config WHERE user_id = %s AND name = %s ORDER BY id DESC LIMIT 1",
            (user_id, name)
        )
        
        if not created:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve created data source"
            )
        
        # Deactivate all other data sources for this user and ensure this one is active
        created_id = created[0]['id']
        await _activate_data_source_for_user(client, user_id, created_id)
        
        # Fetch the updated record to ensure is_active is TRUE
        updated = await client.execute_query_async(
            "SELECT * FROM data_source_config WHERE id = %s AND user_id = %s",
            (created_id, user_id)
        )
        
        if updated:
            return DataSourceResponse(**_convert_datetime_fields(updated[0]))
        else:
            return DataSourceResponse(**_convert_datetime_fields(created[0]))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to upload file: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to upload file: {str(e)}"
        )


@router.post("/upload_excel", response_model=DataSourceResponse, status_code=status.HTTP_201_CREATED)
async def upload_excel_file(
    user_id: str = Form(...),
    name: str = Form(...),
    file: UploadFile = File(...)
):
    """
    Upload an Excel file and create a data source (legacy endpoint, use /upload_file for CSV support).
    Supports multiple sheets - all sheets are available as separate tables.
    """
    return await upload_file(user_id, name, file)


@router.post("/upload_file_multiple", response_model=DataSourceResponse, status_code=status.HTTP_201_CREATED)
async def upload_multiple_files(
    user_id: str = Form(...),
    name: str = Form(...),
    files: List[UploadFile] = File(...)
):
    """
    Upload multiple Excel or CSV files and merge them into a single data source.
    All sheets from Excel files and CSV files will be available as separate tables.
    
    Args:
        user_id: User ID
        name: Data source name
        files: List of Excel (.xlsx, .xls) or CSV (.csv) files to upload and merge
        
    Returns:
        Created data source with all sheets/tables from all files
    """
    try:
        if not files or len(files) == 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="At least one file is required"
            )
        
        # Validate all files are Excel or CSV
        for file in files:
            if not file.filename:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="All files must have a filename"
                )
            is_excel = file.filename.endswith(('.xlsx', '.xls'))
            is_csv = file.filename.endswith('.csv')
            if not (is_excel or is_csv):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"File '{file.filename}' must be an Excel file (.xlsx, .xls) or CSV file (.csv)"
                )
        
        # Create uploads directory
        upload_dir = Path("uploads") / user_id
        upload_dir.mkdir(parents=True, exist_ok=True)
        
        # Create merged file path
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        merged_file_path = upload_dir / f"{timestamp}_merged_{name}.xlsx"
        
        # Read all files and merge their sheets
        all_sheets = {}
        total_sheets = 0
        
        for file in files:
            # Save temp file
            temp_file_path = upload_dir / f"temp_{timestamp}_{file.filename}"
            with open(temp_file_path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)
            
            try:
                is_excel = file.filename.endswith(('.xlsx', '.xls'))
                is_csv = file.filename.endswith('.csv')
                
                if is_excel:
                    # Read Excel file
                    engine = get_excel_file_engine(str(temp_file_path))
                    try:
                        xl_file = pd.ExcelFile(temp_file_path, engine=engine)
                    except Exception:
                        # Try alternative engine
                        alt_engine = 'xlrd' if engine == 'openpyxl' else 'openpyxl'
                        xl_file = pd.ExcelFile(temp_file_path, engine=alt_engine)
                        engine = alt_engine
                    
                    sheet_names = xl_file.sheet_names
                    
                    if not sheet_names:
                        logger.warning(f"File '{file.filename}' contains no sheets, skipping")
                        continue
                    
                    # Read all sheets from this file
                    for sheet_name in sheet_names:
                        # Handle duplicate sheet names by adding file prefix
                        final_sheet_name = sheet_name
                        if final_sheet_name in all_sheets:
                            # Add file name prefix to avoid conflicts
                            file_prefix = Path(file.filename).stem
                            counter = 1
                            final_sheet_name = f"{file_prefix}_{sheet_name}"
                            while final_sheet_name in all_sheets:
                                final_sheet_name = f"{file_prefix}_{sheet_name}_{counter}"
                                counter += 1
                        
                        try:
                            df = read_excel_with_engine(str(temp_file_path), sheet_name=sheet_name, engine=engine)
                            all_sheets[final_sheet_name] = df
                            total_sheets += 1
                            logger.info(f"Loaded sheet '{final_sheet_name}' from file '{file.filename}'")
                        except Exception as e:
                            logger.warning(f"Failed to read sheet '{sheet_name}' from '{file.filename}': {str(e)}")
                            continue
                    
                    xl_file.close()
                elif is_csv:
                    # Keep CSV as CSV: read directly and treat the CSV file as a single "table"
                    try:
                        df = read_csv_with_encoding(str(temp_file_path))
                        file_prefix = Path(file.filename).stem
                        final_sheet_name = file_prefix or "csv_table"

                        # Handle duplicate names
                        if final_sheet_name in all_sheets:
                            counter = 1
                            base_name = final_sheet_name
                            final_sheet_name = f"{base_name}_{counter}"
                            while final_sheet_name in all_sheets:
                                counter += 1
                                final_sheet_name = f"{base_name}_{counter}"

                        all_sheets[final_sheet_name] = df
                        total_sheets += 1
                        logger.info(f"Loaded CSV '{final_sheet_name}' from file '{file.filename}' (no CSV->Excel conversion)")
                    except Exception as e:
                        logger.warning(f"Failed to read CSV from '{file.filename}': {str(e)}")
                        continue
            except Exception as e:
                logger.error(f"Failed to process file '{file.filename}': {str(e)}")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Failed to read file '{file.filename}': {str(e)}"
                )
            finally:
                # Clean up temp file
                try:
                    if temp_file_path.exists():
                        temp_file_path.unlink()
                except:
                    pass
        
        if not all_sheets:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No sheets or tables found in any of the uploaded files"
            )
        
        # Write all sheets to merged file
        with pd.ExcelWriter(merged_file_path, engine='openpyxl', mode='w') as writer:
            for sheet_name, df in all_sheets.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        logger.info(f"Merged {len(files)} files into {total_sheets} sheets/tables in '{merged_file_path}'")
        
        # Create data source record (use 'excel' type for merged files as they're saved as .xlsx, set as active by default)
        client = get_postgres_client()
        try:
            await client.execute_update_async(
                """
                INSERT INTO data_source_config 
                (user_id, name, type, file_path, is_active)
                VALUES (%s, %s, 'excel', %s, TRUE)
                """,
                (user_id, name, str(merged_file_path))
            )
        except psycopg2.errors.UniqueViolation as e:
            # Clean up merged file
            try:
                if merged_file_path.exists():
                    merged_file_path.unlink()
            except:
                pass
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"A data source with the name '{name}' already exists. Please choose a different name."
            )
        
        # Fetch the created record
        created = await client.execute_query_async(
            "SELECT * FROM data_source_config WHERE user_id = %s AND name = %s ORDER BY id DESC LIMIT 1",
            (user_id, name)
        )
        
        if not created:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve created data source"
            )
        
        # Deactivate all other data sources for this user and ensure this one is active
        created_id = created[0]['id']
        await _activate_data_source_for_user(client, user_id, created_id)
        
        # Fetch the updated record to ensure is_active is TRUE
        updated = await client.execute_query_async(
            "SELECT * FROM data_source_config WHERE id = %s AND user_id = %s",
            (created_id, user_id)
        )
        
        if updated:
            return DataSourceResponse(**_convert_datetime_fields(updated[0]))
        else:
            return DataSourceResponse(**_convert_datetime_fields(created[0]))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to upload and merge files: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to upload and merge files: {str(e)}"
        )


@router.post("/upload_excel_multiple", response_model=DataSourceResponse, status_code=status.HTTP_201_CREATED)
async def upload_multiple_excel_files(
    user_id: str = Form(...),
    name: str = Form(...),
    files: List[UploadFile] = File(...)
):
    """
    Upload multiple Excel files and merge them (legacy endpoint, use /upload_file_multiple for CSV support).
    """
    return await upload_multiple_files(user_id, name, files)


@router.post("/excel_sheets/{data_source_id}/append", response_model=Dict[str, Any])
async def append_excel_sheets(
    data_source_id: int,
    user_id: str = Form(...),
    file: UploadFile = File(...)
):
    """
    Append sheets from a new Excel file to an existing Excel data source.
    
    Args:
        data_source_id: Existing Excel data source ID
        user_id: User ID
        file: New Excel file containing sheets to append
        
    Returns:
        Updated sheet information
    """
    try:
        # Validate file type
        if not file.filename or not file.filename.endswith(('.xlsx', '.xls')):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="File must be an Excel file (.xlsx or .xls)"
            )
        
        client = get_postgres_client()
        
        # Get existing data source
        existing = await client.execute_query_async(
            "SELECT * FROM data_source_config WHERE id = %s AND user_id = %s",
            (data_source_id, user_id)
        )
        
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data source not found"
            )
        
        source = existing[0]
        if source['type'] != 'excel':
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Data source is not an Excel file"
            )
        
        existing_file_path = Path(source.get('file_path'))
        if not existing_file_path.exists():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Original Excel file not found"
            )
        
        # Read new Excel file
        temp_file_path = Path("uploads") / user_id / f"temp_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{file.filename}"
        temp_file_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(temp_file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        try:
            # Get engines for both files
            new_engine = get_excel_file_engine(str(temp_file_path))
            existing_engine = get_excel_file_engine(str(existing_file_path))
            
            try:
                new_xl_file = pd.ExcelFile(temp_file_path, engine=new_engine)
            except Exception:
                alt_engine = 'xlrd' if new_engine == 'openpyxl' else 'openpyxl'
                new_xl_file = pd.ExcelFile(temp_file_path, engine=alt_engine)
                new_engine = alt_engine
            
            new_sheet_names = new_xl_file.sheet_names
            
            if not new_sheet_names:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="New Excel file contains no sheets"
                )
            
            # Read existing Excel file to get sheet names
            try:
                existing_xl_file = pd.ExcelFile(existing_file_path, engine=existing_engine)
            except Exception:
                alt_engine = 'xlrd' if existing_engine == 'openpyxl' else 'openpyxl'
                existing_xl_file = pd.ExcelFile(existing_file_path, engine=alt_engine)
                existing_engine = alt_engine
            
            existing_sheet_names = existing_xl_file.sheet_names
            existing_xl_file.close()
            
            # Use pandas and openpyxl together to append sheets
            # This approach avoids StyleProxy issues by using pandas for data transfer
            appended_sheets = []
            skipped_sheets = []
            
            # Read all existing sheets into memory
            existing_sheets = {}
            for sheet_name in existing_sheet_names:
                existing_sheets[sheet_name] = read_excel_with_engine(
                    str(existing_file_path), sheet_name=sheet_name, engine=existing_engine
                )
            
            # Read all new sheets
            new_sheets_data = {}
            for sheet_name in new_sheet_names:
                new_sheets_data[sheet_name] = read_excel_with_engine(
                    str(temp_file_path), sheet_name=sheet_name, engine=new_engine
                )
            
            # Process each new sheet
            for new_sheet_name in new_sheet_names:
                final_sheet_name = new_sheet_name
                
                # If sheet name already exists, rename it
                if new_sheet_name in existing_sheet_names:
                    counter = 1
                    final_sheet_name = f"{new_sheet_name}_new"
                    while final_sheet_name in existing_sheet_names or final_sheet_name in [s['name'] for s in appended_sheets]:
                        final_sheet_name = f"{new_sheet_name}_new_{counter}"
                        counter += 1
                    skipped_sheets.append({"original": new_sheet_name, "renamed": final_sheet_name})
                
                # Add the new sheet data
                existing_sheets[final_sheet_name] = new_sheets_data[new_sheet_name]
                appended_sheets.append({"name": final_sheet_name})
            
            # Write all sheets back to the file using pandas
            with pd.ExcelWriter(existing_file_path, engine='openpyxl', mode='w') as writer:
                for sheet_name, df in existing_sheets.items():
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            # Clean up temp file
            try:
                temp_file_path.unlink()
            except:
                pass
            
            logger.info(f"Appended {len(appended_sheets)} sheet(s) to Excel file: {existing_file_path}")
            
            return {
                "success": True,
                "message": f"Successfully appended {len(appended_sheets)} sheet(s) to the Excel file",
                "appended_sheets": appended_sheets,
                "skipped_sheets": skipped_sheets if skipped_sheets else None,
                "data_source_id": data_source_id
            }
            
        except Exception as e:
            # Clean up temp file on error
            try:
                if temp_file_path.exists():
                    temp_file_path.unlink()
            except:
                pass
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to append sheets: {str(e)}"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to append Excel sheets: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to append Excel sheets: {str(e)}"
        )


@router.get("/excel_sheets/{data_source_id}")
async def get_excel_sheets(data_source_id: int, user_id: str):
    """
    Get list of sheets/tables from an Excel or CSV data source.
    
    Args:
        data_source_id: Data source ID
        user_id: User ID
        
    Returns:
        List of sheet/table names
    """
    try:
        client = get_postgres_client()
        results = await client.execute_query_async(
            "SELECT * FROM data_source_config WHERE id = %s AND user_id = %s",
            (data_source_id, user_id)
        )
        
        if not results:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data source not found"
            )
        
        source = results[0]
        file_type = source['type']
        
        if file_type not in ['excel', 'csv']:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Data source is not an Excel or CSV file"
            )
        
        file_path = source.get('file_path')
        if not file_path or not Path(file_path).exists():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="File not found"
            )
        
        sheet_info = []
        
        if file_type == 'excel':
            # Read sheet names from Excel
            engine = get_excel_file_engine(file_path)
            try:
                xl_file = pd.ExcelFile(file_path, engine=engine)
            except Exception:
                # Try alternative engine
                alt_engine = 'xlrd' if engine == 'openpyxl' else 'openpyxl'
                xl_file = pd.ExcelFile(file_path, engine=alt_engine)
                engine = alt_engine
            
            for sheet_name in xl_file.sheet_names:
                try:
                    df = read_excel_with_engine(file_path, sheet_name=sheet_name, engine=engine, nrows=0)
                    sheet_info.append({
                        "name": sheet_name,
                        "columns": list(df.columns),
                        "column_count": len(df.columns)
                    })
                except Exception as e:
                    logger.warning(f"Failed to read sheet '{sheet_name}': {str(e)}")
                    sheet_info.append({
                        "name": sheet_name,
                        "columns": [],
                        "column_count": 0,
                        "error": str(e)
                    })
        elif file_type == 'csv':
            # For CSV, return single table info
            try:
                df = read_csv_with_encoding(file_path, nrows=0)
                table_name = Path(file_path).stem
                sheet_info.append({
                    "name": table_name,
                    "columns": list(df.columns),
                    "column_count": len(df.columns)
                })
            except Exception as e:
                logger.warning(f"Failed to read CSV file: {str(e)}")
                table_name = Path(file_path).stem
                sheet_info.append({
                    "name": table_name,
                    "columns": [],
                    "column_count": 0,
                    "error": str(e)
                })
        
        return {
            "data_source_id": data_source_id,
            "file_path": file_path,
            "sheets": sheet_info
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get Excel sheets: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get Excel sheets: {str(e)}"
        )


@router.post("/activate", response_model=DataSourceResponse)
async def activate_data_source(request: DataSourceActivateRequest):
    """
    Activate a data source (deactivates all others for the user).
    
    Args:
        request: Activation request
        
    Returns:
        Activated data source
    """
    try:
        client = get_postgres_client()
        
        # Deactivate all data sources for the user
        await client.execute_update_async(
            "UPDATE data_source_config SET is_active = FALSE WHERE user_id = %s",
            (request.user_id,)
        )
        
        # Activate the specified data source
        await client.execute_update_async(
            "UPDATE data_source_config SET is_active = TRUE WHERE id = %s AND user_id = %s",
            (request.data_source_id, request.user_id)
        )
        
        # Fetch the activated record
        activated = await client.execute_query_async(
            "SELECT * FROM data_source_config WHERE id = %s AND user_id = %s",
            (request.data_source_id, request.user_id)
        )
        
        if not activated:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data source not found"
            )
        
        return DataSourceResponse(**_convert_datetime_fields(activated[0]))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to activate data source: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to activate data source: {str(e)}"
        )


@router.delete("/{data_source_id}", response_model=Dict[str, Any])
async def delete_data_source(data_source_id: int, user_id: str):
    """
    Delete a data source.
    
    Args:
        data_source_id: Data source ID to delete
        user_id: User ID
        
    Returns:
        Success message
    """
    try:
        client = get_postgres_client()
        
        # First, check if data source exists and belongs to user
        existing = await client.execute_query_async(
            "SELECT * FROM data_source_config WHERE id = %s AND user_id = %s",
            (data_source_id, user_id)
        )
        
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data source not found"
            )
        
        source = existing[0]
        source_name = source['name']
        file_path = source.get('file_path')
        
        # Delete the data source record
        await client.execute_update_async(
            "DELETE FROM data_source_config WHERE id = %s AND user_id = %s",
            (data_source_id, user_id)
        )
        
        # If it's an Excel file, optionally delete the file (or keep it for safety)
        # For now, we'll keep the file in case user wants to re-upload
        # Uncomment below if you want to delete the file:
        # if file_path and Path(file_path).exists() and source['type'] == 'excel':
        #     try:
        #         Path(file_path).unlink()
        #         logger.info(f"Deleted Excel file: {file_path}")
        #     except Exception as e:
        #         logger.warning(f"Failed to delete Excel file {file_path}: {str(e)}")
        
        return {
            "success": True,
            "message": f"Data source '{source_name}' has been deleted",
            "data_source_id": data_source_id
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete data source: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete data source: {str(e)}"
        )


@router.post("/deactivate", response_model=Dict[str, Any])
async def deactivate_data_source(request: DataSourceActivateRequest):
    """
    Deactivate a data source.
    
    Args:
        request: Deactivation request (uses same structure as activate)
        
    Returns:
        Success message
    """
    try:
        client = get_postgres_client()
        
        # Deactivate the specified data source
        await client.execute_update_async(
            "UPDATE data_source_config SET is_active = FALSE WHERE id = %s AND user_id = %s",
            (request.data_source_id, request.user_id)
        )
        
        # Verify the data source exists
        deactivated = await client.execute_query_async(
            "SELECT * FROM data_source_config WHERE id = %s AND user_id = %s",
            (request.data_source_id, request.user_id)
        )
        
        if not deactivated:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data source not found"
            )
        
        return {
            "success": True,
            "message": f"Data source '{deactivated[0]['name']}' has been deactivated",
            "data_source_id": request.data_source_id
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to deactivate data source: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to deactivate data source: {str(e)}"
        )


@router.post("/test")
async def test_data_source(request: DataSourceTestRequest):
    """
    Test a data source connection.
    
    Args:
        request: Test request
        
    Returns:
        Test result
    """
    try:
        client = get_postgres_client()
        
        # Get data source config
        if request.data_source_id:
            results = await client.execute_query_async(
                "SELECT * FROM data_source_config WHERE id = %s AND user_id = %s",
                (request.data_source_id, request.user_id)
            )
            if not results:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Data source not found"
                )
            config = results[0]
            # Convert to dict, excluding id and metadata fields
            gateway_config = {
                "type": config["type"],
                "host": config.get("host"),
                "port": config.get("port"),
                "username": config.get("username"),
                "password": config.get("password"),
                "database_name": config.get("database_name"),
                "file_path": config.get("file_path"),
            }
        elif request.config:
            gateway_config = request.config
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Either data_source_id or config must be provided"
            )
        
        # Test connection
        gateway = DataSourceGateway(gateway_config)
        is_connected = await gateway.test_connection()
        gateway.close()
        
        return {
            "success": is_connected,
            "message": "Connection successful" if is_connected else "Connection failed"
        }
    except HTTPException:
        raise
    except DatabaseException as e:
        logger.error(f"Database connection test failed: {str(e)}")
        return {
            "success": False,
            "message": str(e)
        }
    except Exception as e:
        logger.error(f"Failed to test data source: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to test data source: {str(e)}"
        )


@router.post("/test_all")
async def test_all_data_sources(user_id: str):
    """
    Test connections for all data sources for a user.
    
    Args:
        user_id: User ID (query parameter)
        
    Returns:
        Dictionary mapping data source IDs to connection status
    """
    try:
        client = get_postgres_client()
        
        # Get all data sources for the user
        sources = await client.execute_query_async(
            "SELECT * FROM data_source_config WHERE user_id = %s",
            (user_id,)
        )
        
        results = {}
        
        # Test each data source
        for source in sources:
            source_id = source["id"]
            try:
                gateway_config = {
                    "type": source["type"],
                    "host": source.get("host"),
                    "port": source.get("port"),
                    "username": source.get("username"),
                    "password": source.get("password"),
                    "database_name": source.get("database_name"),
                    "file_path": source.get("file_path"),
                    "user_id": source.get("user_id"),  # Add user_id for SAP Datasphere connection test
                }
                
                gateway = DataSourceGateway(gateway_config)
                is_connected = await gateway.test_connection()
                gateway.close()
                
                results[source_id] = {
                    "success": is_connected,
                    "message": "Connection successful" if is_connected else "Connection failed"
                }
            except Exception as e:
                logger.error(f"Failed to test data source {source_id}: {str(e)}")
                results[source_id] = {
                    "success": False,
                    "message": str(e)[:200]  # Truncate long error messages
                }
        
        return results
    except Exception as e:
        logger.error(f"Failed to test all data sources: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to test data sources: {str(e)}"
        )


@router.get("/active", response_model=DataSourceResponse)
async def get_active_data_source(user_id: str):
    """
    Get the active data source for a user.
    
    Args:
        user_id: User ID
        
    Returns:
        Active data source or 404 if none
    """
    try:
        client = get_postgres_client()
        results = await client.execute_query_async(
            "SELECT * FROM data_source_config WHERE user_id = %s AND is_active = TRUE LIMIT 1",
            (user_id,)
        )
        
        if not results:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No active data source found"
            )
        
        return DataSourceResponse(**_convert_datetime_fields(results[0]))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get active data source: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get active data source: {str(e)}"
        )
