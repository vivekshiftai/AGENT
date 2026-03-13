"""Service for analyzing data sources and generating column descriptions."""
import logging
import json
from typing import Dict, Any, List, Optional
from datetime import datetime
from config.settings import settings
from infrastructure.database.data_source_gateway import DataSourceGateway
from infrastructure.database.postgres_client_singleton import get_shared_postgres_client
from infrastructure.llm.azure_openai import AzureOpenAIClient
from shared.exceptions import DatabaseException

logger = logging.getLogger(__name__)


def quote_identifier(identifier: str, data_source_type: str) -> str:
    """
    Quote SQL identifier based on data source type.
    
    Args:
        identifier: Table or column name
        data_source_type: Type of data source (postgres, mysql, clickhouse, excel, csv)
        
    Returns:
        Properly quoted identifier
    """
    # If already quoted, return as-is
    if (identifier.startswith('"') and identifier.endswith('"')) or \
       (identifier.startswith('[') and identifier.endswith(']')) or \
       (identifier.startswith('`') and identifier.endswith('`')):
        return identifier
    
    # Quote based on data source type
    if data_source_type == "mysql":
        # MySQL uses backticks
        return f"`{identifier}`"
    elif data_source_type in ["postgres", "clickhouse", "excel", "csv"]:
        # PostgreSQL, ClickHouse, and DuckDB (for Excel/CSV) use double quotes
        return f'"{identifier}"'
    else:
        # Default to double quotes (for backward compatibility)
        return f'"{identifier}"'


class DataSourceAnalysisService:
    """Service for analyzing data sources and generating column descriptions using LLM."""
    
    def __init__(self):
        self.postgres_client = get_shared_postgres_client(ensure_tables=False)
        self.llm_client = AzureOpenAIClient()
    
    async def get_unique_values(
        self, 
        gateway: DataSourceGateway, 
        table_name: str, 
        column_name: str, 
        limit: int = 20
    ) -> List[Any]:
        """
        Get top N unique values for a column in a table.
        
        Args:
            gateway: Data source gateway instance
            table_name: Name of the table
            column_name: Name of the column
            limit: Maximum number of unique values to return
            
        Returns:
            List of unique values (up to limit)
        """
        try:
            # Build query to get unique values
            # Different SQL dialects handle DISTINCT differently
            data_source_type = gateway.type
            
            # Quote identifiers to handle spaces and special characters
            quoted_column = quote_identifier(column_name, data_source_type)
            quoted_table = quote_identifier(table_name, data_source_type)
            
            if data_source_type == "mysql":
                # MySQL uses backticks and supports LIMIT
                query = f"""
                    SELECT DISTINCT {quoted_column} 
                    FROM {quoted_table} 
                    WHERE {quoted_column} IS NOT NULL 
                    LIMIT {limit}
                """
            elif data_source_type in ["postgres", "clickhouse"]:
                # PostgreSQL and ClickHouse support LIMIT
                query = f"""
                    SELECT DISTINCT {quoted_column} 
                    FROM {quoted_table} 
                    WHERE {quoted_column} IS NOT NULL 
                    LIMIT {limit}
                """
            elif data_source_type in ["excel", "csv"]:
                # For Excel/CSV, use DuckDB via gateway
                query = f"""
                    SELECT DISTINCT {quoted_column} 
                    FROM {quoted_table} 
                    WHERE {quoted_column} IS NOT NULL 
                    LIMIT {limit}
                """
            else:
                logger.warning(f"Unknown data source type {data_source_type}, using standard SQL")
                query = f"""
                    SELECT DISTINCT {quoted_column} 
                    FROM {quoted_table} 
                    WHERE {quoted_column} IS NOT NULL 
                    LIMIT {limit}
                """
            
            result = await gateway.execute_sql(query)
            unique_values = []
            
            if result and result.get("data"):
                for row in result["data"]:
                    # Handle both tuple rows (from ClickHouse) and dict rows
                    if isinstance(row, tuple):
                        # Tuple row - first element is the column value (single column SELECT)
                        value = row[0] if len(row) > 0 else None
                    elif isinstance(row, dict):
                        # Dict row - use column name as key
                        value = row.get(column_name)
                    else:
                        # Unknown format, try to use as-is
                        value = row
                    
                    if value is not None:
                        # Convert to JSON-serializable format
                        if isinstance(value, (dict, list)):
                            unique_values.append(json.dumps(value))
                        else:
                            unique_values.append(str(value))
                        if len(unique_values) >= limit:
                            break
            
            logger.info(f"Retrieved {len(unique_values)} unique values for {table_name}.{column_name}")
            return unique_values[:limit]
            
        except Exception as e:
            logger.error(f"Failed to get unique values for {table_name}.{column_name}: {str(e)}")
            return []
    
    async def generate_table_column_descriptions(
        self,
        data_source_description: str,
        table_name: str,
        columns_data: List[Dict[str, Any]],
        analysis_id: Optional[int] = None
    ) -> Dict[str, Dict[str, str]]:
        """
        Generate column descriptions for all columns in a table using a single LLM call.
        
        Args:
            data_source_description: User-provided description of the data source
            table_name: Name of the table
            columns_data: List of dictionaries with column info: [{"name": str, "type": str, "unique_values": List}]
            
        Returns:
            Dictionary mapping column_name to {"description": str, "usage_suggestions": str}
        """
        try:
            system_prompt = """You are a data analyst expert. Your task is to analyze database table columns and provide for each column:
1. A clear description of what kind of data is stored in the column
2. Practical suggestions on how the data can be used for analysis

Be concise but informative. Focus on business value and analytical use cases."""
            
            # Prepare columns information for the prompt
            columns_text = ""
            for col_data in columns_data:
                col_name = col_data.get("name", "")
                col_type = col_data.get("type", "Unknown")
                unique_values = col_data.get("unique_values", [])
                
                values_text = ""
                if unique_values:
                    values_preview = unique_values[:20]  # Limit to 20 values
                    values_text = f"\n    Sample values: {', '.join([str(v) for v in values_preview[:10]])}"  # Show first 10
                    if len(unique_values) > 10:
                        values_text += f" ... ({len(unique_values)} total unique values)"
                
                columns_text += f"\n  - {col_name} ({col_type}){values_text}"
            
            user_prompt = f"""Analyze all columns in the following database table:

Data Source Context: {data_source_description}

Table: {table_name}
Columns:{columns_text}

Please provide descriptions for ALL columns. Respond in JSON format with the column names as keys:
{{
    "column_name_1": {{
        "description": "Clear description of the data type and content",
        "usage_suggestions": "Practical suggestions on how to use this data for analysis"
    }},
    "column_name_2": {{
        "description": "...",
        "usage_suggestions": "..."
    }},
    ...
}}

Make sure to include ALL columns listed above."""
            
            logger.info(f"Generating column descriptions for table {table_name} ({len(columns_data)} columns) using LLM")
            # Use analysis_id as query_id to link LLM usage to this analysis
            analysis_query_id = f"analysis_{analysis_id}" if analysis_id else None
            response = await self.llm_client._call_llm_unified(
                model=settings.analytics_data_source_analysis_model,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                node_name="data_source_table_analysis",
                query_id=analysis_query_id,
                temperature=0.3,
                use_json_mode=True
            )
            
            # Parse JSON response
            try:
                result = json.loads(response)
                # Validate that we got descriptions for all columns
                column_names = {col["name"] for col in columns_data}
                descriptions = {}
                
                for col_name in column_names:
                    if col_name in result:
                        descriptions[col_name] = {
                            "description": result[col_name].get("description", ""),
                            "usage_suggestions": result[col_name].get("usage_suggestions", "")
                        }
                    else:
                        # If a column is missing, provide a default
                        logger.warning(f"Missing description for column {table_name}.{col_name} in LLM response")
                        descriptions[col_name] = {
                            "description": "Description not generated",
                            "usage_suggestions": ""
                        }
                
                return descriptions
            except json.JSONDecodeError:
                # If JSON parsing fails, return empty descriptions for all columns
                logger.warning(f"Failed to parse JSON response for table {table_name}, using empty descriptions")
                return {
                    col["name"]: {
                        "description": f"Error parsing LLM response: {response[:200]}",
                        "usage_suggestions": ""
                    }
                    for col in columns_data
                }
                
        except Exception as e:
            logger.error(f"Failed to generate column descriptions for table {table_name}: {str(e)}")
            return {
                col["name"]: {
                    "description": f"Error generating description: {str(e)[:200]}",
                    "usage_suggestions": ""
                }
                for col in columns_data
            }
    
    async def refresh_sap_token_if_needed(
        self,
        data_source_type: str,
        user_id: str
    ) -> None:
        """
        Refresh SAP Datasphere token if the data source is SAP.
        
        **IMPORTANT**: This method is ONLY called before query start (in process_analysis).
        Token refresh does NOT happen during API calls - API methods only retrieve tokens.
        
        This method checks if the data source type is SAP and refreshes
        the user's SAP Datasphere access token before starting the query process.
        The refresh only happens if the token is older than 45 minutes.
        
        Args:
            data_source_type: Type of the data source (e.g., "sap", "sap_datasphere")
            user_id: User ID for token refresh
        """
        try:
            # Check if this is an SAP data source
            is_sap = data_source_type and data_source_type.lower() in ("sap", "sap_datasphere")
            
            if not is_sap:
                logger.debug(f"Skipping SAP token refresh - data source type is {data_source_type}")
                return
            
            logger.info(f"Detected SAP data source - refreshing token for user {user_id} before starting query process")
            
            # Import here to avoid circular dependencies
            try:
                from infrastructure.services.datasphere_service import get_datasphere_service
                datasphere_service = get_datasphere_service()
                
                # Refresh the user's token
                await datasphere_service.refresh_user_token(user_id)
                logger.info(f"Successfully refreshed SAP Datasphere token for user {user_id}")
                
            except ImportError as e:
                logger.warning(f"Failed to import DatasphereService: {str(e)}")
                logger.warning("SAP token refresh skipped - DatasphereService not available")
            except Exception as e:
                logger.error(f"Failed to refresh SAP token for user {user_id}: {str(e)}")
                # Don't fail the analysis if token refresh fails - it might still work with existing token
                logger.warning("Continuing with analysis despite token refresh failure")
                
        except Exception as e:
            logger.error(f"Unexpected error during SAP token refresh check: {str(e)}")
            # Don't fail the analysis - continue with existing token
    
    async def process_analysis(
        self,
        analysis_id: int
    ) -> None:
        """
        Process a complete data source analysis in the background.
        
        Args:
            analysis_id: Analysis ID to process
        """
        try:
            logger.info(f"Starting background processing for analysis {analysis_id}")
            
            # Initialize token usage registry for this analysis
            from infrastructure.llm.token_usage_registry import TokenUsageRegistry, set_token_usage_registry
            analysis_query_id = f"analysis_{analysis_id}"
            registry = TokenUsageRegistry(query_id=analysis_query_id)
            set_token_usage_registry(registry)
            logger.info(f"Initialized token usage registry for analysis {analysis_id}")
            
            # Get analysis status
            analysis_status = await self.get_analysis_status(analysis_id)
            if not analysis_status:
                logger.error(f"Analysis {analysis_id} not found")
                return
            
            data_source_id = analysis_status['data_source_id']
            user_id = analysis_status.get('user_id')
            description = analysis_status.get('description', '')
            
            # Get data source configuration
            results = await self.postgres_client.execute_query_async(
                "SELECT * FROM data_source_config WHERE id = %s",
                (data_source_id,)
            )
            
            if not results:
                await self.update_analysis_status(
                    analysis_id,
                    "failed",
                    error_message="Data source not found"
                )
                return
            
            config = results[0]
            data_source_type = config.get("type", "")
            gateway_config = {
                "type": data_source_type,
                "host": config.get("host"),
                "port": config.get("port"),
                "username": config.get("username"),
                "password": config.get("password"),
                "database_name": config.get("database_name"),
                "file_path": config.get("file_path"),
            }
            
            # Refresh SAP token if needed before starting the query process
            if user_id:
                await self.refresh_sap_token_if_needed(data_source_type, user_id)
            else:
                logger.warning(f"No user_id found in analysis {analysis_id} - skipping SAP token refresh")
            
            # Initialize gateway
            gateway = DataSourceGateway(gateway_config)
            
            # Get list of tables
            logger.info(f"Getting tables for analysis {analysis_id}")
            tables = await gateway.list_tables()
            
            if not tables:
                await self.update_analysis_status(
                    analysis_id,
                    "failed",
                    error_message="No tables found in data source"
                )
                return
            
            total_tables = len(tables)
            logger.info(f"Found {total_tables} tables to analyze for analysis {analysis_id}")
            
            # Update analysis status
            await self.update_analysis_status(
                analysis_id,
                "in_progress",
                progress_percent=5,
                total_tables=total_tables
            )
            
            # Process each table
            processed_tables = 0
            for table_idx, table_name in enumerate(tables):
                try:
                    logger.info(f"Processing table {table_idx + 1}/{total_tables}: {table_name} (analysis {analysis_id})")
                    
                    # Update status
                    progress = 5 + int((table_idx / total_tables) * 90)
                    await self.update_analysis_status(
                        analysis_id,
                        "in_progress",
                        progress_percent=progress,
                        current_table=table_name,
                        processed_tables=processed_tables
                    )
                    
                    # Analyze table (no progress callback needed - we update DB directly)
                    result = await self.analyze_table(
                        gateway,
                        analysis_id,
                        data_source_id,
                        table_name,
                        description,
                        progress_callback=None  # No WebSocket callback
                    )
                    
                    processed_tables += 1
                    
                    # Update progress
                    progress = 5 + int((processed_tables / total_tables) * 90)
                    await self.update_analysis_status(
                        analysis_id,
                        "in_progress",
                        progress_percent=progress,
                        processed_tables=processed_tables
                    )
                    
                except Exception as e:
                    logger.error(f"Failed to analyze table {table_name} in analysis {analysis_id}: {str(e)}")
                    # Continue with next table
                    continue
            
            # Save LLM usage data before marking as completed
            try:
                from infrastructure.database.llm_usage_repository import LLMUsageRepository
                from infrastructure.llm.token_usage_registry import get_token_usage_registry, clear_token_usage_registry
                
                analysis_query_id = f"analysis_{analysis_id}"
                registry = get_token_usage_registry()
                
                if registry:
                    usage_records = registry.get_all_records()
                    if usage_records:
                        usage_repo = LLMUsageRepository()
                        logger.info(f"Saving {len(usage_records)} LLM usage records for analysis {analysis_id}")
                        usage_repo.insert_batch_usage(
                            query_id=analysis_query_id,
                            usage_records=usage_records,
                        )
                        logger.info(f"Successfully saved {len(usage_records)} LLM usage records for analysis {analysis_id}")
                    else:
                        logger.debug(f"No LLM usage records to save for analysis {analysis_id}")
                    # Clear registry after saving
                    clear_token_usage_registry()
                else:
                    logger.debug(f"No token usage registry available for analysis {analysis_id}")
            except Exception as e:
                logger.warning(f"Failed to save LLM usage for analysis {analysis_id}: {str(e)}")
                # Don't fail the analysis if usage saving fails
                try:
                    from infrastructure.llm.token_usage_registry import clear_token_usage_registry
                    clear_token_usage_registry()
                except Exception:
                    pass
            
            # Mark as completed
            await self.update_analysis_status(
                analysis_id,
                "completed",
                progress_percent=100,
                processed_tables=processed_tables
            )
            
            logger.info(f"Analysis {analysis_id} completed successfully: {processed_tables}/{total_tables} tables processed")
            
        except Exception as e:
            logger.error(f"Error processing analysis {analysis_id}: {str(e)}")
            try:
                # Try to save any accumulated LLM usage even on failure
                try:
                    from infrastructure.database.llm_usage_repository import LLMUsageRepository
                    from infrastructure.llm.token_usage_registry import get_token_usage_registry, clear_token_usage_registry
                    
                    analysis_query_id = f"analysis_{analysis_id}"
                    registry = get_token_usage_registry()
                    if registry:
                        usage_records = registry.get_all_records()
                        if usage_records:
                            usage_repo = LLMUsageRepository()
                            usage_repo.insert_batch_usage(
                                query_id=analysis_query_id,
                                usage_records=usage_records,
                            )
                            logger.info(f"Saved {len(usage_records)} LLM usage records for failed analysis {analysis_id}")
                        clear_token_usage_registry()
                except Exception as usage_error:
                    logger.warning(f"Failed to save LLM usage on analysis failure: {str(usage_error)}")
                
                await self.update_analysis_status(
                    analysis_id,
                    "failed",
                    error_message=str(e)
                )
            except Exception:
                pass
    
    async def analyze_table(
        self,
        gateway: DataSourceGateway,
        analysis_id: int,
        data_source_id: int,
        table_name: str,
        data_source_description: str,
        progress_callback: Optional[Any] = None
    ) -> Dict[str, Any]:
        """
        Analyze a single table: get unique values for each column and generate descriptions.
        
        Args:
            gateway: Data source gateway instance
            analysis_id: Analysis ID
            data_source_id: Data source ID
            table_name: Name of the table to analyze
            data_source_description: User-provided description of the data source
            progress_callback: Optional callback function for progress updates
            
        Returns:
            Dictionary with analysis results
        """
        try:
            logger.info(f"Starting analysis of table: {table_name}")
            
            # Get table schema to get column names and types
            schema_str = await gateway.get_table_schema(table_name)
            
            # Parse schema to get column names and types
            columns_info = []
            for line in schema_str.split('\n'):
                if ':' in line and not line.strip().startswith('Table:'):
                    # Format: "  - column_name: data_type"
                    parts = line.strip().lstrip('- ').split(':')
                    if len(parts) >= 2:
                        col_name = parts[0].strip()
                        col_type = ':'.join(parts[1:]).strip()  # Handle types with colons
                        columns_info.append({"name": col_name, "type": col_type})
            
            if not columns_info:
                logger.warning(f"No columns found in schema for {table_name}, trying alternative method")
                # Try to get columns by querying a sample row
                try:
                    # Quote table name to handle spaces and special characters
                    quoted_table = quote_identifier(table_name, gateway.type)
                    sample_result = await gateway.execute_sql(f"SELECT * FROM {quoted_table} LIMIT 1")
                    if sample_result and sample_result.get("columns"):
                        for col_name in sample_result["columns"]:
                            columns_info.append({"name": col_name, "type": "Unknown"})
                except Exception as e:
                    logger.error(f"Failed to get columns for {table_name}: {str(e)}")
                    return {"table_name": table_name, "columns": [], "error": str(e)}
            
            logger.info(f"Found {len(columns_info)} columns in {table_name}")
            
            # Step 1: Get unique values for all columns first
            columns_data = []
            for col_info in columns_info:
                col_name = col_info["name"]
                col_type = col_info.get("type", "Unknown")
                
                # Get unique values
                logger.info(f"Getting unique values for {table_name}.{col_name}")
                unique_values = await self.get_unique_values(gateway, table_name, col_name, limit=20)
                
                columns_data.append({
                    "name": col_name,
                    "type": col_type,
                    "unique_values": unique_values
                })
            
            # Step 2: Generate descriptions for all columns in one LLM call
            logger.info(f"Generating descriptions for all {len(columns_data)} columns in table {table_name}")
            column_descriptions_dict = await self.generate_table_column_descriptions(
                data_source_description,
                table_name,
                columns_data,
                analysis_id=analysis_id
            )
            
            # Step 3: Save all column descriptions to database
            column_descriptions = []
            for col_data in columns_data:
                col_name = col_data["name"]
                col_type = col_data["type"]
                unique_values = col_data["unique_values"]
                
                description_result = column_descriptions_dict.get(col_name, {
                    "description": "",
                    "usage_suggestions": ""
                })
                
                # Save to database
                try:
                    await self.postgres_client.execute_update_async(
                        """
                        INSERT INTO column_descriptions 
                        (analysis_id, data_source_id, table_name, column_name, data_type, unique_values, description, usage_suggestions)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (analysis_id, table_name, column_name) 
                        DO UPDATE SET
                            data_type = EXCLUDED.data_type,
                            unique_values = EXCLUDED.unique_values,
                            description = EXCLUDED.description,
                            usage_suggestions = EXCLUDED.usage_suggestions
                        """,
                        (
                            analysis_id,
                            data_source_id,
                            table_name,
                            col_name,
                            col_type,
                            json.dumps(unique_values),
                            description_result["description"],
                            description_result["usage_suggestions"]
                        )
                    )
                except Exception as e:
                    logger.error(f"Failed to save column description for {table_name}.{col_name}: {str(e)}")
                
                column_descriptions.append({
                    "column_name": col_name,
                    "data_type": col_type,
                    "unique_values_count": len(unique_values),
                    "description": description_result["description"],
                    "usage_suggestions": description_result["usage_suggestions"]
                })
            
            logger.info(f"Completed analysis of table {table_name}: {len(column_descriptions)} columns")
            return {
                "table_name": table_name,
                "columns": column_descriptions,
                "total_columns": len(column_descriptions)
            }
            
        except Exception as e:
            logger.error(f"Failed to analyze table {table_name}: {str(e)}")
            return {
                "table_name": table_name,
                "columns": [],
                "error": str(e)
            }
    
    async def start_analysis(
        self,
        data_source_id: int,
        user_id: str,
        description: str,
        gateway_config: Dict[str, Any]
    ) -> int:
        """
        Start a new data source analysis.
        
        Args:
            data_source_id: Data source ID
            user_id: User ID
            description: User-provided description of the data source
            gateway_config: Data source configuration for gateway
            
        Returns:
            Analysis ID
        """
        try:
            # Create analysis record
            await self.postgres_client.execute_update_async(
                """
                INSERT INTO data_source_analysis 
                (data_source_id, user_id, description, status, started_at)
                VALUES (%s, %s, %s, 'in_progress', CURRENT_TIMESTAMP)
                """,
                (data_source_id, user_id, description)
            )
            
            # Fetch the created record to get the ID
            created = await self.postgres_client.execute_query_async(
                """
                SELECT id FROM data_source_analysis 
                WHERE data_source_id = %s AND user_id = %s AND description = %s 
                ORDER BY id DESC LIMIT 1
                """,
                (data_source_id, user_id, description)
            )
            
            if not created:
                raise DatabaseException("Failed to create analysis record")
            
            analysis_id = created[0]['id']
            logger.info(f"Created analysis record with ID: {analysis_id}")
            return analysis_id
            
        except Exception as e:
            logger.error(f"Failed to start analysis: {str(e)}")
            raise DatabaseException(f"Failed to start analysis: {str(e)}") from e
    
    async def update_analysis_status(
        self,
        analysis_id: int,
        status: str,
        progress_percent: Optional[int] = None,
        current_table: Optional[str] = None,
        total_tables: Optional[int] = None,
        processed_tables: Optional[int] = None,
        error_message: Optional[str] = None
    ):
        """Update analysis status in database."""
        try:
            update_fields = ["status = %s", "updated_at = CURRENT_TIMESTAMP"]
            params = [status]
            
            if progress_percent is not None:
                update_fields.append("progress_percent = %s")
                params.append(progress_percent)
            
            if current_table is not None:
                update_fields.append("current_table = %s")
                params.append(current_table)
            
            if total_tables is not None:
                update_fields.append("total_tables = %s")
                params.append(total_tables)
            
            if processed_tables is not None:
                update_fields.append("processed_tables = %s")
                params.append(processed_tables)
            
            if error_message is not None:
                update_fields.append("error_message = %s")
                params.append(error_message)
            
            if status == "completed":
                update_fields.append("completed_at = CURRENT_TIMESTAMP")
            elif status == "failed":
                update_fields.append("completed_at = CURRENT_TIMESTAMP")
            
            params.append(analysis_id)
            
            await self.postgres_client.execute_update_async(
                f"""
                UPDATE data_source_analysis 
                SET {', '.join(update_fields)}
                WHERE id = %s
                """,
                tuple(params)
            )
            
        except Exception as e:
            logger.error(f"Failed to update analysis status: {str(e)}")
    
    async def get_analysis_status(self, analysis_id: int) -> Optional[Dict[str, Any]]:
        """Get current analysis status."""
        try:
            results = await self.postgres_client.execute_query_async(
                """
                SELECT * FROM data_source_analysis WHERE id = %s
                """,
                (analysis_id,)
            )
            
            if results:
                result = results[0]
                # Convert datetime fields to ISO format
                if result.get('started_at'):
                    result['started_at'] = result['started_at'].isoformat() if hasattr(result['started_at'], 'isoformat') else str(result['started_at'])
                if result.get('completed_at'):
                    result['completed_at'] = result['completed_at'].isoformat() if hasattr(result['completed_at'], 'isoformat') else str(result['completed_at'])
                if result.get('created_at'):
                    result['created_at'] = result['created_at'].isoformat() if hasattr(result['created_at'], 'isoformat') else str(result['created_at'])
                if result.get('updated_at'):
                    result['updated_at'] = result['updated_at'].isoformat() if hasattr(result['updated_at'], 'isoformat') else str(result['updated_at'])
                return result
            return None
            
        except Exception as e:
            logger.error(f"Failed to get analysis status: {str(e)}")
            return None
    
    async def get_all_active_analyses(self) -> List[Dict[str, Any]]:
        """Get all active analyses (in_progress or pending) visible to all users."""
        try:
            results = await self.postgres_client.execute_query_async(
                """
                SELECT * FROM data_source_analysis 
                WHERE status IN ('pending', 'in_progress')
                ORDER BY started_at DESC, created_at DESC
                """
            )
            
            # Convert datetime fields
            for result in results:
                if result.get('started_at'):
                    result['started_at'] = result['started_at'].isoformat() if hasattr(result['started_at'], 'isoformat') else str(result['started_at'])
                if result.get('completed_at'):
                    result['completed_at'] = result['completed_at'].isoformat() if hasattr(result['completed_at'], 'isoformat') else str(result['completed_at'])
                if result.get('created_at'):
                    result['created_at'] = result['created_at'].isoformat() if hasattr(result['created_at'], 'isoformat') else str(result['created_at'])
                if result.get('updated_at'):
                    result['updated_at'] = result['updated_at'].isoformat() if hasattr(result['updated_at'], 'isoformat') else str(result['updated_at'])
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to get active analyses: {str(e)}")
            return []
    
    async def get_latest_analyses_by_data_source(self) -> List[Dict[str, Any]]:
        """Get the latest analysis status for each data source (including completed ones)."""
        try:
            results = await self.postgres_client.execute_query_async(
                """
                SELECT DISTINCT ON (data_source_id) *
                FROM data_source_analysis 
                ORDER BY data_source_id, created_at DESC
                """
            )
            
            # Convert datetime fields
            for result in results:
                if result.get('started_at'):
                    result['started_at'] = result['started_at'].isoformat() if hasattr(result['started_at'], 'isoformat') else str(result['started_at'])
                if result.get('completed_at'):
                    result['completed_at'] = result['completed_at'].isoformat() if hasattr(result['completed_at'], 'isoformat') else str(result['completed_at'])
                if result.get('created_at'):
                    result['created_at'] = result['created_at'].isoformat() if hasattr(result['created_at'], 'isoformat') else str(result['created_at'])
                if result.get('updated_at'):
                    result['updated_at'] = result['updated_at'].isoformat() if hasattr(result['updated_at'], 'isoformat') else str(result['updated_at'])
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to get latest analyses by data source: {str(e)}")
            return []

