"""Data export endpoints - Download full source data on demand.

Optimized for large-scale financial data exports (3M+ rows):
- Cache-first approach: reads from cache instead of re-querying database
- Streaming export to prevent memory overflow (fallback)
- Gzip compression for faster downloads
- Non-blocking async execution

Flow:
1. Check cache for data (saved during initial query)
2. If cache hit: stream from Parquet file (fast, no DB query)
3. If cache miss: query database with streaming chunks (fallback)
"""
from fastapi import APIRouter, HTTPException, status, Query
from fastapi.responses import StreamingResponse
import logging
from typing import Optional, AsyncGenerator
import io
import gzip
import asyncio
from datetime import datetime

from infrastructure.database.clickhouse import ClickHouseClient
from infrastructure.database.data_source_gateway import DataSourceGateway
from infrastructure.database.postgres_client_singleton import get_shared_postgres_client
from infrastructure.cache.data_cache import get_query_cache

router = APIRouter(prefix="/export", tags=["export"])
logger = logging.getLogger(__name__)

# Chunk size for streaming exports (rows per chunk)
# 100K rows balances memory usage vs. overhead
EXPORT_CHUNK_SIZE = 100_000


async def _stream_sap_table_csv(
    api_url: str,
    user_id: str,
    compress: bool = True,
) -> AsyncGenerator[bytes, None]:
    """
    Re-fetch SAP table data from the stored API URL and stream as CSV.
    Uses the user's SAP token so the same data they saw in the query is returned.
    """
    import httpx
    import pandas as pd

    try:
        from infrastructure.services.datasphere_service import get_datasphere_service
        datasphere = get_datasphere_service()
        token = datasphere._get_user_token(user_id)
    except Exception as e:
        logger.error(f"📥 [Export SAP] Failed to get SAP token for user: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="SAP authentication failed. Please sign in again.",
        ) from e

    async with httpx.AsyncClient(timeout=120.0) as client:
        resp = await client.get(
            api_url,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        )
        resp.raise_for_status()
        data = resp.json()

    # OData returns { "value": [ { ... }, ... ] } or { "d": { "results": [...] } }
    d = data.get("d")
    rows = data.get("value") or (d.get("results") if isinstance(d, dict) else [])
    if not rows:
        # No rows: yield empty (valid CSV)
        yield gzip.compress(b"") if compress else b""
        return

    df = pd.DataFrame(rows)
    csv_content = df.to_csv(index=False)
    csv_bytes = csv_content.encode("utf-8")
    if compress:
        csv_bytes = gzip.compress(csv_bytes)
    yield csv_bytes
    logger.info(f"📥 [Export SAP] Streamed {len(rows):,} rows from stored API URL")


async def _stream_csv_chunks_with_query(
    db_client: DataSourceGateway,
    sql_query: str,
    compress: bool = True
) -> AsyncGenerator[bytes, None]:
    """
    Stream CSV data by re-running the cached SQL query.
    
    This uses the exact same query that was used during the initial fetch,
    ensuring the user gets the same data they saw in the preview.
    
    For 3M+ row exports, this prevents loading all data into memory by
    adding LIMIT/OFFSET to the query.
    
    Args:
        db_client: DataSourceGateway instance
        sql_query: The SQL query to execute (from cache or default)
        compress: Whether to gzip compress each chunk
        
    Yields:
        Bytes chunks of CSV data (optionally compressed)
    """
    import pandas as pd
    import re
    
    # Always use subquery approach for count to ensure consistency across all data sources
    # This is especially important for Excel/CSV where table names may be cleaned/mapped
    # Remove any existing LIMIT/OFFSET from the base query
    base_query = re.sub(r'\s+LIMIT\s+\d+(\s+OFFSET\s+\d+)?', '', sql_query, flags=re.IGNORECASE)
    count_query = f"SELECT COUNT(*) as cnt FROM ({base_query}) as subq"
    
    try:
        logger.debug(f"📥 [Export Stream] Executing count query: {count_query[:200]}...")
        count_result = await db_client.execute_sql(count_query)
        
        # Handle different result formats
        if count_result and count_result.get("data"):
            # Try to extract count from result
            count_data = count_result["data"]
            if isinstance(count_data, list) and len(count_data) > 0:
                if isinstance(count_data[0], list) and len(count_data[0]) > 0:
                    total_rows = int(count_data[0][0])
                elif isinstance(count_data[0], dict):
                    # Result might be in dict format
                    total_rows = int(count_data[0].get("cnt", count_data[0].get("count", 0)))
                else:
                    total_rows = int(count_data[0])
            else:
                total_rows = 0
        else:
            total_rows = 0
            
        logger.info(f"📥 [Export Stream] Total rows to export: {total_rows:,}")
    except Exception as e:
        logger.warning(f"📥 [Export Stream] Count query failed: {str(e)}, will stream without progress")
        logger.debug(f"📥 [Export Stream] Failed count query: {count_query[:200]}...")
        total_rows = 0
    
    # Remove any existing LIMIT/OFFSET from the query
    base_query = re.sub(r'\s+LIMIT\s+\d+(\s+OFFSET\s+\d+)?', '', sql_query, flags=re.IGNORECASE)
    
    # Stream in chunks
    header_written = False
    rows_exported = 0
    chunk_idx = 0
    
    while True:
        offset = chunk_idx * EXPORT_CHUNK_SIZE
        
        # Add LIMIT/OFFSET to query for chunking
        chunk_query = f"{base_query} LIMIT {EXPORT_CHUNK_SIZE} OFFSET {offset}"
        
        try:
            result = await db_client.execute_sql(chunk_query)
            
            if not result or not result.get("data"):
                # No more data
                break
            
            data_rows = result["data"]
            columns = result.get("columns", [])
            
            if len(data_rows) == 0:
                break
            
            # Create DataFrame for this chunk
            df = pd.DataFrame(data_rows, columns=columns) if columns else pd.DataFrame(data_rows)
            
            # Convert to CSV
            # Only include header for first chunk
            csv_content = df.to_csv(index=False, header=not header_written)
            header_written = True
            
            # Convert to bytes and optionally compress
            csv_bytes = csv_content.encode('utf-8')
            
            if compress:
                buffer = io.BytesIO()
                with gzip.GzipFile(fileobj=buffer, mode='wb') as gz:
                    gz.write(csv_bytes)
                csv_bytes = buffer.getvalue()
            
            rows_exported += len(df)
            
            if chunk_idx % 5 == 0:
                if total_rows > 0:
                    logger.info(f"📥 [Export Stream] Chunk {chunk_idx + 1}: {rows_exported:,}/{total_rows:,} rows exported")
                else:
                    logger.info(f"📥 [Export Stream] Chunk {chunk_idx + 1}: {rows_exported:,} rows exported")
            
            yield csv_bytes
            
            # If we got fewer rows than chunk size, we're done
            if len(data_rows) < EXPORT_CHUNK_SIZE:
                break
            
            chunk_idx += 1
            
            # Small delay to prevent overwhelming the event loop
            await asyncio.sleep(0.01)
            
        except Exception as e:
            logger.error(f"📥 [Export Stream] Error in chunk {chunk_idx}: {str(e)}")
            raise
    
    logger.info(f"📥 [Export Stream] Complete: {rows_exported:,} rows exported in {chunk_idx + 1} chunks")


@router.get("/table/{table_name}")
async def download_table_data(
    table_name: str,
    user_id: str = Query(..., description="User ID for data source lookup"),
    query_id: Optional[str] = Query(None, description="Query ID to fetch from cache (avoids re-querying DB)"),
    format: str = Query("csv", description="Export format: csv or json"),
    compress: bool = Query(True, description="Whether to gzip compress the output"),
):
    """
    Download full table data on demand.
    
    This endpoint uses a cache-first approach:
    1. If query_id is provided, try to load from cache (NO database query!)
    2. If cache miss or no query_id, fall back to database query with streaming
    
    For 3M+ row tables, the cache approach is much faster and avoids
    putting load on the database.
    
    Args:
        table_name: Name of the table to export
        user_id: User ID to look up active data source
        query_id: Query ID from the original query (enables cache lookup)
        format: Export format (csv or json)
        compress: Whether to compress the output with gzip
        
    Returns:
        StreamingResponse with the table data
    """
    logger.info(f"📥 [Export] Download request for table '{table_name}' by user '{user_id}' (query_id: {query_id[:8] if query_id else 'none'}...)")
    start_time = datetime.now()
    
    # Generate filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    safe_table_name = "".join(c for c in table_name if c.isalnum() or c in ('_', '-'))[:50]
    
    try:
        # STEP 1: Get active data source for user (needed for any query)
        postgres_client = get_shared_postgres_client(ensure_tables=False)
        active_sources = await postgres_client.execute_query_async(
            "SELECT * FROM data_source_config WHERE user_id = %s AND is_active = TRUE LIMIT 1",
            (user_id,)
        )
        
        if not active_sources:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No active data source configured for this user"
            )
        
        source = active_sources[0]
        data_source_type = (source.get("type") or "").lower()
        data_source_config = {
            "type": source["type"],
            "host": source.get("host"),
            "port": source.get("port"),
            "username": source.get("username"),
            "password": source.get("password"),
            "database_name": source.get("database_name"),
            "file_path": source.get("file_path"),
        }
        
        # SAP: stream from stored API URL (no SQL)
        if data_source_type in ("sap", "sap_datasphere"):
            if not query_id:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="query_id is required for SAP export. Run a query first, then use the download link from the results.",
                )
            cache = get_query_cache()
            api_url = cache.get_sap_api_url(query_id, table_name)
            if not api_url:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"No cached data for table '{table_name}' for this query. Run a query that loads this table, then try download again.",
                )
            logger.info(f"📥 [Export] SAP: streaming table '{table_name}' from stored API URL (query_id={query_id[:8]}...)")
            if format.lower() == "csv":
                extension = "csv.gz" if compress else "csv"
                filename = f"{safe_table_name}_{timestamp}.{extension}"
                content_type = "application/gzip" if compress else "text/csv"
                return StreamingResponse(
                    _stream_sap_table_csv(api_url, user_id, compress),
                    media_type=content_type,
                    headers={
                        "Content-Disposition": f'attachment; filename="{filename}"',
                        "X-Export-Mode": "sap-api",
                        "X-Source": "sap-cached-api",
                    },
                )
            # JSON: fetch once and return
            import httpx
            try:
                from infrastructure.services.datasphere_service import get_datasphere_service
                datasphere = get_datasphere_service()
                token = datasphere._get_user_token(user_id)
            except Exception as e:
                logger.error(f"📥 [Export SAP] Failed to get token: {e}")
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="SAP authentication failed.") from e
            async with httpx.AsyncClient(timeout=120.0) as client:
                resp = await client.get(api_url, headers={"Authorization": f"Bearer {token}", "Accept": "application/json"})
                resp.raise_for_status()
                data = resp.json()
            d = data.get("d")
            rows = data.get("value") or (d.get("results") if isinstance(d, dict) else [])
            import json
            body = json.dumps(rows, default=str).encode("utf-8")
            if compress:
                body = gzip.compress(body)
            extension = "json.gz" if compress else "json"
            filename = f"{safe_table_name}_{timestamp}.{extension}"
            content_type = "application/gzip" if compress else "application/json"
            return StreamingResponse(
                iter([body]),
                media_type=content_type,
                headers={"Content-Disposition": f'attachment; filename="{filename}"', "X-Export-Mode": "sap-api"},
            )
        
        # STEP 2: Try to get cached SQL query (re-run same query used during initial fetch)
        sql_query = None
        if query_id:
            cache = get_query_cache()
            sql_query = cache.get_query(query_id, table_name)
            
            if sql_query:
                logger.info(f"📥 [Export] Found cached query for {table_name} (query_id={query_id[:8]}...)")
                logger.debug(f"📥 [Export] Cached SQL: {sql_query[:200]}...")
            else:
                logger.info(f"📥 [Export] No cached query found, will use SELECT * FROM {table_name}")
        
        # STEP 3: If no cached query, use simple SELECT *
        if not sql_query:
            sql_query = f"SELECT * FROM {table_name}"
            logger.info(f"📥 [Export] Using default query: {sql_query}")
        
        logger.info(f"📥 [Export] Re-running query against {source['name']} ({source['type']})")
        
        # Initialize database gateway
        db_client = DataSourceGateway(data_source_config)
        
        # For large datasets with CSV format, use streaming to prevent memory overflow
        if format.lower() == "csv":
            extension = "csv.gz" if compress else "csv"
            filename = f"{safe_table_name}_{timestamp}.{extension}"
            
            content_type = "application/gzip" if compress else "text/csv"
            
            logger.info(f"📥 [Export] Starting streaming export: {filename}")
            
            # Use the cached query for streaming (not SELECT * FROM table)
            return StreamingResponse(
                _stream_csv_chunks_with_query(db_client, sql_query, compress),
                media_type=content_type,
                headers={
                    "Content-Disposition": f'attachment; filename="{filename}"',
                    "X-Export-Mode": "streaming",
                    "X-Chunk-Size": str(EXPORT_CHUNK_SIZE),
                    "X-Source": "cached-query" if query_id and cache.get_query(query_id, table_name) else "default-query",
                }
            )
        
        # JSON format - need to load all data
        logger.info(f"📥 [Export] Executing query for JSON export")
        
        result = await db_client.execute_sql(sql_query)
        
        if not result or not result.get("data"):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No data found in table '{table_name}'"
            )
        
        data_rows = result["data"]
        columns = result.get("columns", [])
        
        import pandas as pd
        df = pd.DataFrame(data_rows, columns=columns) if columns else pd.DataFrame(data_rows)
        
        return _export_dataframe(
            df=df,
            table_name=safe_table_name,
            timestamp=timestamp,
            format=format,
            compress=compress,
            source="cached-query" if query_id else "default-query",
            start_time=start_time
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"📥 [Export] Failed to export table '{table_name}': {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to export data: {str(e)}"
        )


def _export_dataframe(
    df,
    table_name: str,
    timestamp: str,
    format: str,
    compress: bool,
    source: str,
    start_time: datetime
) -> StreamingResponse:
    """
    Export a DataFrame as a downloadable file.
    
    Args:
        df: pandas DataFrame to export
        table_name: Safe table name for filename
        timestamp: Timestamp string for filename
        format: Export format (csv or json)
        compress: Whether to gzip compress
        source: Data source (cache or database)
        start_time: Start time for duration logging
        
    Returns:
        StreamingResponse with the file
    """
    import pandas as pd
    
    row_count = len(df)
    col_count = len(df.columns)
    
    # Generate file content
    if format.lower() == "json":
        content = df.to_json(orient='records', date_format='iso')
        content_type = "application/json"
        extension = "json"
    else:
        content = df.to_csv(index=False)
        content_type = "text/csv"
        extension = "csv"
    
    # Convert to bytes
    content_bytes = content.encode('utf-8')
    
    # Compress if requested
    if compress:
        buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode='wb') as gz:
            gz.write(content_bytes)
        content_bytes = buffer.getvalue()
        content_type = "application/gzip"
        extension = f"{extension}.gz"
    
    filename = f"{table_name}_{timestamp}.{extension}"
    
    duration = (datetime.now() - start_time).total_seconds()
    size_mb = len(content_bytes) / (1024 * 1024)
    logger.info(f"📥 [Export] Complete: {filename} ({row_count:,} rows, {size_mb:.2f}MB) from {source} in {duration:.2f}s")
    
    return StreamingResponse(
        io.BytesIO(content_bytes),
        media_type=content_type,
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "X-Row-Count": str(row_count),
            "X-Column-Count": str(col_count),
            "X-Export-Mode": "full",
            "X-Source": source,
        }
    )


@router.get("/health")
async def export_health():
    """Health check for export endpoint."""
    return {"status": "ok", "service": "export"}
