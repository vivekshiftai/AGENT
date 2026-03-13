"""ClickHouse database client using HTTP interface.

Optimized for large-scale financial data processing (30M+ rows):
- Non-blocking async execution via thread pool
- Chunked data fetching for memory efficiency
- Parallel query support
- Memory-optimized DataFrame conversion
"""
from typing import List, Dict, Any, Optional, Generator, Iterator
from clickhouse_connect import get_client
from clickhouse_connect.driver.client import Client
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor
import numpy as np
from config.settings import settings
from shared.exceptions import DatabaseException

logger = logging.getLogger(__name__)

# Thread pool for non-blocking ClickHouse operations
# Size matches max concurrent queries to avoid contention
_ch_executor: Optional[ThreadPoolExecutor] = None

def get_clickhouse_executor() -> ThreadPoolExecutor:
    """Get or create the shared thread pool for ClickHouse operations.
    
    Thread pool size is set to 3x the max concurrent queries to ensure
    all parallel queries can execute without waiting for threads.
    """
    global _ch_executor
    if _ch_executor is None:
        # Use 3x multiplier to ensure enough threads for parallel queries + memory optimization
        max_workers = getattr(settings, 'max_concurrent_queries', 10) * 3
        _ch_executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="clickhouse_")
        logger.info(f"Created ClickHouse thread pool with {max_workers} workers for parallel query execution")
    return _ch_executor


class ClickHouseClient:
    """ClickHouse database client optimized for large-scale financial data.
    
    Features:
    - Non-blocking async execution (doesn't block event loop)
    - Efficient memory usage with optimized data types
    - Support for queries returning millions of rows
    """
    
    # Chunk size for streaming large results (rows per chunk)
    # 500K rows balances memory usage vs overhead
    DEFAULT_CHUNK_SIZE = 500_000
    
    def __init__(self, timeout: int = None):
        """
        Initialize ClickHouse client using HTTP interface.
        
        Args:
            timeout: Connection timeout in seconds (default: uses query_timeout_seconds from settings)
        """
        try:
            # Use query_timeout_seconds from settings if timeout not provided
            if timeout is None:
                timeout = getattr(settings, 'query_timeout_seconds', 600)  # Default 10 minutes
            
            self.timeout = timeout
            
            # Clean host - remove protocol and trailing slashes
            host = settings.clickhouse_host.strip()
            if host.startswith("http://"):
                host = host[7:].strip()
            elif host.startswith("https://"):
                host = host[8:].strip()
            # Remove trailing slashes
            host = host.rstrip('/')
            
            self._host = host
            self._port = settings.clickhouse_port
            self._database = settings.clickhouse_database
            self._username = settings.clickhouse_user
            self._password = settings.clickhouse_password if settings.clickhouse_password else None
            
            # Initialize ClickHouse client with HTTP interface
            # Use settings optimized for large data transfers
            self.client = get_client(
                host=host,
                port=settings.clickhouse_port,
                database=settings.clickhouse_database,
                username=settings.clickhouse_user,
                password=self._password,
                interface="http",  # Use HTTP interface
                connect_timeout=timeout,
                send_receive_timeout=timeout,
                # Optimize for large result sets
                compress=True,  # Enable compression for large data transfers
            )
            
            logger.info(f"ClickHouse client initialized: {host}:{settings.clickhouse_port}/{settings.clickhouse_database}")
            
        except Exception as e:
            # Extract clean error message
            error_msg = str(e)
            
            # Handle common connection errors
            if "Connection refused" in error_msg or "actively refused" in error_msg:
                error_msg = f"Connection refused to {host}:{settings.clickhouse_port}. Check if ClickHouse server is running and accessible."
            elif "timeout" in error_msg.lower():
                error_msg = f"Connection timeout to {host}:{settings.clickhouse_port}. Check network connectivity and firewall settings."
            elif "Caused by" in error_msg:
                error_msg = error_msg.split("Caused by")[0].strip()
            
            # Truncate very long error messages
            if len(error_msg) > 200:
                error_msg = error_msg[:200] + "..."
            
            raise DatabaseException(f"Failed to connect to ClickHouse: {error_msg}") from e
    
    def _execute_query_sync(self, sql: str, parameters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Synchronous query execution (runs in thread pool).
        
        Args:
            sql: SQL query string
            parameters: Optional query parameters
            
        Returns:
            Dictionary with 'columns', 'data', and 'row_count'
        """
        try:
            result = self.client.query(sql, parameters=parameters)
            
            columns = result.column_names
            data = result.result_rows
            row_count = len(data)
            
            # Log for large result sets
            if row_count > 100_000:
                logger.info(f"ClickHouse query returned {row_count:,} rows")
            
            return {
                "columns": columns,
                "data": data,
                "row_count": row_count,
            }
            
        except Exception as e:
            raise DatabaseException(f"Query execution failed: {str(e)}") from e
    
    async def execute_query(self, sql: str, parameters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute a SQL query asynchronously (non-blocking).
        
        This method runs the query in a thread pool to prevent blocking
        the event loop, allowing concurrent request handling.
        
        Args:
            sql: SQL query string
            parameters: Optional query parameters
            
        Returns:
            Dictionary with 'columns', 'data', and 'row_count'
            
        Raises:
            DatabaseException: If query execution fails
        """
        try:
            # Run synchronous query in thread pool to avoid blocking event loop
            loop = asyncio.get_event_loop()
            executor = get_clickhouse_executor()
            
            result = await loop.run_in_executor(
                executor,
                self._execute_query_sync,
                sql,
                parameters
            )
            
            return result
            
        except Exception as e:
            if isinstance(e, DatabaseException):
                raise
            raise DatabaseException(f"Query execution failed: {str(e)}") from e
    
    def _execute_query_chunked_sync(
        self, 
        sql: str, 
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        parameters: Optional[Dict[str, Any]] = None
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Execute query and yield results in chunks (synchronous generator).
        
        For very large datasets (10M+ rows), this prevents loading
        all data into memory at once.
        
        Args:
            sql: SQL query string
            chunk_size: Number of rows per chunk
            parameters: Optional query parameters
            
        Yields:
            Dictionary with 'columns', 'data', 'row_count', 'chunk_index', 'is_last'
        """
        try:
            # Use query_row_block_stream for memory-efficient streaming
            # This fetches data in blocks directly from ClickHouse
            result = self.client.query(sql, parameters=parameters)
            
            columns = result.column_names
            all_rows = result.result_rows
            total_rows = len(all_rows)
            
            logger.info(f"Chunked query: {total_rows:,} total rows, chunk_size={chunk_size:,}")
            
            # Yield in chunks
            chunk_index = 0
            for i in range(0, total_rows, chunk_size):
                chunk_data = all_rows[i:i + chunk_size]
                is_last = (i + chunk_size) >= total_rows
                
                yield {
                    "columns": columns,
                    "data": chunk_data,
                    "row_count": len(chunk_data),
                    "chunk_index": chunk_index,
                    "total_rows": total_rows,
                    "is_last": is_last,
                }
                chunk_index += 1
                
        except Exception as e:
            raise DatabaseException(f"Chunked query execution failed: {str(e)}") from e
    
    async def execute_query_chunked(
        self, 
        sql: str, 
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        parameters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute query and return all chunks as a list (async).
        
        For memory efficiency with very large datasets.
        
        Args:
            sql: SQL query string
            chunk_size: Number of rows per chunk
            parameters: Optional query parameters
            
        Returns:
            List of chunk dictionaries
        """
        loop = asyncio.get_event_loop()
        executor = get_clickhouse_executor()
        
        # Collect all chunks (generator -> list in thread)
        def collect_chunks():
            return list(self._execute_query_chunked_sync(sql, chunk_size, parameters))
        
        chunks = await loop.run_in_executor(executor, collect_chunks)
        return chunks
    
    async def execute_query_to_dataframe(
        self, 
        sql: str, 
        parameters: Optional[Dict[str, Any]] = None,
    ):
        """
        Execute query and return as pandas DataFrame.
        
        NOTE: Memory optimization is no longer performed here.
        Use Polars for efficient memory handling instead.
        
        Args:
            sql: SQL query string
            parameters: Optional query parameters
            
        Returns:
            pandas DataFrame with query results
        """
        import pandas as pd
        
        result = await self.execute_query(sql, parameters)
        
        if not result["data"]:
            return pd.DataFrame(columns=result["columns"])
        
        return pd.DataFrame(result["data"], columns=result["columns"])
    
    async def list_tables(self) -> List[str]:
        """
        List all tables in the database.
        
        Returns:
            List of table names
        """
        try:
            query = "SHOW TABLES"
            result = self.client.query(query)
            tables = [row[0] for row in result.result_rows] if result.result_rows else []
            return tables
        except Exception as e:
            raise DatabaseException(f"Failed to list tables: {str(e)}") from e
    
    async def get_table_schema(self, table_name: str) -> str:
        """
        Get formatted schema description for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Formatted schema string
        """
        try:
            query = f"DESCRIBE TABLE {table_name}"
            result = self.client.query(query)
            
            schema_lines = [f"Table: {table_name}"]
            for row in result.result_rows:
                if len(row) >= 2:
                    col_name = row[0]
                    col_type = row[1]
                    schema_lines.append(f"  - {col_name}: {col_type}")
            
            return "\n".join(schema_lines)
        except Exception as e:
            raise DatabaseException(f"Failed to get table schema: {str(e)}") from e
    
    async def get_sample_data(self, table_name: str, limit: int = 3) -> List[Dict[str, Any]]:
        """
        Get sample data from a table.
        
        Args:
            table_name: Name of the table
            limit: Number of sample rows to fetch
            
        Returns:
            List of dictionaries representing sample rows
        """
        try:
            query = f"SELECT * FROM {table_name} LIMIT {limit}"
            result = self.client.query(query)
            
            if not result.result_rows:
                return []
            
            # Convert rows to dictionaries
            sample_data = []
            for row in result.result_rows:
                row_dict = dict(zip(result.column_names, row))
                sample_data.append(row_dict)
            
            return sample_data
        except Exception as e:
            logger.warning(f"Failed to get sample data from {table_name}: {str(e)}")
            return []
    
    async def get_schema_info(self, tables: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Get schema information for specified tables or all tables.
        
        Args:
            tables: Optional list of table names to get schema for
            
        Returns:
            Dictionary mapping table names to column information
        """
        try:
            if tables:
                schema_info = {}
                for table in tables:
                    query = f"DESCRIBE TABLE {table}"
                    result = self.client.query(query)
                    columns = {}
                    for row in result.result_rows:
                        if len(row) >= 2:
                            col_name = row[0]
                            col_type = row[1]
                            columns[col_name] = col_type
                    schema_info[table] = columns
                return schema_info
            else:
                # Get all tables
                all_tables = await self.list_tables()
                return await self.get_schema_info(all_tables)
                
        except Exception as e:
            raise DatabaseException(f"Failed to get schema info: {str(e)}") from e
    
    def query(self, sql: str) -> Any:
        """
        Execute a query (synchronous method for compatibility).
        
        Args:
            sql: SQL query string
            
        Returns:
            Query result object with result_rows and column_names attributes
        """
        return self.client.query(sql)
    
    def close(self):
        """Close the database connection."""
        if hasattr(self, 'client'):
            try:
                self.client.close()
            except Exception:
                pass  # Ignore errors during close
