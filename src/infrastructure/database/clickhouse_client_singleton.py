"""Shared ClickHouse client singleton for application-wide use.

Optimized for large-scale financial data processing:
- Per-process singleton pattern for multi-worker deployments
- Connection reuse to avoid connection overhead
- Thread pool for non-blocking async operations
- Supports 30M+ rows per user request
"""
import logging
import os
from typing import Optional, Dict, Any
from clickhouse_connect import get_client
from clickhouse_connect.driver.client import Client
from config.settings import settings
from shared.exceptions import DatabaseException

logger = logging.getLogger(__name__)

# Per-process singleton instances (each worker process has its own client)
_shared_clickhouse_clients: Dict[int, 'ClickHouseClientSingleton'] = {}


class ClickHouseClientSingleton:
    """
    Shared ClickHouse client optimized for large financial datasets.
    
    Features:
    - Connection reuse across multiple queries
    - Compression enabled for large data transfers
    - Configurable timeouts for long-running queries
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize ClickHouse client.
        
        Args:
            config: Optional configuration dictionary. If not provided, uses settings.
                   Expected keys: host, port, database_name, username, password
        """
        try:
            # Get timeout from settings
            timeout = getattr(settings, 'query_timeout_seconds', 600)
            
            if config:
                # Use provided config
                host = config.get('host', 'localhost').strip()
                port = config.get('port', 8123)
                database = config.get('database_name', 'default')
                username = config.get('username', 'default')
                password = config.get('password', '')
            else:
                # Use global settings
                host = settings.clickhouse_host.strip()
                port = settings.clickhouse_port
                database = settings.clickhouse_database
                username = settings.clickhouse_user
                password = settings.clickhouse_password
            
            # Clean host - remove protocol prefixes
            if host.startswith("http://"):
                host = host[7:].strip()
            elif host.startswith("https://"):
                host = host[8:].strip()
            host = host.rstrip('/')
            
            self._host = host
            self._port = port
            self._database = database
            
            # Create ClickHouse client with optimized settings
            self.client: Client = get_client(
                host=host,
                port=port,
                database=database,
                username=username,
                password=password if password else None,
                interface="http",
                connect_timeout=timeout,
                send_receive_timeout=timeout,
                compress=True,  # Enable compression for large data transfers
            )
            
            logger.info(f"ClickHouse client initialized: {host}:{port}/{database}")
            
        except Exception as e:
            error_msg = str(e)
            if "Connection refused" in error_msg:
                error_msg = f"Connection refused. Check if ClickHouse is running."
            elif "timeout" in error_msg.lower():
                error_msg = f"Connection timeout. Check network connectivity."
            
            logger.error(f"ClickHouse client init failed: {error_msg}")
            raise DatabaseException(f"Failed to connect to ClickHouse: {error_msg}") from e
    
    def query(self, sql: str, parameters: Optional[Dict[str, Any]] = None) -> Any:
        """
        Execute a synchronous query.
        
        Args:
            sql: SQL query string
            parameters: Optional query parameters
            
        Returns:
            Query result object with result_rows and column_names
        """
        return self.client.query(sql, parameters=parameters)
    
    def execute_query_sync(self, sql: str, parameters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute query and return results as dictionary.
        
        Args:
            sql: SQL query string
            parameters: Optional query parameters
            
        Returns:
            Dictionary with 'columns', 'data', 'row_count'
        """
        try:
            result = self.client.query(sql, parameters=parameters)
            row_count = len(result.result_rows)
            
            if row_count > 100_000:
                logger.info(f"ClickHouse query returned {row_count:,} rows")
            
            return {
                "columns": result.column_names,
                "data": result.result_rows,
                "row_count": row_count,
            }
        except Exception as e:
            raise DatabaseException(f"Query execution failed: {str(e)}") from e
    
    def is_connected(self) -> bool:
        """Check if client is connected."""
        try:
            self.client.query("SELECT 1")
            return True
        except Exception:
            return False
    
    def close(self):
        """Close the connection."""
        try:
            if hasattr(self, 'client') and self.client:
                self.client.close()
                logger.debug("ClickHouse client closed")
        except Exception as e:
            logger.warning(f"Error closing ClickHouse client: {str(e)}")


def get_shared_clickhouse_client(config: Optional[Dict[str, Any]] = None) -> ClickHouseClientSingleton:
    """
    Get or create the shared ClickHouse client instance for this process.
    
    With multiple uvicorn workers, each worker process has its own client.
    This ensures connection reuse within each process.
    
    Args:
        config: Optional configuration dictionary for custom connections.
                If provided, creates a NEW client (not cached).
                
    Returns:
        ClickHouseClientSingleton instance
    """
    global _shared_clickhouse_clients
    
    # If custom config provided, create a new client (not cached)
    if config:
        return ClickHouseClientSingleton(config)
    
    # For default config, use per-process singleton
    process_id = os.getpid()
    
    if process_id not in _shared_clickhouse_clients:
        try:
            logger.info(f"Initializing ClickHouse client for process {process_id}")
            _shared_clickhouse_clients[process_id] = ClickHouseClientSingleton()
        except Exception as e:
            logger.error(f"ClickHouse init failed for process {process_id}: {str(e)[:100]}")
            raise
    
    return _shared_clickhouse_clients[process_id]


def reset_shared_clickhouse_client():
    """Reset the shared ClickHouse client for current process."""
    global _shared_clickhouse_clients
    process_id = os.getpid()
    
    if process_id in _shared_clickhouse_clients:
        try:
            _shared_clickhouse_clients[process_id].close()
            logger.info(f"ClickHouse client closed for process {process_id}")
        except Exception as e:
            logger.warning(f"Error closing ClickHouse client: {str(e)}")
        
        del _shared_clickhouse_clients[process_id]
        logger.info(f"ClickHouse client reset for process {process_id}")


def cleanup_all_clickhouse_clients():
    """Clean up all ClickHouse clients (for graceful shutdown)."""
    global _shared_clickhouse_clients
    
    for process_id, client in _shared_clickhouse_clients.items():
        try:
            client.close()
            logger.info(f"ClickHouse client closed for process {process_id}")
        except Exception as e:
            logger.warning(f"Error closing ClickHouse client: {str(e)}")
    
    _shared_clickhouse_clients.clear()
    logger.info("All ClickHouse clients cleaned up")

