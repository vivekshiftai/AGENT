"""Shared PostgreSQL client singleton for application-wide use."""
import logging
import os
from typing import Optional, Dict
from .postgres import PostgreSQLClient
from shared.exceptions import DatabaseException

logger = logging.getLogger(__name__)

# Per-process singleton instances (each worker process has its own pool)
_shared_postgres_clients: Dict[int, PostgreSQLClient] = {}


def get_shared_postgres_client(ensure_tables: bool = True) -> PostgreSQLClient:
    """
    Get or create the shared PostgreSQL client instance per process.

    With multiple uvicorn workers, each worker process needs its own connection pool.
    This ensures each worker has its own pool while still being efficient within each process.

    Args:
        ensure_tables: Whether to ensure tables exist on first initialization.
                      Set to False for subsequent calls to avoid redundant checks.

    Returns:
        Shared PostgreSQLClient instance for this process
    """
    global _shared_postgres_clients
    process_id = os.getpid()

    if process_id not in _shared_postgres_clients:
        try:
            logger.info(f"Initializing PostgreSQL client for process {process_id}")
            _shared_postgres_clients[process_id] = PostgreSQLClient(ensure_tables=ensure_tables)
        except Exception as e:
            logger.error(f"PostgreSQL init failed for process {process_id}: {str(e)[:100]}")
            raise DatabaseException(f"Failed to initialize PostgreSQL client: {str(e)}") from e

    return _shared_postgres_clients[process_id]


def reset_shared_postgres_client():
    """Reset the shared PostgreSQL client for current process (useful for testing or reconnection)."""
    global _shared_postgres_clients
    process_id = os.getpid()

    if process_id in _shared_postgres_clients:
        try:
            _shared_postgres_clients[process_id].close()
            logger.info(f"PostgreSQL client closed for process {process_id}")
        except Exception as e:
            logger.warning(f"Error closing PostgreSQL client for process {process_id}: {str(e)}")

        del _shared_postgres_clients[process_id]
        logger.info(f"PostgreSQL client reset for process {process_id}")
    else:
        logger.debug(f"No PostgreSQL client to reset for process {process_id}")


def cleanup_all_postgres_clients():
    """Clean up all PostgreSQL clients (useful for graceful shutdown)."""
    global _shared_postgres_clients

    for process_id, client in _shared_postgres_clients.items():
        try:
            client.close()
            logger.info(f"PostgreSQL client closed for process {process_id}")
        except Exception as e:
            logger.warning(f"Error closing PostgreSQL client for process {process_id}: {str(e)}")

    _shared_postgres_clients.clear()
    logger.info("All PostgreSQL clients cleaned up")

