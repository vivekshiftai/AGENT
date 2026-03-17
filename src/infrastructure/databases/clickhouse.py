"""ClickHouse connection for data source queries."""
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Optional

from clickhouse_connect import get_client

from src.core.config import settings
from src.core.exceptions import DatabaseException

logger = logging.getLogger(__name__)

_ch_executor: Optional[ThreadPoolExecutor] = None


def get_clickhouse_executor() -> ThreadPoolExecutor:
    """Get or create shared thread pool for ClickHouse operations."""
    global _ch_executor
    if _ch_executor is None:
        _ch_executor = ThreadPoolExecutor(max_workers=10, thread_name_prefix="clickhouse_")
    return _ch_executor


class ClickHouseConnection:
    """ClickHouse client for executing queries via HTTP interface."""

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        host = (host or settings.clickhouse_host).strip()
        if host.startswith("http://"):
            host = host[7:].strip()
        elif host.startswith("https://"):
            host = host[8:].strip()
        host = host.rstrip("/")

        self._host = host
        self._port = port or settings.clickhouse_port
        self._database = database or settings.clickhouse_database
        self._username = username or settings.clickhouse_user
        self._password = password or settings.clickhouse_password or None
        self._timeout = settings.query_timeout_seconds
        self._client = None

    def _get_client(self):
        if self._client is not None:
            return self._client
        self._client = get_client(
            host=self._host,
            port=self._port,
            database=self._database,
            username=self._username,
            password=self._password,
            interface="http",
            connect_timeout=self._timeout,
            send_receive_timeout=self._timeout,
            compress=True,
        )
        logger.info("ClickHouse connected: %s:%s/%s", self._host, self._port, self._database)
        return self._client

    def query(self, sql: str) -> Any:
        """Execute query synchronously."""
        try:
            return self._get_client().query(sql)
        except Exception as e:
            raise DatabaseException(f"ClickHouse query failed: {e}") from e

    async def execute_query(self, sql: str) -> Dict[str, Any]:
        """Execute query asynchronously via thread pool."""
        def _run():
            result = self.query(sql)
            cols = result.column_names
            rows = result.result_rows or []
            return {"columns": cols, "data": rows, "row_count": len(rows)}

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(get_clickhouse_executor(), _run)

    def close(self):
        """Close the connection."""
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None
