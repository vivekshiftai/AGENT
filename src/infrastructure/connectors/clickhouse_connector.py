"""ClickHouse connector - executes SQL via HTTP interface."""
import logging
import re
from typing import Any, Dict

import pandas as pd

from src.core.exceptions import DatabaseException
from src.infrastructure.connectors.base_connector import BaseConnector, QueryPlan

logger = logging.getLogger(__name__)


def _extract_table_name(query: str, index: int) -> str:
    match = re.search(r"\bFROM\s+([\"`]?)(\w+)\1", query, re.IGNORECASE)
    return match.group(2) if match else f"data_table_{index + 1}"


class ClickHouseConnector(BaseConnector):
    """Connector for ClickHouse. Executes SQL via HTTP interface."""

    def __init__(self, config: Dict[str, Any], **kwargs: Any) -> None:
        super().__init__(config, **kwargs)
        self._client = None

    def _get_client(self):
        if self._client is not None:
            return self._client
        from clickhouse_connect import get_client

        from src.core.config import settings

        host = (self.config.get("host") or "localhost").strip()
        if host.startswith("http://"):
            host = host[7:].strip()
        elif host.startswith("https://"):
            host = host[8:].strip()
        host = host.rstrip("/")
        port = int(self.config.get("port", 8123))
        database = self.config.get("database_name") or self.config.get("database") or "default"
        username = self.config.get("username", "default")
        password = self.config.get("password") or ""
        timeout = settings.query_timeout_seconds

        self._client = get_client(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password if password else None,
            interface="http",
            connect_timeout=timeout,
            send_receive_timeout=timeout,
            compress=True,
        )
        logger.info("ClickHouse connector: client initialized %s:%s/%s", host, port, database)
        return self._client

    def connect(self) -> None:
        self._get_client()

    def test_connection(self) -> bool:
        try:
            self._get_client().query("SELECT 1")
            return True
        except Exception as e:
            logger.warning("ClickHouse test_connection failed: %s", e)
            return False

    def fetch_data(self, query_plan: QueryPlan) -> Dict[str, pd.DataFrame]:
        """Execute SQL queries against ClickHouse and return DataFrames."""
        queries = query_plan.get("queries") or []
        if not queries or not isinstance(queries, list):
            logger.warning("ClickHouseConnector: no queries in query_plan")
            return {}

        client = self._get_client()
        results: Dict[str, pd.DataFrame] = {}

        for index, query in enumerate(queries):
            if not isinstance(query, str) or not query.strip():
                continue
            table_name = _extract_table_name(query, index)
            try:
                result = client.query(query)
                cols = result.column_names
                rows = result.result_rows or []
                results[table_name] = (
                    pd.DataFrame(columns=cols)
                    if not rows
                    else pd.DataFrame(rows, columns=cols)
                )
            except Exception as e:
                logger.error("ClickHouseConnector: query %s failed: %s", index + 1, e)
                raise DatabaseException(f"ClickHouse query failed: {e}") from e

        return results

    def get_schema(self, table_name: str) -> str:
        """Return formatted schema for a ClickHouse table."""
        client = self._get_client()
        result = client.query(f"DESCRIBE TABLE {table_name}")
        lines = [f"Table: {table_name}"]
        for row in (result.result_rows or []):
            if len(row) >= 2:
                lines.append(f"  - {row[0]}: {row[1]}")
        return "\n".join(lines)

    def close(self) -> None:
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None
