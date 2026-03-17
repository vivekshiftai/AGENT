"""MySQL connector - executes SQL via PyMySQL."""
import logging
import re
from typing import Any, Dict, List

import pandas as pd

from src.core.exceptions import DatabaseException
from src.infrastructure.connectors.base_connector import BaseConnector, QueryPlan

logger = logging.getLogger(__name__)


def _extract_table_name(query: str, index: int) -> str:
    match = re.search(r"\bFROM\s+([\"`]?)(\w+)\1", query, re.IGNORECASE)
    return match.group(2) if match else f"data_table_{index + 1}"


class MySQLConnector(BaseConnector):
    """Connector for MySQL. Executes SQL via PyMySQL."""

    def __init__(self, config: Dict[str, Any], **kwargs: Any) -> None:
        super().__init__(config, **kwargs)
        self._conn = None

    def _get_connection(self):
        if self._conn is not None:
            return self._conn
        import pymysql
        host = self.config.get("host") or "localhost"
        port = int(self.config.get("port", 3306))
        database = self.config.get("database_name") or self.config.get("database") or ""
        user = self.config.get("username", "root")
        password = self.config.get("password") or ""
        self._conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database or None,
            cursorclass=pymysql.cursors.DictCursor,
        )
        logger.info("MySQL connector: connected %s:%s/%s", host, port, database)
        return self._conn

    def connect(self) -> None:
        self._get_connection()

    def test_connection(self) -> bool:
        try:
            conn = self._get_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            return True
        except Exception as e:
            logger.warning("MySQL test_connection failed: %s", e)
            return False

    def fetch_data(self, query_plan: QueryPlan) -> Dict[str, pd.DataFrame]:
        queries = query_plan.get("queries") or []
        if not queries or not isinstance(queries, list):
            logger.warning("MySQLConnector: no queries in query_plan")
            return {}
        conn = self._get_connection()
        results: Dict[str, pd.DataFrame] = {}
        for index, query in enumerate(queries):
            if not isinstance(query, str) or not query.strip():
                continue
            table_name = _extract_table_name(query, index)
            try:
                with conn.cursor() as cur:
                    cur.execute(query)
                    rows = cur.fetchall()
                results[table_name] = pd.DataFrame(rows) if rows else pd.DataFrame()
            except Exception as e:
                logger.error("MySQLConnector: query %s failed: %s", index + 1, e)
                raise DatabaseException(f"MySQL query failed: {e}") from e
        return results

    def get_schema(self, table_name: str) -> str:
        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute("DESCRIBE `%s`" % table_name.replace("`", "``"))
            rows = cur.fetchall()
        lines = [f"Table: {table_name}"]
        for r in rows:
            lines.append("  - %s: %s" % (r.get("Field", ""), r.get("Type", "")))
        return "\n".join(lines)

    def close(self) -> None:
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
