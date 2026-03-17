"""PostgreSQL connector - executes SQL via psycopg2."""
import logging
import re
from typing import Any, Dict

import pandas as pd

from src.core.exceptions import DatabaseException
from src.infrastructure.connectors.base_connector import BaseConnector, QueryPlan
from src.infrastructure.databases.postgres import PostgreSQLConnection

logger = logging.getLogger(__name__)


def _extract_table_name(query: str, index: int) -> str:
    match = re.search(r"\bFROM\s+([\"`]?)(\w+)\1", query, re.IGNORECASE)
    return match.group(2) if match else f"data_table_{index + 1}"


class PostgresConnector(BaseConnector):
    """Connector for PostgreSQL. Executes SQL via psycopg2."""

    def __init__(self, config: Dict[str, Any], **kwargs: Any) -> None:
        super().__init__(config, **kwargs)
        self._client = None

    def _get_client(self) -> PostgreSQLConnection:
        if self._client is not None:
            return self._client
        host = self.config.get("host") or "localhost"
        port = self.config.get("port", 5432)
        database = self.config.get("database_name") or self.config.get("database") or "postgres"
        user = self.config.get("username", "postgres")
        password = self.config.get("password", "")
        conn_str = (
            f"host={host} port={port} dbname={database} user={user} "
            f"password={password}"
        )
        self._client = PostgreSQLConnection(connection_string=conn_str)
        logger.info("PostgreSQL connector: client initialized %s:%s/%s", host, port, database)
        return self._client

    def connect(self) -> None:
        self._get_client()

    def test_connection(self) -> bool:
        try:
            self._get_client().execute_query("SELECT 1")
            return True
        except Exception as e:
            logger.warning("PostgreSQL test_connection failed: %s", e)
            return False

    def fetch_data(self, query_plan: QueryPlan) -> Dict[str, pd.DataFrame]:
        """Execute SQL queries against PostgreSQL and return DataFrames."""
        queries = query_plan.get("queries") or []
        if not queries or not isinstance(queries, list):
            logger.warning("PostgresConnector: no queries in query_plan")
            return {}

        client = self._get_client()
        results: Dict[str, pd.DataFrame] = {}

        for index, query in enumerate(queries):
            if not isinstance(query, str) or not query.strip():
                continue
            table_name = _extract_table_name(query, index)
            try:
                rows = client.execute_query(query)
                if not rows:
                    results[table_name] = pd.DataFrame()
                else:
                    results[table_name] = pd.DataFrame(rows)
            except Exception as e:
                logger.error("PostgresConnector: query %s failed: %s", index + 1, e)
                raise DatabaseException(f"PostgreSQL query failed: {e}") from e

        return results

    def get_schema(self, table_name: str) -> str:
        """Return formatted schema for a PostgreSQL table."""
        client = self._get_client()
        rows = client.execute_query(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        lines = [f"Table: {table_name}"]
        for row in rows:
            lines.append(f"  - {row['column_name']}: {row['data_type']}")
        return "\n".join(lines)

    def close(self) -> None:
        if self._client:
            self._client.close()
            self._client = None
