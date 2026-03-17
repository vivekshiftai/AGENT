"""PostgreSQL connection for data source queries."""
import logging
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2.extras import RealDictCursor

from src.core.config import settings
from src.core.exceptions import DatabaseException

logger = logging.getLogger(__name__)


def _clean_host(host_str: Optional[str]) -> str:
    if not host_str:
        return "localhost"
    h = host_str.strip()
    if h.startswith("http://"):
        h = h[7:]
    elif h.startswith("https://"):
        h = h[8:]
    return h.rstrip("/")


class PostgreSQLConnection:
    """PostgreSQL client for executing queries against postgres data sources."""

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        connection_string: Optional[str] = None,
    ):
        if connection_string:
            self.connection_string = connection_string
        else:
            host = _clean_host(host or settings.postgres_host)
            port = port or settings.postgres_port
            database = database or settings.postgres_database
            user = user or settings.postgres_user
            password = password or settings.postgres_password
            timeout = settings.query_timeout_seconds
            self.connection_string = (
                f"host={host} port={port} dbname={database} user={user} "
                f"password={password} connect_timeout={timeout} "
                "keepalives=1 keepalives_idle=30 keepalives_interval=10 keepalives_count=5"
            )
        self._conn = None

    def _get_connection(self):
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self.connection_string)
        return self._conn

    def execute_query(self, sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute SELECT query and return list of dicts."""
        conn = None
        cur = None
        try:
            conn = self._get_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error("PostgreSQL query failed: %s", e)
            raise DatabaseException(f"PostgreSQL query failed: {e}") from e
        finally:
            if cur:
                cur.close()

    def close(self):
        """Close the connection."""
        if self._conn and not self._conn.closed:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
