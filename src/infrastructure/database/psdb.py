"""Ensure psdb exists and run schema initialization. No-op when PostgreSQL is unavailable."""
import logging
from typing import Optional

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from src.core.config import settings

logger = logging.getLogger(__name__)


def _normalize_host(host: Optional[str]) -> str:
    """Return host without http:// or https:// (PostgreSQL needs hostname only)."""
    if not host:
        return "localhost"
    h = host.strip()
    for prefix in ("https://", "http://"):
        if h.lower().startswith(prefix):
            h = h[len(prefix) :].strip()
            break
    return h or "localhost"


def _get_admin_connection():
    """Connect to postgres (or configured host) without specifying database."""
    host = _normalize_host(settings.psdb_host or settings.postgres_host)
    port = settings.psdb_port
    user = settings.psdb_user or settings.postgres_user or "postgres"
    password = settings.psdb_password or settings.postgres_password or ""
    return psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
    )


def _get_psdb_connection():
    """Connect to psdb database."""
    host = _normalize_host(settings.psdb_host or settings.postgres_host)
    port = settings.psdb_port
    database = settings.psdb_database
    user = settings.psdb_user or settings.postgres_user or "postgres"
    password = settings.psdb_password or settings.postgres_password or ""
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=database,
        user=user,
        password=password,
    )


def ensure_psdb_exists() -> bool:
    """
    Create psdb database if it does not exist.
    Returns True if successful, False if PostgreSQL is not available (no traceback).
    """
    db_name = settings.psdb_database
    host = _normalize_host(settings.psdb_host or settings.postgres_host)
    port = settings.psdb_port
    try:
        conn = _get_admin_connection()
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (db_name,),
        )
        if cur.fetchone() is None:
            cur.execute(f'CREATE DATABASE "{db_name}"')
            logger.info("Created database: %s on %s:%s", db_name, host, port)
        else:
            logger.info("Database already exists: %s on %s:%s", db_name, host, port)
        cur.close()
        conn.close()
        return True
    except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
        logger.warning(
            "PostgreSQL not available at %s:%s (psdb skipped; datasource management disabled): %s",
            settings.psdb_host or settings.postgres_host or "localhost",
            settings.psdb_port,
            e,
        )
        return False
    except Exception as e:
        logger.exception("Failed to ensure psdb exists: %s", e)
        raise


def init_psdb_schema() -> bool:
    """
    Create datasources table if not exists.
    Returns True if successful, False if PostgreSQL is not available (no traceback).
    """
    sql = """
    CREATE TABLE IF NOT EXISTS datasources (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        type VARCHAR(64) NOT NULL,
        host VARCHAR(255),
        port INTEGER,
        database VARCHAR(255),
        username VARCHAR(255),
        password VARCHAR(512),
        extra_config JSONB,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        conn = _get_psdb_connection()
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()
        logger.info("psdb schema initialized (datasources table)")
        return True
    except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
        logger.warning(
            "PostgreSQL not available (psdb schema init skipped): %s",
            e,
        )
        return False
    except Exception as e:
        logger.exception("Failed to init psdb schema: %s", e)
        raise
