"""PostgreSQL implementation of IDatasourceRepository (psdb.datasources)."""
import json
import logging
from datetime import datetime
from typing import List, Optional

from src.core.exceptions import DatabaseException
from src.domain.entities.datasource_entity import DatasourceEntity
from src.domain.repositories.datasource_repository import IDatasourceRepository

from .psdb import _get_psdb_connection

logger = logging.getLogger(__name__)


class DatasourceRepositoryImpl(IDatasourceRepository):
    """Persist datasource metadata in psdb.datasources."""

    def add(self, datasource: DatasourceEntity) -> DatasourceEntity:
        conn = None
        try:
            conn = _get_psdb_connection()
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO datasources
                (name, type, host, port, database, username, password, extra_config)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, created_at
                """,
                (
                    datasource.name,
                    datasource.type,
                    datasource.host,
                    datasource.port,
                    datasource.database,
                    datasource.username,
                    datasource.password,
                    json.dumps(datasource.extra_config) if datasource.extra_config else None,
                ),
            )
            row = cur.fetchone()
            conn.commit()
            cur.close()
            if row:
                return DatasourceEntity(
                    id=row[0],
                    name=datasource.name,
                    type=datasource.type,
                    host=datasource.host,
                    port=datasource.port,
                    database=datasource.database,
                    username=datasource.username,
                    password=datasource.password,
                    extra_config=datasource.extra_config,
                    created_at=row[1],
                )
            return datasource
        except Exception as e:
            if conn:
                conn.rollback()
            logger.exception("DatasourceRepository.add failed: %s", e)
            raise DatabaseException(f"Failed to add datasource: {e}") from e
        finally:
            if conn and not conn.closed:
                conn.close()

    def get_all(self) -> List[DatasourceEntity]:
        conn = None
        try:
            conn = _get_psdb_connection()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, name, type, host, port, database, username, password, extra_config, created_at
                FROM datasources ORDER BY id
                """
            )
            rows = cur.fetchall()
            cur.close()
            return [
                DatasourceEntity(
                    id=r[0],
                    name=r[1],
                    type=r[2],
                    host=r[3],
                    port=r[4],
                    database=r[5],
                    username=r[6],
                    password=r[7],
                    extra_config=json.loads(r[8]) if r[8] else None,
                    created_at=r[9],
                )
                for r in rows
            ]
        except Exception as e:
            logger.exception("DatasourceRepository.get_all failed: %s", e)
            raise DatabaseException(f"Failed to list datasources: {e}") from e
        finally:
            if conn and not conn.closed:
                conn.close()

    def get_by_id(self, id: int) -> Optional[DatasourceEntity]:
        conn = None
        try:
            conn = _get_psdb_connection()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, name, type, host, port, database, username, password, extra_config, created_at
                FROM datasources WHERE id = %s
                """,
                (id,),
            )
            r = cur.fetchone()
            cur.close()
            if not r:
                return None
            return DatasourceEntity(
                id=r[0],
                name=r[1],
                type=r[2],
                host=r[3],
                port=r[4],
                database=r[5],
                username=r[6],
                password=r[7],
                extra_config=json.loads(r[8]) if r[8] else None,
                created_at=r[9],
            )
        except Exception as e:
            logger.exception("DatasourceRepository.get_by_id failed: %s", e)
            raise DatabaseException(f"Failed to get datasource: {e}") from e
        finally:
            if conn and not conn.closed:
                conn.close()

    def delete(self, id: int) -> bool:
        conn = None
        try:
            conn = _get_psdb_connection()
            cur = conn.cursor()
            cur.execute("DELETE FROM datasources WHERE id = %s", (id,))
            n = cur.rowcount
            conn.commit()
            cur.close()
            return n > 0
        except Exception as e:
            if conn:
                conn.rollback()
            logger.exception("DatasourceRepository.delete failed: %s", e)
            raise DatabaseException(f"Failed to delete datasource: {e}") from e
        finally:
            if conn and not conn.closed:
                conn.close()
