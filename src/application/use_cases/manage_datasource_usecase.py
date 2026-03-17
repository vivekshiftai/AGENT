"""Use case: add, list, delete datasources with connection validation."""
import logging
from typing import Any, Dict, List, Optional

from src.domain.entities.datasource_entity import DatasourceEntity
from src.domain.repositories.datasource_repository import IDatasourceRepository
from src.infrastructure.connectors.connector_factory import ConnectorFactory

logger = logging.getLogger(__name__)


class ManageDatasourceUseCase:
    """Add (with test_connection), list, and delete datasources."""

    def __init__(self, datasource_repository: IDatasourceRepository):
        self._repo = datasource_repository

    def add_datasource(
        self,
        name: str,
        type: str,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        extra_config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Test connection first; if successful, persist and return the new datasource.
        Returns { "success": True, "datasource": {...} } or { "success": False, "error": "..." }.
        """
        entity = DatasourceEntity(
            name=name,
            type=type.strip().lower(),
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            extra_config=extra_config,
        )
        config = entity.to_connection_config()
        if type.lower() in ("excel", "csv"):
            file_path = (extra_config or {}).get("file_path") or host
            config["file_path"] = file_path
        try:
            connector = ConnectorFactory.get_connector(config)
            if not connector.test_connection():
                return {"success": False, "error": "Connection test failed"}
            saved = self._repo.add(entity)
            return {
                "success": True,
                "datasource": _entity_to_response(saved),
            }
        except Exception as e:
            logger.exception("Add datasource failed: %s", e)
            return {"success": False, "error": str(e)}

    def test_connection(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Test connection without saving. config must include type and connection params."""
        try:
            connector = ConnectorFactory.get_connector(config)
            ok = connector.test_connection()
            return {"success": ok, "error": None if ok else "Connection test failed"}
        except Exception as e:
            logger.exception("Test connection failed: %s", e)
            return {"success": False, "error": str(e)}

    def list_datasources(self) -> List[Dict[str, Any]]:
        """Return all datasources (password masked)."""
        try:
            all_ = self._repo.get_all()
            return [_entity_to_response(d) for d in all_]
        except Exception as e:
            logger.exception("List datasources failed: %s", e)
            raise

    def delete_datasource(self, id: int) -> Dict[str, Any]:
        """Remove datasource by id. Returns { "success": True } or { "success": False, "error": "..." }."""
        try:
            deleted = self._repo.delete(id)
            return {"success": deleted, "error": None if deleted else "Datasource not found"}
        except Exception as e:
            logger.exception("Delete datasource failed: %s", e)
            return {"success": False, "error": str(e)}


def _entity_to_response(d: DatasourceEntity) -> Dict[str, Any]:
    out = {
        "id": d.id,
        "name": d.name,
        "type": d.type,
        "host": d.host,
        "port": d.port,
        "database": d.database,
        "username": d.username,
        "password": "********" if d.password else None,
        "extra_config": d.extra_config,
        "created_at": d.created_at.isoformat() if d.created_at else None,
    }
    return out
