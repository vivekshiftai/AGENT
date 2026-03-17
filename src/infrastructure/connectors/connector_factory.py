"""Factory that selects the correct connector based on data source type."""
import logging
from typing import Any, Dict

from src.core.exceptions import DatabaseException
from src.infrastructure.connectors.base_connector import BaseConnector
from src.infrastructure.connectors.clickhouse_connector import ClickHouseConnector
from src.infrastructure.connectors.excel_connector import ExcelConnector
from src.infrastructure.connectors.mysql_connector import MySQLConnector
from src.infrastructure.connectors.postgres_connector import PostgresConnector
from src.infrastructure.connectors.sap_connector import SAPConnector

logger = logging.getLogger(__name__)

SUPPORTED_SOURCE_TYPES = ("clickhouse", "excel", "csv", "sap", "postgres", "mysql")


class ConnectorFactory:
    """Creates the appropriate connector for a given data source config."""

    @classmethod
    def get_connector(cls, config: Dict[str, Any], **kwargs: Any) -> BaseConnector:
        """Return a connector instance for the configured data source type."""
        if not config or not isinstance(config, dict):
            raise DatabaseException("Data source config is required")
        raw_type = config.get("type") or ""
        source_type = raw_type.lower().strip()
        if source_type == "sap_datasphere":
            source_type = "sap"
        if source_type not in SUPPORTED_SOURCE_TYPES:
            raise DatabaseException(
                f"Unsupported data source type: '{raw_type}'. "
                f"Supported: {', '.join(SUPPORTED_SOURCE_TYPES)}"
            )
        if source_type == "clickhouse":
            return ClickHouseConnector(config, **kwargs)
        if source_type == "postgres":
            return PostgresConnector(config, **kwargs)
        if source_type == "mysql":
            return MySQLConnector(config, **kwargs)
        if source_type in ("excel", "csv"):
            return ExcelConnector(config, **kwargs)
        if source_type == "sap":
            return SAPConnector(config, **kwargs)
        raise DatabaseException(f"Unsupported data source type: {source_type}")
