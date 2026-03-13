"""Factory that selects the correct connector based on data source type."""
import logging
from typing import Any, Dict, Optional

from .base import BaseConnector
from .clickhouse_connector import ClickHouseConnector
from .excel_connector import ExcelConnector
from .sap_connector import SAPConnector
from shared.exceptions import DatabaseException

logger = logging.getLogger(__name__)

SUPPORTED_SOURCE_TYPES = ("clickhouse", "excel", "csv", "sap")


class ConnectorFactory:
    """Creates the appropriate connector for a given data source config."""

    @classmethod
    def get_connector(
        cls,
        config: Dict[str, Any],
        **kwargs: Any,
    ) -> BaseConnector:
        """Return a connector instance for the configured data source type.
        
        Args:
            config: Data source configuration with at least "type" (clickhouse, excel, sap).
            **kwargs: Passed to connector (e.g. sap_view_schemas, sap_access_token for SAP).
            
        Returns:
            BaseConnector implementation.
            
        Raises:
            DatabaseException: If type is unsupported or config is invalid.
        """
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
        if source_type in ("excel", "csv"):
            return ExcelConnector(config, **kwargs)
        if source_type == "sap":
            return SAPConnector(config, **kwargs)
        raise DatabaseException(f"Unsupported data source type: {source_type}")
