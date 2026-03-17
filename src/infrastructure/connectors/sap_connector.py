"""SAP Datasphere connector - schema via sap_view_schemas; data fetch via DatasphereService."""
import logging
from typing import Any, Dict

import pandas as pd

from src.infrastructure.connectors.base_connector import BaseConnector, QueryPlan

logger = logging.getLogger(__name__)


class SAPConnector(BaseConnector):
    """Connector for SAP Datasphere. Schema from sap_view_schemas; data fetch via DatasphereService."""

    def __init__(self, config: Dict[str, Any], **kwargs: Any) -> None:
        super().__init__(config, **kwargs)
        self._sap_view_schemas = kwargs.get("sap_view_schemas") or {}
        self._sap_access_token = kwargs.get("sap_access_token")

    def connect(self) -> None:
        pass

    def test_connection(self) -> bool:
        try:
            from src.infrastructure.external_services.datasphere_service import (
                get_datasphere_service,
            )
            user_id = self.config.get("user_id")
            if not user_id:
                logger.warning("SAP test_connection: user_id required")
                return False
            import asyncio
            ds = get_datasphere_service()
            asyncio.run(ds.list_catalog_assets(user_id))
            return True
        except Exception as e:
            logger.warning("SAP test_connection failed: %s", e)
            return False

    def fetch_data(self, query_plan: QueryPlan) -> Dict[str, pd.DataFrame]:
        """SAP data fetch is handled by DatasphereService directly (OData API)."""
        logger.debug("SAPConnector.fetch_data: SAP uses DatasphereService for data fetch")
        return {}

    def get_schema(self, table_name: str) -> str:
        """Return formatted schema for an SAP view from cached sap_view_schemas."""
        if self._sap_view_schemas and table_name in self._sap_view_schemas:
            info = self._sap_view_schemas[table_name]
            cols = info.get("columns", [])
            lines = [f"Table: {table_name}"]
            for col in cols:
                if isinstance(col, dict):
                    lines.append(f"  - {col.get('name', '')}: {col.get('data_type', 'String')}")
                else:
                    lines.append(f"  - {col}")
            return "\n".join(lines)
        return f"Table: {table_name}\n  (Schema not in sap_view_schemas)"
