"""SAP connector for the Data Source Abstraction Layer.

Supports analytical views and CDS views via:
- OData endpoints (SAP Datasphere / BTP) — data fetch is handled by the dedicated sap_data_fetch node
- Schema retrieval from state (sap_view_schemas) or SAP HANA when configured
"""
import logging
from typing import Any, Dict, List

import pandas as pd

from .base import BaseConnector, QueryPlan
from shared.exceptions import DatabaseException

logger = logging.getLogger(__name__)


class SAPConnector(BaseConnector):
    """Connector for SAP: schema via sap_view_schemas or $metadata; data fetch via dedicated node."""

    def __init__(self, config: Dict[str, Any], **kwargs: Any) -> None:
        super().__init__(config, **kwargs)
        self._sap_view_schemas = kwargs.get("sap_view_schemas") or {}
        self._sap_access_token = kwargs.get("sap_access_token")

    def fetch_data(self, query_plan: QueryPlan) -> Dict[str, pd.DataFrame]:
        """SAP data fetch is handled by the dedicated sap_data_fetch node (OData / analytical views).
        This connector is used for get_schema; fetch_data for SAP should not be called from the
        generic fetch_data node (router sends SAP to sap_data_fetch_simple_node).
        """
        logger.debug("SAPConnector.fetch_data: SAP uses dedicated sap_data_fetch node")
        return {}

    def get_schema(self, table_name: str) -> str:
        """Return formatted schema for an SAP view from cached sap_view_schemas (from state)."""
        if self._sap_view_schemas and table_name in self._sap_view_schemas:
            info = self._sap_view_schemas[table_name]
            cols = info.get("columns", [])
            lines = [f"Table: {table_name}"]
            for col in cols:
                if isinstance(col, dict):
                    lines.append(
                        f"  - {col.get('name', '')}: {col.get('data_type', 'String')}"
                    )
                else:
                    lines.append(f"  - {col}")
            return "\n".join(lines)
        return f"Table: {table_name}\n  (Schema not in sap_view_schemas)"
