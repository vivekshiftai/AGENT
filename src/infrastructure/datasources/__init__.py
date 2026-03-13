"""Data Source Abstraction Layer for analytics and production planning.

Supported source types: clickhouse, excel, sap.
Each connector exposes fetch_data(query_plan) -> Dict[str, pd.DataFrame] and get_schema(table_name) -> str.
"""
from .base import BaseConnector, QueryPlan
from .clickhouse_connector import ClickHouseConnector
from .connector_factory import ConnectorFactory, SUPPORTED_SOURCE_TYPES
from .excel_connector import ExcelConnector
from .sap_connector import SAPConnector

__all__ = [
    "BaseConnector",
    "QueryPlan",
    "ClickHouseConnector",
    "ExcelConnector",
    "SAPConnector",
    "ConnectorFactory",
    "SUPPORTED_SOURCE_TYPES",
]
