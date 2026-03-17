"""Unified data repository - connects to data sources and fetches data."""
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from src.infrastructure.connectors.connector_factory import ConnectorFactory
from src.core.exceptions import DatabaseException

logger = logging.getLogger(__name__)


class DataRepository:
    """
    Unified repository for executing queries across data sources.

    Supports: ClickHouse, Excel, CSV, SAP Datasphere.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.type = (config.get("type") or "").lower()
        if self.type == "sap_datasphere":
            self.type = "sap"
        self._connector = None

    def _get_connector(self):
        if self._connector is None:
            self._connector = ConnectorFactory.get_connector(self.config)
        return self._connector

    def fetch_data(
        self,
        queries: List[str],
        date_range: Optional[Dict[str, str]] = None,
        cached_dataframes: Optional[Dict[str, pd.DataFrame]] = None,
    ) -> Dict[str, pd.DataFrame]:
        """Execute SQL queries and return DataFrames. date_range: {start, end} for filtering."""
        if self.type == "sap":
            logger.warning(
                "SAP data fetch: use DatasphereService.execute_odata_query directly"
            )
            return {}
        query_plan = {"queries": queries, "config": self.config, "date_range": date_range}
        if cached_dataframes:
            query_plan["cached_dataframes"] = cached_dataframes
        connector = self._get_connector()
        return connector.fetch_data(query_plan)

    def get_schema(self, table_name: str) -> str:
        """Get schema for a table/view."""
        return self._get_connector().get_schema(table_name)

    def list_tables(self) -> List[str]:
        """List available tables/sheets."""
        if self.type == "excel":
            from src.infrastructure.utils.file_utils import (
                get_excel_file_engine,
                read_excel_with_engine,
            )
            file_path = self.config.get("file_path")
            if file_path and Path(file_path).exists():
                engine = get_excel_file_engine(file_path)
                try:
                    xl = pd.ExcelFile(file_path, engine=engine)
                except Exception:
                    xl = pd.ExcelFile(
                        file_path,
                        engine="xlrd" if engine == "openpyxl" else "openpyxl",
                    )
                return xl.sheet_names
            return []
        if self.type == "csv":
            from src.infrastructure.utils.file_utils import read_csv_with_encoding

            file_path = self.config.get("file_path")
            if file_path and Path(file_path).exists():
                return [Path(file_path).stem]
            return []
        if self.type == "clickhouse":
            connector = self._get_connector()
            client = connector._get_client()
            result = client.query("SHOW TABLES")
            return [row[0] for row in (result.result_rows or [])]
        if self.type == "postgres":
            connector = self._get_connector()
            client = connector._get_client()
            rows = client.execute_query(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            )
            return [r["table_name"] for r in rows]
        return []

    def test_connection(self) -> bool:
        """Test the data source connection."""
        try:
            if self.type in ("excel", "csv"):
                file_path = self.config.get("file_path")
                if not file_path or not Path(file_path).exists():
                    return False
                if self.type == "csv":
                    read_csv_with_encoding(file_path, nrows=0)
                return True
            if self.type == "clickhouse":
                connector = self._get_connector()
                connector._get_client().query("SELECT 1")
                return True
            if self.type == "postgres":
                connector = self._get_connector()
                connector._get_client().execute_query("SELECT 1")
                return True
            if self.type == "sap":
                from src.infrastructure.external_services.datasphere_service import (
                    get_datasphere_service,
                )

                ds = get_datasphere_service()
                user_id = self.config.get("user_id")
                if not user_id:
                    raise DatabaseException(
                        "user_id required for SAP connection test"
                    )
                import asyncio

                result = asyncio.run(ds.list_catalog_assets(user_id))
                return len(result.view_names) >= 0
            return False
        except Exception as e:
            logger.error("Connection test failed: %s", e)
            raise DatabaseException(f"Connection test failed: {e}") from e

    def close(self):
        """Release resources."""
        if self._connector:
            self._connector.close()
            self._connector = None
