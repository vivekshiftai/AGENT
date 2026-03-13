"""Base connector and query plan types for the Data Source Abstraction Layer."""
from abc import ABC, abstractmethod
from typing import Any, Dict, List

import pandas as pd


# Query plan: unified structure passed to fetch_data.
# - queries: list of executable SQL strings (or OData URLs for SAP)
# - config: data source configuration dict
# - cached_dataframes: optional pre-loaded DataFrames (Excel/CSV)
QueryPlan = Dict[str, Any]


class BaseConnector(ABC):
    """Abstract base for all data source connectors.
    
    Each connector implements fetch_data(query_plan) and get_schema(table_name)
    so the LangGraph pipeline can use a single abstraction for any supported source.
    """

    def __init__(self, config: Dict[str, Any], **kwargs: Any) -> None:
        """Initialize connector with data source configuration.
        
        Args:
            config: Data source config (type, host, file_path, etc.).
            **kwargs: Optional extra context (e.g. sap_access_token, sap_view_schemas).
        """
        self.config = config
        self._extra = kwargs

    @abstractmethod
    def fetch_data(self, query_plan: QueryPlan) -> Dict[str, pd.DataFrame]:
        """Execute the query plan and return one or more DataFrames.
        
        Args:
            query_plan: Dict with at least "queries" (list of SQL or OData URLs)
                       and "config". May include "cached_dataframes" for file sources.
        
        Returns:
            Dict mapping table/source name to pandas DataFrame.
        """
        pass

    @abstractmethod
    def get_schema(self, table_name: str) -> str:
        """Retrieve metadata/schema for a single table or view.
        
        Args:
            table_name: Name of the table, sheet, or view.
        
        Returns:
            Formatted schema string (e.g. "Table: X\\n  - col: type").
        """
        pass

    def get_schema_for_tables(self, table_names: List[str]) -> Dict[str, str]:
        """Retrieve schema for multiple tables. Default: call get_schema for each.
        
        Override for sources that support batch metadata.
        """
        return {name: self.get_schema(name) for name in table_names}

    def close(self) -> None:
        """Release resources (connections, file handles). No-op by default."""
        pass

    def __enter__(self) -> "BaseConnector":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
