"""Base connector and query plan types for the Data Source Abstraction Layer."""
from abc import ABC, abstractmethod
from typing import Any, Dict, List

import pandas as pd

QueryPlan = Dict[str, Any]


class BaseConnector(ABC):
    """
    Abstract base for all data source connectors.

    All connectors must implement:
    - connect(): establish connection (may be lazy)
    - test_connection(): return True if connection works
    - fetch_data(query_plan): query_plan may include date_range, queries, config
    - get_schema(table_name)
    """

    def __init__(self, config: Dict[str, Any], **kwargs: Any) -> None:
        self.config = config
        self._extra = kwargs

    def connect(self) -> None:
        """Establish connection; default no-op (lazy connect in fetch_data/test_connection)."""
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        """Return True if connection is successful. Raise or return False on failure."""
        pass

    @abstractmethod
    def fetch_data(self, query_plan: QueryPlan) -> Dict[str, pd.DataFrame]:
        """Execute the query plan and return one or more DataFrames.
        query_plan may contain: queries, config, date_range (start/end), cached_dataframes.
        """
        pass

    @abstractmethod
    def get_schema(self, table_name: str) -> str:
        """Retrieve metadata/schema for a single table or view."""
        pass

    def get_schema_for_tables(self, table_names: List[str]) -> Dict[str, str]:
        """Retrieve schema for multiple tables."""
        return {name: self.get_schema(name) for name in table_names}

    def close(self) -> None:
        """Release resources."""
        pass

    def __enter__(self) -> "BaseConnector":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
