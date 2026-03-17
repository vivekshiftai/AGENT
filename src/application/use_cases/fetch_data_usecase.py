"""Use case: fetch data from configured data source(s) with optional date filter."""
import logging
from datetime import date
from typing import Any, Dict, List, Optional

import pandas as pd

from src.domain.entities.production_record import ProductionRecord
from src.domain.repositories.data_repository import DataRepository

logger = logging.getLogger(__name__)


def _dataframe_to_production_records(
    df: pd.DataFrame, source: str,
) -> List[ProductionRecord]:
    """Normalize a DataFrame to list of ProductionRecord (guess product, quantity, date columns)."""
    if df is None or df.empty:
        return []
    records = []
    cols = {c.lower(): c for c in df.columns}
    product_col = cols.get("product") or cols.get("product_id") or cols.get("item") or cols.get("item_id")
    qty_col = cols.get("quantity") or cols.get("qty") or cols.get("amount")
    date_col = cols.get("date") or cols.get("order_date") or cols.get("created_at") or cols.get("start")
    if not product_col:
        product_col = df.columns[0] if len(df.columns) > 0 else "product"
    if not qty_col:
        qty_col = df.columns[1] if len(df.columns) > 1 else "quantity"
    if not date_col:
        date_col = df.columns[2] if len(df.columns) > 2 else "date"
    for _, row in df.iterrows():
        try:
            qty = float(row.get(qty_col, 0) or 0)
            d = row.get(date_col)
            if hasattr(d, "date"):
                d = d.date()
            elif isinstance(d, str) and len(d) >= 10:
                d = date.fromisoformat(d[:10])
            else:
                d = date.today()
            records.append(
                ProductionRecord(
                    source=source,
                    product=str(row.get(product_col, "")),
                    quantity=qty,
                    date=d,
                )
            )
        except Exception as e:
            logger.debug("Skip row: %s", e)
            continue
    return records


class FetchDataUseCase:
    """Fetches data using a DataRepository (config-driven). Supports date_range in query_plan."""

    def __init__(self, data_repository: DataRepository):
        self._repo = data_repository

    def execute(
        self,
        queries: List[str],
        date_range: Optional[Dict[str, str]] = None,
        cached_dataframes: Optional[Dict[str, pd.DataFrame]] = None,
    ) -> Dict[str, Any]:
        """
        Execute queries and return DataFrames keyed by table name.
        date_range: optional {"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"} for connectors to use.
        """
        try:
            result = self._repo.fetch_data(
                queries=queries,
                date_range=date_range,
                cached_dataframes=cached_dataframes,
            )
            return {"data": result, "error": None}
        except Exception as e:
            logger.exception("FetchDataUseCase failed: %s", e)
            return {"data": {}, "error": str(e)}

    def execute_and_normalize(
        self,
        queries: List[str],
        date_range: Optional[Dict[str, str]] = None,
        source_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Execute and return both data and normalized production records."""
        out = self.execute(queries=queries, date_range=date_range)
        if out.get("error"):
            return {**out, "records": []}
        source = source_name or (self._repo.config.get("name") or self._repo.type)
        records = []
        for _table, df in (out.get("data") or {}).items():
            records.extend(_dataframe_to_production_records(df, source))
        return {**out, "records": [r.to_dict() for r in records]}
