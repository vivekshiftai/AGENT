"""Excel file connector for the Data Source Abstraction Layer."""
import logging
import re
from pathlib import Path
from typing import Any, Dict, List

import duckdb
import pandas as pd

from .base import BaseConnector, QueryPlan
from ..database.data_source_gateway import (
    _enforce_safe_duckdb_date_comparisons,
    _normalize_dataframe_for_duckdb,
    get_excel_file_engine,
    read_excel_with_engine,
)
from shared.exceptions import DatabaseException

logger = logging.getLogger(__name__)


def _extract_table_name(query: str, index: int) -> str:
    """Extract table name from SQL query."""
    match = re.search(r"\bFROM\s+([\"`]?)(\w+)\1", query, re.IGNORECASE)
    if match:
        return match.group(2)
    return f"data_table_{index + 1}"


def _clean_table_name(name: str) -> str:
    """Clean table name for DuckDB registration."""
    clean = name.replace(" ", "_").replace("-", "_")
    clean = re.sub(r"[^a-zA-Z0-9_]", "_", clean)
    if clean and not clean[0].isalpha() and clean[0] != "_":
        clean = "_" + clean
    return clean or "sheet_1"


class ExcelConnector(BaseConnector):
    """Connector for Excel files. Executes SQL via DuckDB on loaded/cached DataFrames."""

    def __init__(self, config: Dict[str, Any], **kwargs: Any) -> None:
        super().__init__(config, **kwargs)
        self._file_path = config.get("file_path")

    def fetch_data(self, query_plan: QueryPlan) -> Dict[str, pd.DataFrame]:
        """Execute SQL on Excel data (cached DataFrames or load from file)."""
        queries = query_plan.get("queries") or []
        cached_dataframes = query_plan.get("cached_dataframes")
        if not queries or not isinstance(queries, list):
            logger.warning("ExcelConnector: no queries in query_plan")
            return {}

        if cached_dataframes:
            return self._fetch_via_duckdb(queries, cached_dataframes)

        file_path = self._file_path or query_plan.get("config", {}).get("file_path")
        if not file_path or not Path(file_path).exists():
            raise DatabaseException(f"File not found: {file_path}")

        source_type = (self.config.get("type") or "").lower()
        if source_type == "csv":
            from ..database.data_source_gateway import read_csv_with_encoding
            df = read_csv_with_encoding(file_path)
            table_name = Path(file_path).stem or "csv_table"
            cached = {table_name: df}
            return self._fetch_via_duckdb(queries, cached)

        # Load all sheets and treat as tables
        engine = get_excel_file_engine(file_path)
        try:
            xl = pd.ExcelFile(file_path, engine=engine)
        except Exception as e:
            alt = "xlrd" if engine == "openpyxl" else "openpyxl"
            try:
                xl = pd.ExcelFile(file_path, engine=alt)
                engine = alt
            except Exception as e2:
                raise DatabaseException(f"Failed to read Excel file: {e2}") from e2
        cached = {}
        for sheet in xl.sheet_names:
            df = read_excel_with_engine(file_path, sheet_name=sheet, engine=engine)
            cached[sheet] = df
        return self._fetch_via_duckdb(queries, cached)

    def _fetch_via_duckdb(
        self, queries: List[str], cached_dataframes: Dict[str, pd.DataFrame]
    ) -> Dict[str, pd.DataFrame]:
        """Run SQL against cached DataFrames using DuckDB."""
        conn = duckdb.connect()
        try:
            clean_to_original: Dict[str, str] = {}
            date_columns: List[str] = []
            for table_name, df in cached_dataframes.items():
                clean_name = _clean_table_name(table_name)
                df_norm = _normalize_dataframe_for_duckdb(df)
                conn.register(clean_name, df_norm)
                clean_to_original[clean_name] = table_name
                for c in df.columns:
                    if pd.api.types.is_datetime64_any_dtype(df[c]):
                        date_columns.append(c)

            table_name_map = {
                _clean_table_name(name): name for name in cached_dataframes.keys()
            }
            results: Dict[str, pd.DataFrame] = {}
            for index, sql in enumerate(queries):
                if not isinstance(sql, str) or not sql.strip():
                    continue
                table_name = _extract_table_name(sql, index)
                modified_sql = sql
                for orig, clean in table_name_map.items():
                    if orig != clean:
                        modified_sql = re.sub(
                            rf"\bFROM\s+{re.escape(orig)}\b",
                            f"FROM {clean}",
                            modified_sql,
                            flags=re.IGNORECASE,
                        )
                        modified_sql = re.sub(
                            rf"\bJOIN\s+{re.escape(orig)}\b",
                            f"JOIN {clean}",
                            modified_sql,
                            flags=re.IGNORECASE,
                        )
                if date_columns:
                    modified_sql = _enforce_safe_duckdb_date_comparisons(
                        modified_sql, date_columns
                    )
                try:
                    results[table_name] = conn.execute(modified_sql).fetchdf()
                except Exception as e:
                    logger.error("ExcelConnector: query failed: %s", e)
                    raise DatabaseException(f"Excel query failed: {e}") from e
            return results
        finally:
            conn.close()

    def get_schema(self, table_name: str) -> str:
        """Return formatted schema for an Excel sheet or CSV table."""
        file_path = self._file_path or self.config.get("file_path")
        if not file_path or not Path(file_path).exists():
            return f"Table: {table_name}\n  (File not found or not accessible)"
        source_type = (self.config.get("type") or "").lower()
        if source_type == "csv":
            from ..database.data_source_gateway import read_csv_with_encoding
            df = read_csv_with_encoding(file_path, nrows=100)
            lines = [f"Table: {Path(file_path).stem} (CSV File)"]
            for col in df.columns:
                dtype = str(df[col].dtype)
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    dtype = "datetime"
                elif df[col].dtype == "object":
                    dtype = "text"
                lines.append(f"  - {col}: {dtype}")
            return "\n".join(lines)
        engine = get_excel_file_engine(file_path)
        try:
            xl = pd.ExcelFile(file_path, engine=engine)
        except Exception:
            alt = "xlrd" if engine == "openpyxl" else "openpyxl"
            xl = pd.ExcelFile(file_path, engine=alt)
            engine = alt
        if table_name in xl.sheet_names:
            df = read_excel_with_engine(file_path, sheet_name=table_name, engine=engine, nrows=100)
        else:
            for sheet in xl.sheet_names:
                clean = _clean_table_name(sheet)
                if clean == table_name or sheet == table_name:
                    df = read_excel_with_engine(file_path, sheet_name=sheet, engine=engine, nrows=100)
                    table_name = sheet
                    break
            else:
                return f"Table: {table_name}\n  (Sheet not found)"
        lines = [f"Table: {table_name} (Excel Sheet)"]
        for col in df.columns:
            dtype = str(df[col].dtype)
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                dtype = "datetime"
            elif df[col].dtype == "object":
                dtype = "text"
            lines.append(f"  - {col}: {dtype}")
        return "\n".join(lines)
