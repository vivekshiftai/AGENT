"""File reading utilities for Excel and CSV data sources."""
import csv
import logging
import re
from pathlib import Path
from typing import List, Optional

import pandas as pd

from src.core.exceptions import DatabaseException

logger = logging.getLogger(__name__)


def get_excel_file_engine(file_path: str) -> str:
    """Get the appropriate engine for an Excel file (openpyxl or xlrd)."""
    ext = Path(file_path).suffix.lower()
    if ext == ".xlsx":
        return "openpyxl"
    if ext == ".xls":
        return "xlrd"
    return "openpyxl"


def read_excel_with_engine(
    file_path: str,
    sheet_name: Optional[str] = None,
    engine: Optional[str] = None,
    **kwargs,
) -> pd.DataFrame:
    """Read Excel file with automatic engine detection."""
    kwargs.pop("engine", None)
    ext = Path(file_path).suffix.lower()

    if engine is None:
        engine = get_excel_file_engine(file_path)

    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, engine=engine, **kwargs)
        df = df.dropna(how="all").dropna(axis=1, how="all")
        return df
    except Exception as e:
        alt = "xlrd" if engine == "openpyxl" else "openpyxl"
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name, engine=alt, **kwargs)
            df = df.dropna(how="all").dropna(axis=1, how="all")
            return df
        except Exception as e2:
            raise DatabaseException(f"Failed to read Excel file: {e2}") from e2


def read_csv_with_encoding(file_path: str, **kwargs) -> pd.DataFrame:
    """Read CSV file with automatic encoding and delimiter detection."""
    delimiters = [",", ";", "\t", "|", " "]
    encodings = ["utf-8-sig", "utf-8", "latin-1", "cp1252", "iso-8859-1", "utf-16"]

    explicit_delimiter = kwargs.pop("sep", None) or kwargs.pop("delimiter", None)
    if explicit_delimiter:
        delimiters = [explicit_delimiter]

    last_error = None
    for encoding in encodings:
        for delimiter in delimiters:
            try:
                df = pd.read_csv(
                    file_path,
                    encoding=encoding,
                    sep=delimiter,
                    on_bad_lines="skip",
                    engine="python",
                    **kwargs,
                )
                if len(df.columns) > 1 or (len(df.columns) == 1 and len(df) > 0):
                    df = df.dropna(how="all").dropna(axis=1, how="all")
                    return df
            except UnicodeDecodeError:
                continue
            except (pd.errors.ParserError, csv.Error):
                continue
            except Exception as e:
                last_error = e
                continue

    try:
        df = pd.read_csv(file_path, on_bad_lines="skip", engine="python", **kwargs)
        if len(df.columns) > 0:
            df = df.dropna(how="all").dropna(axis=1, how="all")
            return df
    except Exception as e:
        last_error = e

    raise DatabaseException(
        f"Failed to read CSV file. Tried encodings: {encodings}. Last error: {last_error}"
    )


def normalize_dataframe_for_duckdb(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize DataFrame to prevent DuckDB type casting errors on mixed-type columns."""
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            continue
        non_null = df[col].dropna()
        if len(non_null) == 0:
            continue
        if df[col].dtype == "object":
            has_non_numeric = False
            for val in non_null:
                if isinstance(val, str):
                    try:
                        float(val.strip())
                    except (ValueError, TypeError):
                        has_non_numeric = True
                        break
                elif not isinstance(val, (int, float)):
                    has_non_numeric = True
                    break
            if has_non_numeric:
                try:
                    df[col] = df[col].astype(str)
                except Exception:
                    pass
    return df


_ISO_DATE_PATTERN = re.compile(
    r'(?P<col>"[^"]+"|\b[a-zA-Z_][a-zA-Z0-9_]*\b)\s*'
    r"(?P<op>>=|<=|<>|!=|<|>|=)\s*"
    r"(?P<q>'|\")(?P<date>\d{4}-\d{2}-\d{2})(?P=q)",
    flags=re.IGNORECASE,
)
_NUMERIC_PATTERN = re.compile(
    r'(?P<col>"[^"]+"|\b[a-zA-Z_][a-zA-Z0-9_]*\b)\s*'
    r"(?P<op>>=|<=|<>|!=|<|>|=)\s*"
    r"(?P<num>\d+)\b",
    flags=re.IGNORECASE,
)


def enforce_safe_duckdb_date_comparisons(sql: str, date_columns: List[str]) -> str:
    """Rewrite string date literals to DATE literals for DuckDB."""
    if not sql or not date_columns:
        return sql
    date_cols_norm = {str(c).lower(): c for c in date_columns if isinstance(c, str)}

    for m in _NUMERIC_PATTERN.finditer(sql):
        col = m.group("col").strip('"').strip().lower()
        if col in date_cols_norm:
            raise DatabaseException(
                "Unsafe SQL: numeric comparison against date column. Use DATE literals."
            )

    def repl(m: re.Match) -> str:
        col, op, date_str = m.group("col"), m.group("op"), m.group("date")
        col_name = col.strip('"').strip().lower()
        if col_name not in date_cols_norm:
            return m.group(0)
        return f"{col} {op} DATE '{date_str}'"

    return _ISO_DATE_PATTERN.sub(repl, sql)
