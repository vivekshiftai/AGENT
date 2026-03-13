"""Data models for Polars-first analytics pipeline.

This module provides core data structures for the refactored pipeline:
- DataResult: Wraps Polars LazyFrame with metadata
- FetchIntent: Controls data fetching limits based on use case
- Processing abstractions for lazy evaluation
- Safe Pandas→Polars conversion utilities
"""
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Any, Optional, List, Union
import polars as pl
import logging
import math

logger = logging.getLogger(__name__)

# Check for pyarrow availability at import time
_PYARROW_AVAILABLE = False
try:
    import pyarrow as pa
    _PYARROW_AVAILABLE = True
except ImportError:
    pass


def pandas_to_polars(df_pandas) -> pl.DataFrame:
    """
    Convert Pandas DataFrame to Polars DataFrame using PyArrow for safe type preservation.
    
    This function ensures that nullable Pandas dtypes (Int64, string, datetime64[ns])
    are correctly preserved during conversion. It uses PyArrow as an intermediate
    format to avoid lossy type conversions.
    
    Args:
        df_pandas: Pandas DataFrame to convert
        
    Returns:
        Polars DataFrame with preserved dtypes
        
    Raises:
        ImportError: If pyarrow is not installed (with helpful error message)
        ValueError: If conversion fails
        
    Example:
        >>> import pandas as pd
        >>> df = pd.DataFrame({'a': [1, 2, None], 'b': ['x', 'y', None]})
        >>> df['a'] = df['a'].astype('Int64')  # Nullable integer
        >>> df_polars = pandas_to_polars(df)
    """
    if not _PYARROW_AVAILABLE:
        raise ImportError(
            "pyarrow is required for converting Pandas DataFrames to Polars "
            "when using nullable dtypes (Int64, string, datetime64[ns]). "
            "Please install pyarrow: pip install pyarrow"
        )
    
    try:
        # Convert Pandas DataFrame to PyArrow Table
        # preserve_index=False ensures index is not included as a column
        arrow_table = pa.Table.from_pandas(df_pandas, preserve_index=False)
        
        # Convert PyArrow Table to Polars DataFrame
        # This preserves all nullable dtypes correctly
        df_polars = pl.from_arrow(arrow_table)
        
        return df_polars
        
    except Exception as e:
        raise ValueError(
            f"Failed to convert Pandas DataFrame to Polars: {e}. "
            f"Ensure pyarrow is installed and the DataFrame has valid data types."
        ) from e


def _normalize_dict_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Normalize dictionary records to handle type inconsistencies that cause Polars errors.
    
    This function:
    - Converts float values that are actually integers (e.g., 1.0 -> 1) to integers
    - Converts NaN values to None
    - Handles inf values appropriately
    
    Args:
        records: List of row dictionaries
        
    Returns:
        Normalized list of row dictionaries
    """
    if not records:
        return records
    
    normalized = []
    for record in records:
        normalized_record = {}
        for key, value in record.items():
            if value is None:
                normalized_record[key] = None
            elif isinstance(value, float):
                # Check for NaN
                if math.isnan(value):
                    normalized_record[key] = None
                # Check for inf
                elif math.isinf(value):
                    # Keep inf as is, but log a warning
                    logger.warning(f"Found inf value in column '{key}', keeping as float")
                    normalized_record[key] = value
                # Check if float is actually an integer (e.g., 1.0 -> 1)
                elif value.is_integer():
                    normalized_record[key] = int(value)
                else:
                    normalized_record[key] = value
            else:
                normalized_record[key] = value
        normalized.append(normalized_record)
    
    return normalized


class FetchIntent(str, Enum):
    """Intent for data fetching - controls behavior (not row limits).
    
    All intents now process full data:
    - RAW: For export/streaming - no processing overhead
    - ANALYSIS: For computation - full dataset with optimizations
    - VISUALIZATION: For charts - full dataset with aggregation
    
    NOTE: Row limits were removed - we process ALL data using Polars
    lazy evaluation which handles large datasets efficiently.
    """
    RAW = "raw"
    ANALYSIS = "analysis"
    VISUALIZATION = "visualization"
    
    @property
    def max_rows(self) -> Optional[int]:
        """Get maximum rows allowed for this intent.
        
        Returns None for all intents - no artificial limits.
        Polars LazyFrames handle large datasets efficiently.
        """
        # No limits - process full data for all intents
        return None
    
    @property
    def description(self) -> str:
        """Human-readable description of the intent."""
        descriptions = {
            FetchIntent.RAW: "Full data export/streaming",
            FetchIntent.ANALYSIS: "Full data analysis with lazy evaluation",
            FetchIntent.VISUALIZATION: "Full data for chart generation with aggregation",
        }
        return descriptions.get(self, "Unknown intent")


@dataclass
class DataResult:
    """Container for fetched data with Polars LazyFrame.
    
    This is the core data structure returned by the fetch layer.
    All heavy processing should operate on the LazyFrame without collecting.
    
    Attributes:
        lf: Polars LazyFrame for lazy evaluation
        row_count: Approximate or exact row count (may be estimated for large datasets)
        schema: Dictionary mapping column names to Polars dtypes
        table_name: Optional name of the source table
        query_id: Optional query identifier for caching/export
        is_truncated: Whether data was truncated due to intent limits
        intent: The fetch intent that was used
    """
    lf: pl.LazyFrame
    row_count: int
    schema: Dict[str, str]
    table_name: Optional[str] = None
    query_id: Optional[str] = None
    is_truncated: bool = False
    intent: FetchIntent = FetchIntent.ANALYSIS
    
    @classmethod
    def from_rows_and_columns(
        cls,
        rows: List[tuple],
        columns: List[str],
        table_name: Optional[str] = None,
        intent: FetchIntent = FetchIntent.ANALYSIS,
    ) -> "DataResult":
        """Create DataResult from raw rows and column names.
        
        This is the primary factory for data arriving from database queries.
        Converts to Polars DataFrame and wraps as LazyFrame immediately.
        
        Args:
            rows: List of tuples (database result rows)
            columns: List of column names
            table_name: Optional source table name
            intent: Fetch intent (for metadata only, no limits)
            
        Returns:
            DataResult with LazyFrame ready for processing
        """
        if not rows:
            # Empty result - create empty DataFrame with schema
            df = pl.DataFrame(schema={col: pl.Utf8 for col in columns})
            return cls(
                lf=df.lazy(),
                row_count=0,
                schema={col: str(pl.Utf8) for col in columns},
                table_name=table_name,
                intent=intent,
            )
        
        # Create DataFrame from rows with orient="row"
        # This is the most efficient way for database results
        df = pl.DataFrame(rows, schema=columns, orient="row")
        
        # Extract schema as string dict
        schema = {col: str(dtype) for col, dtype in df.schema.items()}
        
        return cls(
            lf=df.lazy(),
            row_count=len(df),
            schema=schema,
            table_name=table_name,
            is_truncated=False,  # No truncation - full data
            intent=intent,
        )
    
    @classmethod
    def from_arrow(
        cls,
        arrow_table: Any,
        table_name: Optional[str] = None,
        intent: FetchIntent = FetchIntent.ANALYSIS,
    ) -> "DataResult":
        """Create DataResult from PyArrow Table.
        
        Zero-copy conversion for Arrow-native data sources.
        
        Args:
            arrow_table: PyArrow Table
            table_name: Optional source table name
            intent: Fetch intent (for metadata only)
            
        Returns:
            DataResult with LazyFrame
        """
        df = pl.from_arrow(arrow_table)
        schema = {col: str(dtype) for col, dtype in df.schema.items()}
        
        return cls(
            lf=df.lazy(),
            row_count=len(df),
            schema=schema,
            table_name=table_name,
            is_truncated=False,  # No truncation - full data
            intent=intent,
        )
    
    @classmethod
    def from_dict_records(
        cls,
        records: List[Dict[str, Any]],
        table_name: Optional[str] = None,
        intent: FetchIntent = FetchIntent.ANALYSIS,
    ) -> "DataResult":
        """Create DataResult from list of dictionaries.
        
        Fallback for data arriving as dict records (e.g., from some APIs).
        
        Args:
            records: List of row dictionaries
            table_name: Optional source table name
            intent: Fetch intent (for metadata only)
            
        Returns:
            DataResult with LazyFrame
        """
        if not records:
            return cls(
                lf=pl.DataFrame().lazy(),
                row_count=0,
                schema={},
                table_name=table_name,
                intent=intent,
            )
        
        # Normalize data to handle float values that should be integers
        normalized_records = _normalize_dict_records(records)
        
        try:
            # Use infer_schema_length=None to infer schema from all rows, not just first few
            # This helps when columns have mixed types (e.g., integers and floats)
            df = pl.DataFrame(normalized_records, infer_schema_length=None)
        except Exception as e:
            logger.error(f"Failed to create Polars DataFrame from records: {e}")
            logger.debug(f"First record sample: {normalized_records[0] if normalized_records else 'No records'}")
            # Fallback: convert all values to strings as last resort
            try:
                logger.warning("Attempting fallback: converting all values to strings")
                string_records = [
                    {k: str(v) if v is not None else None for k, v in row.items()}
                    for row in normalized_records
                ]
                df = pl.DataFrame(string_records, infer_schema_length=None)
            except Exception as e2:
                logger.error(f"Failed even with string fallback: {e2}")
                raise
        
        schema = {col: str(dtype) for col, dtype in df.schema.items()}
        
        return cls(
            lf=df.lazy(),
            row_count=len(df),
            schema=schema,
            table_name=table_name,
            is_truncated=False,  # No truncation - full data
            intent=intent,
        )
    
    @classmethod
    def from_polars_dataframe(
        cls,
        df: pl.DataFrame,
        table_name: Optional[str] = None,
        intent: FetchIntent = FetchIntent.ANALYSIS,
    ) -> "DataResult":
        """Create DataResult from existing Polars DataFrame.
        
        Args:
            df: Polars DataFrame
            table_name: Optional source table name
            intent: Fetch intent (for metadata only)
            
        Returns:
            DataResult with LazyFrame
        """
        schema = {col: str(dtype) for col, dtype in df.schema.items()}
        
        return cls(
            lf=df.lazy(),
            row_count=len(df),
            schema=schema,
            table_name=table_name,
            is_truncated=False,  # No truncation - full data
            intent=intent,
        )
    
    def collect(self) -> pl.DataFrame:
        """Collect the LazyFrame into a DataFrame.
        
        WARNING: Only call this when final results are needed.
        Prefer lazy operations for all processing.
        """
        return self.lf.collect()
    
    def to_dicts(self) -> List[Dict[str, Any]]:
        """Convert to list of dictionaries for JSON serialization.
        
        WARNING: This materializes all data. Use sparingly.
        """
        return self.lf.collect().to_dicts()
    
    def head(self, n: int = 5) -> pl.DataFrame:
        """Get first n rows as DataFrame (collected)."""
        return self.lf.head(n).collect()
    
    def validate_for_intent(self) -> None:
        """Validate data meets intent requirements.
        
        NOTE: This is now a no-op since we process full data for all intents.
        Kept for API compatibility.
        """
        # No validation needed - all intents process full data
        pass


@dataclass
class MultiTableResult:
    """Container for multiple DataResults from different tables.
    
    Used when fetching from multiple SQL queries or tables.
    """
    tables: Dict[str, DataResult] = field(default_factory=dict)
    query_id: Optional[str] = None
    total_row_count: int = 0
    
    def add_table(self, name: str, result: DataResult) -> None:
        """Add a table result."""
        self.tables[name] = result
        self.total_row_count += result.row_count
    
    def get_lazyframe(self, table_name: str) -> Optional[pl.LazyFrame]:
        """Get LazyFrame for a specific table."""
        result = self.tables.get(table_name)
        return result.lf if result else None
    
    def get_all_lazyframes(self) -> Dict[str, pl.LazyFrame]:
        """Get all LazyFrames as a dictionary."""
        return {name: result.lf for name, result in self.tables.items()}
    
    @property
    def table_names(self) -> List[str]:
        """Get list of table names."""
        return list(self.tables.keys())
    
    def validate_all(self) -> None:
        """Validate all tables meet their intent requirements."""
        for name, result in self.tables.items():
            try:
                result.validate_for_intent()
            except ValueError as e:
                raise ValueError(f"Table '{name}': {e}")


class RowLimitExceededError(Exception):
    """DEPRECATED: Row limits have been removed.
    
    This exception is kept for backward compatibility but will never be raised
    since all intents now process full data.
    """
    
    def __init__(self, actual_rows: int, max_rows: int, intent: FetchIntent):
        self.actual_rows = actual_rows
        self.max_rows = max_rows
        self.intent = intent
        super().__init__(
            f"DEPRECATED: Row limits have been removed. All data is now processed."
        )
