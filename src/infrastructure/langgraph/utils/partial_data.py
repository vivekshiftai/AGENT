"""Source data extraction and partial-update field builders for the data analysis graph."""
import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def _detect_date_columns_and_grouping_options(raw_dataframes: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Detect date columns and determine if date grouping control should be shown.

    Supports both Polars and pandas DataFrames.
    """
    if not raw_dataframes:
        return None

    total_records = 0
    for df in raw_dataframes.values():
        try:
            if hasattr(df, '__len__'):
                total_records += len(df)
            elif hasattr(df, 'collect'):
                import polars as pl
                total_records += df.select(pl.len()).collect().item()
        except Exception:
            continue

    if total_records < 30:
        return None

    date_columns: List[str] = []

    for df in raw_dataframes.values():
        try:
            if hasattr(df, 'schema'):
                import polars as pl
                if hasattr(df, 'collect'):
                    schema = df.collect_schema()
                else:
                    schema = df.schema

                for col_name, dtype in schema.items():
                    col_lower = col_name.lower()
                    if any(keyword in col_lower for keyword in ["date", "time", "timestamp", "created", "updated", "period"]):
                        dtype_str = str(dtype).lower()
                        if any(dt in dtype_str for dt in ["date", "time", "datetime"]):
                            if col_name not in date_columns:
                                date_columns.append(col_name)
                        else:
                            try:
                                if hasattr(df, 'collect'):
                                    sample = df.select(pl.col(col_name)).head(10).collect()
                                else:
                                    sample = df.select(pl.col(col_name)).head(10)
                                sample_values = sample.to_series().drop_nulls()
                                if len(sample_values) > 0:
                                    first_val = str(sample_values[0])
                                    if any(p in first_val for p in ["-", "/", "T", ":"]) and len(first_val) > 8:
                                        if col_name not in date_columns:
                                            date_columns.append(col_name)
                            except Exception:
                                pass

            elif hasattr(df, 'dtypes'):
                import pandas as pd
                if hasattr(df, 'empty') and df.empty:
                    continue

                for col_name in df.columns:
                    col_lower = col_name.lower()
                    if any(keyword in col_lower for keyword in ["date", "time", "timestamp", "created", "updated", "period"]):
                        if pd.api.types.is_datetime64_any_dtype(df[col_name]):
                            if col_name not in date_columns:
                                date_columns.append(col_name)
                        else:
                            sample_values = df[col_name].dropna().head(10)
                            if len(sample_values) > 0:
                                first_val = sample_values.iloc[0]
                                if isinstance(first_val, str):
                                    if any(p in str(first_val) for p in ["-", "/", "T", ":"]) and len(str(first_val)) > 8:
                                        if col_name not in date_columns:
                                            date_columns.append(col_name)
        except Exception as e:
            logger.warning(f"Failed to detect date columns: {e}")
            continue

    if not date_columns:
        return None

    return {
        "enabled": True,
        "date_columns": date_columns,
        "default_grouping": "month",
        "options": [
            {"value": "none", "label": "Show Individual Records"},
            {"value": "day", "label": "Group by Day"},
            {"value": "week", "label": "Group by Week"},
            {"value": "month", "label": "Group by Month"},
            {"value": "quarter", "label": "Group by Quarter"},
            {"value": "year", "label": "Group by Year"},
        ],
        "record_count": total_records,
        "message": f"You have {total_records} records. Choose how to group the data by date."
    }


def extract_source_data_preview(
    raw_dataframes: Dict[str, Any], max_rows: int = 1000
) -> Dict[str, Any]:
    """
    Extract source_data preview from raw_dataframes for streaming.

    Args:
        raw_dataframes: Dictionary of table_name -> DataFrame/LazyFrame
        max_rows: Maximum rows per table to include in preview

    Returns:
        Dictionary of table_name -> list of records (preview)
    """
    source_data = {}
    if not raw_dataframes:
        return source_data

    for table_name, table_rows in raw_dataframes.items():
        try:
            if hasattr(table_rows, "collect"):
                table_rows = table_rows.collect()
            if hasattr(table_rows, "schema"):
                if len(table_rows) == 0:
                    continue
                preview_df = table_rows.head(max_rows)
                source_data[table_name] = preview_df.to_dicts()
            elif hasattr(table_rows, "empty"):
                if table_rows.empty:
                    continue
                preview_rows = table_rows.head(max_rows)
                try:
                    table_records = json.loads(
                        preview_rows.to_json(orient="records", date_format="iso")
                    )
                except Exception:
                    table_records = preview_rows.to_dict("records")
                source_data[table_name] = table_records
        except Exception as e:
            logger.warning(f"Failed to extract source_data preview for {table_name}: {e}")
    return source_data


def extract_full_source_data(raw_dataframes: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract full source_data from raw_dataframes for chunked transmission (all rows).

    Args:
        raw_dataframes: Dictionary of table_name -> DataFrame/LazyFrame

    Returns:
        Dictionary of table_name -> list of records (full data)
    """
    source_data = {}
    if not raw_dataframes:
        return source_data

    for table_name, table_rows in raw_dataframes.items():
        try:
            if hasattr(table_rows, "collect"):
                table_rows = table_rows.collect()
            if hasattr(table_rows, "schema"):
                if len(table_rows) == 0:
                    continue
                source_data[table_name] = table_rows.to_dicts()
            elif hasattr(table_rows, "empty"):
                if table_rows.empty:
                    continue
                try:
                    table_records = json.loads(
                        table_rows.to_json(orient="records", date_format="iso")
                    )
                except Exception:
                    table_records = table_rows.to_dict("records")
                source_data[table_name] = table_records
        except Exception as e:
            logger.warning(f"Failed to extract full source_data for {table_name}: {e}")
    return source_data


def extract_source_data_metadata(
    raw_dataframes: Dict[str, Any], query_id: str = "", max_rows: int = 1000
) -> Dict[str, Any]:
    """
    Extract source_data_metadata from raw_dataframes for streaming.

    Args:
        raw_dataframes: Dictionary of table_name -> DataFrame/LazyFrame
        query_id: Query ID for download API
        max_rows: Maximum rows per table to include in preview

    Returns:
        Dictionary of table_name -> metadata dict
    """
    source_data_metadata = {}
    if not raw_dataframes:
        return source_data_metadata

    for table_name, table_rows in raw_dataframes.items():
        try:
            if hasattr(table_rows, "collect"):
                table_rows = table_rows.collect()
            if hasattr(table_rows, "schema"):
                if len(table_rows) == 0:
                    continue
                total_rows = len(table_rows)
                preview_rows = min(max_rows, total_rows)
                is_truncated = total_rows > max_rows
                source_data_metadata[table_name] = {
                    "total_rows": total_rows,
                    "preview_rows": preview_rows,
                    "is_truncated": is_truncated,
                    "columns": list(table_rows.columns),
                    "dtypes": {col: str(dtype) for col, dtype in table_rows.schema.items()},
                    "query_id": query_id,
                    "table_name": table_name,
                    "download_available": is_truncated,
                    "download_message": (
                        f"Showing {preview_rows:,} of {total_rows:,} rows. Click to download full data."
                        if is_truncated
                        else None
                    ),
                }
            elif hasattr(table_rows, "empty"):
                if table_rows.empty:
                    continue
                total_rows = len(table_rows)
                preview_rows = min(max_rows, total_rows)
                is_truncated = total_rows > max_rows
                source_data_metadata[table_name] = {
                    "total_rows": total_rows,
                    "preview_rows": preview_rows,
                    "is_truncated": is_truncated,
                    "columns": list(table_rows.columns),
                    "dtypes": {col: str(dtype) for col, dtype in table_rows.dtypes.items()},
                    "query_id": query_id,
                    "table_name": table_name,
                    "download_available": is_truncated,
                    "download_message": (
                        f"Showing {preview_rows:,} of {total_rows:,} rows. Click to download full data."
                        if is_truncated
                        else None
                    ),
                }
        except Exception as e:
            logger.warning(
                f"Failed to extract source_data_metadata for {table_name}: {e}"
            )
    return source_data_metadata


def extract_all_partial_data_fields(
    state: Dict[str, Any],
    include_chart_plan: bool = False,
    include_operation_plan: bool = False,
    max_rows: int = 50,
) -> Dict[str, Any]:
    """
    Extract all fields needed for partial updates from state.
    Ensures partial updates contain the same data as the full response.

    Args:
        state: Full state dictionary (use last_state, not node_output)
        include_chart_plan: Whether to include chart_plan (for chart partial updates)
        include_operation_plan: Whether to include operation_plan (for metrics partial updates)
        max_rows: Max rows per table for source_data/sample_data preview

    Returns:
        Dictionary with all fields for partial updates
    """
    plan = state.get("plan")
    generated_queries = state.get("generated_queries")
    selected_tables = state.get("selected_tables")
    query_id = state.get("query_id", "")
    raw_dataframes = state.get("raw_dataframes", {})

    chart_plan = state.get("chart_plan") if include_chart_plan else None
    operation_plan = state.get("operation_plan") if include_operation_plan else None

    table_count = len(raw_dataframes) if isinstance(raw_dataframes, dict) else 0
    logger.debug(
        "[extract_all_partial_data_fields] raw_dataframes available: %s, count: %s",
        bool(raw_dataframes),
        table_count,
    )

    source_data = {}
    source_data_metadata = {}
    if raw_dataframes:
        try:
            source_data = extract_source_data_preview(raw_dataframes, max_rows=max_rows)
            source_data_metadata = extract_source_data_metadata(
                raw_dataframes, query_id=query_id, max_rows=max_rows
            )
            if source_data:
                logger.debug(
                    "[extract_all_partial_data_fields] Extracted source_data: %s tables",
                    len(source_data),
                )
            if source_data_metadata:
                logger.debug(
                    "[extract_all_partial_data_fields] Extracted source_data_metadata: %s tables",
                    len(source_data_metadata),
                )
        except Exception as e:
            logger.warning(
                "[extract_all_partial_data_fields] Failed to extract source_data: %s",
                e,
            )
    else:
        logger.warning(
            "[extract_all_partial_data_fields] No raw_dataframes in state - cannot extract source_data"
        )

    date_grouping = None
    if raw_dataframes:
        try:
            date_grouping = _detect_date_columns_and_grouping_options(raw_dataframes)
        except Exception as e:
            logger.warning("Failed to extract date_grouping: %s", e)

    additional_fields = {}
    if plan:
        additional_fields["plan"] = plan
        additional_fields["sql_plan"] = plan
    if generated_queries:
        additional_fields["generated_queries"] = generated_queries
        additional_fields["generated_sql"] = generated_queries
    if selected_tables:
        additional_fields["selected_tables"] = selected_tables
    if include_chart_plan and chart_plan:
        additional_fields["chart_plan"] = chart_plan
    if include_operation_plan and operation_plan:
        additional_fields["operation_plan"] = operation_plan
    if source_data is not None:
        additional_fields["source_data"] = source_data
    if source_data_metadata is not None:
        additional_fields["source_data_metadata"] = source_data_metadata
    if date_grouping:
        additional_fields["date_grouping"] = date_grouping

    additional_fields.setdefault("source_data", {})
    additional_fields.setdefault("source_data_metadata", {})
    additional_fields.setdefault("sql_plan", {})
    additional_fields.setdefault("generated_sql", "")
    additional_fields.setdefault("selected_tables", [])
    if query_id:
        additional_fields.setdefault("botMessageId", query_id)

    if additional_fields.get("source_data"):
        logger.info(
            "[extract_all_partial_data_fields] Including source_data in partial update: %s tables",
            len(additional_fields["source_data"]),
        )
    if additional_fields.get("source_data_metadata"):
        logger.info(
            "[extract_all_partial_data_fields] Including source_data_metadata in partial update: %s tables",
            len(additional_fields["source_data_metadata"]),
        )

    return additional_fields
