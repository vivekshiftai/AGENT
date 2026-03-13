"""Computation engine node - Polars-first implementation.

This module provides high-performance aggregation using Polars LazyFrames.
Key design principles:
- Use LazyFrame for ALL processing until final result
- Single-scan aggregation patterns
- No pandas dependencies in core logic
- Clean, composable functions
"""
from typing import Dict, Any, List, Optional, Tuple, Union
from datetime import datetime
import logging
import re

import polars as pl
import numpy as np

from ..state import AnalyticsState
from ..data_models import pandas_to_polars
from ..polars_engine import (
    execute_aggregations_polars,
    build_calculation_chain,
    convert_result_dict,
    to_json_serializable,
)
from ..utils import extract_date_filters_from_state

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers migrated from chart_preparation.py
# ---------------------------------------------------------------------------

def _get_current_year_ytd_date_range() -> Dict[str, Any]:
    """Return default date range: current year YTD (Jan 1 to today).
    Used when the user did not specify a date range for charts/metrics.
    """
    today = datetime.now().date()
    start = today.replace(month=1, day=1)
    return {
        "start_date": start.isoformat(),
        "end_date": today.isoformat(),
    }


def _resolve_date_column_for_table(
    table_name: str,
    date_range: Optional[Dict[str, Any]],
    available_columns_by_table: Dict[str, List[str]],
    available_date_ranges: Optional[Dict[str, Dict[str, Any]]] = None,
    sap_date_columns_by_view: Optional[Dict[str, List[str]]] = None,
) -> Optional[str]:
    """Resolve which date column to use for a table when injecting default date filters.
    Returns the column name (no table prefix), or None if none found.
    """
    if not date_range or not table_name:
        return None
    columns = available_columns_by_table.get(table_name, [])
    if not columns:
        for key, cols in available_columns_by_table.items():
            if key == table_name or (isinstance(key, str) and key.startswith(table_name + "__")):
                columns = cols
                break
    if not columns:
        return None
    columns_lower = {c.lower(): c for c in columns}
    dr_col = date_range.get("date_column") or ""
    if dr_col:
        if dr_col in columns:
            return dr_col
        if dr_col.lower() in columns_lower:
            return columns_lower[dr_col.lower()]
        if "." in dr_col:
            _, col_only = dr_col.split(".", 1)
            if col_only in columns or col_only.lower() in columns_lower:
                return col_only if col_only in columns else columns_lower[col_only.lower()]
    sap = sap_date_columns_by_view or {}
    date_cols_sap = sap.get(table_name) if isinstance(sap.get(table_name), list) else []
    for col in date_cols_sap:
        if col in columns or (col.lower() in columns_lower):
            return col if col in columns else columns_lower[col.lower()]
    adr = available_date_ranges or {}
    tbl_info = adr.get(table_name)
    date_cols_adr = (tbl_info.get("date_columns", []) if isinstance(tbl_info, dict) else []) if tbl_info else []
    for col in date_cols_adr if isinstance(date_cols_adr, list) else []:
        if col in columns or (col.lower() in columns_lower):
            return col if col in columns else columns_lower[col.lower()]
    for c in columns:
        if any(kw in c.lower() for kw in ["date", "day", "calday", "created", "posted", "time", "_0cal"]):
            return c
    return None


def _normalize_past_date_filters_to_current_ytd(
    filter_list: List[Dict[str, Any]],
    default_date_range: Dict[str, Any],
    table_name: str,
    node_name: str,
) -> List[Dict[str, Any]]:
    """If filter_list contains a single two-sided date range whose end date is in the past,
    replace it with current year YTD so metrics don't return no rows when data is current year.
    """
    if not filter_list or len(filter_list) < 2:
        return filter_list
    today = datetime.now().date()
    date_like = []
    for f in filter_list:
        if not isinstance(f, dict):
            continue
        op = (f.get("operator") or "").strip().lower()
        val = f.get("value")
        if op in (">=", "ge", "gte") and val and isinstance(val, str) and len(val) >= 10:
            date_like.append(("start", f))
        elif op in ("<", "<=", "lt", "lte") and val and isinstance(val, str) and len(val) >= 10:
            try:
                end_d = datetime.strptime(val[:10], "%Y-%m-%d").date()
                date_like.append(("end", f, end_d))
            except ValueError:
                pass
    if len(date_like) != 2:
        return filter_list
    kind0, kind1 = date_like[0][0], date_like[1][0]
    if kind0 == kind1:
        return filter_list
    end_entry = date_like[0] if date_like[0][0] == "end" else date_like[1]
    end_date_val = end_entry[2]
    if end_date_val >= today:
        return filter_list
    ytd = _get_current_year_ytd_date_range()
    new_start = ytd.get("start_date") or ""
    new_end = ytd.get("end_date") or ""
    if not new_start or not new_end:
        return filter_list
    out = []
    for f in filter_list:
        if not isinstance(f, dict):
            out.append(f)
            continue
        op = (f.get("operator") or "").strip().lower()
        if op in (">=", "ge", "gte"):
            out.append({**f, "value": new_start})
        elif op in ("<", "<=", "lt", "lte"):
            out.append({**f, "value": new_end})
        else:
            out.append(f)
    logger.info(
        f"[{node_name}] Normalized past date range to current YTD for aggregation (table={table_name}): "
        f"{new_start} to {new_end}"
    )
    return out


def _clean_agg_filters_and_inject_default_dates(
    agg_spec: Dict[str, Any],
    table_name: str,
    default_date_range: Optional[Dict[str, Any]],
    available_columns_by_table: Dict[str, List[str]],
    available_date_ranges: Optional[Dict[str, Any]] = None,
    sap_date_columns_by_view: Optional[Dict[str, Any]] = None,
    node_name: str = "chart_preparation",
    date_column_non_null_counts: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    """Clean aggregation filters (remove Fiscal_Period, Calendar_Year; drop filters for missing columns).
    Use only filters provided by the LLM; do not inject any default date or fiscal filter.
    Returns a new spec dict (does not mutate agg_spec).
    """
    spec = dict(agg_spec)
    agg_filter = spec.get("filter", [])
    if not isinstance(agg_filter, list):
        return spec
    table_columns = available_columns_by_table.get(table_name, [])
    if not table_columns:
        for key, cols in available_columns_by_table.items():
            if key == table_name or (isinstance(key, str) and key.startswith(table_name + "__")):
                table_columns = cols
                break
    table_columns_lower = {c.lower(): c for c in table_columns} if table_columns else {}

    cleaned = []
    for f in agg_filter:
        if not isinstance(f, dict):
            cleaned.append(f)
            continue
        field = (f.get("field") or "").split(".", 1)[-1]
        if field and field.lower() in ("fiscal_period", "calendar_year"):
            continue
        if field and table_columns_lower and field.lower() not in table_columns_lower:
            continue
        cleaned.append(f)

    if cleaned and default_date_range:
        cleaned = _normalize_past_date_filters_to_current_ytd(
            cleaned, default_date_range, table_name, node_name
        )
    spec["filter"] = cleaned if cleaned else None
    return spec


def _build_operation_plan_from_charts(state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Build a minimal operation_plan from recommended_charts when no dedicated plan node ran.
    Merges aggregations from all charts into one plan so computation_engine can run.
    """
    recommended_charts = state.get("recommended_charts") or []
    if not recommended_charts:
        return None
    aggregations = {}
    for chart in recommended_charts:
        if not isinstance(chart, dict):
            continue
        aggs = chart.get("aggregations") or {}
        for k, v in aggs.items():
            if k not in aggregations and isinstance(v, dict) and (v.get("column") or v.get("group_by")):
                aggregations[k] = dict(v)
    if not aggregations:
        return None
    return {"aggregations": aggregations, "derived": {}, "steps": []}


def _get_date_column_non_null_counts(
    lazyframes: Dict[str, pl.LazyFrame],
    default_date_range: Optional[Dict[str, Any]],
    available_columns_by_table: Dict[str, List[str]],
    available_date_ranges: Optional[Dict[str, Any]],
    sap_date_columns_by_view: Optional[Dict[str, Any]],
    node_name: str,
) -> Dict[str, int]:
    """For each table, resolve the date column and count non-null values.
    Used to skip default date filter when the column is missing or entirely null (avoids removing all rows).
    Returns dict keyed by 'table_name|date_col' -> non_null_count.
    """
    out = {}
    if not default_date_range or not lazyframes:
        return out
    for table_name, lf in lazyframes.items():
        try:
            date_col = _resolve_date_column_for_table(
                table_name,
                default_date_range,
                available_columns_by_table,
                available_date_ranges,
                sap_date_columns_by_view,
            )
            if not date_col:
                continue
            schema_names = lf.collect_schema().names()
            if date_col not in schema_names:
                # Try case-insensitive
                col_match = next((c for c in schema_names if c.lower() == date_col.lower()), None)
                if col_match is None:
                    logger.info(
                        f"[{node_name}] Date filter validation: table={table_name}, column={date_col} — "
                        f"column not in frame (available: {len(schema_names)} cols), skipping date filter"
                    )
                    out[f"{table_name}|{date_col}"] = 0
                    continue
                date_col = col_match
            try:
                non_null = lf.filter(pl.col(date_col).is_not_null()).select(pl.len()).collect().item()
            except Exception as e:
                logger.warning(f"[{node_name}] Could not get non-null count for '{table_name}.{date_col}': {e}")
                continue
            key = f"{table_name}|{date_col}"
            out[key] = int(non_null) if non_null is not None else 0
            logger.info(
                f"[{node_name}] Date filter validation: table={table_name}, column={date_col}, "
                f"non_null_count={out[key]}"
            )
        except Exception as e:
            logger.warning(f"[{node_name}] Date column validation failed for table '{table_name}': {e}")
    return out


def _convert_state_dataframes_to_lazyframes(
    raw_dataframes: Dict[str, Any],
) -> Dict[str, pl.LazyFrame]:
    """Convert state DataFrames to Polars LazyFrames.
    
    State can contain:
    - Polars DataFrames (from new fetch_data)
    - Pandas DataFrames (legacy compatibility)
    - Lists of dicts (very old legacy)
    - Batch frames (e.g., view_batch1, view_batch2) from sap_data_fetch
    
    For batch frames, creates lookup entries for base view name to enable
    chart aggregations to find the appropriate batch frame(s).
    
    Returns:
        Dictionary of table_name -> LazyFrame
    """
    lazyframes = {}
    batch_frames = {}  # Track batch frames by base view name
    
    for name, data in raw_dataframes.items():
        try:
            # Skip None or empty data
            if data is None:
                logger.warning(f"Skipping table '{name}': data is None")
                continue
            
            if isinstance(data, pl.LazyFrame):
                lazyframes[name] = data
            elif isinstance(data, pl.DataFrame):
                lazyframes[name] = data.lazy()
                # Log column info for debugging
                schema = data.schema
                logger.info(f"[_convert_state_dataframes_to_lazyframes] Table '{name}': Polars DataFrame with {len(data)} rows, columns: {list(schema.keys())[:10]}")
            elif hasattr(data, 'to_dict'):
                # Pandas DataFrame
                import pandas as pd
                if isinstance(data, pd.DataFrame):
                    # Convert pandas to polars using pyarrow for safe type preservation
                    if not data.empty:
                        logger.info(f"[_convert_state_dataframes_to_lazyframes] Converting pandas DataFrame for table '{name}': {len(data)} rows, columns: {list(data.columns)[:10]}")
                        df_polars = pandas_to_polars(data)
                        lazyframes[name] = df_polars.lazy()
                        # Log converted column info
                        schema = df_polars.schema
                        logger.info(f"[_convert_state_dataframes_to_lazyframes] Converted table '{name}': Polars DataFrame with {len(df_polars)} rows, columns: {list(schema.keys())[:10]}")
                    else:
                        logger.warning(f"Skipping table '{name}': pandas DataFrame is empty")
                else:
                    # Unknown type with to_dict - try conversion
                    try:
                        records = data.to_dict('records')
                        if records:
                            lazyframes[name] = pl.DataFrame(records).lazy()
                        else:
                            logger.warning(f"Skipping table '{name}': no records to convert")
                    except Exception as conv_e:
                        logger.warning(f"Skipping table '{name}': failed to convert to records: {conv_e}")
            elif isinstance(data, list):
                # List of dicts
                if data and len(data) > 0:
                    if isinstance(data[0], dict):
                        lazyframes[name] = pl.DataFrame(data).lazy()
                    else:
                        logger.warning(f"Skipping table '{name}': list items are not dicts, got {type(data[0])}")
                else:
                    logger.warning(f"Skipping table '{name}': empty list")
            else:
                logger.warning(f"Skipping table '{name}': unknown type {type(data)}")
        except Exception as e:
            logger.error(f"Failed to convert table '{name}': {e}", exc_info=True)
            continue
        
        # Detect batch frames (e.g., view_batch1, view_batch2)
        if "_batch" in name:
            base_name = name.rsplit("_batch", 1)[0]
            if base_name not in batch_frames:
                batch_frames[base_name] = []
            batch_frames[base_name].append(name)
    
    # Create aliases for batch frames: map base view name to first batch
    # This allows charts to reference the base view name and find the appropriate batch
    # IMPORTANT: Batch frames have different row counts, so they represent different data sets.
    # When aggregating on separate batches, each batch is processed independently to avoid
    # duplicating or multiplying values. Results from different batches should be UNIONed, not summed.
    for base_name, batch_names in batch_frames.items():
        if base_name not in lazyframes and batch_names:
            # Use first batch as default alias (charts should use find_frame_by_columns for column-based selection)
            first_batch = batch_names[0]
            lazyframes[base_name] = lazyframes[first_batch]
            logger.info(
                f"[_convert_state_dataframes_to_lazyframes] Created alias '{base_name}' -> '{first_batch}' "
                f"(from {len(batch_names)} batch frames: {', '.join(batch_names)}). "
                f"Charts referencing '{base_name}' will use '{first_batch}'. "
                f"For column-based selection, use find_frame_by_columns() or reference specific batch names."
            )
            logger.info(
                f"[_convert_state_dataframes_to_lazyframes] ⚠️ Note: Batch frames have different row counts. "
                f"Aggregations on separate batches are processed independently to prevent value duplication."
            )
    
    return lazyframes


def _build_aggregation_details(
    metric_key: str,
    aggregations: Dict[str, Any],
    derived: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Build aggregation metadata for a metric."""
    if metric_key in aggregations:
        agg_spec = aggregations[metric_key]
        column = agg_spec.get("column", "")
        agg_func = (agg_spec.get("agg") or "sum").upper()
        group_by = agg_spec.get("group_by", "")
        
        # Extract table name from column
        table_name = None
        column_name = column
        if column and "." in column:
            parts = column.split(".", 1)
            table_name = parts[0]
            column_name = parts[1]
        
        formula = f"{agg_func}({column})"
        if group_by:
            formula += f" GROUP BY {group_by}"
        
        return {
            "type": "aggregation",
            "table": table_name,
            "column": column_name,
            "full_column": column,
            "operation": agg_func,
            "group_by": group_by or None,
            "formula": formula,
            "description": f"{agg_func} on {column}" + (f" grouped by {group_by}" if group_by else ""),
        }
    
    elif metric_key in derived:
        formula_raw = derived[metric_key]
        
        # Handle different formula formats: string, dict with 'formula' key, or other
        if isinstance(formula_raw, str):
            formula = formula_raw
        elif isinstance(formula_raw, dict):
            # If it's a dict, try to extract the formula
            formula = formula_raw.get("formula", "")
            if not formula:
                # Try other common keys
                formula = formula_raw.get("expression", formula_raw.get("value", ""))
        else:
            # Convert to string as fallback
            formula = str(formula_raw) if formula_raw is not None else ""
        
        return {
            "type": "derived",
            "table": None,
            "column": None,
            "full_column": None,
            "operation": "DERIVED",
            "group_by": None,
            "formula": formula,
            "description": f"Derived metric: {formula}",
        }
    
    return None


def _resolve_tables_used(
    metric_key: str,
    aggregations: Dict[str, Any],
    derived: Dict[str, Any],
    available_tables: List[str],
) -> List[str]:
    """Resolve which tables a metric uses."""
    tables_used = []
    
    if metric_key in aggregations:
        agg_spec = aggregations[metric_key]
        column = agg_spec.get("column", "")
        if column and "." in column:
            table_name = column.split(".", 1)[0]
            # Case-insensitive lookup
            table_lower = table_name.lower()
            for avail in available_tables:
                if avail.lower() == table_lower:
                    tables_used.append(avail)
                    break
    
    elif metric_key in derived:
        # Extract metric references from formula
        formula_raw = derived[metric_key]
        
        # Handle different formula formats: string, dict with 'formula' key, or other
        if isinstance(formula_raw, str):
            formula = formula_raw
        elif isinstance(formula_raw, dict):
            # If it's a dict, try to extract the formula
            formula = formula_raw.get("formula", "")
            if not formula:
                # Try other common keys
                formula = formula_raw.get("expression", formula_raw.get("value", ""))
        else:
            # Convert to string as fallback
            formula = str(formula_raw) if formula_raw is not None else ""
        
        # Only process if we have a valid string formula
        if formula and isinstance(formula, str):
            try:
                metric_refs = re.findall(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', formula)
                
                for ref in metric_refs:
                    if ref in aggregations:
                        ref_tables = _resolve_tables_used(ref, aggregations, {}, available_tables)
                        tables_used.extend(ref_tables)
                
                tables_used = list(set(tables_used))
            except (TypeError, AttributeError) as e:
                logger.warning(f"[computation_engine] Failed to parse formula for metric '{metric_key}': {formula_raw} (type: {type(formula_raw)}). Error: {e}")
        else:
            logger.warning(f"[computation_engine] Invalid formula type for metric '{metric_key}': {type(formula_raw)}. Expected string or dict with 'formula' key.")
    
    # Fallback: if no specific tables found, use all available
    if not tables_used and available_tables:
        return available_tables
    
    return tables_used


async def computation_engine_node(state: AnalyticsState) -> Dict[str, Any]:
    """Execute operation plan using Polars LazyFrames.
    
    This node:
    1. Converts state DataFrames to Polars LazyFrames
    2. Executes aggregations using lazy evaluation
    3. Builds metric metadata with calculation chains
    4. Returns results for downstream nodes
    
    CRITICAL: This node is triggered when data is ready (db_execution or sap_data_fetch).
    It uses operation_plan from state; when absent, builds one from recommended_charts (chart plan).
    
    Args:
        state: Current analytics state with:
            - operation_plan: Aggregation specifications
            - raw_dataframes: Data from fetch_data or sap_data_fetch
            
    Returns:
        Updated state with:
            - computation_execution_log: Execution log
            - computation_metrics: Performance metrics
            - computation_results: Metric results with metadata
    """
    start_time = datetime.now()
    node_name = "computation_engine"
    
    # Use registry for atomic duplicate detection (prevents race conditions in parallel execution)
    from ..node_timing_registry import get_node_timing_registry
    registry = get_node_timing_registry()
    
    # Log registry status for debugging
    if registry:
        is_completed = registry.is_node_completed(node_name)
        is_in_progress = registry.is_node_in_progress(node_name)
        logger.info(f"[{node_name}] 🔍 Registry check: completed={is_completed}, in_progress={is_in_progress}")
    else:
        logger.warning(f"[{node_name}] ⚠️ Registry not available - duplicate detection may not work")
    
    # Atomically check if we can start and mark as started
    # This prevents duplicate execution when LangGraph invokes the node multiple times
    if registry:
        can_start = registry.try_start_node(node_name, start_time)
        logger.info(f"[{node_name}] 🔍 try_start_node returned: {can_start}")
        if not can_start:
            # Node already completed or in progress - skip execution but notify frontend
            logger.info(f"[{node_name}] ⏭️ Skipping duplicate invocation (already completed or in progress)")
            
            # Send simple notification to frontend that we're skipping duplicate
            # This is a WebSocket update, so we notify but don't execute
            ws_manager = state.get("ws_manager")
            if ws_manager:
                try:
                    import json
                    from ...websocket.connection_manager import MessageType
                    # Note: datetime is already imported at module level (line 20)
                    
                    # Simple progress message to notify frontend
                    skip_notification = {
                        "type": MessageType.PROGRESS,
                        "data": {
                            "node": node_name,
                            "status": "skipped",
                            "message": "Computation already completed - skipping duplicate execution",
                        },
                        "timestamp": datetime.now().isoformat()
                    }
                    
                    # Use the connection manager's prepare method
                    prepared = ws_manager._prepare_for_json(skip_notification)
                    await ws_manager.websocket.send_text(json.dumps(prepared))
                    logger.info(f"[{node_name}] ✅ Sent skip notification to frontend (WebSocket update)")
                except Exception as e:
                    logger.warning(f"[{node_name}] Failed to send skip notification to frontend: {e}")
            
            # Return existing values if available, otherwise empty dict
            existing_results = state.get("computation_results") or []
            existing_metrics = state.get("computation_metrics") or {}
            existing_log = state.get("computation_execution_log") or []
            
            if existing_results or existing_metrics or existing_log:
                return {
                    "computation_results": existing_results,
                    "computation_metrics": existing_metrics,
                    "computation_execution_log": existing_log
                }
            return {}
    
    logger.info(f"[{node_name}] ========== Starting Computation Engine ==========")
    logger.info(f"[{node_name}] 🔄 This node is triggered by data fetch; uses operation_plan from state")
    
    # Skip if errors
    if state.get("errors"):
        logger.warning(f"[{node_name}] Errors detected - skipping")
        return {}
    
    operation_plan = state.get("operation_plan", {})
    raw_dataframes = state.get("raw_dataframes", {})
    has_plan = bool(operation_plan) and isinstance(operation_plan, dict) and len(operation_plan.get("aggregations", {})) > 0
    if not has_plan and raw_dataframes:
        built = _build_operation_plan_from_charts(state)
        if built:
            operation_plan = built
            has_plan = True
            logger.info(f"[{node_name}] Built operation_plan from recommended_charts ({len(operation_plan.get('aggregations', {}))} aggregations)")
    has_data = bool(raw_dataframes)
    agg_count = len(operation_plan.get("aggregations", {})) if isinstance(operation_plan, dict) else 0
    table_count = len(raw_dataframes) if raw_dataframes else 0
    logger.info(f"[{node_name}] ✅ Synchronization check: plan={has_plan} ({agg_count} aggregations), data={has_data} ({table_count} tables)")
    if not has_data:
        logger.error(f"[{node_name}] ❌ Data fetch did not complete before computation_engine!")
    if not operation_plan or not isinstance(operation_plan, dict) or not operation_plan.get("aggregations"):
        logger.warning(f"[{node_name}] No operation_plan and could not build from charts - skipping computation")
        return {
            "computation_execution_log": [],
            "computation_metrics": {},
            "computation_results": [],
        }
    
    # Check for no_data_available flag
    if state.get("no_data_available"):
        logger.warning(f"[{node_name}] ⚠️ No data available flag set - skipping computation")
        return {
            "computation_execution_log": [],
            "computation_metrics": {},
            "computation_results": [],
            "no_data_available": True,
        }
    
    if not raw_dataframes:
        logger.warning(f"[{node_name}] No DataFrames available")
        return {
            "computation_execution_log": [],
            "computation_metrics": {},
            "computation_results": [],
            "no_data_available": True,
        }
    
    # Convert to LazyFrames
    lazyframes = _convert_state_dataframes_to_lazyframes(raw_dataframes)
    
    if not lazyframes:
        logger.warning(f"[{node_name}] No LazyFrames after conversion - no data available")
        return {
            "computation_execution_log": [],
            "computation_metrics": {},
            "computation_results": [],
            "no_data_available": True,
        }
    
    # Log info
    total_rows = 0
    for name, lf in lazyframes.items():
        # Get row count efficiently
        try:
            row_count = lf.select(pl.len()).collect().item()
            total_rows += row_count
            schema = lf.collect_schema()
            logger.info(f"[{node_name}] Table '{name}': {row_count:,} rows, {len(schema)} columns")
        except Exception as e:
            logger.warning(f"[{node_name}] Could not get info for '{name}': {e}")
    
    # Max rows per table for drill-down table_data in operational_plan (frontend prefers this over global source_data)
    _DRILL_DOWN_PREVIEW_ROWS = 100

    # Build available columns from actual frames (only use columns we have)
    available_columns_by_table = {}
    for table_name, df in raw_dataframes.items():
        try:
            if hasattr(df, "collect_schema"):
                available_columns_by_table[table_name] = df.collect_schema().names()
            elif hasattr(df, "columns"):
                available_columns_by_table[table_name] = list(df.columns)
        except Exception as e:
            logger.warning(f"[{node_name}] Could not get columns from '{table_name}': {e}")

    # Default date range: user/plan range if set, else current year YTD (for metrics without filters)
    date_filter_info = state.get("applied_date_filters") or extract_date_filters_from_state(state)
    default_date_range = None
    if date_filter_info and date_filter_info.get("filter_applied"):
        dr = date_filter_info.get("date_range") or {}
        if dr.get("start_date") and dr.get("end_date"):
            default_date_range = dr
    # If extracted range end_date is in the past, data may be current year — use current year YTD to avoid empty results
    today = datetime.now().date()
    if default_date_range:
        try:
            end_str = default_date_range.get("end_date", "") or ""
            if end_str:
                end_date = datetime.strptime(end_str[:10], "%Y-%m-%d").date()
                if end_date < today:
                    default_date_range = _get_current_year_ytd_date_range()
                    logger.info(
                        f"[{node_name}] Extracted date range end ({end_str}) is in the past; using current year YTD for metric filters: "
                        f"{default_date_range.get('start_date')} to {default_date_range.get('end_date')}"
                    )
        except Exception as e:
            logger.debug(f"[{node_name}] Could not parse end_date for staleness check: {e}")
    if default_date_range is None:
        default_date_range = _get_current_year_ytd_date_range()
        logger.info(
            f"[{node_name}] No user date range; using current year YTD for metric filters: "
            f"{default_date_range.get('start_date')} to {default_date_range.get('end_date')}"
        )
    available_date_ranges = state.get("available_date_ranges") or {}
    sap_date_columns_by_view = state.get("sap_date_columns_by_view") or {}

    # Build non-null counts for date columns so we skip default date filter when column is empty (avoids removing all rows)
    date_column_non_null_counts = _get_date_column_non_null_counts(
        lazyframes,
        default_date_range,
        available_columns_by_table,
        available_date_ranges,
        sap_date_columns_by_view,
        node_name,
    )

    # Clean aggregation filters and inject default date where missing (only columns from available frames)
    aggregations_raw = operation_plan.get("aggregations", {}) or {}
    cleaned_aggregations = {}
    for agg_key, agg_spec in aggregations_raw.items():
        if not isinstance(agg_spec, dict):
            cleaned_aggregations[agg_key] = agg_spec
            continue
        table_name = (agg_spec.get("column") or "").strip().split(".", 1)[0]
        cleaned_spec = _clean_agg_filters_and_inject_default_dates(
            agg_spec,
            table_name,
            default_date_range,
            available_columns_by_table,
            available_date_ranges,
            sap_date_columns_by_view,
            node_name=node_name,
            date_column_non_null_counts=date_column_non_null_counts,
        )
        cleaned_aggregations[agg_key] = cleaned_spec
    operation_plan = {**operation_plan, "aggregations": cleaned_aggregations}

    try:
        # Execute aggregations using Polars engine
        results, errors = execute_aggregations_polars(operation_plan, lazyframes)
        
        # If date filters may have removed all rows: when all results are empty/failed, retry without date filter
        def _all_results_empty_or_failed(res: Dict[str, Any], errs: Dict[str, str]) -> bool:
            if not res:
                return True
            for v in res.values():
                if v is None:
                    continue
                if isinstance(v, (int, float)):
                    if v != v or v == 0 or v == 0.0:  # NaN or zero
                        continue
                return False  # at least one non-empty value
            return True
        if default_date_range and _all_results_empty_or_failed(results, errors):
            logger.warning(
                f"[{node_name}] All aggregations returned empty or failed; date filter may have removed all rows. "
                f"Errors: {list(errors.values())[:3]}. Retrying without date filter."
            )
            cleaned_no_date = {}
            for agg_key, agg_spec in aggregations_raw.items():
                if not isinstance(agg_spec, dict):
                    cleaned_no_date[agg_key] = agg_spec
                    continue
                table_name = (agg_spec.get("column") or "").strip().split(".", 1)[0]
                cleaned_spec = _clean_agg_filters_and_inject_default_dates(
                    agg_spec,
                    table_name,
                    None,  # no default date range — do not inject date filter
                    available_columns_by_table,
                    available_date_ranges,
                    sap_date_columns_by_view,
                    node_name=node_name,
                    date_column_non_null_counts=None,
                )
                cleaned_no_date[agg_key] = cleaned_spec
            operation_plan_no_date = {**operation_plan, "aggregations": cleaned_no_date}
            results_retry, errors_retry = execute_aggregations_polars(operation_plan_no_date, lazyframes)
            if not _all_results_empty_or_failed(results_retry, errors_retry):
                results, errors = results_retry, errors_retry
                logger.info(f"[{node_name}] Retry without date filter produced results; using retry results")
            else:
                logger.warning(f"[{node_name}] Retry without date filter still produced no results; keeping original results")
        
        # Build table_data preview once per table for drill-down (per-metric operational_plan.table_data)
        table_data_previews: Dict[str, List[Dict[str, Any]]] = {}
        for table_name, lf in lazyframes.items():
            try:
                preview_df = lf.head(_DRILL_DOWN_PREVIEW_ROWS).collect()
                table_data_previews[table_name] = preview_df.to_dicts()
            except Exception as e:
                logger.warning(f"[{node_name}] Could not build table_data preview for '{table_name}': {e}")
        if table_data_previews:
            logger.info(f"[{node_name}] Built table_data preview for {len(table_data_previews)} table(s) (drill-down in operational_plan)")
        
        # Build computation results with metadata
        computation_results = []
        aggregations = operation_plan.get("aggregations", {})
        derived = operation_plan.get("derived", {})
        available_tables = list(lazyframes.keys())
        
        for key, value in results.items():
            error_msg = errors.get(key, "")
            status = "completed" if value is not None else "failed"
            
            if status == "failed" and not error_msg:
                error_msg = f"Aggregation '{key}' failed with unknown error"
            
            # Build metadata
            aggregation_details = _build_aggregation_details(key, aggregations, derived)
            calculation_chain = build_calculation_chain(key, operation_plan, results)
            tables_used = _resolve_tables_used(key, aggregations, derived, available_tables)
            
            # Build operational_plan structure
            operational_plan = {
                "calculation_chain": calculation_chain,
                "tables_used": tables_used,
            }
            
            if aggregation_details:
                operational_plan["aggregations"] = {
                    key: {
                        "agg": aggregation_details.get("operation", "").lower(),
                        "column": aggregation_details.get("full_column", ""),
                        "table": aggregation_details.get("table", ""),
                        "group_by": aggregation_details.get("group_by"),
                    }
                }
            
            if key in derived:
                operational_plan["derived"] = {key: derived[key]}
            
            # Per-metric table_data for drill-down (frontend prefers this over global source_data filtered by tables_used)
            if tables_used and table_data_previews:
                operational_plan["table_data"] = {
                    t: table_data_previews[t] for t in tables_used if t in table_data_previews
                }
            
            # Extract reasoning/description for consistent metric schema (metric_id, display_name, description, value)
            reasoning = None
            display_name = None
            if key in aggregations:
                agg_spec = aggregations[key]
                if isinstance(agg_spec, dict):
                    reasoning = agg_spec.get("reasoning") or agg_spec.get("description")
                    display_name = agg_spec.get("display_name")

            column_used = None
            if aggregation_details:
                column_used = aggregation_details.get("column") or aggregation_details.get("full_column")
            computation_results.append({
                "metric": key,
                "value": value,
                "status": status,
                "error": error_msg or None,
                "tables_used": tables_used,
                "aggregation": aggregation_details,
                "calculation_chain": calculation_chain,
                "operational_plan": operational_plan,
                "reasoning": reasoning,
                "description": reasoning,
                "display_name": display_name or key.replace("_", " ").title(),
                "column": column_used,
            })
        
        # Build execution log
        execution_log = [{
            "status": "passed",
            "aggregations_executed": len(aggregations),
            "derived_executed": len(derived),
        }]
        
        # Build metrics
        duration = (datetime.now() - start_time).total_seconds()
        metrics = {
            "total_duration_seconds": duration,
            "initial_rows": int(total_rows),
            "final_rows": int(total_rows),  # Aggregations don't reduce row count
            "aggregations_executed": len(aggregations),
        }
        
        successful_count = len([r for r in computation_results if r.get("status") == "completed"])
        logger.info(
            f"[{node_name}] Completed in {duration:.2f}s: "
            f"{len(computation_results)} metrics ({successful_count} completed) | "
            f"Metric flow: computation_results written for downstream (analytical_summary, intelligence_analysis)"
        )
        
        # Send summary to frontend via WebSocket with structured data
        ws_manager = state.get("ws_manager")
        if ws_manager:
            try:
                # Count successful vs failed metrics
                successful_metrics = len([r for r in computation_results if r.get("status") == "completed"])
                failed_metrics = len([r for r in computation_results if r.get("status") == "failed"])
                
                # Build summary message
                summary_message = f"Computed {len(computation_results)} metrics in {duration:.2f}s"
                summary_details = f"{successful_metrics} successful, {failed_metrics} failed, {len(aggregations)} aggregations executed, {total_rows:,} rows processed"
                
                # Prepare structured summary data for WebSocket
                summary_data = {
                    "computation_summary": {
                        "total_metrics": len(computation_results),
                        "successful_metrics": successful_metrics,
                        "failed_metrics": failed_metrics,
                        "aggregations_executed": len(aggregations),
                        "derived_metrics": len(derived),
                        "initial_rows": int(total_rows),
                        "tables_processed": len(lazyframes),
                        "duration_seconds": round(duration, 2)
                    }
                }
                
                await ws_manager.send_progress(
                    node_name=node_name,
                    message=summary_message,
                    status="complete",
                    details=summary_details,
                    data=summary_data
                )
                logger.info(f"[{node_name}] ✅ Sent computation summary to frontend with structured data")
            except Exception as e:
                logger.warning(f"[{node_name}] ⚠️ Failed to send summary to frontend: {e}")
        
        # Record node completion in registry
        if registry:
            registry.record_node_completion(node_name)
        
        return {
            "computation_execution_log": execution_log,
            "computation_metrics": metrics,
            "computation_results": computation_results,
        }
        
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"[{node_name}] Failed after {duration:.2f}s: {e}", exc_info=True)
        
        # Record node completion even on error (to prevent retries)
        if registry:
            registry.record_node_completion(node_name)
        
        return {
            "computation_execution_log": [],
            "computation_metrics": {
                "error": str(e),
                "total_duration_seconds": duration,
            },
            "computation_results": [],
            "errors": [f"Computation failed: {str(e)}"],
        }


# =============================================================================
# LEGACY COMPATIBILITY: execute_aggregations function
# =============================================================================

def execute_aggregations(
    plan: Dict[str, Any],
    raw_dataframes: Dict[str, Any],
    copy_dataframes: bool = False,
) -> Tuple[Dict[str, Any], Dict[str, str], Dict[str, Any]]:
    """Execute aggregations - Polars implementation.
    
    This function provides backward compatibility for chart_preparation.py
    which calls execute_aggregations directly.
    
    Args:
        plan: Aggregation plan with 'aggregations' and 'derived'
        raw_dataframes: Dictionary of table_name -> DataFrame
        copy_dataframes: Ignored (Polars uses lazy evaluation)
        
    Returns:
        Tuple of (results, errors, original_dataframes)
    """
    # Convert to LazyFrames
    lazyframes = _convert_state_dataframes_to_lazyframes(raw_dataframes)
    
    if not lazyframes:
        return {}, {"error": "No data available"}, raw_dataframes
    
    # Execute using Polars engine
    results, errors = execute_aggregations_polars(plan, lazyframes)
    
    # Return original dataframes (not modified - Polars is immutable)
    return results, errors, raw_dataframes
