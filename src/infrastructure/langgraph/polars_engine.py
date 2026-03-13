"""Polars-first computation engine for high-performance analytics.

This module provides:
- Lazy evaluation for all heavy processing
- Single-scan aggregation patterns
- Efficient type conversion utilities
- Chart data preparation with minimal memory footprint

Design principles:
- NEVER call .collect() until final result is needed
- Use LazyFrame for all transformations
- Compute ALL metrics in a single aggregation pass
- No pandas dependencies in core processing
"""
from typing import Dict, Any, List, Optional, Tuple, Union
from datetime import datetime, date, timedelta
import logging
import re
import ast
import operator as pyop
import math

import polars as pl
import numpy as np

from .data_models import DataResult, FetchIntent, MultiTableResult

logger = logging.getLogger(__name__)

# Analytical fetch dataset key format: view + "__by_" + dim_col
# (e.g. AM_Sales_Order_v1_Summary__by_Fiscal_Week_Fiscal_Hier_Ke). Must match analytical_fetch_plan.
ANALYTICAL_KEY_BY_PREFIX = "__by_"


# =============================================================================
# TYPE CONVERSION UTILITIES
# =============================================================================

def to_json_serializable(value: Any) -> Any:
    """Convert Polars/numpy types to JSON-serializable Python types.
    
    This is the ONLY place where type conversion should happen,
    called after final .collect() on results.
    """
    if value is None:
        return None
    
    # Handle Polars types
    if hasattr(value, 'to_list'):
        return value.to_list()
    
    # Handle numpy types
    if isinstance(value, (np.integer,)):
        return int(value)
    elif isinstance(value, (np.floating,)):
        if np.isnan(value):
            return None
        if np.isinf(value):
            # Handle infinity: convert to None or large number
            # Using None to indicate invalid/overflow value
            return None
        return float(value)
    elif isinstance(value, np.ndarray):
        return value.tolist()
    elif isinstance(value, np.bool_):
        return bool(value)
    
    # Handle datetime
    if isinstance(value, datetime):
        return value.isoformat()
    
    # Handle Python native types
    if isinstance(value, (int, str, bool)):
        return value
    elif isinstance(value, float):
        # Handle infinity and NaN for Python floats
        if value != value:  # NaN check
            return None
        if value == float('inf') or value == float('-inf'):
            return None
        return value
    
    # Fallback to string
    return str(value)


def convert_result_dict(obj: Any) -> Any:
    """Recursively convert all values in a dict/list to JSON-serializable types."""
    if isinstance(obj, dict):
        return {key: convert_result_dict(val) for key, val in obj.items()}
    elif isinstance(obj, list):
        return [convert_result_dict(item) for item in obj]
    else:
        return to_json_serializable(obj)


# =============================================================================
# EXPRESSION EVALUATION (for derived metrics)
# =============================================================================

_ALLOWED_BINOPS = {
    ast.Add: pyop.add,
    ast.Sub: pyop.sub,
    ast.Mult: pyop.mul,
    ast.Div: pyop.truediv,
    ast.Mod: pyop.mod,
    ast.Pow: pyop.pow,
    ast.FloorDiv: pyop.floordiv,
}

_ALLOWED_UNARYOPS = {
    ast.UAdd: pyop.pos,
    ast.USub: pyop.neg,
}


def safe_eval_expr(expr: str, metrics: Dict[str, Any]) -> Any:
    """Safely evaluate arithmetic expression with metric references.
    
    Supports: + - * / % ** and numeric literals.
    Example: "(total_sales - total_spends) / total_sales * 100"
    
    Args:
        expr: Arithmetic expression string
        metrics: Dictionary of metric_name -> value
        
    Returns:
        Evaluated result
        
    Raises:
        ValueError: If expression is invalid or references unknown metrics
        ZeroDivisionError: If division by zero occurs
    """
    def _eval(node):
        if isinstance(node, ast.Expression):
            return _eval(node.body)
        # Handle numeric literals (Python 3.8+ uses ast.Constant, older uses ast.Num)
        if hasattr(ast, "Constant") and isinstance(node, ast.Constant):
            if isinstance(node.value, (int, float)):
                return node.value
            raise ValueError(f"Only numeric constants allowed, got: {type(node.value)}")
        if isinstance(node, ast.Num):  # Python <3.8 compatibility
            return node.n
        if isinstance(node, ast.BinOp):
            left = _eval(node.left)
            right = _eval(node.right)
            op_type = type(node.op)
            if op_type in _ALLOWED_BINOPS:
                try:
                    # Check for division by zero BEFORE the operation
                    if op_type in (ast.Div, ast.FloorDiv, ast.Mod):
                        if right == 0 or right is None:
                            # Return None for division by zero instead of raising error
                            # This allows derived metrics to gracefully handle zero denominators
                            logger.debug(f"Division by zero in '{expr}' - returning None")
                            return None
                    
                    # Check for None operands
                    if left is None or right is None:
                        return None
                    
                    result = _ALLOWED_BINOPS[op_type](left, right)
                    return result
                except (ZeroDivisionError, OverflowError) as e:
                    # Fallback: return None for any arithmetic errors
                    logger.debug(f"Arithmetic error in '{expr}': {e} - returning None")
                    return None
            raise ValueError(f"Unsupported operator: {op_type}")
        if isinstance(node, ast.UnaryOp):
            operand = _eval(node.operand)
            op_type = type(node.op)
            if op_type in _ALLOWED_UNARYOPS:
                return _ALLOWED_UNARYOPS[op_type](operand)
            raise ValueError(f"Unsupported unary op: {op_type}")
        if isinstance(node, ast.Name):
            if node.id in metrics:
                val = metrics[node.id]
                if val is None:
                    raise ValueError(f"Metric '{node.id}' is None")
                try:
                    # Convert to float, handling various numeric types
                    if isinstance(val, (int, float)):
                        return float(val)
                    elif isinstance(val, str):
                        # Try to parse string as number
                        return float(val)
                    else:
                        raise ValueError(f"Metric '{node.id}' has non-numeric value: {type(val)}")
                except (ValueError, TypeError) as e:
                    raise ValueError(f"Cannot convert metric '{node.id}' to number: {e}")
            raise ValueError(f"Unknown metric: '{node.id}'. Available: {list(metrics.keys())}")
        if isinstance(node, ast.Call):
            raise ValueError("Function calls not allowed in derived metric expressions")
        raise ValueError(f"Unsupported AST node: {type(node).__name__}")
    
    try:
        parsed = ast.parse(expr, mode="eval")
        return _eval(parsed)
    except SyntaxError as e:
        raise ValueError(f"Invalid expression syntax '{expr}': {e}")
    except Exception as e:
        raise ValueError(f"Failed to evaluate '{expr}': {e}")


# =============================================================================
# COLUMN RESOLUTION
# =============================================================================

def resolve_column(
    column_ref: str,
    lazyframes: Dict[str, pl.LazyFrame],
) -> Tuple[str, str, pl.LazyFrame]:
    """Resolve a column reference like 'table.column' or 'column'.
    
    Args:
        column_ref: Column reference string
        lazyframes: Dictionary of table_name -> LazyFrame
        
    Returns:
        Tuple of (actual_column_name, table_name, lazyframe)
        
    Raises:
        ValueError: If column cannot be resolved
    """
    if not lazyframes:
        raise ValueError("No LazyFrames available for column resolution")
    
    # Validate lazyframes contains only LazyFrames (filter out any lists or invalid types)
    validated_lazyframes = {}
    for name, lf in lazyframes.items():
        if isinstance(lf, pl.LazyFrame):
            validated_lazyframes[name] = lf
        else:
            logger.warning(f"Skipping invalid LazyFrame for table '{name}': expected pl.LazyFrame, got {type(lf)}")
    
    if not validated_lazyframes:
        raise ValueError("No valid LazyFrames available for column resolution")
    
    # Special case: single table - ignore table prefix
    if len(validated_lazyframes) == 1:
        table_name = list(validated_lazyframes.keys())[0]
        lf = validated_lazyframes[table_name]
        
        # Extract column name (ignore table prefix if present)
        if "." in column_ref:
            _, col_name = column_ref.split(".", 1)
        else:
            col_name = column_ref
        
        # Get schema to check column exists
        schema = lf.collect_schema()
        if col_name in schema:
            return col_name, table_name, lf
        
        # Try case-insensitive match
        col_name_lower = col_name.lower().strip()
        for actual_col in schema.keys():
            if actual_col.lower().strip() == col_name_lower:
                logger.info(f"[resolve_column] Column name case mismatch: '{col_name}' -> '{actual_col}'")
                return actual_col, table_name, lf
        
        # Column not found in main table - check if there are batch frames
        # Look for batch frames: table_name_batch1, table_name_batch2, etc.
        base_name = table_name.rsplit("_batch", 1)[0] if "_batch" in table_name else table_name
        batch_frames = []
        for name, batch_lf in validated_lazyframes.items():
            if name != table_name and (name.startswith(base_name + "_batch") or name == base_name + "_batch"):
                batch_frames.append((name, batch_lf))
        
        # Check all batch frames for the column
        if batch_frames:
            logger.info(f"[resolve_column] Column '{col_name}' not found in '{table_name}', checking {len(batch_frames)} batch frame(s)")
            for batch_name, batch_lf in batch_frames:
                try:
                    batch_schema = batch_lf.collect_schema()
                    if col_name in batch_schema:
                        logger.info(f"[resolve_column] ✅ Found column '{col_name}' in batch frame '{batch_name}'")
                        return col_name, batch_name, batch_lf
                    
                    # Try case-insensitive match in batch
                    for actual_col in batch_schema.keys():
                        if actual_col.lower().strip() == col_name_lower:
                            logger.info(f"[resolve_column] ✅ Found column '{col_name}' (case mismatch -> '{actual_col}') in batch frame '{batch_name}'")
                            return actual_col, batch_name, batch_lf
                except Exception as e:
                    logger.warning(f"[resolve_column] Error checking batch frame '{batch_name}': {e}")
                    continue
        
        # Log available columns for debugging
        available_cols = list(schema.keys())
        if batch_frames:
            # Collect columns from all batch frames too
            for batch_name, batch_lf in batch_frames:
                try:
                    batch_schema = batch_lf.collect_schema()
                    available_cols.extend(batch_schema.keys())
                except Exception:
                    pass
            available_cols = sorted(list(set(available_cols)))
        
        logger.error(f"[resolve_column] Column '{col_name}' not found in table '{table_name}' or any batch frames. Available columns: {available_cols[:30]}")
        raise ValueError(f"Column '{col_name}' not found in table '{table_name}' or any batch frames. Available columns: {available_cols[:30]}")
    
    # Multiple tables
    if "." in column_ref:
        table_name, col_name = column_ref.split(".", 1)
        # Case-insensitive table lookup
        table_name_lower = table_name.lower()
        
        # First, try to find exact match
        matched_frame = None
        matched_name = None
        for name, lf in validated_lazyframes.items():
            if name.lower() == table_name_lower:
                matched_frame = lf
                matched_name = name
                break
        
        # If exact match found, check for column
        column_found_in_match = False
        # CRITICAL: Use 'is not None' instead of truthiness to avoid LazyFrame boolean context error
        if matched_frame is not None:
            schema = matched_frame.collect_schema()
            if col_name in schema:
                return col_name, matched_name, matched_frame
            
            # Try case-insensitive match
            col_name_lower = col_name.lower().strip()
            for actual_col in schema.keys():
                if actual_col.lower().strip() == col_name_lower:
                    logger.info(f"[resolve_column] Column name case mismatch: '{col_name}' -> '{actual_col}'")
                    return actual_col, matched_name, matched_frame
            # Column not found in exact match - will check batch frames below
        
        # If column not found in exact match (or no exact match), check if this is a base view name with batch frames
        # Look for batch frames: table_name_batch1, table_name_batch2, etc.
        # Also check if the matched_name itself is a batch frame and extract base name
        base_name = table_name_lower
        if matched_name and "_batch" in matched_name.lower():
            # If matched_name is a batch frame, extract base name
            base_name = matched_name.lower().rsplit("_batch", 1)[0]
            logger.debug(f"[resolve_column] Matched name '{matched_name}' is a batch frame, using base name '{base_name}'")
        
        batch_frames = []
        for name, lf in validated_lazyframes.items():
            name_lower = name.lower()
            # Skip the matched frame itself (already checked)
            if matched_name and name_lower == matched_name.lower():
                continue
            # Check if this frame is a batch frame for the base table
            if name_lower.startswith(base_name + "_batch") or name_lower == base_name + "_batch":
                batch_frames.append((name, lf))
            # Also check if it matches the original table_name pattern
            elif name_lower.startswith(table_name_lower + "_batch") or name_lower == table_name_lower + "_batch":
                batch_frames.append((name, lf))
        
        # If we found batch frames, check all of them for the column
        if batch_frames:
            logger.info(f"[resolve_column] Table '{table_name}' has {len(batch_frames)} batch frame(s), checking all for column '{col_name}'")
            for batch_name, batch_lf in batch_frames:
                try:
                    schema = batch_lf.collect_schema()
                    if col_name in schema:
                        logger.info(f"[resolve_column] ✅ Found column '{col_name}' in batch frame '{batch_name}'")
                        return col_name, batch_name, batch_lf
                    
                    # Try case-insensitive match
                    col_name_lower = col_name.lower().strip()
                    for actual_col in schema.keys():
                        if actual_col.lower().strip() == col_name_lower:
                            logger.info(f"[resolve_column] ✅ Found column '{col_name}' (case mismatch -> '{actual_col}') in batch frame '{batch_name}'")
                            return actual_col, batch_name, batch_lf
                except Exception as e:
                    logger.warning(f"[resolve_column] Error checking batch frame '{batch_name}': {e}")
                    continue
            
            # Column not found in any batch frame - collect all available columns for error message
            all_columns = set()
            for batch_name, batch_lf in batch_frames:
                try:
                    schema = batch_lf.collect_schema()
                    all_columns.update(schema.keys())
                except Exception:
                    pass
            
            available_cols = sorted(list(all_columns))
            logger.error(f"[resolve_column] Column '{col_name}' not found in table '{table_name}' or any of its {len(batch_frames)} batch frames. Available columns across all batches: {available_cols[:30]}")
            raise ValueError(f"Column '{col_name}' not found in table '{table_name}' or any of its batch frames. Available columns: {available_cols[:30]}")
        
        # No batch frames found and exact match didn't have the column
        # CRITICAL: Use 'is not None' instead of truthiness to avoid LazyFrame boolean context error
        if matched_frame is not None:
            available_cols = list(matched_frame.collect_schema().keys())
            logger.error(f"[resolve_column] Column '{col_name}' not found in table '{matched_name}'. Available columns: {available_cols[:20]}")
            raise ValueError(f"Column '{col_name}' not found in table '{matched_name}'. Available columns: {available_cols[:20]}")
        
        # Analytical fetch path: keys = view + ANALYTICAL_KEY_BY_PREFIX + dim_col (e.g. View__by_Fiscal_Week_Fiscal_Hier_Ke)
        table_name_lower = table_name.lower()
        col_name_lower = col_name.lower().strip()

        def _find_in_frame(name: str, lf: pl.LazyFrame) -> tuple:
            """Return (actual_col, name, lf) if column found, else (None, None, None)."""
            try:
                schema = lf.collect_schema()
                if col_name in schema:
                    return col_name, name, lf
                for actual_col in schema.keys():
                    if actual_col.lower().strip() == col_name_lower:
                        return actual_col, name, lf
            except Exception as e:
                logger.warning(f"[resolve_column] Error checking key '{name}' for column '{col_name}': {e}")
            return None, None, None

        # 1) Keys starting with view__by_ (dimension slice)
        by_prefix = f"{table_name}{ANALYTICAL_KEY_BY_PREFIX}"
        by_prefix_lower = by_prefix.lower()
        for name, lf in validated_lazyframes.items():
            if not name.lower().startswith(by_prefix_lower):
                continue
            actual_col, resolved_name, resolved_lf = _find_in_frame(name, lf)
            if actual_col is not None:
                logger.info(f"[resolve_column] ✅ Resolved '{table_name}' → '{name}' (analytical __by_ key) for column '{actual_col}'")
                return actual_col, resolved_name, resolved_lf

        # 2) Totals key: {view}__totals
        totals_key = f"{table_name}__totals"
        for name, lf in validated_lazyframes.items():
            if name.lower() != totals_key.lower():
                continue
            actual_col, resolved_name, resolved_lf = _find_in_frame(name, lf)
            if actual_col is not None:
                logger.info(f"[resolve_column] ✅ Resolved '{table_name}' → '{name}' (analytical __totals key) for column '{actual_col}'")
                return actual_col, resolved_name, resolved_lf

        # 3) Any key starting with {view}__
        prefix = f"{table_name}__"
        prefix_lower = prefix.lower()
        for name, lf in validated_lazyframes.items():
            if not name.lower().startswith(prefix_lower):
                continue
            actual_col, resolved_name, resolved_lf = _find_in_frame(name, lf)
            if actual_col is not None:
                logger.info(f"[resolve_column] ✅ Resolved '{table_name}' → '{name}' (analytical tagged key) for column '{actual_col}'")
                return actual_col, resolved_name, resolved_lf

        # Fallback: try keys starting with table_name + "_" (single underscore, e.g. ViewName_by_Dim)
        for name, lf in validated_lazyframes.items():
            if not name.lower().startswith(table_name_lower + "_"):
                continue
            try:
                schema = lf.collect_schema()
                if col_name in schema:
                    logger.info(f"[resolve_column] ✅ Resolved '{table_name}' → '{name}' (prefix table_) for column '{col_name}'")
                    return col_name, name, lf
                for actual_col in schema.keys():
                    if actual_col.lower().strip() == col_name_lower:
                        logger.info(f"[resolve_column] ✅ Resolved '{table_name}' → '{name}' (prefix table_), column '{col_name}' → '{actual_col}'")
                        return actual_col, name, lf
            except Exception as e:
                logger.warning(f"[resolve_column] Error checking key '{name}' for column '{col_name}': {e}")
                continue
        
        # Fallback: any key containing table_name (e.g. ViewName in ViewName__by_Fiscal_Week_Fiscal_Hier_Ke)
        for name, lf in validated_lazyframes.items():
            if table_name_lower not in name.lower():
                continue
            try:
                schema = lf.collect_schema()
                if col_name in schema:
                    logger.info(f"[resolve_column] ✅ Resolved '{table_name}' → '{name}' (table name in key) for column '{col_name}'")
                    return col_name, name, lf
                for actual_col in schema.keys():
                    if actual_col.lower().strip() == col_name_lower:
                        logger.info(f"[resolve_column] ✅ Resolved '{table_name}' → '{name}' (table name in key), column '{col_name}' → '{actual_col}'")
                        return actual_col, name, lf
            except Exception as e:
                logger.warning(f"[resolve_column] Error checking key '{name}' for column '{col_name}': {e}")
                continue
        
        available_keys = list(validated_lazyframes.keys())[:15]
        raise ValueError(
            f"Table '{table_name}' not found (no key starting with '{table_name}__' or containing table name with column '{col_name}'). "
            f"Available keys: {available_keys}"
        )
    
    # Plain column name - find first table containing it
    # Check all frames (including batch frames) for the column
    # IMPORTANT: For SAP split dataframes, columns may be in any batch frame
    # We need to check ALL frames systematically
    
    # First, check all frames (including batch frames) for exact match
    for table_name, lf in validated_lazyframes.items():
        try:
            schema = lf.collect_schema()
            if column_ref in schema:
                logger.info(f"[resolve_column] ✅ Found column '{column_ref}' in table '{table_name}'")
                return column_ref, table_name, lf
        except Exception as e:
            logger.warning(f"[resolve_column] Error checking table '{table_name}' for column '{column_ref}': {e}")
            continue
    
    # Try case-insensitive match across all tables (including batch frames)
    col_ref_lower = column_ref.lower().strip()
    for table_name, lf in validated_lazyframes.items():
        try:
            schema = lf.collect_schema()
            for actual_col in schema.keys():
                if actual_col.lower().strip() == col_ref_lower:
                    logger.info(f"[resolve_column] ✅ Found column '{column_ref}' (case mismatch -> '{actual_col}') in table '{table_name}'")
                    return actual_col, table_name, lf
        except Exception as e:
            logger.warning(f"[resolve_column] Error checking table '{table_name}' for column '{column_ref}': {e}")
            continue
    
    # If still not found, check if there are batch frames we might have missed
    # Group frames by base name (extract base name from batch frames)
    base_names = set()
    for name in validated_lazyframes.keys():
        if "_batch" in name:
            base_name = name.rsplit("_batch", 1)[0]
            base_names.add(base_name)
        else:
            base_names.add(name)
    
    # For each base name, check all its batch frames
    for base_name in base_names:
        batch_frames = []
        for name, lf in validated_lazyframes.items():
            # Check if this is a batch frame for this base name
            if name == base_name or name.startswith(f"{base_name}_batch"):
                batch_frames.append((name, lf))
        
        # Check all batch frames for this base name
        if len(batch_frames) > 1:  # Only if there are multiple frames
            logger.info(f"[resolve_column] Checking {len(batch_frames)} batch frames for base '{base_name}' for column '{column_ref}'")
            for batch_name, batch_lf in batch_frames:
                try:
                    schema = batch_lf.collect_schema()
                    if column_ref in schema:
                        logger.info(f"[resolve_column] ✅ Found column '{column_ref}' in batch frame '{batch_name}'")
                        return column_ref, batch_name, batch_lf
                    
                    # Try case-insensitive match
                    for actual_col in schema.keys():
                        if actual_col.lower().strip() == col_ref_lower:
                            logger.info(f"[resolve_column] ✅ Found column '{column_ref}' (case mismatch -> '{actual_col}') in batch frame '{batch_name}'")
                            return actual_col, batch_name, batch_lf
                except Exception as e:
                    logger.warning(f"[resolve_column] Error checking batch frame '{batch_name}': {e}")
                    continue
    
    # Collect all available columns for error message
    all_columns = set()
    for table_name, lf in validated_lazyframes.items():
        try:
            schema = lf.collect_schema()
            all_columns.update(schema.keys())
        except Exception:
            pass
    
    available_cols = sorted(list(all_columns))
    logger.error(f"[resolve_column] Column '{column_ref}' not found in any table or batch frame. Available columns: {available_cols[:50]}{'...' if len(available_cols) > 50 else ''}")
    raise ValueError(f"Column '{column_ref}' not found in any table or batch frame. Available columns: {available_cols[:50]}")


# =============================================================================
# PERIOD GROUPING (month, quarter, year)
# =============================================================================

def build_period_expression(
    group_by_spec: str,
    lazyframes: Dict[str, pl.LazyFrame],
    preferred_table: Optional[str] = None,
) -> Tuple[pl.Expr, str, str]:
    """Build Polars expression for period-based grouping.
    
    Supports:
        - day(column) -> "YYYY-MM-DD"
        - month(column) -> "YYYY-MM"
        - month_number(column) -> 1-12
        - quarter(column) -> "YYYY-Qn"
        - year(column) -> "YYYY"
        - week(column) -> "YYYY-WW"
        - plain column name
    
    Args:
        group_by_spec: Group by specification string
        lazyframes: Available LazyFrames for column resolution
        preferred_table: Optional table name to prefer when resolving plain column names
        
    Returns:
        Tuple of (polars_expression, group_column_alias, kind)
    """
    # CRITICAL: Check if group_by_spec is None or empty string, not use it directly in boolean context
    # This prevents "truth value of a LazyFrame is ambiguous" errors
    if group_by_spec is None:
        return None, None, None
    if not isinstance(group_by_spec, str):
        logger.warning(f"[build_period_expression] group_by_spec is not a string: {type(group_by_spec)}")
        return None, None, None
    if not group_by_spec.strip():
        return None, None, None
    
    spec = group_by_spec.strip()
    
    # Check for period functions (including day and week)
    match = re.match(r'^(day|week|month|month_number|quarter|year)\(([^)]+)\)$', spec, re.IGNORECASE)
    
    if match:
        kind = match.group(1).lower()
        col_ref = match.group(2).strip()
        
        # Resolve column (prefer preferred_table if provided)
        try:
            # CRITICAL: Check preferred_table is not None and is a string before using in boolean context
            if preferred_table is not None and isinstance(preferred_table, str) and preferred_table in lazyframes:
                # Try to resolve from preferred table first
                try:
                    lf_pref = lazyframes[preferred_table]
                    schema_pref = lf_pref.collect_schema()
                    # Check if column exists in preferred table
                    if "." in col_ref:
                        table_part, col_part = col_ref.split(".", 1)
                        if table_part.lower() == preferred_table.lower() and col_part in schema_pref:
                            col_name, table_name, lf = col_part, preferred_table, lf_pref
                        else:
                            col_name, table_name, lf = resolve_column(col_ref, lazyframes)
                    elif col_ref in schema_pref:
                        col_name, table_name, lf = col_ref, preferred_table, lf_pref
                    else:
                        col_name, table_name, lf = resolve_column(col_ref, lazyframes)
                except ValueError:
                    col_name, table_name, lf = resolve_column(col_ref, lazyframes)
            else:
                col_name, table_name, lf = resolve_column(col_ref, lazyframes)
        except ValueError as e:
            # Column doesn't exist - try to find a date column automatically
            target_table = preferred_table or (col_ref.split(".", 1)[0] if "." in col_ref else None)
            date_col_candidates = []
            
            # Search for date columns in the target table or all tables
            search_tables = [target_table] if target_table else list(lazyframes.keys())
            for table_name_search in search_tables:
                if table_name_search and table_name_search in lazyframes:
                    lf_search = lazyframes[table_name_search]
                    schema = lf_search.collect_schema()
                    available_cols = list(schema.keys())
                    
                    # Look for date columns
                    date_keywords = ("date", "dt", "time", "timestamp", "created", "posted", "updated", "on")
                    date_col_candidates = [
                        col for col in available_cols
                        if any(keyword in col.lower() for keyword in date_keywords)
                    ]
                    
                    if date_col_candidates:
                        # Prefer "Created On" if available
                        preferred = [col for col in date_col_candidates if "created on" in col.lower()]
                        auto_date_col = preferred[0] if preferred else date_col_candidates[0]
                        
                        logger.warning(
                            f"Date column '{col_ref}' not found in group_by. "
                            f"Auto-detected date column '{auto_date_col}' from table '{table_name_search}'. "
                            f"Using it for period grouping."
                        )
                        
                        col_name = auto_date_col
                        table_name = table_name_search
                        lf = lf_search
                        break
            
            if not date_col_candidates:
                # Re-raise the original error if no date columns found
                raise ValueError(
                    f"Column '{col_ref}' not found and no date columns detected. "
                    f"Original error: {str(e)}"
                ) from e
        
        # Build expression based on kind
        #
        # IMPORTANT: Date columns coming from CSV/XLSX often arrive as strings, floats, or nulls/NaNs.
        # Polars will raise if you do a strict cast like Float64 -> Date. We always use strict=False
        # and bucket invalid/missing values so aggregations don't fail.
        def _safe_date_col_expr() -> pl.Expr:
            col = pl.col(col_name)
            # 1) Already Date/Datetime-like
            coerced_date = col.cast(pl.Date, strict=False)
            coerced_datetime = col.cast(pl.Datetime, strict=False).cast(pl.Date, strict=False)

            # 2) Numeric YYYYMMDD (SAP _0CALDAY: int 20240216 or float 20240216.0) — only when in valid range to avoid confusing with Unix/Excel
            num_val = col.cast(pl.Float64, strict=False)
            numeric_yyyymmdd = (
                pl.when(num_val.is_between(10000101, 99991231))
                .then(num_val.cast(pl.Int64, strict=False).cast(pl.Utf8).str.strptime(pl.Date, format="%Y%m%d", strict=False))
                .otherwise(pl.lit(None, dtype=pl.Date))
            )
            # 3) String YYYYMMDD ("20240216")
            s = col.cast(pl.Utf8, strict=False).str.strip_chars()
            int_yyyymmdd = s.str.strptime(pl.Date, format="%Y%m%d", strict=False)
            # 4) Unix timestamp (seconds: 10 digits, ms: 13 digits)
            unix_sec = (
                pl.when(num_val.is_between(1e9, 2e9))
                .then(pl.from_epoch(num_val.cast(pl.Int64, strict=False), time_unit="s").cast(pl.Date))
                .otherwise(pl.lit(None, dtype=pl.Date))
            )
            unix_ms = (
                pl.when(num_val.is_between(1e12, 2e12))
                .then(pl.from_epoch(num_val.cast(pl.Int64, strict=False), time_unit="ms").cast(pl.Date))
                .otherwise(pl.lit(None, dtype=pl.Date))
            )
            # 5) Excel serial date (days since 1899-12-30; typical range ~1e3–6e4)
            excel_serial = num_val.cast(pl.Float64, strict=False)
            excel_epoch_seconds = (excel_serial - 25569) * 86400  # 25569 = days 1899-12-30 -> 1970-01-01
            excel_date = (
                pl.when(excel_serial.is_between(1000, 100000))
                .then(pl.from_epoch(excel_epoch_seconds.cast(pl.Int64, strict=False), time_unit="s").cast(pl.Date))
                .otherwise(pl.lit(None, dtype=pl.Date))
            )

            # 6) String dates with separators and datetimes
            date_formats = [
                "%Y-%m-%d",
                "%Y/%m/%d",
                "%m/%d/%Y",
                "%d/%m/%Y",
                "%m-%d-%Y",
                "%d-%m-%Y",
                "%Y.%m.%d",
                "%d.%m.%Y",
                "%m.%d.%Y",
            ]
            datetime_formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M",
                "%Y/%m/%d %H:%M:%S",
                "%Y/%m/%d %H:%M",
                "%m/%d/%Y %H:%M:%S",
                "%m/%d/%Y %H:%M",
                "%d/%m/%Y %H:%M:%S",
                "%d/%m/%Y %H:%M",
                "%Y-%m-%d %H:%M:%S%.f",
                "%Y-%m-%dT%H:%M:%S%.f",
            ]
            parsed_candidates: List[pl.Expr] = []
            for fmt in date_formats:
                parsed_candidates.append(s.str.strptime(pl.Date, format=fmt, strict=False))
            iso_date = s.str.slice(0, 10).str.strptime(pl.Date, format="%Y-%m-%d", strict=False)
            parsed_candidates.append(iso_date)
            for datetime_fmt in datetime_formats:
                parsed_datetime = s.str.strptime(pl.Datetime, format=datetime_fmt, strict=False)
                parsed_candidates.append(parsed_datetime.cast(pl.Date, strict=False))
            parsed_date = pl.coalesce(parsed_candidates) if parsed_candidates else pl.lit(None, dtype=pl.Date)

            # Coalesce: native date → numeric YYYYMMDD → string YYYYMMDD → Unix s/ms → Excel → datetime cast → string formats
            return pl.coalesce([
                coerced_date,
                numeric_yyyymmdd,
                int_yyyymmdd,
                unix_sec,
                unix_ms,
                excel_date,
                coerced_datetime,
                parsed_date,
            ])

        if kind == "day":
            # Format as "YYYY-MM-DD"
            expr = _safe_date_col_expr().dt.strftime("%Y-%m-%d").fill_null("Unknown")
            alias = "period"
        elif kind == "week":
            # Format as "YYYY-WW" using ISO week format
            # Use ISO week number (%V) which is 01-53, zero-padded
            d = _safe_date_col_expr()
            # Get year and ISO week number
            # Note: For ISO weeks, we should use ISO year, but if not available, use regular year
            # The week number %V is ISO week (01-53, zero-padded)
            try:
                # Try ISO year first (if available in Polars version)
                year_expr = d.dt.iso_year().cast(pl.Utf8)
            except (AttributeError, Exception):
                # Fallback to regular year if iso_year not available
                year_expr = d.dt.year().cast(pl.Utf8)
            week_expr = d.dt.strftime("%V")  # ISO week number (01-53, already zero-padded)
            expr = (year_expr + pl.lit("-W") + week_expr).fill_null("Unknown")
            alias = "period"
        elif kind == "month":
            # Format as "YYYY-MM"
            expr = _safe_date_col_expr().dt.strftime("%Y-%m").fill_null("Unknown")
            alias = "period"
        elif kind == "month_number":
            # Just month number 1-12
            expr = _safe_date_col_expr().dt.month().cast(pl.Utf8).fill_null("Unknown")
            alias = "period"
        elif kind == "quarter":
            # Format as "YYYY-Qn"
            d = _safe_date_col_expr()
            year_expr = d.dt.year().cast(pl.Utf8)
            quarter_expr = d.dt.quarter().cast(pl.Utf8)
            expr = (year_expr + pl.lit("-Q") + quarter_expr).fill_null("Unknown")
            alias = "period"
        elif kind == "year":
            expr = _safe_date_col_expr().dt.year().cast(pl.Utf8).fill_null("Unknown")
            alias = "period"
        else:
            raise ValueError(f"Unknown period function: {kind}")
        
        return expr, alias, "period"
    
    # Plain column name - prefer preferred_table if provided
    if preferred_table and preferred_table in lazyframes:
        try:
            lf_pref = lazyframes[preferred_table]
            schema_pref = lf_pref.collect_schema()
            # Check if column exists in preferred table
            if "." in spec:
                table_part, col_part = spec.split(".", 1)
                if table_part.lower() == preferred_table.lower() and col_part in schema_pref:
                    return pl.col(col_part), col_part, "group"
            elif spec in schema_pref:
                return pl.col(spec), spec, "group"
        except:
            pass
    
    # Fallback to normal resolution
    col_name, table_name, lf = resolve_column(spec, lazyframes)
    return pl.col(col_name), col_name, "group"


# =============================================================================
# FILTER EXPRESSIONS
# =============================================================================

# NOTE: Filter modifications removed - the LLM provides the correct date filters
# We use them as-is without any automatic modification.
# The prompts guide the LLM to create proper month-specific filters when needed.


def build_filter_expression(
    filter_spec: Union[Dict[str, Any], List[Dict[str, Any]], None],
    lazyframes: Dict[str, pl.LazyFrame],
    table_name: Optional[str] = None,
) -> Optional[pl.Expr]:
    """Build Polars filter expression from filter specification.
    
    Supports:
        - {"field": "date_col", "year": 2023}
        - {"field": "col", "operator": ">=", "value": "2023-01-01"}
        - {"field": "col", "operator": "in", "value": ["a", "b"]}
        - {"field": "table.col"} - table.column format
        - [{"field": "col", "value": "x"}] - list of filters (combined with AND)
    
    Args:
        filter_spec: Filter specification dictionary or list of dictionaries
        lazyframes: Available LazyFrames
        table_name: Optional table name hint for column resolution
        
    Returns:
        Polars expression for filtering, or None if no filter
    """
    # CRITICAL: Validate lazyframes doesn't contain non-LazyFrame objects
    # This prevents "truth value of a LazyFrame is ambiguous" errors
    validated_lazyframes = {}
    for name, lf in lazyframes.items():
        if isinstance(lf, pl.LazyFrame):
            validated_lazyframes[name] = lf
        else:
            logger.warning(f"[build_filter_expression] Skipping invalid LazyFrame for table '{name}': expected pl.LazyFrame, got {type(lf)}")
    
    if not validated_lazyframes:
        logger.warning(f"[build_filter_expression] No valid LazyFrames available")
        return None
    
    # CRITICAL: Check filter_spec is None or empty, not use it directly in boolean context
    # Using filter_spec directly in boolean context causes "truth value of a LazyFrame is ambiguous" error
    if filter_spec is None:
        return None
    if isinstance(filter_spec, (dict, list)) and len(filter_spec) == 0:
        return None
    
    # Handle case where filter_spec is a list - combine all filters with AND
    # CRITICAL: Filter out Fiscal_Period and Calendar_Year - they cause type mismatch errors
    filter_list = []
    if isinstance(filter_spec, list):
        if len(filter_spec) == 0:
            return None
        # Process all filters in the list - skip Fiscal_Period and Calendar_Year
        for f in filter_spec:
            if isinstance(f, dict):
                field = f.get("field", "")
                # Extract column name (remove table prefix if present)
                col_name = field.split(".", 1)[-1] if "." in field else field
                
                # CRITICAL: Skip Fiscal_Period and Calendar_Year - they cause type mismatch errors
                # Fiscal_Period is numeric (Int32/Int64) but filters use string values like '01.2023'
                if col_name and col_name.lower() in ["fiscal_period", "calendar_year"]:
                    logger.debug(f"[build_filter_expression] Skipping {col_name} filter - causes type mismatch errors")
                    continue
                filter_list.append(f)
        if not filter_list:
            logger.debug(f"[build_filter_expression] All filters were skipped (Fiscal_Period/Calendar_Year), returning None")
            return None
    elif isinstance(filter_spec, dict):
        # Check if this single filter should be skipped
        field = filter_spec.get("field", "")
        col_name = field.split(".", 1)[-1] if "." in field else field
        if col_name and col_name.lower() in ["fiscal_period", "calendar_year"]:
            logger.debug(f"[build_filter_expression] Skipping {col_name} filter - causes type mismatch errors")
            return None
        filter_list = [filter_spec]
    else:
        logger.warning(f"filter_spec must be a dict or list of dicts, got {type(filter_spec)}, skipping filter")
        return None
    
    # Build filter expressions for all filters and combine with AND
    filter_exprs = []
    for f in filter_list:
        filter_expr = _build_single_filter_expression(f, validated_lazyframes, table_name)
        if filter_expr is not None:
            # Safety check: ensure filter_expr is a pl.Expr, not a LazyFrame
            # This prevents "truth value of a LazyFrame is ambiguous" errors
            if isinstance(filter_expr, pl.LazyFrame):
                logger.error(f"build_filter_expression: _build_single_filter_expression returned a LazyFrame instead of Expr. Skipping filter.")
                continue
            if not isinstance(filter_expr, pl.Expr):
                logger.warning(f"build_filter_expression: _build_single_filter_expression returned {type(filter_expr)} instead of Expr. Skipping filter.")
                continue
            filter_exprs.append(filter_expr)
    
    if not filter_exprs:
        return None
    
    # Combine all filters with AND
    if len(filter_exprs) == 1:
        return filter_exprs[0]
    else:
        # Combine with AND (all must be true)
        combined = filter_exprs[0]
        for expr in filter_exprs[1:]:
            # Safety check: ensure expr is a pl.Expr before combining
            if not isinstance(expr, pl.Expr):
                logger.error(f"build_filter_expression: Attempted to combine non-Expr {type(expr)}. Skipping.")
                continue
            combined = combined & expr
        return combined


def _coerce_filter_value_for_column(value: Any, col_type: Any) -> Any:
    """Coerce a filter value to match the column's Polars type for comparison operators.

    When the column is numeric (Int64, Float64, etc.) and the filter value is a
    string representation of a number, cast the value to the appropriate Python
    numeric type so Polars does not raise
    ``ComputeError: cannot compare string with numeric type``.
    """
    if col_type is None or value is None:
        return value
    type_str = str(col_type).lower()
    is_numeric_col = any(t in type_str for t in ("int", "float", "decimal", "uint"))
    if is_numeric_col and isinstance(value, str):
        stripped = value.strip()
        if stripped:
            try:
                if "." in stripped:
                    return float(stripped)
                return int(stripped)
            except (ValueError, TypeError):
                pass
    return value


def _build_single_filter_expression(
    filter_spec: Dict[str, Any],
    lazyframes: Dict[str, pl.LazyFrame],
    table_name: Optional[str] = None,
) -> Optional[pl.Expr]:
    """Build a single filter expression from a filter dictionary.
    
    This is a helper function that builds one filter expression.
    Multiple filters should be combined in build_filter_expression.
    """
    if not isinstance(filter_spec, dict):
        return None
    
    field = filter_spec.get("field")
    if not field:
        return None
    
    # Resolve column (handle table.column format)
    if "." in field:
        table_from_field, col_name = field.split(".", 1)
        col_name = (col_name or "").strip()
    else:
        col_name = (field or "").strip()
    
    if not col_name:
        return None
    
    # CRITICAL: For aggregations we use ONLY the current frame's columns. No cross-frame filter.
    # When table_name is provided (the aggregation's frame), resolve the filter column only
    # in that frame. If the column is not in that frame, skip this filter (e.g. date was
    # applied at API fetch for analytical slices; slice frame has no date column).
    try:
        resolved_in_current = False
        if table_name:
            hint_lf = lazyframes.get(table_name)
            if isinstance(hint_lf, pl.LazyFrame):
                schema = hint_lf.collect_schema()
                if col_name in schema:
                    resolved_col = col_name
                    resolved_table = table_name
                    resolved_lf = hint_lf
                    resolved_in_current = True
                else:
                    col_name_lower = col_name.lower()
                    for actual_col in schema.keys():
                        if actual_col.lower() == col_name_lower:
                            resolved_col = actual_col
                            resolved_table = table_name
                            resolved_lf = hint_lf
                            resolved_in_current = True
                            break
                if not resolved_in_current:
                    logger.debug(
                        f"[_build_single_filter_expression] Filter column '{col_name}' not in current frame '{table_name}'. "
                        f"Skipping filter (current frame cols only, no cross-frame)."
                    )
                    return None
            else:
                # table_name provided but frame not in lazyframes — skip filter
                logger.debug(f"[_build_single_filter_expression] Current frame '{table_name}' not in lazyframes. Skipping filter.")
                return None
        
        if not resolved_in_current:
            # No table_name hint (e.g. generic filter build): resolve from any frame
            resolved_col, resolved_table, resolved_lf = resolve_column(field, lazyframes)
        
        col_name = resolved_col
        target_table = resolved_table
    except ValueError as e:
        # Column doesn't exist - try to find a date column automatically
        # Get available columns to search for date-like columns
        available_cols = []
        target_lf = None
        
        if target_table:
            table_lower = target_table.lower()
            for name, lf in lazyframes.items():
                if name.lower() == table_lower:
                    schema = lf.collect_schema()
                    available_cols = list(schema.keys())
                    target_lf = lf
                    break
        
        if not available_cols:
            # Try to get columns from any table
            for name, lf in lazyframes.items():
                schema = lf.collect_schema()
                available_cols = list(schema.keys())
                target_lf = lf
                target_table = name
                break
        
        # Try to find a date column automatically
        date_keywords = ("date", "dt", "time", "timestamp", "created", "posted", "updated", "on")
        date_col_candidates = [
            col for col in available_cols
            if any(keyword in col.lower() for keyword in date_keywords)
        ]
        
        if date_col_candidates:
            # Use the first date column found (prefer "Created On" if available)
            preferred_date_cols = [col for col in date_col_candidates if "created on" in col.lower()]
            if preferred_date_cols:
                auto_date_col = preferred_date_cols[0]
            else:
                auto_date_col = date_col_candidates[0]
            
            logger.warning(
                f"Filter column '{field}' not found. "
                f"Auto-detected date column '{auto_date_col}' from table '{target_table}'. "
                f"Using it for date filtering. Available columns: {available_cols[:10]}{'...' if len(available_cols) > 10 else ''}"
            )
            
            # Retry with the auto-detected date column
            try:
                if "." in field:
                    # Preserve table prefix if it was in the original field
                    table_part = field.split(".", 1)[0]
                    new_field = f"{table_part}.{auto_date_col}"
                else:
                    new_field = auto_date_col
                
                resolved_col, resolved_table, resolved_lf = resolve_column(new_field, lazyframes)
                col_name = resolved_col
                target_table = resolved_table
                # Continue with the resolved column
            except ValueError:
                logger.error(
                    f"Failed to resolve auto-detected date column '{auto_date_col}'. "
                    f"Skipping filter. Available columns: {available_cols[:20]}{'...' if len(available_cols) > 20 else ''}"
                )
                return None
        else:
            logger.warning(
                f"Filter column '{field}' not found and no date columns detected. "
                f"Skipping filter to allow aggregation to proceed. "
                f"Available columns: {available_cols[:20]}{'...' if len(available_cols) > 20 else ''}"
            )
            return None  # Skip filter if column doesn't exist and no date columns found
    
    # Helper: dynamically detect and parse date/time values from various formats
    def _parse_date_time_value(value: Any, column_is_date: bool = False) -> Optional[Any]:
        """Parse a date/time value from various formats dynamically.
        
        Args:
            value: The filter value to parse.
            column_is_date: True when the target column is a date/datetime type.
                When False and value is a plain number, numeric-to-date heuristics
                (Excel serial date, Unix timestamp) are skipped to avoid
                misinterpreting year integers (e.g. 2025) as dates.
        
        Returns:
            Parsed date/datetime object or original value if not date-like
        """
        if value is None:
            return None
        
        # Already a date/datetime object
        if isinstance(value, (date, datetime)):
            return value
        
        # Try to parse as string
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return None
            
            # Try ISO format first (most common)
            try:
                # Handle ISO format with timezone
                if "T" in value or "Z" in value or "+" in value or value.count("-") >= 3:
                    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                    return dt.date() if dt.hour == 0 and dt.minute == 0 and dt.second == 0 else dt
            except (ValueError, AttributeError):
                pass
            
            # Try common date formats
            date_patterns = [
                ("%Y-%m-%d", date),
                ("%Y/%m/%d", date),
                ("%m/%d/%Y", date),
                ("%d/%m/%Y", date),
                ("%m-%d-%Y", date),
                ("%d-%m-%Y", date),
                ("%Y.%m.%d", date),
                ("%d.%m.%Y", date),
                ("%m.%d.%Y", date),
            ]
            
            datetime_patterns = [
                ("%Y-%m-%d %H:%M:%S", datetime),
                ("%Y-%m-%d %H:%M", datetime),
                ("%Y/%m/%d %H:%M:%S", datetime),
                ("%Y/%m/%d %H:%M", datetime),
                ("%m/%d/%Y %H:%M:%S", datetime),
                ("%m/%d/%Y %H:%M", datetime),
                ("%d/%m/%Y %H:%M:%S", datetime),
                ("%d/%m/%Y %H:%M", datetime),
                ("%Y-%m-%dT%H:%M:%S", datetime),
                ("%Y-%m-%dT%H:%M", datetime),
            ]
            
            # Try date patterns
            for pattern, target_type in date_patterns:
                try:
                    parsed = datetime.strptime(value, pattern)
                    return parsed.date() if target_type == date else parsed
                except (ValueError, AttributeError):
                    continue
            
            # Try datetime patterns
            for pattern, target_type in datetime_patterns:
                try:
                    parsed = datetime.strptime(value, pattern)
                    return parsed
                except (ValueError, AttributeError):
                    continue
        
        # Try numeric → date ONLY when the column is a date/datetime type.
        # Without this guard, year values like 2025 or fiscal period numbers
        # are misinterpreted as Excel serial dates (2025 → ~1905-07-07).
        if column_is_date and isinstance(value, (int, float)):
            try:
                # YYYYMMDD compact format (e.g. 20250115 → 2025-01-15)
                if isinstance(value, int) and 19000101 <= value <= 21001231:
                    y, rest = divmod(value, 10000)
                    m, d = divmod(rest, 100)
                    if 1 <= m <= 12 and 1 <= d <= 31:
                        try:
                            return date(y, m, d)
                        except ValueError:
                            pass
                
                # Excel serial date (days since 1900-01-01)
                if 1 <= value <= 100000:
                    try:
                        excel_epoch = date(1900, 1, 1)
                        days = int(value) - (2 if value >= 60 else 1)
                        return excel_epoch + timedelta(days=days)
                    except (ValueError, OverflowError):
                        pass
                
                # Unix timestamp (seconds since 1970-01-01)
                if 0 <= value <= 2147483647:
                    try:
                        return datetime.fromtimestamp(value)
                    except (ValueError, OSError):
                        pass
            except Exception:
                pass
        
        # Return original value if we can't parse it
        return value
    
    # Helper: safely coerce a column to Date/Datetime dynamically based on column type
    def _safe_date_time_expr(expr: pl.Expr, target_type: Optional[str] = None) -> pl.Expr:
        """Dynamically coerce a column to Date or Datetime, handling various input formats.
        
        Args:
            expr: Polars expression (column)
            target_type: Optional target type hint ('date', 'datetime', 'time', or None for auto-detect)
        
        Returns:
            Polars expression that can be used for date/time comparisons
        """
        # First try direct cast (works if column is already Date/Datetime)
        if target_type == "datetime" or target_type is None:
            coerced_datetime = expr.cast(pl.Datetime, strict=False)
        else:
            coerced_datetime = None
        
        if target_type == "date" or target_type is None:
            coerced_date = expr.cast(pl.Date, strict=False)
        else:
            coerced_date = None
        
        # Convert to string for parsing
        s = expr.cast(pl.Utf8, strict=False).str.strip_chars()
        
        # Comprehensive date formats
        date_formats = [
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%m/%d/%Y",
            "%d/%m/%Y",
            "%m-%d-%Y",
            "%d-%m-%Y",
            "%Y.%m.%d",
            "%d.%m.%Y",
            "%m.%d.%Y",
        ]
        
        # Comprehensive datetime formats
        datetime_formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
            "%m/%d/%Y %H:%M:%S",
            "%m/%d/%Y %H:%M",
            "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y %H:%M",
            "%Y-%m-%d %H:%M:%S%.f",  # With microseconds (chrono: %.f consumes leading dot)
            "%Y-%m-%dT%H:%M:%S%.f",
        ]
        
        parsed_candidates: List[pl.Expr] = []
        
        # Try date formats
        for fmt in date_formats:
            parsed_candidates.append(s.str.strptime(pl.Date, format=fmt, strict=False))
        
        # Try datetime formats
        for fmt in datetime_formats:
            parsed_candidates.append(
                s.str.strptime(pl.Datetime, format=fmt, strict=False)
            )
        
        # Handle ISO format with timezone (extract date part)
        iso_date = s.str.slice(0, 10).str.strptime(pl.Date, format="%Y-%m-%d", strict=False)
        parsed_candidates.append(iso_date)
        
        # Handle datetime strings by extracting date part
        datetime_to_date = s.str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False).cast(pl.Date, strict=False)
        parsed_candidates.append(datetime_to_date)
        
        # Coalesce all parsing attempts
        parsed_result = pl.coalesce(parsed_candidates) if parsed_candidates else pl.lit(None, dtype=pl.Date)
        
        # Combine with direct casts
        if coerced_date is not None and coerced_datetime is not None:
            return pl.coalesce([coerced_datetime, coerced_date, parsed_result])
        elif coerced_datetime is not None:
            return pl.coalesce([coerced_datetime, parsed_result.cast(pl.Datetime, strict=False)])
        elif coerced_date is not None:
            return pl.coalesce([coerced_date, parsed_result])
        else:
            return parsed_result
    
    # Backward compatibility: keep _safe_date_expr for existing code
    def _safe_date_expr(expr: pl.Expr) -> pl.Expr:
        """Legacy function - use _safe_date_time_expr for new code."""
        return _safe_date_time_expr(expr, target_type="date")

    # Final validation: ensure column exists in the resolved LazyFrame
    # CRITICAL: If table_name hint was provided, ensure the resolved column is in that table
    # This prevents filter expressions from referencing columns in different batches
    try:
        schema = resolved_lf.collect_schema()
        if col_name not in schema:
            logger.warning(
                f"Filter column '{col_name}' not found in resolved table '{target_table}'. "
                f"Available columns: {list(schema.keys())[:20]}{'...' if len(schema) > 20 else ''}. "
                f"Skipping filter."
            )
            return None
        
        # If table_name hint was provided, verify the resolved table matches
        # This ensures filter columns are in the same frame as the metric column
        if table_name and resolved_table != table_name:
            # Check if the column exists in the hinted table
            hinted_table_lf = None
            for name, lf in lazyframes.items():
                if name == table_name:
                    hinted_table_lf = lf
                    break
            
            if hinted_table_lf:
                hinted_schema = hinted_table_lf.collect_schema()
                if col_name in hinted_schema:
                    # Column exists in hinted table - use that instead
                    logger.info(
                        f"[_build_single_filter_expression] Filter column '{col_name}' found in both '{resolved_table}' and '{table_name}'. "
                        f"Using '{table_name}' to match metric column frame."
                    )
                    resolved_table = table_name
                    resolved_lf = hinted_table_lf
                else:
                    # Column doesn't exist in hinted table - this will cause an error
                    logger.warning(
                        f"[_build_single_filter_expression] ⚠️ Filter column '{col_name}' found in '{resolved_table}' but not in hinted table '{table_name}'. "
                        f"Filter may fail when applied. Available columns in '{table_name}': {list(hinted_schema.keys())[:20]}"
                    )
    except Exception as e:
        logger.warning(f"Error validating filter column '{col_name}': {e}. Skipping filter.")
        return None
    
    # Year filter
    if "year" in filter_spec:
        year = int(filter_spec["year"])
        return _safe_date_expr(pl.col(col_name)).dt.year() == year
    
    # Operator-based filter
    operator = filter_spec.get("operator", "==").lower().strip()
    value = filter_spec.get("value")
    
    if value is None:
        return None
    
    # Handle relative date placeholders (LLM sometimes uses these instead of actual dates)
    # NOTE: Do not import date/timedelta here; use module-level imports so the inner
    # _parse_date_time_value() closure does not see 'date' as an unassigned local.
    if isinstance(value, str):
        value_lower = value.lower().strip()
        # Try to import relativedelta, fallback to manual calculation if not available
        try:
            from dateutil.relativedelta import relativedelta
            
            def add_months(d, months):
                return d + relativedelta(months=months)
        except ImportError:
            # Fallback: manual month addition
            def add_months(d, months):
                month = d.month - 1 + months
                year = d.year + month // 12
                month = month % 12 + 1
                day = min(d.day, [31, 28 + (1 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 0), 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1])
                return date(year, month, day)
        
        today = date.today()
        
        # Map common relative date placeholders to actual dates
        try:
            relative_date_map = {
                "today": today,
                "yesterday": today - timedelta(days=1),
                "tomorrow": today + timedelta(days=1),
                "last_month_start": add_months(today.replace(day=1), -1),
                "last_month_end": today.replace(day=1) - timedelta(days=1),
                "this_month_start": today.replace(day=1),
                "this_month_end": add_months(today.replace(day=1), 1) - timedelta(days=1),
                "last_week_start": today - timedelta(days=today.weekday() + 7),
                "last_week_end": today - timedelta(days=today.weekday() + 1),
                "this_week_start": today - timedelta(days=today.weekday()),
                "last_quarter_start": add_months(today.replace(day=1, month=((today.month - 1) // 3) * 3 + 1), -3),
                "this_quarter_start": today.replace(day=1, month=((today.month - 1) // 3) * 3 + 1),
                "last_year_start": date(today.year - 1, 1, 1),
                "last_year_end": date(today.year - 1, 12, 31),
                "this_year_start": date(today.year, 1, 1),
            }
            
            if value_lower in relative_date_map:
                value = relative_date_map[value_lower]
                logger.debug(f"Converted relative date '{value_lower}' to {value}")
            elif value_lower.replace("_", "").replace("-", "").replace(" ", "") in [k.replace("_", "") for k in relative_date_map.keys()]:
                # Try without underscores
                for k, v in relative_date_map.items():
                    if value_lower.replace("_", "").replace("-", "").replace(" ", "") == k.replace("_", ""):
                        value = v
                        logger.debug(f"Converted relative date '{value_lower}' to {value}")
                        break
        except Exception as e:
            logger.warning(f"Failed to parse relative date '{value}': {e}")
    
    col_expr = pl.col(col_name)
    
    # Special handling for Cal_MonthFP - check actual format and convert if needed
    if col_name and "cal_monthfp" in col_name.lower():
        # Log sample values to diagnose format mismatch
        try:
            # Get a sample of actual values from the column to check format
            sample_lf = resolved_lf.select(pl.col(col_name)).head(100)
            sample_df = sample_lf.collect()
            if not sample_df.is_empty():
                sample_values = sample_df[col_name].unique().head(10).to_list()
                logger.info(f"[_build_single_filter_expression] Cal_MonthFP sample values: {sample_values}, filter value: '{value}'")
                
                # Try to match the filter value format to actual data format
                # Common formats: '2025.01', '2025-01', '202501', '2025/01', '202411' (YYYYMM)
                if isinstance(value, str):
                    # Convert YYYY-MM format to YYYYMM (most common format in SAP)
                    import re
                    yyyy_mm_match = re.match(r'^(\d{4})-(\d{2})$', value)
                    if yyyy_mm_match:
                        year, month = yyyy_mm_match.groups()
                        yyyymm_format = f"{year}{month}"  # '2025-01' -> '202501'
                        if yyyymm_format in sample_values:
                            logger.info(f"[_build_single_filter_expression] Matched Cal_MonthFP format: '{value}' -> '{yyyymm_format}'")
                            value = yyyymm_format
                        else:
                            # Try other variants
                            value_variants = [
                                yyyymm_format,  # '202501'
                                value.replace("-", "."),  # '2025.01'
                                value.replace("-", "/"),  # '2025/01'
                                value,  # Original: '2025-01'
                            ]
                            for variant in value_variants:
                                if variant in sample_values:
                                    logger.info(f"[_build_single_filter_expression] Matched Cal_MonthFP format: '{value}' -> '{variant}'")
                                    value = variant
                                    break
                    elif "." in value:
                        # Try alternative formats for dot-separated values
                        value_variants = [
                            value,  # Original: '2025.01'
                            value.replace(".", "-"),  # '2025-01'
                            value.replace(".", ""),  # '202501'
                            value.replace(".", "/"),  # '2025/01'
                        ]
                        # Check which format exists in the data
                        for variant in value_variants:
                            if variant in sample_values:
                                logger.info(f"[_build_single_filter_expression] Matched Cal_MonthFP format: '{value}' -> '{variant}'")
                                value = variant
                                break
        except Exception as e:
            logger.debug(f"[_build_single_filter_expression] Could not check Cal_MonthFP format: {e}")
    
    # Detect if this is a date/time column or string column by checking column type in schema
    is_date_time_column = False
    is_string_column = False
    col_type = None
    try:
        schema = resolved_lf.collect_schema()
        if col_name in schema:
            col_type = schema[col_name]
            type_str = str(col_type).lower()
            is_date_time_column = any(keyword in type_str for keyword in ["date", "time", "datetime", "timestamp"])
            is_string_column = "utf8" in type_str or "string" in type_str
    except Exception:
        pass
    
    # Parse filter value dynamically (pass column type so numeric years aren't misread as dates)
    parsed_value = _parse_date_time_value(value, column_is_date=is_date_time_column)
    is_date_time_value = isinstance(parsed_value, (date, datetime))
    
    # Use date/time comparison if either column or value is date/time
    use_date_time_comparison = is_date_time_column or is_date_time_value
    
    # For date range filters: use string comparison when value is date/datetime so we work for both
    # string columns (e.g. SAP/OData "2026-01-24") and date columns (cast to Utf8 gives ISO string).
    # This avoids "no data after filter" when schema says Date but data is string or vice versa.
    def _date_range_expr_string_fallback(op: str) -> Optional[pl.Expr]:
        if not (is_date_time_value and parsed_value is not None):
            return None
        iso_val = parsed_value.isoformat() if isinstance(parsed_value, date) else parsed_value.strftime("%Y-%m-%d")
        col_str = col_expr.cast(pl.Utf8, strict=False).str.strip_chars().str.slice(0, 10)
        logger.debug(f"[_build_single_filter_expression] Date range on '{col_name}' using string comparison: {op} {iso_val}")
        if op == ">=":
            return col_str >= iso_val
        if op == "<=":
            return col_str <= iso_val
        if op == ">":
            return col_str > iso_val
        if op == "<":
            return col_str < iso_val
        return None

    if operator in ["=", "=="]:
        if use_date_time_comparison and is_date_time_value:
            if isinstance(parsed_value, datetime):
                return _safe_date_time_expr(col_expr, target_type="datetime") == parsed_value
            elif isinstance(parsed_value, date):
                col_date_expr = _safe_date_time_expr(col_expr, target_type="date")
                return col_date_expr == parsed_value
        # Cross-type comparison: cast both sides to string so
        # int 2025 matches "2025" and string "5500" matches int 5500.
        if isinstance(value, str) and value.strip():
            return col_expr.cast(pl.Utf8, strict=False) == value
        if isinstance(value, (int, float)) and is_string_column:
            return col_expr == str(value)
        return col_expr == value
    elif operator == "!=":
        if use_date_time_comparison and is_date_time_value:
            if isinstance(parsed_value, datetime):
                return _safe_date_time_expr(col_expr, target_type="datetime") != parsed_value
            elif isinstance(parsed_value, date):
                col_date_expr = _safe_date_time_expr(col_expr, target_type="date")
                return col_date_expr != parsed_value
        if isinstance(value, str) and value.strip():
            return col_expr.cast(pl.Utf8, strict=False) != value
        if isinstance(value, (int, float)) and is_string_column:
            return col_expr != str(value)
        return col_expr != value
    elif operator == ">=":
        string_expr = _date_range_expr_string_fallback(">=")
        if string_expr is not None:
            return string_expr
        if use_date_time_comparison and is_date_time_value:
            if isinstance(parsed_value, datetime):
                return _safe_date_time_expr(col_expr, target_type="datetime") >= parsed_value
            elif isinstance(parsed_value, date):
                col_date_expr = _safe_date_time_expr(col_expr, target_type="date")
                return col_date_expr >= parsed_value
        # Numeric column + string value → cast value to match column type
        cmp_value = _coerce_filter_value_for_column(value, col_type)
        return col_expr >= cmp_value
    elif operator == "<=":
        string_expr = _date_range_expr_string_fallback("<=")
        if string_expr is not None:
            return string_expr
        if use_date_time_comparison and is_date_time_value:
            if isinstance(parsed_value, datetime):
                return _safe_date_time_expr(col_expr, target_type="datetime") <= parsed_value
            elif isinstance(parsed_value, date):
                col_date_expr = _safe_date_time_expr(col_expr, target_type="date")
                return col_date_expr <= parsed_value
        cmp_value = _coerce_filter_value_for_column(value, col_type)
        return col_expr <= cmp_value
    elif operator == ">":
        string_expr = _date_range_expr_string_fallback(">")
        if string_expr is not None:
            return string_expr
        if use_date_time_comparison and is_date_time_value:
            if isinstance(parsed_value, datetime):
                return _safe_date_time_expr(col_expr, target_type="datetime") > parsed_value
            elif isinstance(parsed_value, date):
                col_date_expr = _safe_date_time_expr(col_expr, target_type="date")
                return col_date_expr > parsed_value
        cmp_value = _coerce_filter_value_for_column(value, col_type)
        return col_expr > cmp_value
    elif operator == "<":
        string_expr = _date_range_expr_string_fallback("<")
        if string_expr is not None:
            return string_expr
        if use_date_time_comparison and is_date_time_value:
            if isinstance(parsed_value, datetime):
                return _safe_date_time_expr(col_expr, target_type="datetime") < parsed_value
            elif isinstance(parsed_value, date):
                col_date_expr = _safe_date_time_expr(col_expr, target_type="date")
                return col_date_expr < parsed_value
        cmp_value = _coerce_filter_value_for_column(value, col_type)
        return col_expr < cmp_value
    elif operator == "in" and isinstance(value, list):
        if is_string_column and value and isinstance(value[0], (int, float)):
            str_values = [str(v) for v in value]
            return col_expr.is_in(str_values)
        if not is_string_column and value and isinstance(value[0], str):
            return col_expr.cast(pl.Utf8, strict=False).is_in(value)
        return col_expr.is_in(value)
    elif operator == "not in" and isinstance(value, list):
        if is_string_column and value and isinstance(value[0], (int, float)):
            str_values = [str(v) for v in value]
            return ~col_expr.is_in(str_values)
        if not is_string_column and value and isinstance(value[0], str):
            return ~col_expr.cast(pl.Utf8, strict=False).is_in(value)
        return ~col_expr.is_in(value)
    
    logger.warning(f"Unsupported filter operator: {operator}")
    return None


# =============================================================================
# AGGREGATION EXECUTION (SINGLE-PASS)
# =============================================================================

def execute_aggregations_polars(
    operation_plan: Dict[str, Any],
    lazyframes: Dict[str, pl.LazyFrame],
) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """Execute all aggregations using Polars LazyFrame.
    
    This is the core aggregation engine. Key design:
    - Uses LazyFrame to defer computation
    - Processes each aggregation with minimal scans
    - Returns scalar or grouped results
    - ONLY calls .collect() once per aggregation
    
    Args:
        operation_plan: Dictionary with:
            - aggregations: Dict of metric_key -> {column, agg, group_by, filter}
            - derived: Dict of metric_key -> expression_string
        lazyframes: Dictionary of table_name -> LazyFrame
        
    Returns:
        Tuple of (results_dict, errors_dict)
        - results_dict: metric_key -> value (scalar) or list of {group, value}
        - errors_dict: metric_key -> error_message for failed metrics
    """
    results: Dict[str, Any] = {}
    errors: Dict[str, str] = {}
    
    # Clear execution cache at start of new batch to allow fresh execution
    _clear_execution_cache()
    
    aggregations_raw = operation_plan.get("aggregations", {}) or {}
    derived = operation_plan.get("derived", {}) or {}
    
    # Normalize aggregations: ensure it's a dict, not a list
    aggregations: Dict[str, Any] = {}
    if isinstance(aggregations_raw, dict):
        aggregations = aggregations_raw
    elif isinstance(aggregations_raw, list):
        # Convert list to dict if needed (shouldn't happen, but handle gracefully)
        logger.warning(f"aggregations is a list instead of dict, converting: {len(aggregations_raw)} items")
        for idx, item in enumerate(aggregations_raw):
            if isinstance(item, dict):
                # Try to extract a key from the item, or use index
                key = item.get("metric_key") or item.get("key") or f"metric_{idx}"
                aggregations[key] = item
            else:
                logger.warning(f"Skipping invalid aggregation item at index {idx}: {type(item)}")
    else:
        logger.warning(f"aggregations has unexpected type: {type(aggregations_raw)}, using empty dict")
        aggregations = {}
    
    # Validate that all aggregation values are dicts
    for key, value in list(aggregations.items()):
        if not isinstance(value, dict):
            logger.warning(f"Removing invalid aggregation '{key}': expected dict, got {type(value)}")
            aggregations.pop(key, None)
    
    # Process each aggregation
    for metric_key, agg_spec in aggregations.items():
        # Validate agg_spec is a dict
        if not isinstance(agg_spec, dict):
            error_msg = f"Aggregation spec must be a dict, got {type(agg_spec)}"
            logger.warning(f"Aggregation '{metric_key}' failed: {error_msg}")
            results[metric_key] = None
            errors[metric_key] = error_msg
            continue
            
        try:
            result = _execute_single_aggregation(
                metric_key, agg_spec, lazyframes
            )
            # If result is None, it means duplicate execution was skipped
            # Don't overwrite existing result if we have one
            if result is not None:
                results[metric_key] = result
            elif metric_key not in results:
                # If we skipped and don't have a result, mark as None
                results[metric_key] = None
                errors[metric_key] = "Duplicate execution skipped"
        except Exception as e:
            error_msg = _build_error_message(metric_key, agg_spec, e)
            logger.warning(f"Aggregation '{metric_key}' failed: {error_msg}")
            results[metric_key] = None
            errors[metric_key] = error_msg
    
    # Process derived metrics (depend on aggregation results)
    for dkey, expr in derived.items():
        try:
            # Handle case where expr is a dict with 'expression' key (LLM sometimes returns this format)
            if isinstance(expr, dict):
                expr = expr.get('expression', expr.get('formula', ''))
                if not expr:
                    logger.warning(f"Derived metric '{dkey}' has no expression in dict: {derived[dkey]}")
                    results[dkey] = None
                    errors[dkey] = "No expression provided in derived metric specification"
                    continue
            
            # Ensure expr is a string
            if not isinstance(expr, str):
                logger.warning(f"Derived metric '{dkey}' expression is not a string: {type(expr)}")
                results[dkey] = None
                errors[dkey] = f"Expression must be a string, got {type(expr)}"
                continue
            
            # Extract all metric references from expression
            metric_refs = re.findall(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', expr)
            
            # Filter out Python keywords and built-ins (like 'if', 'and', 'or', etc.)
            python_keywords = {'if', 'else', 'and', 'or', 'not', 'in', 'is', 'None', 'True', 'False'}
            metric_refs = [ref for ref in metric_refs if ref not in python_keywords]
            
            # Check for missing metrics
            missing_metrics = []
            for ref in metric_refs:
                if ref not in results:
                    missing_metrics.append(ref)
            
            if missing_metrics:
                raise ValueError(
                    f"Missing base metrics: {missing_metrics}. "
                    f"Available metrics: {list(results.keys())}"
                )
            
            # Check for None values in referenced metrics - log warning but continue
            # safe_eval_expr will return None if any operand is None
            none_metrics = []
            for ref in metric_refs:
                val = results.get(ref)
                if val is None:
                    none_metrics.append(ref)
            
            if none_metrics:
                # Don't raise error - just log and let safe_eval_expr handle None values
                logger.debug(
                    f"Base metrics are None/zero for derived metric '{dkey}': {none_metrics}. "
                    f"Result will be None."
                )
                # Set result to None and continue (don't try to evaluate)
                results[dkey] = None
                errors[dkey] = f"Cannot compute: base metrics {none_metrics} are None/zero"
                continue
            
            # Check for grouped results (can't use in derived)
            grouped_metrics = []
            for ref in metric_refs:
                val = results.get(ref)
                if isinstance(val, list) and val and isinstance(val[0], dict):
                    if "group" in val[0] and "value" in val[0]:
                        grouped_metrics.append(ref)
            
            if grouped_metrics:
                raise ValueError(
                    f"Cannot use grouped metrics {grouped_metrics} in derived expressions. "
                    f"Derived metrics can only use scalar (non-grouped) base metrics."
                )
            
            # Evaluate the expression
            logger.debug(f"Evaluating derived metric '{dkey}': {expr} with metrics: {[ref for ref in metric_refs]}")
            val = safe_eval_expr(expr, results)
            
            # Handle None result (from division by zero)
            if val is None:
                logger.debug(f"Derived metric '{dkey}' evaluated to None (likely division by zero)")
                results[dkey] = None
                # Don't add to errors - this is expected behavior for division by zero
                continue
            
            # Check for invalid results (infinity, NaN)
            if isinstance(val, (int, float)):
                if not (math.isfinite(val) if hasattr(math, 'isfinite') else True):
                    logger.debug(f"Derived metric '{dkey}' result is not finite: {val}")
                    results[dkey] = None
                    continue
            
            results[dkey] = to_json_serializable(val)
            logger.debug(f"✅ Derived metric '{dkey}' = {val}")
            
        except Exception as e:
            error_msg = f"Derived metric failed: {e}. Expression: {expr}"
            logger.warning(f"❌ Derived metric '{dkey}' failed: {error_msg}")
            # Log available metrics for debugging
            available_metrics = [k for k, v in results.items() if v is not None and not (isinstance(v, list) and v and isinstance(v[0], dict) and "group" in v[0])]
            logger.debug(f"Available scalar metrics for derived expressions: {available_metrics}")
            results[dkey] = None
            errors[dkey] = error_msg
    
    return results, errors


def _find_frame_with_columns(
    lazyframes: Dict[str, pl.LazyFrame],
    required_columns: List[str],
    base_table_name: Optional[str] = None,
) -> Optional[Tuple[str, pl.LazyFrame]]:
    """Find a LazyFrame that contains all required columns.
    
    This is useful when metric columns and filter columns are in different batches.
    
    Args:
        lazyframes: Dictionary of table_name -> LazyFrame
        required_columns: List of column names that must be present
        base_table_name: Optional base table name to filter candidates (e.g., "AM_Sales_Order_v1_Summary")
    
    Returns:
        Tuple of (table_name, lazyframe) if found, None otherwise
    """
    if not required_columns:
        return None
    
    required_set = set(required_columns)
    
    # Filter candidates by base_table_name if provided
    candidates = {}
    if base_table_name:
        # Look for frames that match this table (could be batch keys)
        base_name = base_table_name.rsplit("_batch", 1)[0] if "_batch" in base_table_name else base_table_name
        for name, lf in lazyframes.items():
            if name == base_table_name or name.startswith(f"{base_name}_batch"):
                candidates[name] = lf
        # Also search sibling analytical slices (__by_) when base_name is itself a slice key.
        # E.g. base_name = "View__by_Fiscal_Year_Fisca" → extract view prefix "View"
        # and include all "View__by_*" frames as candidates.
        if ANALYTICAL_KEY_BY_PREFIX in base_name:
            view_prefix = base_name.split(ANALYTICAL_KEY_BY_PREFIX, 1)[0]
            sibling_prefix = f"{view_prefix}{ANALYTICAL_KEY_BY_PREFIX}"
            for name, lf in lazyframes.items():
                if name.startswith(sibling_prefix) and name not in candidates:
                    candidates[name] = lf
    else:
        candidates = lazyframes
    
    # Find frame that contains all required columns
    for name, lf in candidates.items():
        try:
            if not isinstance(lf, pl.LazyFrame):
                continue
            schema = lf.collect_schema()
            frame_columns = set(schema.keys())
            
            if required_set.issubset(frame_columns):
                logger.info(
                    f"[_find_frame_with_columns] Found frame '{name}' containing all required columns: {required_columns}"
                )
                return (name, lf)
        except Exception as e:
            logger.warning(f"[_find_frame_with_columns] Error checking columns for frame '{name}': {e}")
            continue
    
    logger.warning(
        f"[_find_frame_with_columns] No frame found containing all required columns: {required_columns}"
    )
    return None


def _extract_filter_columns(filter_spec: Union[Dict[str, Any], List[Dict[str, Any]], None]) -> List[str]:
    """Extract column names from filter specification.
    
    Args:
        filter_spec: Filter specification dictionary or list of dictionaries
    
    Returns:
        List of column names referenced in filters
    """
    filter_columns = []
    
    if filter_spec is None:
        return filter_columns
    
    filter_list = []
    if isinstance(filter_spec, list):
        filter_list = [f for f in filter_spec if isinstance(f, dict)]
    elif isinstance(filter_spec, dict):
        filter_list = [filter_spec]
    
    for f in filter_list:
        field = f.get("field", "")
        if field:
            # Extract column name (remove table prefix if present)
            col_name = field.split(".", 1)[-1] if "." in field else field
            
            # CRITICAL: Skip Fiscal_Period and Calendar_Year - they cause type mismatch errors
            # Fiscal_Period is numeric (Int32/Int64) but filters use string values like '01.2023'
            if col_name and col_name.lower() in ["fiscal_period", "calendar_year"]:
                continue
            
            filter_columns.append(col_name)
    
    return filter_columns


# Thread-local cache to track executed metrics within a single execution context
# This prevents the same metric from being executed twice in the same aggregation batch
import threading
_execution_cache = threading.local()

def _get_execution_cache():
    """Get thread-local execution cache for current execution context."""
    if not hasattr(_execution_cache, 'cache'):
        _execution_cache.cache = {}
    return _execution_cache.cache

def _clear_execution_cache():
    """Clear the execution cache (call at start of new execution batch)."""
    if hasattr(_execution_cache, 'cache'):
        _execution_cache.cache.clear()

def _is_metric_executed(metric_key: str) -> bool:
    """Check if a metric has already been executed in current execution context."""
    cache = _get_execution_cache()
    return metric_key in cache

def _mark_metric_executed(metric_key: str):
    """Mark a metric as executed in current execution context."""
    cache = _get_execution_cache()
    cache[metric_key] = True

def _resolve_table_key_for_aggregation(
    lazyframes: Dict[str, pl.LazyFrame],
    table_name: str,
    group_by_spec: Optional[str],
) -> Optional[str]:
    """Return the dataset key for this aggregation's group_by (view__by_{dim_col}).
    Format: view + ANALYTICAL_KEY_BY_PREFIX + dim_col (e.g. AM_Sales_Order_v1_Summary__by_Fiscal_Week_Fiscal_Hier_Ke).
    """
    if not group_by_spec or not table_name or not lazyframes:
        return None
    dim_col = (group_by_spec.split(".", 1)[1] if "." in group_by_spec else group_by_spec).strip()
    if not dim_col:
        return None
    preferred_key = f"{table_name}{ANALYTICAL_KEY_BY_PREFIX}{dim_col}"
    if preferred_key in lazyframes:
        return preferred_key
    lower_lookup = {k.lower(): k for k in lazyframes}
    if preferred_key.lower() in lower_lookup:
        return lower_lookup[preferred_key.lower()]
    prefix = f"{table_name}{ANALYTICAL_KEY_BY_PREFIX}"
    prefix_lower = prefix.lower()
    dim_col_lower = dim_col.lower()
    for k in lazyframes:
        if not k.lower().startswith(prefix_lower):
            continue
        if k[len(prefix):].lower() == dim_col_lower:
            return k
    return None


def _execute_single_aggregation(
    metric_key: str,
    agg_spec: Dict[str, Any],
    lazyframes: Dict[str, pl.LazyFrame],
) -> Any:
    """Execute a single aggregation and return result.
    
    For analytical dimension slices (ViewName__by_Dim), uses the frame that contains
    the aggregation's group_by dimension so each chart uses the correct slice.
    
    Returns:
        - Scalar value for non-grouped aggregations
        - List of {group, value} for grouped aggregations
        - None if duplicate execution was detected and skipped
    """
    # Check if this metric was already executed in current batch (duplicate detection)
    if _is_metric_executed(metric_key):
        logger.warning(f"[_execute_single_aggregation] ⚠️ Metric '{metric_key}' already executed in this batch - skipping duplicate execution")
        # Return None to indicate this was skipped (caller should handle gracefully)
        return None
    
    # Mark as executed
    _mark_metric_executed(metric_key)
    
    # Validate agg_spec is a dict
    if not isinstance(agg_spec, dict):
        raise TypeError(f"Aggregation spec for '{metric_key}' must be a dict, got {type(agg_spec)}: {agg_spec}")
    
    col_ref = agg_spec.get("column")
    func = (agg_spec.get("agg") or "sum").lower()
    group_by_spec = agg_spec.get("group_by")
    filter_spec = agg_spec.get("filter")
    
    # Use the dataframe that contains this aggregation's group_by dimension (view__by_{dim})
    # so we don't use the wrong slice (e.g. Fiscal_Week when we need Prod_Class).
    all_lazyframes = lazyframes
    table_from_col = (col_ref or "").split(".", 1)[0].strip() if col_ref and "." in (col_ref or "") else ""
    if not table_from_col and group_by_spec and "." in group_by_spec:
        table_from_col = group_by_spec.split(".", 1)[0].strip()
    resolved_table_key = _resolve_table_key_for_aggregation(lazyframes, table_from_col, group_by_spec)
    if resolved_table_key:
        lazyframes = {resolved_table_key: lazyframes[resolved_table_key]}
        logger.info(
            f"[_execute_single_aggregation] Metric '{metric_key}': using frame '{resolved_table_key}' (group_by={group_by_spec})"
        )
    
    # Special handling for COUNT(*) - col_ref is "*" or None
    is_count_star = (col_ref == "*" or col_ref is None) and func == "count"
    
    if not col_ref and func != "count":
        raise ValueError(f"Column required for aggregation '{func}'")
    
    # For COUNT(*), we don't need to resolve a column - just get any table
    if is_count_star:
        # Get the first available table for COUNT(*)
        if not lazyframes:
            raise ValueError("No LazyFrames available for COUNT(*) aggregation")
        table_name = list(lazyframes.keys())[0]
        lf = lazyframes[table_name]
        col_name = None  # No column needed for COUNT(*)
        logger.info(f"[_execute_single_aggregation] Metric '{metric_key}': COUNT(*) on table '{table_name}'")
    else:
        # Resolve column for normal aggregations (lazyframes may be restricted to the correct __by_ frame)
        col_name, table_name, lf = resolve_column(col_ref, lazyframes)
        # Log column resolution for debugging
        logger.info(f"[_execute_single_aggregation] Metric '{metric_key}': resolved column '{col_ref}' -> table '{table_name}', column '{col_name}'")
    
    # Extract filter columns to check if they exist in the current frame
    filter_columns = _extract_filter_columns(filter_spec)
    
    # Check if all required columns (metric + filter) exist in current frame
    # For COUNT(*), we don't need col_name in required_columns
    if is_count_star:
        required_columns = filter_columns
    else:
        required_columns = [col_name] + filter_columns
    current_schema = lf.collect_schema()
    current_columns = set(current_schema.keys())
    
    # Check if all required columns are in current frame
    missing_columns = [col for col in required_columns if col not in current_columns]
    
    if missing_columns:
        # Some columns are missing - try to find a frame that contains all required columns.
        # Search ALL frames (all_lazyframes), not just the restricted set, so sibling
        # analytical slices (e.g. __by_Fiscal_Period1_Fisca) are also considered.
        base_table_name = table_name.rsplit("_batch", 1)[0] if "_batch" in table_name else table_name
        logger.info(
            f"[_execute_single_aggregation] Metric column '{col_name}' in '{table_name}', but filter columns {missing_columns} are missing. "
            f"Searching across all frames for one containing all columns: {required_columns}"
        )
        
        frame_result = _find_frame_with_columns(all_lazyframes, required_columns, base_table_name)
        if frame_result:
            found_table_name, found_lf = frame_result
            logger.info(
                f"[_execute_single_aggregation] ✅ Found frame '{found_table_name}' containing both metric column '{col_name}' and filter columns {filter_columns}. "
                f"Using this frame for aggregation."
            )
            table_name = found_table_name
            lf = found_lf
            lazyframes = {found_table_name: found_lf}
        else:
            logger.warning(
                f"[_execute_single_aggregation] ⚠️ No single frame contains all required columns {required_columns}. "
                f"Will attempt to apply filter, but it may fail if filter columns are missing."
            )
    
    # Check if column exists and get sample data type
    # Skip column validation for COUNT(*)
    if is_count_star:
        schema = lf.collect_schema()
        logger.info(f"[_execute_single_aggregation] COUNT(*) on table '{table_name}' with {len(schema)} columns")
    else:
        schema = lf.collect_schema()
        if col_name in schema:
            col_type = schema[col_name]
            logger.info(f"[_execute_single_aggregation] Column '{col_name}' found in table '{table_name}' with type: {col_type}")
            
            # Check for null/NaN values in the column
            try:
                # Check for both null and NaN values (they are different in Polars!)
                # null = missing value, NaN = Not a Number (floating point)
                null_count = lf.select(pl.col(col_name).is_null().sum()).collect().item()
                total_count = lf.select(pl.len()).collect().item()
                
                # For numeric columns, also check for NaN values
                nan_count = 0
                col_type_str = str(col_type).lower()
                if "float" in col_type_str or "int" in col_type_str:
                    try:
                        nan_count = lf.select(pl.col(col_name).is_nan().sum()).collect().item()
                    except Exception:
                        pass  # is_nan not available for non-float columns
                
                if nan_count > 0:
                    logger.warning(f"[_execute_single_aggregation] Column '{col_name}': {null_count} null + {nan_count} NaN out of {total_count} values")
                    logger.info(f"[_execute_single_aggregation] NaN values will be treated as missing and excluded from aggregation")
                else:
                    logger.info(f"[_execute_single_aggregation] Column '{col_name}': {null_count}/{total_count} null values")
                
                # If all values are null or NaN, log a warning
                if (null_count + nan_count) == total_count and total_count > 0:
                    logger.warning(f"[_execute_single_aggregation] ⚠️ Column '{col_name}' has ALL null/NaN values! This will result in None aggregations.")
            except Exception as e:
                logger.warning(f"[_execute_single_aggregation] Could not check null count for column '{col_name}': {e}")
        else:
            logger.error(f"[_execute_single_aggregation] Column '{col_name}' NOT found in table '{table_name}' schema: {list(schema.keys())[:10]}")
            logger.error(f"[_execute_single_aggregation] Available columns in '{table_name}': {list(schema.keys())}")
            raise ValueError(f"Column '{col_name}' not found in table '{table_name}'. Available columns: {list(schema.keys())[:20]}")
    
    # Use the filter as provided by LLM - no automatic modifications
    # The LLM is responsible for providing correct date filters
    
    # Apply filter (pass table_name hint for better column resolution)
    # CRITICAL: Ensure filter_expr is not a LazyFrame before using in boolean context
    # First, verify all filter columns exist in current frame before building expression.
    # Use case-insensitive check so we skip filter when column is missing (e.g. analytical
    # slice has no _0CALDAY — date was applied at API fetch).
    current_schema_for_filter = lf.collect_schema()
    current_columns_for_filter = set(current_schema_for_filter.keys())
    current_columns_lower = {c.lower() for c in current_columns_for_filter}
    missing_filter_cols = [
        col for col in filter_columns
        if col not in current_columns_for_filter and (col.lower() if col else "") not in current_columns_lower
    ]
    
    if missing_filter_cols and filter_columns:
        logger.warning(
            f"[_execute_single_aggregation] ⚠️ Cannot build filter: columns {missing_filter_cols} not found in frame '{table_name}'. "
            f"Available columns: {list(current_columns_for_filter)[:20]}. Skipping filter to avoid ColumnNotFoundError."
        )
        filter_expr = None
    else:
        filter_expr = build_filter_expression(filter_spec, lazyframes, table_name)
    
    # Use isinstance check instead of truthiness to avoid LazyFrame boolean context error
    if filter_expr is not None and isinstance(filter_expr, pl.Expr):
        # Double-check filter columns exist before applying (safety check)
        try:
            lf = lf.filter(filter_expr)
            logger.info(f"✅ Applied filter for metric '{metric_key}': {filter_spec}")
        except Exception as filter_error:
            error_msg = str(filter_error)
            if "ColumnNotFoundError" in error_msg or "unable to find column" in error_msg.lower():
                logger.error(
                    f"[_execute_single_aggregation] ❌ Filter application failed: {error_msg}. "
                    f"This suggests filter column was resolved from a different frame. Skipping filter."
                )
                # Don't re-raise - continue without filter
            else:
                # Re-raise if it's a different error
                raise
    elif filter_expr is not None:
        logger.warning(f"[_execute_single_aggregation] Filter expression is not a pl.Expr (got {type(filter_expr)}), skipping filter")
    else:
        # Check if this looks like a comparison metric (has period suffix like jan2025, nov2024)
        if filter_spec is None or (isinstance(filter_spec, list) and len(filter_spec) == 0):
            metric_lower = metric_key.lower()
            period_indicators = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec", "2024", "2025", "2023"]
            if any(indicator in metric_lower for indicator in period_indicators):
                logger.warning(
                    f"[_execute_single_aggregation] ⚠️ WARNING: Metric '{metric_key}' appears to be a period comparison metric "
                    f"(contains period indicator) but has NO date filter! This will aggregate ALL data, not just the specified period. "
                    f"Please add date filters to limit data to the specific period."
                )
        else:
            # Check for single-sided date filters (missing either lower or upper bound)
            if isinstance(filter_spec, list):
                date_filters = [f for f in filter_spec if isinstance(f, dict) and f.get("field")]
                if date_filters:
                    # Group filters by field (date column)
                    filters_by_field = {}
                    for f in date_filters:
                        field = f.get("field", "")
                        operator = f.get("operator", "").lower()
                        # Extract column name (remove table prefix)
                        col_name = field.split(".", 1)[-1] if "." in field else field
                        if col_name not in filters_by_field:
                            filters_by_field[col_name] = []
                        filters_by_field[col_name].append({"operator": operator, "field": field})
                    
                    # Check each date column for two-sided range
                    for col_name, filters in filters_by_field.items():
                        operators = [f["operator"] for f in filters]
                        has_lower = any(op in [">=", ">"] for op in operators)
                        has_upper = any(op in ["<", "<="] for op in operators)
                        
                        if has_lower and not has_upper:
                            logger.warning(
                                f"[_execute_single_aggregation] ⚠️ WARNING: Metric '{metric_key}' has date filter for '{col_name}' "
                                f"with only LOWER bound (>= or >) but MISSING UPPER bound (< or <=)! "
                                f"This will include incorrect data beyond the intended period. "
                                f"Please add an upper bound filter (e.g., < end_date) to create a proper two-sided range."
                            )
                        elif has_upper and not has_lower:
                            logger.warning(
                                f"[_execute_single_aggregation] ⚠️ WARNING: Metric '{metric_key}' has date filter for '{col_name}' "
                                f"with only UPPER bound (< or <=) but MISSING LOWER bound (>= or >)! "
                                f"This will include incorrect data before the intended period. "
                                f"Please add a lower bound filter (e.g., >= start_date) to create a proper two-sided range."
                            )
    
    # Check sample values before aggregation for debugging (skip for COUNT(*))
    if not is_count_star:
        try:
            sample_data = lf.select(pl.col(col_name)).head(10).collect()
            if not sample_data.is_empty():
                sample_values = [val for val in sample_data[col_name].to_list()]
                logger.info(f"[_execute_single_aggregation] Sample values from column '{col_name}': {sample_values}")
            else:
                logger.warning(f"[_execute_single_aggregation] No data in LazyFrame after filtering for column '{col_name}'")
            
            # If filter was applied but no data, check filter column values
            if filter_spec:
                filter_fields = [f.get("field", "") for f in (filter_spec if isinstance(filter_spec, list) else [filter_spec]) if isinstance(f, dict)]
                for filter_field in filter_fields:
                    if filter_field:
                        filter_col = filter_field.split(".", 1)[-1] if "." in filter_field else filter_field
                        try:
                            # Check original LazyFrame (before filter) for filter column values
                            # Use "is not None" and isinstance to avoid LazyFrame in boolean context
                            original_lf = lazyframes.get(table_name)
                            if original_lf is not None and isinstance(original_lf, pl.LazyFrame):
                                # Get unique values and their counts
                                filter_sample = original_lf.select(pl.col(filter_col)).collect()
                                if not filter_sample.is_empty():
                                    # Get value counts to see what values actually exist
                                    value_counts = filter_sample[filter_col].value_counts().sort("count", descending=True)
                                    unique_values = value_counts[filter_col].head(20).to_list()
                                    value_counts_list = value_counts.head(20).to_dict(as_series=False)
                                    
                                    # Get all filter conditions for this column (may be a range: >= and <)
                                    filter_dicts = [f for f in (filter_spec if isinstance(filter_spec, list) else [filter_spec]) if isinstance(f, dict) and f.get("field") == filter_field]
                                    is_range_filter = len(filter_dicts) > 1
                                    filter_dict = filter_dicts[0] if filter_dicts else None
                                    filter_value = filter_dict.get("value") if filter_dict else None
                                    filter_operator = filter_dict.get("operator", "==") if filter_dict else "=="
                                    
                                    if is_range_filter:
                                        range_desc = ", ".join(f"{d.get('operator', '?')} {d.get('value', '?')}" for d in filter_dicts)
                                        logger.warning(
                                            f"[_execute_single_aggregation] ⚠️ No data after applying date range filter on '{filter_col}': {range_desc}. "
                                            f"Column has {len(unique_values)} unique value(s). Top values: {unique_values[:10]}. "
                                            f"Value counts: {value_counts_list}"
                                        )
                                    else:
                                        logger.warning(
                                            f"[_execute_single_aggregation] ⚠️ No data after filtering '{filter_col}' {filter_operator} '{filter_value}'. "
                                            f"Column has {len(unique_values)} unique values. Top values: {unique_values[:10]}. "
                                            f"Value counts: {value_counts_list}"
                                        )
                                    
                                    # For equality filter only: check if filter value exists in the data (NOT for range filters)
                                    if not is_range_filter and filter_value is not None:
                                        if filter_value in unique_values:
                                            logger.warning(f"[_execute_single_aggregation] Filter value '{filter_value}' EXISTS in data but filter still returned no rows - possible type mismatch or operator issue")
                                        else:
                                            logger.warning(f"[_execute_single_aggregation] Filter value '{filter_value}' NOT FOUND in data. Available values: {unique_values[:20]}")
                        except Exception as e:
                            logger.warning(f"[_execute_single_aggregation] Could not check filter column values: {e}")
        except Exception as e:
            logger.warning(f"[_execute_single_aggregation] Could not get sample values: {e}")
    
    # Build aggregation expression
    # For COUNT(*), pass None as col_name
    agg_expr = _build_agg_expression(col_name if not is_count_star else None, func)
    
    # Grouped or scalar?
    # CRITICAL: Check if group_by_spec is not None and is a string, not use it directly in boolean context
    if group_by_spec is not None and isinstance(group_by_spec, str) and group_by_spec.strip():
        return _execute_grouped_aggregation(
            lf, agg_expr, group_by_spec, col_name, lazyframes, table_name, filter_spec
        )
    else:
        # Scalar aggregation
        result_df = lf.select(agg_expr.alias("value")).collect()
        if result_df.is_empty():
            return None
        return to_json_serializable(result_df["value"][0])


def _build_agg_expression(col_name: Optional[str], func: str) -> pl.Expr:
    """Build Polars aggregation expression.
    
    Handles numeric columns that may be stored as strings with comma separators.
    Automatically cleans strings like "32,292.48" before conversion.
    Also handles NaN values by filtering them out before aggregation.
    
    Args:
        col_name: Column name, or None for COUNT(*)
        func: Aggregation function name
    """
    func_lower = func.lower()
    
    # Special handling for COUNT(*) - col_name is None
    if col_name is None and func_lower == "count":
        # COUNT(*) - count all rows
        return pl.len()
    
    # For all other aggregations, col_name must be provided
    if col_name is None:
        raise ValueError(f"Column name required for aggregation '{func}'")
    
    col = pl.col(col_name)
    
    # For numeric aggregations, clean string columns with commas
    if func_lower in ["sum", "mean", "avg", "average"]:
        # Convert to string, remove commas, remove "nan" strings, then cast to float
        # This handles both string columns with commas and already-numeric columns
        # CRITICAL: Replace "nan" strings with empty to avoid NaN propagation
        cleaned_col = (
            col.cast(pl.Utf8, strict=False)
            .str.replace_all(",", "")
            .str.replace_all(r"(?i)^nan$", "")  # Remove "nan", "NaN", "NAN" strings
            .str.replace_all(r"(?i)^null$", "")  # Remove "null" strings
            .str.replace_all(r"^$", "")  # Keep empty as empty
            .cast(pl.Float64, strict=False)
        )
        
        # CRITICAL: Filter out NaN values before aggregation
        # This ensures groups with only NaN values return null instead of NaN
        # which is then properly converted to None in JSON serialization
        if func_lower == "sum":
            # Use .filter to exclude NaN values, then sum
            return cleaned_col.fill_nan(None).sum()
        elif func_lower in ["mean", "avg", "average"]:
            return cleaned_col.fill_nan(None).mean()
    elif func_lower == "min":
        # For min/max, handle NaN by converting to null first
        return col.cast(pl.Float64, strict=False).fill_nan(None).min()
    elif func_lower == "max":
        return col.cast(pl.Float64, strict=False).fill_nan(None).max()
    elif func_lower == "count":
        # COUNT(column) - count non-null values in column
        return col.count()
    elif func_lower in ["count_distinct", "distinct", "nunique"]:
        return col.n_unique()
    else:
        # Default to sum with cleaning
        logger.warning(f"Unknown aggregation '{func}', using sum")
        cleaned_col = (
            col.cast(pl.Utf8, strict=False)
            .str.replace_all(",", "")
            .str.replace_all(r"(?i)^nan$", "")  # Remove "nan" strings
            .str.replace_all(r"(?i)^null$", "")  # Remove "null" strings
            .cast(pl.Float64, strict=False)
        )
        return cleaned_col.fill_nan(None).sum()



def _execute_grouped_aggregation(
    lf: pl.LazyFrame,
    agg_expr: pl.Expr,
    group_by_spec: str,
    value_col: str,
    lazyframes: Dict[str, pl.LazyFrame],
    table_name: Optional[str] = None,
    filter_spec: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
) -> List[Dict[str, Any]]:
    """Execute grouped aggregation and return list of {group, value}.
    
    The LLM provides the correct filters for each aggregation.
    We simply execute the grouping as specified without any automatic modifications.
    """
    # Build group expression and validate column exists in target LazyFrame
    # Pass table_name as preferred table to ensure column resolution from correct table
    group_expr, alias, kind = build_period_expression(group_by_spec, lazyframes, preferred_table=table_name)
    
    # Check if build_period_expression returned None (invalid group_by_spec)
    if group_expr is None or alias is None or kind is None:
        logger.error(f"[_execute_grouped_aggregation] Invalid group_by_spec: '{group_by_spec}'")
        raise ValueError(f"Invalid group_by specification: '{group_by_spec}'")
    
    # Validate that the group_by column actually exists in the target LazyFrame
    # This prevents errors when column is resolved from a different table
    schema = lf.collect_schema()
    available_cols = list(schema.keys())
    
    # For plain column names (not period functions), validate the column exists
    if kind == "group":
        # For plain columns, alias is the column name
        if alias not in schema:
            raise ValueError(
                f"Column '{alias}' from group_by '{group_by_spec}' not found in table. "
                f"Available columns: {available_cols}"
            )
    
    # CRITICAL: Filters are already applied in _execute_single_aggregation before calling this function
    # The lf passed here is already filtered, so we don't need to apply filter_spec again
    # However, we still need to filter out invalid dates to prevent panic errors
    
    # Proactively filter out invalid dates before grouping to prevent panic
    # Get schema to identify date columns
    schema = lf.collect_schema()
    lf_filtered = lf
    
    # Filter out invalid dates from all date columns (prevent out-of-range date panic)
    for col_name, col_type in schema.items():
        if "date" in str(col_type).lower() or "time" in str(col_type).lower() or "datetime" in str(col_type).lower():
            try:
                # Filter nulls and dates outside reasonable range (1900-2100)
                # This prevents "out-of-range date" panic errors
                lf_filtered = lf_filtered.filter(
                    (pl.col(col_name).is_not_null()) &
                    (pl.col(col_name) >= pl.date(1900, 1, 1)) &
                    (pl.col(col_name) <= pl.date(2100, 12, 31))
                )
            except Exception:
                # If date filtering fails (e.g., column is not actually a date type), just filter nulls
                try:
                    lf_filtered = lf_filtered.filter(pl.col(col_name).is_not_null())
                except Exception:
                    # If even null filtering fails, skip this column
                    pass
    
    # Log filter status for debugging
    if filter_spec:
        logger.debug(f"[_execute_grouped_aggregation] Group by '{group_by_spec}' with filters already applied: {filter_spec}")
    
    # For weekly grouping, log date column info for debugging
    if kind == "period" and "week" in group_by_spec.lower():
        # Try to identify the date column used for grouping
        date_col_match = re.search(r'week\(([^)]+)\)', group_by_spec, re.IGNORECASE)
        if date_col_match:
            date_col_ref = date_col_match.group(1).strip()
            date_col_name = date_col_ref.split(".", 1)[-1] if "." in date_col_ref else date_col_ref
            try:
                # Check sample dates to verify they're in the filter range
                sample_dates = lf_filtered.select(pl.col(date_col_name)).head(10).collect()
                if not sample_dates.is_empty():
                    logger.info(f"[_execute_grouped_aggregation] Weekly grouping on '{date_col_name}': sample dates = {sample_dates[date_col_name].to_list()}")
            except Exception as e:
                logger.debug(f"[_execute_grouped_aggregation] Could not get sample dates for weekly grouping: {e}")
    
    # Execute the grouping as specified by the LLM
    # Add error handling for any remaining date issues
    try:
        result_df = (
            lf_filtered
            .with_columns(group_expr.alias("_group_key"))
            .group_by("_group_key")
            .agg(agg_expr.alias("value"))
            .sort("_group_key")
            .collect()
        )
        
        # Log aggregation results for debugging
        if not result_df.is_empty():
            sample_rows = result_df.head(5)
            sample_values = [row["value"] for row in sample_rows.iter_rows(named=True)]
            sample_groups = [str(row["_group_key"]) for row in sample_rows.iter_rows(named=True)]
            logger.info(f"[_execute_grouped_aggregation] Aggregation result: {len(result_df)} groups, sample groups: {sample_groups}, sample raw values: {sample_values}")
            
            # Check for null/NaN values
            null_count = result_df.select(pl.col("value").is_null().sum()).item()
            
            # Also check for NaN values in the result
            nan_count = 0
            try:
                nan_count = result_df.select(pl.col("value").is_nan().sum()).item()
            except Exception:
                pass  # is_nan not available for non-float columns
            
            total_missing = null_count + nan_count
            non_null_count = len(result_df) - total_missing
            
            if total_missing > 0:
                logger.warning(f"[_execute_grouped_aggregation] Found {null_count} null + {nan_count} NaN = {total_missing}/{len(result_df)} missing values in aggregation result (column: '{value_col}', table: '{table_name}')")
                if non_null_count > 0:
                    logger.info(f"[_execute_grouped_aggregation] {non_null_count} groups have valid values")
            
            # CRITICAL: If ALL values are null/NaN, this is a problem
            if total_missing == len(result_df):
                logger.error(
                    f"[_execute_grouped_aggregation] ⚠️ ALL {len(result_df)} aggregation values are NULL or NaN! "
                    f"This suggests a data issue. Column: '{value_col}', Table: '{table_name}', "
                    f"Group by: '{group_by_spec}'. Check if column exists and has non-null/non-NaN values."
                )
                # Try to get a sample of the actual column data to diagnose
                try:
                    sample_col_data = lf_filtered.select(pl.col(value_col)).head(10).collect()
                    if not sample_col_data.is_empty():
                        sample_vals = sample_col_data[value_col].to_list()
                        logger.error(f"[_execute_grouped_aggregation] Sample values from column '{value_col}': {sample_vals}")
                except Exception as e:
                    logger.error(f"[_execute_grouped_aggregation] Could not get sample column data: {e}")
        else:
            logger.warning(f"[_execute_grouped_aggregation] Aggregation returned empty result")
            
    except Exception as e:
        error_msg = str(e)
        # Check if it's still an out-of-range date error (shouldn't happen after filtering, but just in case)
        if "out-of-range date" in error_msg.lower() or "panic" in error_msg.lower() or "PanicException" in error_msg:
            logger.error(f"Out-of-range date error persisted after filtering. Returning empty result: {error_msg}")
            return []
        else:
            # Re-raise if it's not a date-related error
            raise
    
    return [
        {"group": str(row["_group_key"]), "value": to_json_serializable(row["value"])}
        for row in result_df.iter_rows(named=True)
    ]


def _build_error_message(
    metric_key: str,
    agg_spec: Dict[str, Any],
    error: Exception,
) -> str:
    """Build descriptive error message for failed aggregation."""
    # Handle case where agg_spec might not be a dict
    if not isinstance(agg_spec, dict):
        return f"Invalid aggregation spec (expected dict, got {type(agg_spec).__name__}) - {type(error).__name__}: {str(error)}"
    
    func = (agg_spec.get("agg") or "sum").upper()
    col = agg_spec.get("column", "")
    group_by = agg_spec.get("group_by", "")
    
    context = f"{func}({col})"
    if group_by:
        context += f" GROUP BY {group_by}"
    
    error_msg = str(error)
    if not error_msg.strip():
        return f"Unknown error during {context}"
    return f"{context} - {type(error).__name__}: {error_msg}"


# =============================================================================
# CHART DATA PREPARATION
# =============================================================================

def prepare_chart_data(
    aggregation_results: Dict[str, Any],
    x_field: Optional[str],
    aggregations: Dict[str, Any],
    max_points: int = 15,
    sort_descending: bool = True,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Prepare chart data from aggregation results.
    
    Args:
        aggregation_results: Results from execute_aggregations_polars
        x_field: X-axis field name
        aggregations: Original aggregation specifications
        max_points: Maximum data points for chart
        sort_descending: True to sort high to low (for "top"), False to sort low to high (for "bottom/lowest")
        
    Returns:
        Tuple of (chart_data, metadata)
    """
    chart_data = []
    
    if len(aggregations) == 1:
        # Single aggregation - simple conversion
        agg_key = list(aggregations.keys())[0]
        result = aggregation_results.get(agg_key, [])
        
        if isinstance(result, list):
            agg_spec = aggregations[agg_key]
            col_ref = agg_spec.get("column", "")
            y_field = col_ref.split(".")[-1] if "." in col_ref else col_ref
            
            chart_data = [
                {x_field: item.get("group"), y_field: item.get("value")}
                for item in result
                if x_field
            ]
    else:
        # Multiple aggregations - combine by group
        grouped = {}
        for agg_key, result in aggregation_results.items():
            if isinstance(result, list):
                series_label = agg_key.replace("_", " ").title()
                for item in result:
                    group = item.get("group")
                    if group not in grouped:
                        grouped[group] = {x_field: group} if x_field else {}
                    grouped[group][series_label] = item.get("value")
        chart_data = list(grouped.values())
    
    # Apply limiting
    metadata = {
        "is_limited": False,
        "original_count": len(chart_data),
        "displayed_count": len(chart_data),
    }
    
    if len(chart_data) > max_points:
        metadata["is_limited"] = True
        metadata["displayed_count"] = max_points
        
        # Sort by first numeric value (direction based on sort_descending parameter)
        if chart_data:
            first_row = chart_data[0]
            sort_key = next(
                (k for k, v in first_row.items() if k != x_field and isinstance(v, (int, float))),
                None
            )
            if sort_key:
                chart_data = sorted(
                    chart_data,
                    key=lambda x: x.get(sort_key, 0) or 0,
                    reverse=sort_descending
                )[:max_points]
            else:
                chart_data = chart_data[:max_points]
    
    return convert_result_dict(chart_data), metadata


# =============================================================================
# EXPORT UTILITIES (STREAMING)
# =============================================================================

def stream_to_parquet(
    lf: pl.LazyFrame,
    output_path: str,
) -> int:
    """Stream LazyFrame to Parquet file.
    
    Uses Polars sink_parquet for memory-efficient export.
    
    Args:
        lf: LazyFrame to export
        output_path: Path for output Parquet file
        
    Returns:
        Number of rows written
    """
    # Use sink_parquet for streaming (doesn't materialize full dataset)
    lf.sink_parquet(output_path)
    
    # Get row count (scan without loading data)
    return pl.scan_parquet(output_path).select(pl.len()).collect().item()


def stream_to_csv(
    lf: pl.LazyFrame,
    output_path: str,
) -> int:
    """Stream LazyFrame to CSV file.
    
    Uses Polars sink_csv for memory-efficient export.
    
    Args:
        lf: LazyFrame to export
        output_path: Path for output CSV file
        
    Returns:
        Number of rows written
    """
    lf.sink_csv(output_path)
    
    # Get row count
    return pl.scan_csv(output_path).select(pl.len()).collect().item()


# =============================================================================
# UTILITY: BUILD CALCULATION CHAIN METADATA
# =============================================================================

def build_calculation_chain(
    metric_key: str,
    operation_plan: Dict[str, Any],
    results: Dict[str, Any],
) -> Dict[str, Any]:
    """Build calculation chain metadata for a metric.
    
    Shows how a metric is calculated, including dependencies for derived metrics.
    
    Args:
        metric_key: Name of the metric
        operation_plan: Full operation plan
        results: Computed results
        
    Returns:
        Calculation chain dictionary
    """
    aggregations = operation_plan.get("aggregations", {})
    derived = operation_plan.get("derived", {})
    
    if metric_key in aggregations:
        agg_spec = aggregations[metric_key]
        func = (agg_spec.get("agg") or "sum").upper()
        column = agg_spec.get("column", "")
        group_by = agg_spec.get("group_by")
        
        formula = f"{func}({column})"
        if group_by:
            formula += f" GROUP BY {group_by}"
        
        return {
            "type": "direct",
            "formula": formula,
            "description": formula,
            "operation": func,
            "column": column,
            "group_by": group_by,
            "dependencies": [],
        }
    
    elif metric_key in derived:
        formula = derived[metric_key]
        
        # Handle case where formula is a dict with 'expression' key
        if isinstance(formula, dict):
            formula = formula.get('expression', formula.get('formula', ''))
        
        # Ensure formula is a string
        if not isinstance(formula, str):
            return {
                "type": "derived",
                "formula": str(formula),
                "description": f"Invalid formula type for {metric_key}",
                "dependencies": [],
            }
        
        # Extract dependencies
        metric_refs = re.findall(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', formula)
        dependencies = [
            ref for ref in metric_refs
            if ref in aggregations or ref in derived
            if ref != metric_key
        ]
        
        return {
            "type": "derived",
            "formula": formula,
            "description": f"Calculated from: {', '.join(dependencies)}" if dependencies else formula,
            "dependencies": dependencies,
        }
    
    return {
        "type": "unknown",
        "formula": "",
        "description": f"Unknown calculation for {metric_key}",
        "dependencies": [],
    }
