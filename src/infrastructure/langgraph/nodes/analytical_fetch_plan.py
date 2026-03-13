"""Analytical Fetch Plan Node — dimension-based SAP fetch optimization.

**No LLM** — This node is deterministic. It parses the known structure of
``chart_preplan`` and ``analysis_plan`` (from chart_preplan and financial_analyst_planner)
to extract column names (group_by, metrics[].column, columns_used) and builds
one API call per (view, dimension) with at most one dimension per call.

Consumes chart and financial plans from Phase I and converts them into
optimized SAP OData fetch instructions.

Data source (only these drive API calls)
────────────────────────────────────────
* **Chart name (chart_id)** — which charts use which fetch; used for mapping only.
* **Column names used** — from chart_preplan (group_by, metrics[].column) and
  analysis_plan (columns_used, group_by). No columns are added from elsewhere.
* **Per view** — each API call is for one source_view; requirements are grouped
  by (view, dimension set). Filters come from chart date_filter / metric
  context when present; when a bucket has no filters, date filter comes from
  LLM (analytical_column_selection analytical_date_filter) when available.

Design principles
─────────────────
* For each chart / metric, list exactly which columns it needs.
* Validate every column against the view's known dimensions and measures
  (from ``filtered_analytical_dimensions`` / ``filtered_analytical_measures``).
* **No limit on measure columns** — include every measure the chart needs.
* **One dimension per $select** — SAP analytical views aggregate by whatever
  dimensions appear in ``$select``. We use exactly one dimension per API call
  to avoid cartesian explosion and keep payloads small.
* Charts that share the *same* dimension set on the *same* view are merged
  into a single API call (their measures are unioned).
* Totals (summary) fetches (0 dimensions) are not generated; KPIs use dimension
  slices for resolution when needed.
* Results are stored in ``raw_dataframes`` with tagged keys
  (e.g. ``ViewName__by_Plant``) so downstream nodes can look up the correct
  pre-aggregated dataset via ``resolve_dataset_key``.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any, Dict, FrozenSet, List, Optional, Set, Tuple

from ..state import AnalyticsState
from ..utils import log_llm_invalid_columns

logger = logging.getLogger(__name__)

# OData date types — used to pick a date column for default API $filter when plan has none
_DATE_EDM_TYPES = {"Edm.Date", "Edm.DateTimeOffset", "Edm.TimeOfDay"}

# Hard limit: max dimension columns per $select for analytical views.
# SAP expects one dimension per $select; more → cartesian explosion → massive payload.
MAX_DIMENSIONS_PER_SELECT = 1

# Dataset key format for raw_dataframes (must match polars_engine.ANALYTICAL_KEY_BY_PREFIX and utils.resolve_dataset_key):
#   - Dimension slice: f"{view}__by_{dim_suffix}"  e.g. AM_Sales_Order_v1_Summary__by_Fiscal_Week_Fiscal_Hier_Ke
#   - Totals:          f"{view}__totals"
ANALYTICAL_KEY_BY_PREFIX = "__by_"
ANALYTICAL_KEY_TOTALS_SUFFIX = "__totals"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_view_column(ref: str) -> Tuple[str, str]:
    """Split ``view.column`` → (view, column).

    Returns ("", "") when the reference is invalid.
    Strips function wrappers like ``month(view.col)`` → ``view.col``.
    """
    if not ref or not isinstance(ref, str):
        return ("", "")
    func_match = re.match(r"^\w+\((.+)\)$", ref.strip())
    if func_match:
        ref = func_match.group(1).strip()
    parts = ref.split(".", 1)
    if len(parts) == 2 and parts[0] and parts[1]:
        return (parts[0].strip(), parts[1].strip())
    return ("", "")


def _normalize_filter_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _extract_column_refs_from_obj(obj: Any, collected: Set[str]) -> None:
    """Recursively collect view.column-style refs from dicts/lists (column, date_column, group_by, columns, etc.)."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in ("column", "date_column", "group_by") and isinstance(v, str) and v.strip():
                collected.add(v.strip())
            elif k in ("columns", "group_by_columns", "dimensions") and isinstance(v, list):
                for item in v:
                    if isinstance(item, str) and item.strip():
                        collected.add(item.strip())
            else:
                _extract_column_refs_from_obj(v, collected)
    elif isinstance(obj, list):
        for item in obj:
            _extract_column_refs_from_obj(item, collected)


def _extract_all_columns_from_chart(chart: Dict[str, Any]) -> Tuple[str, str, List[str]]:
    """Extract all column references from one chart. Returns (view, dim_col, list of other column names).
    Dimension comes from group_by; all other refs (metrics[].column, columns, dimensions, etc.) are returned
    for caller to classify as measure/dimension and add to the same view's bucket.
    """
    group_by = (chart.get("group_by") or "") if isinstance(chart.get("group_by"), str) else ""
    dim_view, dim_col = _parse_view_column(group_by)

    all_refs: Set[str] = set()
    _extract_column_refs_from_obj(chart, all_refs)

    other_cols: List[str] = []
    seen: Set[str] = set()
    for ref in all_refs:
        v, c = _parse_view_column(ref)
        if not c:
            continue
        if c in seen:
            continue
        # Dimension column (group_by) — skip in "other"; caller uses group_by separately
        if dim_col and c == dim_col:
            continue
        seen.add(c)
        other_cols.append(c)
    return (dim_view or "", dim_col or "", other_cols)


def _chart_date_filters_to_odata(
    date_filter: Dict[str, Any],
    source_view: str,
) -> List[Dict[str, Any]]:
    """Convert a chart preplan ``date_filter`` to OData filter dicts."""
    if not date_filter or not isinstance(date_filter, dict):
        return []
    date_col_ref = date_filter.get("date_column", "")
    _, date_col = _parse_view_column(date_col_ref)
    if not date_col:
        date_col = date_col_ref.strip() if date_col_ref else ""
    if not date_col:
        return []

    filters: List[Dict[str, Any]] = []
    start = date_filter.get("start_date")
    end = date_filter.get("end_date")
    if start:
        v = _normalize_filter_value(start)
        filters.append({"column": date_col, "operator": "ge", "value": v, "odata_syntax": f"{date_col} ge {v}"})
    if end:
        v = _normalize_filter_value(end)
        filters.append({"column": date_col, "operator": "le", "value": v, "odata_syntax": f"{date_col} le {v}"})
    return filters


def _date_columns_by_view(
    filtered_dims: List[Dict[str, Any]],
) -> Dict[str, List[str]]:
    """Build view_name -> [date column names] from dimensions with date data_type."""
    by_view: Dict[str, List[str]] = {}
    for d in filtered_dims or []:
        if not isinstance(d, dict):
            continue
        view_name = (d.get("view_name") or "").strip()
        name = (d.get("name") or "").strip()
        data_type = (d.get("data_type") or "").strip()
        if not name:
            continue
        if data_type in _DATE_EDM_TYPES:
            if view_name not in by_view:
                by_view[view_name] = []
            by_view[view_name].append(name)
    return by_view


def _schema_column_to_data_type(
    filtered_dims: List[Dict[str, Any]],
    filtered_meas: List[Dict[str, Any]],
) -> Dict[str, str]:
    """Build map column name -> data_type from schema (dimensions + measures). First occurrence wins."""
    out: Dict[str, str] = {}
    for col in (filtered_dims or []) + (filtered_meas or []):
        if not isinstance(col, dict):
            continue
        name = (col.get("name") or "").strip()
        if not name or name in out:
            continue
        out[name] = (col.get("data_type") or "Edm.String").strip()
    return out


def _value_filters_to_odata(
    value_filters: List[Dict[str, Any]],
    schema_column_to_data_type: Optional[Dict[str, str]] = None,
    node_name: str = "analytical_fetch_plan",
) -> List[Dict[str, Any]]:
    """Convert value_filters from analytical_column_selection (column, operator, value, data_type) to bucket filter format with odata_syntax.
    If schema_column_to_data_type is provided, use schema data_type for each column (override LLM type when different) so API filter is correct."""
    result: List[Dict[str, Any]] = []
    op_map = {"eq": "eq", "ne": "ne", "gt": "gt", "ge": "ge", "lt": "lt", "le": "le", "=": "eq", "!=": "ne", "<>": "ne", ">": "gt", ">=": "ge", "<": "lt", "<=": "le"}
    schema_types = schema_column_to_data_type or {}
    for vf in value_filters or []:
        if not isinstance(vf, dict):
            continue
        col = (vf.get("column") or "").strip()
        if not col:
            continue
        op = op_map.get((vf.get("operator") or "eq").strip().lower(), "eq")
        val = vf.get("value")
        llm_data_type = (vf.get("data_type") or "Edm.String").strip()
        data_type = schema_types.get(col, llm_data_type)
        if schema_types and col in schema_types and schema_types[col] != llm_data_type:
            logger.info(
                f"[{node_name}] value_filters: column {col!r} LLM type {llm_data_type!r} != schema type {schema_types[col]!r} — using schema type for API filter"
            )
        if data_type in ("Edm.Int64", "Edm.Int32", "Edm.Decimal", "Edm.Double", "Edm.Single"):
            try:
                if isinstance(val, (int, float)):
                    val_str = str(int(val) if data_type in ("Edm.Int64", "Edm.Int32") else val)
                else:
                    val_str = str(int(float(val))) if data_type in ("Edm.Int64", "Edm.Int32") else str(float(val))
            except (ValueError, TypeError):
                val_str = str(val)
            odata_syntax = f"{col} {op} {val_str}"
        else:
            val_str = str(val).replace("'", "''") if val is not None else ""
            odata_syntax = f"{col} {op} '{val_str}'"
        result.append({"column": col, "operator": op, "value": val, "odata_syntax": odata_syntax})
    return result


def _inject_value_filters_into_buckets(
    buckets: Dict[Tuple[str, FrozenSet[str]], "_FetchBucket"],
    totals_bucket: Dict[str, "_FetchBucket"],
    value_filters: List[Dict[str, Any]],
    node_name: str,
    schema_column_to_data_type: Optional[Dict[str, str]] = None,
) -> None:
    """Add value_filters (e.g. plant eq '1100') to every bucket so all API calls get the same filter.
    Schema data_type is used for each column when building OData syntax (overrides LLM type if different)."""
    if not value_filters:
        return
    odata_filters = _value_filters_to_odata(
        value_filters,
        schema_column_to_data_type=schema_column_to_data_type,
        node_name=node_name,
    )
    if not odata_filters:
        return
    for bucket in buckets.values():
        bucket.add_filters(odata_filters)
    for bucket in totals_bucket.values():
        bucket.add_filters(odata_filters)
    logger.info(
        f"[{node_name}] Injected {len(odata_filters)} value filter(s) into all buckets: {[f.get('column') for f in odata_filters]}"
    )


def _inject_date_filters_from_llm(
    buckets: Dict[Tuple[str, FrozenSet[str]], "_FetchBucket"],
    totals_bucket: Dict[str, "_FetchBucket"],
    date_columns_by_view: Dict[str, List[str]],
    node_name: str,
    llm_date_filter: Optional[Dict[str, Any]] = None,
) -> None:
    """If a bucket has no filters but its view has date columns, add date filter from LLM (analytical_column_selection). No hardcoded default."""
    if not date_columns_by_view:
        return
    if not llm_date_filter or not isinstance(llm_date_filter, dict):
        return
    date_col = (llm_date_filter.get("date_column") or "").strip()
    start_str = (llm_date_filter.get("start_date") or "").strip()
    end_str = (llm_date_filter.get("end_date") or "").strip()
    if not date_col or not start_str or not end_str:
        return
    for (view, _), bucket in buckets.items():
        if bucket.filters:
            continue
        date_cols = date_columns_by_view.get(view)
        if not date_cols or date_col not in date_cols:
            continue
        filters = [
            {"column": date_col, "operator": "ge", "value": start_str, "odata_syntax": f"{date_col} ge {start_str}"},
            {"column": date_col, "operator": "le", "value": end_str, "odata_syntax": f"{date_col} le {end_str}"},
        ]
        bucket.add_filters(filters)
        logger.info(
            f"[{node_name}] Using LLM date filter on '{date_col}' ({start_str} to {end_str}) for fetch {view}"
        )
    for view, bucket in totals_bucket.items():
        if bucket.filters:
            continue
        date_cols = date_columns_by_view.get(view)
        if not date_cols or date_col not in date_cols:
            continue
        filters = [
            {"column": date_col, "operator": "ge", "value": start_str, "odata_syntax": f"{date_col} ge {start_str}"},
            {"column": date_col, "operator": "le", "value": end_str, "odata_syntax": f"{date_col} le {end_str}"},
        ]
        bucket.add_filters(filters)
        logger.info(
            f"[{node_name}] Using LLM date filter on '{date_col}' ({start_str} to {end_str}) for totals {view}"
        )


def _inject_date_filters_from_llm_by_view(
    buckets: Dict[Tuple[str, FrozenSet[str]], "_FetchBucket"],
    totals_bucket: Dict[str, "_FetchBucket"],
    date_columns_by_view: Dict[str, List[str]],
    node_name: str,
    llm_date_filter_by_view: Optional[Dict[str, Dict[str, Any]]] = None,
) -> None:
    """Inject per-view date filters when multiple analytical views are present.

    Each view may have a different date column; this mapping lets us inject the
    correct filter into each view's buckets without relying on a single global
    date_column.
    """
    if not date_columns_by_view:
        return
    if not llm_date_filter_by_view or not isinstance(llm_date_filter_by_view, dict):
        return

    def _get_for_view(view: str) -> Optional[Dict[str, Any]]:
        d = llm_date_filter_by_view.get(view)
        return d if isinstance(d, dict) else None

    for (view, _), bucket in buckets.items():
        if bucket.filters:
            continue
        view_filter = _get_for_view(view)
        if not view_filter:
            continue
        date_col = (view_filter.get("date_column") or "").strip()
        start_str = (view_filter.get("start_date") or "").strip()
        end_str = (view_filter.get("end_date") or "").strip()
        if not date_col or not start_str or not end_str:
            continue
        date_cols = date_columns_by_view.get(view)
        if not date_cols or date_col not in date_cols:
            continue
        filters = [
            {"column": date_col, "operator": "ge", "value": start_str, "odata_syntax": f"{date_col} ge {start_str}"},
            {"column": date_col, "operator": "le", "value": end_str, "odata_syntax": f"{date_col} le {end_str}"},
        ]
        bucket.add_filters(filters)
        logger.info(
            f"[{node_name}] Using per-view date filter on '{date_col}' ({start_str} to {end_str}) for fetch {view}"
        )

    for view, bucket in totals_bucket.items():
        if bucket.filters:
            continue
        view_filter = _get_for_view(view)
        if not view_filter:
            continue
        date_col = (view_filter.get("date_column") or "").strip()
        start_str = (view_filter.get("start_date") or "").strip()
        end_str = (view_filter.get("end_date") or "").strip()
        if not date_col or not start_str or not end_str:
            continue
        date_cols = date_columns_by_view.get(view)
        if not date_cols or date_col not in date_cols:
            continue
        filters = [
            {"column": date_col, "operator": "ge", "value": start_str, "odata_syntax": f"{date_col} ge {start_str}"},
            {"column": date_col, "operator": "le", "value": end_str, "odata_syntax": f"{date_col} le {end_str}"},
        ]
        bucket.add_filters(filters)
        logger.info(
            f"[{node_name}] Using per-view date filter on '{date_col}' ({start_str} to {end_str}) for totals {view}"
        )


# ---------------------------------------------------------------------------
# Column validation: per-view bucket (only accept columns that belong to that view)
# ---------------------------------------------------------------------------

def _build_view_column_lookup(
    filtered_dims: List[Dict[str, Any]],
    filtered_meas: List[Dict[str, Any]],
) -> Tuple[Dict[str, Tuple[Set[str], Set[str]]], Set[str], Set[str]]:
    """Build per-view column sets so we only accept columns that exist in that view's bucket.

    Returns:
        view_columns: view_name -> (dims_set, measures_set). Use "" for items without view_name (fallback).
        known_dims, known_meas: global sets for fallback when view is missing from view_columns.
    """
    view_columns: Dict[str, Tuple[Set[str], Set[str]]] = {}
    known_dims: Set[str] = set()
    known_meas: Set[str] = set()

    def _add(view_key: str, name: str, is_dim: bool) -> None:
        if not name:
            return
        if view_key not in view_columns:
            view_columns[view_key] = (set(), set())
        dims, meas = view_columns[view_key]
        if is_dim:
            dims.add(name)
            known_dims.add(name)
        else:
            meas.add(name)
            known_meas.add(name)

    for d in filtered_dims or []:
        if not isinstance(d, dict):
            continue
        name = (d.get("name") or "").strip()
        view_name = (d.get("view_name") or "").strip()
        view_key = view_name or ""
        _add(view_key, name, True)

    for m in filtered_meas or []:
        if not isinstance(m, dict):
            continue
        name = (m.get("name") or "").strip()
        view_name = (m.get("view_name") or "").strip()
        view_key = view_name or ""
        _add(view_key, name, False)

    return view_columns, known_dims, known_meas


def _column_in_view(
    col: str,
    view: str,
    view_columns: Dict[str, Tuple[Set[str], Set[str]]],
    known_dims: Set[str],
    known_meas: Set[str],
) -> str:
    """Return 'dimension', 'measure', or 'unknown'. Only accepts col if it is in that view's bucket."""
    dims, meas = view_columns.get(view) or view_columns.get("") or (set(), set())
    if not dims and not meas:
        # No per-view data for this view — fallback to global
        dims, meas = known_dims, known_meas
    col_lower = col.lower()
    if col in dims or any(d.lower() == col_lower for d in dims):
        return "dimension"
    if col in meas or any(m.lower() == col_lower for m in meas):
        return "measure"
    return "unknown"


# ---------------------------------------------------------------------------
# Requirement bucket — accumulates columns for (view, dims_frozenset)
# ---------------------------------------------------------------------------

class _FetchBucket:
    """Accumulates requirements for a unique (view, dimension-set) pair."""

    __slots__ = ("view", "dimensions", "measures", "filters",
                 "chart_ids", "metric_ids")

    def __init__(self, view: str, dimensions: FrozenSet[str]) -> None:
        self.view = view
        self.dimensions: Set[str] = set(dimensions)
        self.measures: Set[str] = set()
        self.filters: List[Dict[str, Any]] = []
        self.chart_ids: List[str] = []
        self.metric_ids: List[str] = []

    def add_measures(self, cols: List[str]) -> None:
        self.measures.update(c for c in cols if c)

    def add_filters(self, flt: List[Dict[str, Any]]) -> None:
        existing = {f.get("odata_syntax", "") for f in self.filters}
        for f in flt:
            syntax = f.get("odata_syntax", "")
            if syntax and syntax not in existing:
                self.filters.append(f)
                existing.add(syntax)
            elif not syntax:
                self.filters.append(f)

    def add_chart_id(self, cid: str) -> None:
        if cid and cid not in self.chart_ids:
            self.chart_ids.append(cid)

    def add_metric_id(self, mid: str) -> None:
        if mid and mid not in self.metric_ids:
            self.metric_ids.append(mid)


# ---------------------------------------------------------------------------
# Minimal plan from schema (when no chart_preplan / analysis_plan)
# ---------------------------------------------------------------------------

def _build_minimal_buckets_from_schema(
    view_columns: Dict[str, Tuple[Set[str], Set[str]]],
    buckets: Dict[Tuple[str, FrozenSet[str]], _FetchBucket],
    totals_bucket: Dict[str, _FetchBucket],
    node_name: str = "analytical_fetch_plan",
) -> None:
    """
    When chart_preplan and analysis_plan are missing (e.g. moderate workflow
    without prior chart/financial planning), build one fetch per (view, dimension)
    with all measures for that view so sap_data_fetch still runs.
    """
    for view_key, (dims, meas) in view_columns.items():
        view = view_key if view_key else ""
        if not dims and not meas:
            continue
        meas_list = sorted(meas)
        if not meas_list:
            continue
        if dims:
            for dim in sorted(dims):
                dim_set = frozenset([dim])
                key = (view, dim_set)
                if key not in buckets:
                    buckets[key] = _FetchBucket(view, dim_set)
                buckets[key].add_measures(meas_list)
                buckets[key].add_metric_id("minimal_fetch")
            logger.info(
                f"[{node_name}] Minimal plan: view={view} — {len(dims)} dimension bucket(s), "
                f"each with {len(meas_list)} measure(s)"
            )
        else:
            if view not in totals_bucket:
                totals_bucket[view] = _FetchBucket(view, frozenset())
            totals_bucket[view].add_measures(meas_list)
            totals_bucket[view].add_metric_id("minimal_fetch")
            logger.info(f"[{node_name}] Minimal plan: view={view} — totals bucket with {len(meas_list)} measure(s)")


# ---------------------------------------------------------------------------
# Requirement collection from plans
# ---------------------------------------------------------------------------

def _collect_from_chart_preplan(
    chart_preplan: List[Dict[str, Any]],
    buckets: Dict[Tuple[str, FrozenSet[str]], _FetchBucket],
    totals_bucket: Dict[str, _FetchBucket],
    view_columns: Dict[str, Tuple[Set[str], Set[str]]],
    known_dims: Set[str],
    known_meas: Set[str],
) -> None:
    """Walk chart preplan: extract all columns from each chart; use chart_id/chart_name as id per view.
    Only add columns that exist in that view's bucket (per-view validation). Same (view, dimension) → one API call.
    """
    for idx, chart in enumerate(chart_preplan):
        if not isinstance(chart, dict):
            logger.warning("[analytical_fetch_plan] Skipping non-dict chart entry")
            continue

        chart_id = (chart.get("chart_id") or chart.get("chart_name") or "").strip() or f"chart_{idx}"
        dim_view, dim_col, other_cols = _extract_all_columns_from_chart(chart)
        group_by = chart.get("group_by", "")

        logger.info(
            f"[analytical_fetch_plan] [Chart] id='{chart_id}' — group_by raw='{group_by}' → "
            f"view='{dim_view}', dimension='{dim_col}' | all_other_cols={other_cols}"
        )

        chart_filters: List[Dict[str, Any]] = []
        metric_view = ""
        for metric in chart.get("metrics") or []:
            if not isinstance(metric, dict):
                continue
            col_ref = metric.get("column", "")
            m_view, _ = _parse_view_column(col_ref)
            if m_view and not metric_view:
                metric_view = m_view
            df = metric.get("date_filter")
            if df:
                chart_filters.extend(_chart_date_filters_to_odata(df, dim_view or m_view))
        if chart.get("date_filter"):
            chart_filters.extend(_chart_date_filters_to_odata(chart["date_filter"], dim_view or metric_view))

        view = dim_view or metric_view
        if not view:
            logger.warning(f"[analytical_fetch_plan] Chart '{chart_id}': no view found — skipping")
            continue

        # Validate: only columns that are in this view's bucket (dimensions/measures for this view)
        validated_measures: List[str] = []
        invalid_measures: List[str] = []
        for mc in other_cols:
            role = _column_in_view(mc, view, view_columns, known_dims, known_meas)
            if role == "unknown":
                invalid_measures.append(mc)
                continue
            validated_measures.append(mc)
        if invalid_measures:
            dims_v, meas_v = view_columns.get(view) or view_columns.get("") or (set(), set())
            available_sample = list((dims_v or known_dims) | (meas_v or known_meas))[:20]
            log_llm_invalid_columns(
                source="analytical_fetch_plan.chart_preplan.all_columns",
                node_name="analytical_fetch_plan",
                invalid_columns=invalid_measures,
                available_sample=available_sample,
                context=f"chart_id={chart_id} (view={view})",
            )
            logger.info(
                f"[analytical_fetch_plan] [Chart] id='{chart_id}' — cols not in view '{view}' bucket: {invalid_measures}"
            )

        logger.info(
            f"[analytical_fetch_plan] [Chart] id='{chart_id}' — validated_measures (in view bucket)={validated_measures}"
        )

        dim_set: FrozenSet[str] = frozenset()
        if dim_col:
            role = _column_in_view(dim_col, view, view_columns, known_dims, known_meas)
            if role == "unknown":
                dims_v, meas_v = view_columns.get(view) or view_columns.get("") or (set(), set())
                available_sample = list((dims_v or known_dims) | (meas_v or known_meas))[:20]
                log_llm_invalid_columns(
                    source="analytical_fetch_plan.chart_preplan.group_by",
                    node_name="analytical_fetch_plan",
                    invalid_columns=[dim_col],
                    available_sample=available_sample,
                    context=f"chart_id={chart_id} (view={view})",
                )
            else:
                if role == "measure":
                    logger.warning(
                        f"[analytical_fetch_plan] Chart '{chart_id}': group_by '{dim_col}' is measure — treating as dimension"
                    )
                dim_set = frozenset([dim_col])

        logger.info(
            f"[analytical_fetch_plan] Chart '{chart_id}': view={view}, dims={set(dim_set) or '(totals)'}, "
            f"measures={validated_measures}, filters={len(chart_filters)}"
        )

        if not dim_set:
            # No dimensions → totals bucket
            if view not in totals_bucket:
                totals_bucket[view] = _FetchBucket(view, frozenset())
            totals_bucket[view].add_measures(validated_measures)
            totals_bucket[view].add_filters(chart_filters)
            totals_bucket[view].add_chart_id(chart_id)
            logger.info(
                f"[analytical_fetch_plan] [Chart] '{chart_id}' → totals_bucket[{view}] (no dimension)"
            )
            continue

        key = (view, dim_set)
        if key not in buckets:
            buckets[key] = _FetchBucket(view, dim_set)
        buckets[key].add_measures(validated_measures)
        buckets[key].add_filters(chart_filters)
        buckets[key].add_chart_id(chart_id)
        logger.info(
            f"[analytical_fetch_plan] [Chart] '{chart_id}' → bucket (view={view}, dim={dim_col}); "
            f"merged into same API call as other charts with this dimension"
        )


def _collect_from_analysis_plan(
    analysis_plan: Dict[str, Any],
    buckets: Dict[Tuple[str, FrozenSet[str]], _FetchBucket],
    totals_bucket: Dict[str, _FetchBucket],
    view_columns: Dict[str, Tuple[Set[str], Set[str]]],
    known_dims: Set[str],
    known_meas: Set[str],
) -> None:
    """Walk financial/analysis plan: use columns_used, group_by and view. Only add columns that are in that view's bucket."""

    if not analysis_plan or not isinstance(analysis_plan, dict):
        return
    plan_data = analysis_plan.get("analysis_plan", analysis_plan)
    if not isinstance(plan_data, dict):
        plan_data = analysis_plan
    calc_strategy = plan_data.get("calculation_strategy", {})
    if not isinstance(calc_strategy, dict):
        return

    kpi_count = 0
    trend_count = 0

    for kpi in calc_strategy.get("single_number_kpis", []):
        if not isinstance(kpi, dict):
            continue
        metric_name = kpi.get("metric_name", "")
        metric_key = re.sub(r"\s+", "_", metric_name.strip().lower()) if metric_name else ""
        columns_used = kpi.get("columns_used", [])
        logger.info(
            f"[analytical_fetch_plan] [KPI] metric='{metric_key}' columns_used={columns_used} → totals_bucket"
        )
        for col_ref in columns_used:
            v, c = _parse_view_column(col_ref)
            if not v or not c:
                continue
            role = _column_in_view(c, v, view_columns, known_dims, known_meas)
            if role == "unknown":
                dims_v, meas_v = view_columns.get(v) or view_columns.get("") or (set(), set())
                log_llm_invalid_columns(
                    source="analytical_fetch_plan.analysis_plan.single_number_kpis.columns_used",
                    node_name="analytical_fetch_plan",
                    invalid_columns=[c],
                    available_sample=list((dims_v or known_dims) | (meas_v or known_meas))[:20],
                    context=f"metric_key={metric_key} (view={v})",
                )
                continue
            if v not in totals_bucket:
                totals_bucket[v] = _FetchBucket(v, frozenset())
            totals_bucket[v].add_measures([c])
            totals_bucket[v].add_metric_id(metric_key)
            kpi_count += 1

    for trend in calc_strategy.get("trend_metrics", []):
        if not isinstance(trend, dict):
            continue
        metric_name = trend.get("metric_name", "")
        metric_key = re.sub(r"\s+", "_", metric_name.strip().lower()) if metric_name else ""
        group_by = trend.get("group_by", "")
        dim_view, dim_col = _parse_view_column(group_by)
        if not dim_view or not dim_col:
            continue
        dim_role = _column_in_view(dim_col, dim_view, view_columns, known_dims, known_meas)
        if dim_role == "unknown":
            dims_v, meas_v = view_columns.get(dim_view) or view_columns.get("") or (set(), set())
            log_llm_invalid_columns(
                source="analytical_fetch_plan.analysis_plan.trend_metrics.group_by",
                node_name="analytical_fetch_plan",
                invalid_columns=[dim_col],
                available_sample=list((dims_v or known_dims) | (meas_v or known_meas))[:20],
                context=f"metric_key={metric_key} (view={dim_view})",
            )
            continue
        measure_cols: List[str] = []
        invalid_trend_cols: List[str] = []
        for col_ref in trend.get("columns_used", []):
            _, m_col = _parse_view_column(col_ref)
            if m_col:
                role = _column_in_view(m_col, dim_view, view_columns, known_dims, known_meas)
                if role != "unknown":
                    measure_cols.append(m_col)
                else:
                    invalid_trend_cols.append(m_col)
        if invalid_trend_cols:
            dims_v, meas_v = view_columns.get(dim_view) or view_columns.get("") or (set(), set())
            log_llm_invalid_columns(
                source="analytical_fetch_plan.analysis_plan.trend_metrics.columns_used",
                node_name="analytical_fetch_plan",
                invalid_columns=invalid_trend_cols,
                available_sample=list((dims_v or known_dims) | (meas_v or known_meas))[:20],
                context=f"metric_key={metric_key} (view={dim_view})",
            )
        if not measure_cols:
            continue
        dim_set = frozenset([dim_col])
        key = (dim_view, dim_set)
        if key not in buckets:
            buckets[key] = _FetchBucket(dim_view, dim_set)
        buckets[key].add_measures(measure_cols)
        buckets[key].add_metric_id(metric_key)
        logger.info(
            f"[analytical_fetch_plan] [Trend] metric='{metric_key}' group_by='{group_by}' → dim='{dim_col}', "
            f"columns_used→measures={measure_cols} → bucket (view={dim_view}, dim={dim_col})"
        )
        trend_count += 1

    if kpi_count or trend_count:
        logger.info(
            f"[analytical_fetch_plan] From analysis/financial plan: {kpi_count} KPI column(s), {trend_count} trend metric(s)"
        )


# ---------------------------------------------------------------------------
# Instruction generation: one dimension per API call
# ---------------------------------------------------------------------------

def _build_instructions(
    buckets: Dict[Tuple[str, FrozenSet[str]], _FetchBucket],
    totals_bucket: Dict[str, _FetchBucket],
    original_plan: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Convert buckets → fetch instructions + dataset mapping.

    Enforces ``MAX_DIMENSIONS_PER_SELECT`` (1).  If a bucket has
    more dimensions, it is split into sub-buckets of size 1, each getting
    the full set of measures.  No limit on measures.
    """
    instructions: List[Dict[str, Any]] = []
    mapping: Dict[str, Any] = {"charts": {}, "metrics": {}}

    original_views = original_plan.get("views", {}) if isinstance(original_plan, dict) else {}

    def _make_instr(
        view: str,
        dims: List[str],
        measures: Set[str],
        filters: List[Dict[str, Any]],
        chart_ids: List[str],
        metric_ids: List[str],
    ) -> Dict[str, Any]:
        """Build one fetch instruction dict."""
        # Build a human-readable fetch_id (format: ViewName__by_DimSuffix or ViewName__totals)
        if dims:
            dim_suffix = "_".join(sorted(dims))
            fetch_id = f"{view}{ANALYTICAL_KEY_BY_PREFIX}{dim_suffix}"
        else:
            fetch_id = f"{view}{ANALYTICAL_KEY_TOTALS_SUFFIX}"

        # $select uses only columns from charts/metrics (dims + measures); no extra columns
        select_columns = sorted(set(dims) | measures)

        orig_view = original_views.get(view, {})
        input_params = orig_view.get("input_parameters")

        # Use only filters from charts/metrics (date_filter etc.). Do not merge in
        # original_plan filters — API calls are driven only by chart name, column
        # names used, and per view.
        return {
            "fetch_id": fetch_id,
            "source_view": view,
            "dimensions": dims,          # list (len 0 or 1; one dimension per call)
            "dimension": dims[0] if len(dims) == 1 else (dims if dims else None),
            "measures": sorted(measures),
            "select_columns": select_columns,
            "filters": list(filters),
            "input_parameters": input_params,
            "chart_ids": chart_ids,
            "metric_ids": metric_ids,
        }

    # ── Dimension-based instructions ──
    for (view, dim_set), bucket in buckets.items():
        dims_list = sorted(bucket.dimensions)

        if len(dims_list) <= MAX_DIMENSIONS_PER_SELECT:
            # Fits in one call
            instr = _make_instr(
                view, dims_list, bucket.measures,
                bucket.filters, bucket.chart_ids, bucket.metric_ids,
            )
            instructions.append(instr)
            fid = instr["fetch_id"]
            dim_label = dims_list[0] if dims_list else "(totals)"
            sel_cols = instr["select_columns"]
            logger.info(
                f"[analytical_fetch_plan] [API call] fetch_id={fid} dimension={dim_label} "
                f"measures({len(bucket.measures)})=[...] $select will have: 1 dim + {len(bucket.measures)} measures "
                f"→ {len(sel_cols)} cols total"
            )
            for cid in bucket.chart_ids:
                mapping["charts"][cid] = fid
            for mid in bucket.metric_ids:
                mapping["metrics"][mid] = fid
        else:
            # Split into groups of MAX_DIMENSIONS_PER_SELECT
            logger.warning(
                f"[analytical_fetch_plan] Bucket ({view}, {dims_list}) has "
                f"{len(dims_list)} dimensions — splitting into groups of "
                f"{MAX_DIMENSIONS_PER_SELECT}"
            )
            for i in range(0, len(dims_list), MAX_DIMENSIONS_PER_SELECT):
                sub_dims = dims_list[i:i + MAX_DIMENSIONS_PER_SELECT]
                instr = _make_instr(
                    view, sub_dims, bucket.measures,
                    bucket.filters, bucket.chart_ids, bucket.metric_ids,
                )
                instructions.append(instr)
                fid = instr["fetch_id"]
                dim_label = sub_dims[0] if sub_dims else "(totals)"
                logger.info(
                    f"[analytical_fetch_plan] [API call] fetch_id={fid} dimension={dim_label} "
                    f"(split from multi-dim bucket) $select: 1 dim + {len(bucket.measures)} measures"
                )
                for cid in bucket.chart_ids:
                    mapping["charts"].setdefault(cid, fid)
                for mid in bucket.metric_ids:
                    mapping["metrics"].setdefault(mid, fid)

    # Totals (summary) fetch disabled: do not create ViewName__totals dataframes.
    # Single-number KPIs and charts with no dimensions will resolve to an existing
    # __by_ dimension slice when needed (polars_engine / resolve_dataset_key fallback).

    return instructions, mapping


def _instructions_to_plan(instructions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Convert fetch instructions into a ``plan`` dict for
    ``sap_data_fetch_simple_node``."""
    views: Dict[str, Any] = {}
    for instr in instructions:
        key = instr["fetch_id"]
        views[key] = {
            "columns": instr["select_columns"],
            "filters": instr.get("filters", []),
            "input_parameters": instr.get("input_parameters"),
            "source_view": instr["source_view"],
            "dimensions": instr.get("dimensions", []),
            "chart_ids": instr.get("chart_ids", []),
            "metric_ids": instr.get("metric_ids", []),
        }
    return {"views": views}


# ---------------------------------------------------------------------------
# Node entry-point
# ---------------------------------------------------------------------------

async def analytical_fetch_plan_node(state: AnalyticsState) -> Dict[str, Any]:
    """Generate optimized, dimension-based SAP fetch instructions.

    For each chart / metric:
      1. List exactly which columns it needs (dimensions + measures).
      2. Validate them against the view's known columns.
      3. Build ``$select`` — **no limit on measures**, **max 1 dimension**.
      4. Charts sharing the same dimension-set are merged into one API call.

    For non-SAP data sources or when no analytical schema is available the
    node is a **no-op** (returns ``{}``).
    """
    start_time = datetime.now()
    node_name = "analytical_fetch_plan"

    logger.info(f"[{node_name}] ══════════════════════════════════════════")
    logger.info(f"[{node_name}] Starting analytical fetch plan generation")
    logger.info(f"[{node_name}] ══════════════════════════════════════════")

    # ── Gate: SAP only ──
    data_source_config = state.get("data_source_config", {})
    ds_type = (data_source_config.get("type", "") if data_source_config else "").lower()
    if ds_type not in ("sap", "sap_datasphere"):
        logger.info(f"[{node_name}] Not an SAP data source ({ds_type}) — skipping")
        return {}

    # ── Idempotent: already produced instructions (node can be triggered multiple times by multiple predecessors) ──
    if state.get("analytical_fetch_instructions"):
        logger.info(f"[{node_name}] analytical_fetch_instructions already present — skipping (idempotent)")
        return {}

    # ── Gate: need chart/analysis plans and analytical schema (sap_fetch_plan is optional; we build minimal plan when missing) ──
    original_plan = state.get("plan", {}) or state.get("sap_fetch_plan", {}) or {}
    if not original_plan:
        logger.info(
            f"[{node_name}] No plan/sap_fetch_plan — will build minimal plan from chart/analysis plans"
        )
    # ── Gate: need analytical schema ──
    filtered_dims: List[Dict[str, Any]] = state.get("filtered_analytical_dimensions", []) or []
    filtered_meas: List[Dict[str, Any]] = state.get("filtered_analytical_measures", []) or []
    if not filtered_dims and not filtered_meas:
        logger.info(f"[{node_name}] No filtered analytical dimensions/measures — skipping")
        return {}

    # Build per-view column lookup: only accept columns that belong to that view's bucket
    view_columns, known_dims, known_meas = _build_view_column_lookup(filtered_dims, filtered_meas)
    logger.info(
        f"[{node_name}] View schema: {len(view_columns)} view(s), {len(known_dims)} dimensions, {len(known_meas)} measures (cols validated per view)"
    )
    for vk, (dims_v, meas_v) in list(view_columns.items())[:5]:
        logger.debug(f"[{node_name}]   view={vk or '(default)'}: {len(dims_v)} dims, {len(meas_v)} meas")

    chart_preplan: List[Dict[str, Any]] = state.get("chart_preplan", []) or []
    analysis_plan: Dict[str, Any] = state.get("analysis_plan", {}) or {}

    buckets: Dict[Tuple[str, FrozenSet[str]], _FetchBucket] = {}
    totals_bucket: Dict[str, _FetchBucket] = {}

    if chart_preplan or analysis_plan:
        logger.info(
            f"[{node_name}] Inputs: {len(chart_preplan)} chart(s), analysis_plan={'yes' if analysis_plan else 'no'}; "
            f"extract all cols → validate cols in that view bucket → dimension-keyed merge"
        )
        _collect_from_chart_preplan(chart_preplan, buckets, totals_bucket, view_columns, known_dims, known_meas)
        _collect_from_analysis_plan(analysis_plan, buckets, totals_bucket, view_columns, known_dims, known_meas)
    else:
        logger.warning(
            f"[{node_name}] No chart_preplan or analysis_plan — building minimal fetch plan from selected columns so data fetch can proceed"
        )
        _build_minimal_buckets_from_schema(view_columns, buckets, totals_bucket, node_name)

    # Dimension-keyed merge: dict keyed by (view, dimension) — same dimension combines all measures from
    # every chart and metric that use that dimension → one API call per (view, dimension), fewer calls, less SAP load.
    # Do NOT merge totals_bucket measures into dimension buckets.
    num_dim_buckets = len(buckets)
    num_totals = len(totals_bucket)
    total_charts = sum(len(b.chart_ids) for b in buckets.values()) + sum(len(b.chart_ids) for b in totals_bucket.values())
    total_metrics = sum(len(b.metric_ids) for b in buckets.values()) + sum(len(b.metric_ids) for b in totals_bucket.values())
    logger.info(
        f"[{node_name}] Dimension-keyed merge: {total_charts} chart(s) + {total_metrics} metric(s) → "
        f"{num_dim_buckets} unique (view, dimension) bucket(s) → {num_dim_buckets} API call(s) (reduced load on SAP)"
    )

    # Log each bucket: one API call per (view, dimension) with combined measures
    for (view, dim_set), bucket in buckets.items():
        dim_label = ", ".join(sorted(dim_set)) if dim_set else "(none)"
        charts_str = ", ".join(bucket.chart_ids[:8]) or "—"
        if len(bucket.chart_ids) > 8:
            charts_str += f" (+{len(bucket.chart_ids) - 8} more)"
        meas_list = sorted(bucket.measures)
        meas_preview = meas_list[:10] if len(meas_list) > 10 else meas_list
        meas_tail = f", ... +{len(meas_list) - 10} more" if len(meas_list) > 10 else ""
        logger.info(
            f"[{node_name}] [Bucket] (view={view}, dim={dim_label}) charts=[{charts_str}] "
            f"measures_used_with_this_dim({len(meas_list)})=[{', '.join(meas_preview)}{meas_tail}] → 1 API call"
        )

    # DATE / FISCAL FILTER FLOW (2/3): Inject filters into buckets so instructions get them.
    parsed_intent = state.get("parsed_intent") or {}
    analytical_scope = (parsed_intent.get("analytical_scope") or "").strip().lower()
    llm_date_filter = state.get("analytical_date_filter")
    llm_date_filter_by_view = state.get("analytical_date_filter_by_view")
    fiscal_filter = state.get("sap_fiscal_filter")
    fiscal_filter_by_view = state.get("sap_fiscal_filter_by_view")

    if analytical_scope == "full":
        logger.info(f"[{node_name}] [DATE FILTER FLOW] analytical_scope=full — not injecting date filter; using full data")
    elif llm_date_filter_by_view and isinstance(llm_date_filter_by_view, dict):
        date_columns_by_view = _date_columns_by_view(filtered_dims)
        logger.info(
            f"[{node_name}] [DATE FILTER FLOW] Injecting per-view date filters into buckets for {len(llm_date_filter_by_view)} view(s)"
        )
        _inject_date_filters_from_llm_by_view(
            buckets,
            totals_bucket,
            date_columns_by_view,
            node_name,
            llm_date_filter_by_view=llm_date_filter_by_view,
        )
    elif llm_date_filter and isinstance(llm_date_filter, dict) and llm_date_filter.get("date_column"):
        date_columns_by_view = _date_columns_by_view(filtered_dims)
        logger.info(
            f"[{node_name}] [DATE FILTER FLOW] Injecting LLM date filter into buckets: date_column={llm_date_filter.get('date_column')!r}, "
            f"range={llm_date_filter.get('start_date')} to {llm_date_filter.get('end_date')}"
        )
        _inject_date_filters_from_llm(buckets, totals_bucket, date_columns_by_view, node_name, llm_date_filter=llm_date_filter)
    else:
        logger.info(
            f"[{node_name}] [DATE FILTER FLOW] Not injecting date filter: no LLM date filter in state (present={llm_date_filter is not None}, "
            f"has_date_column={bool(llm_date_filter and llm_date_filter.get('date_column')) if llm_date_filter else False})"
        )

    # Value filters (e.g. plant eq '1100') from analytical_date_filter or sap_fiscal_filter; use schema data_type for API filter
    value_filters_to_inject: List[Dict[str, Any]] = []
    if llm_date_filter and isinstance(llm_date_filter, dict):
        vf = llm_date_filter.get("value_filters")
        if isinstance(vf, list) and vf:
            value_filters_to_inject.extend(vf)
    if fiscal_filter and isinstance(fiscal_filter, dict):
        vf = fiscal_filter.get("value_filters")
        if isinstance(vf, list) and vf:
            value_filters_to_inject.extend(vf)
    if value_filters_to_inject:
        schema_column_to_data_type = _schema_column_to_data_type(filtered_dims, filtered_meas)
        _inject_value_filters_into_buckets(
            buckets,
            totals_bucket,
            value_filters_to_inject,
            node_name,
            schema_column_to_data_type=schema_column_to_data_type,
        )

    # Check for fiscal filter (when no Edm.Date columns, fiscal periods are used as input_parameters)
    has_fiscal_filter = bool(
        fiscal_filter and isinstance(fiscal_filter, dict) and fiscal_filter.get("input_parameters")
    )
    if has_fiscal_filter:
        logger.info(
            f"[{node_name}] [FISCAL FILTER FLOW] Fiscal filter present: column={fiscal_filter.get('fiscal_column')!r}, "
            f"range={fiscal_filter.get('start_value')} to {fiscal_filter.get('end_value')}, "
            f"granularity={fiscal_filter.get('granularity')}"
        )

    if not buckets and not totals_bucket:
        logger.warning(f"[{node_name}] No dimension or totals requirements found — skipping")
        return {}

    # When sap_fetch_plan was skipped (analytical path), build minimal plan from view names in buckets
    plan_for_build = original_plan
    if not plan_for_build:
        views_seen: Set[str] = {view for (view, _) in buckets} | set(totals_bucket.keys())
        plan_for_build = {"views": {view: {} for view in views_seen}}
        logger.info(
            f"[{node_name}] Using minimal plan (no sap_fetch_plan) for views: {sorted(views_seen)}"
        )

    # ── Build fetch instructions (1 dimension per call; no LLM — format is known) ──
    instructions, mapping = _build_instructions(buckets, totals_bucket, plan_for_build)

    # ── Inject fiscal input_parameters into instructions when present ──
    if has_fiscal_filter:
        global_input_params = fiscal_filter.get("input_parameters", {})
        for instr in instructions:
            source_view = instr.get("source_view", "")
            # Per-view fiscal filter takes priority over global
            view_fiscal = None
            if fiscal_filter_by_view and isinstance(fiscal_filter_by_view, dict):
                view_fiscal = fiscal_filter_by_view.get(source_view)
            if view_fiscal and isinstance(view_fiscal, dict) and view_fiscal.get("input_parameters"):
                instr["input_parameters"] = view_fiscal["input_parameters"]
            elif global_input_params:
                instr["input_parameters"] = global_input_params
            if instr.get("input_parameters"):
                logger.info(
                    f"[{node_name}] [FISCAL] Injected input_parameters into {instr['fetch_id']}: {instr['input_parameters']}"
                )

    # ── Convert to plan format ──
    optimized_plan = _instructions_to_plan(instructions)

    duration = (datetime.now() - start_time).total_seconds()

    # ── Summary log ──
    logger.info(f"[{node_name}] ──────────────────────────────────────────")
    logger.info(f"[{node_name}] Analytical Fetch Plan ({duration:.2f}s)")
    logger.info(f"[{node_name}]   Total API calls:      {len(instructions)}")
    logger.info(f"[{node_name}]   Max dims per $select:  {MAX_DIMENSIONS_PER_SELECT}")
    if has_fiscal_filter:
        logger.info(f"[{node_name}]   Fiscal input params:  {fiscal_filter.get('input_parameters')}")
    for instr in instructions:
        dims = instr.get("dimensions", [])
        dim_label = ", ".join(dims) if dims else "(totals)"
        n_meas = len(instr["measures"])
        n_sel = len(instr["select_columns"])
        n_flt = len(instr.get("filters", []))
        inp = instr.get("input_parameters")
        charts = ", ".join(instr.get("chart_ids", [])[:5]) or "—"
        metrics = ", ".join(instr.get("metric_ids", [])[:5]) or "—"
        logger.info(
            f"[{node_name}]   • {instr['fetch_id']}:"
        )
        logger.info(
            f"[{node_name}]       dims=[{dim_label}], {n_meas} measures, "
            f"{n_sel} cols in $select, {n_flt} filters"
            f"{f', input_params={inp}' if inp else ''}"
        )
        logger.info(
            f"[{node_name}]       charts=[{charts}], metrics=[{metrics}]"
        )
    logger.info(f"[{node_name}] ──────────────────────────────────────────")

    return {
        "plan": optimized_plan,
        "sap_fetch_plan": optimized_plan,
        "analytical_fetch_instructions": instructions,
        "analytical_dataset_mapping": mapping,
    }
