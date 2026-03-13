"""Simple analytical fetch plan — builds SAP fetch instructions from column selection only.

Simple flow only: we do NOT use chart_preplan or financial_analyst_planner.
- Build API calls using exactly ONE dimension and ALL measures per view (no chart/plan).
- Uses only: filtered_analytical_dimensions, filtered_analytical_measures,
  analytical_date_filter, and sap_fiscal_filter from state.
- One API call per (view, dimension); same instruction format as analytical_fetch_plan
  so sap_data_fetch can execute them.

Flow after column selection: plan API calls (here) → sap_data_fetch → analytical_summary → user response
(no data_sufficiency_check; LLM in analytical_summary gives agent-style response when no data).
"""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Set, Tuple

from ..state import AnalyticsState

logger = logging.getLogger(__name__)

ANALYTICAL_KEY_BY_PREFIX = "__by_"
MAX_DIMENSIONS_PER_SELECT = 1


def _view_columns_from_filtered(
    filtered_dims: List[Dict[str, Any]],
    filtered_meas: List[Dict[str, Any]],
) -> Tuple[Dict[str, Tuple[Set[str], Set[str]]], Dict[str, str]]:
    """Build per-view (dims_set, meas_set) and view_name by dimension name for date column lookup."""
    view_columns: Dict[str, Tuple[Set[str], Set[str]]] = {}
    view_by_dim: Dict[str, str] = {}

    def _ensure_view(view_key: str) -> Tuple[Set[str], Set[str]]:
        if view_key not in view_columns:
            view_columns[view_key] = (set(), set())
        return view_columns[view_key]

    for d in filtered_dims or []:
        if not isinstance(d, dict):
            continue
        name = (d.get("name") or "").strip()
        view_name = (d.get("view_name") or "").strip() or ""
        if name:
            dims_set, _ = _ensure_view(view_name)
            dims_set.add(name)
            view_by_dim[name] = view_name

    for m in filtered_meas or []:
        if not isinstance(m, dict):
            continue
        name = (m.get("name") or "").strip()
        view_name = (m.get("view_name") or "").strip() or ""
        if name:
            _, meas_set = _ensure_view(view_name)
            meas_set.add(name)

    return dict(view_columns), view_by_dim


def _date_columns_by_view(filtered_dims: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    """Edm.Date columns per view from dimensions (for date filter injection)."""
    by_view: Dict[str, List[str]] = defaultdict(list)
    date_types = {"Edm.Date", "Edm.DateTimeOffset", "Edm.DateTime", "Edm.TimeOfDay"}
    for d in filtered_dims or []:
        if not isinstance(d, dict):
            continue
        name = (d.get("name") or "").strip()
        view_name = (d.get("view_name") or "").strip() or ""
        dt = (d.get("data_type") or "").strip()
        if name and dt in date_types:
            by_view[view_name].append(name)
    return dict(by_view)


def _value_filters_to_odata(value_filters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Convert value_filters to bucket format with odata_syntax."""
    result = []
    op_map = {"eq": "eq", "ne": "ne", "gt": "gt", "ge": "ge", "lt": "lt", "le": "le"}
    for vf in value_filters or []:
        if not isinstance(vf, dict):
            continue
        col = (vf.get("column") or "").strip()
        if not col:
            continue
        op = op_map.get((vf.get("operator") or "eq").strip().lower(), "eq")
        val = vf.get("value")
        data_type = (vf.get("data_type") or "Edm.String").strip()
        if data_type in ("Edm.Int64", "Edm.Int32", "Edm.Decimal", "Edm.Double", "Edm.Single"):
            try:
                val_str = str(int(val)) if data_type in ("Edm.Int64", "Edm.Int32") else str(float(val))
            except (ValueError, TypeError):
                val_str = str(val)
            odata_syntax = f"{col} {op} {val_str}"
        else:
            val_str = str(val).replace("'", "''") if val is not None else ""
            odata_syntax = f"{col} {op} '{val_str}'"
        result.append({"column": col, "operator": op, "value": val, "odata_syntax": odata_syntax})
    return result


async def simple_analytical_fetch_plan_node(state: AnalyticsState) -> Dict[str, Any]:
    """
    Build analytical_fetch_instructions from column selection only (no chart/analysis plans).
    One instruction per (view, dimension) with all measures for that view; date and value
    filters from analytical_date_filter / sap_fiscal_filter are injected.
    """
    start_time = datetime.now()
    node_name = "simple_analytical_fetch_plan"

    logger.info(f"[{node_name}] Building simple fetch plan from column selection only")

    data_source_config = state.get("data_source_config", {})
    ds_type = (data_source_config.get("type") or "").lower()
    if ds_type not in ("sap", "sap_datasphere"):
        logger.info(f"[{node_name}] Not SAP — skipping")
        return {}

    filtered_dims = state.get("filtered_analytical_dimensions") or []
    filtered_meas = state.get("filtered_analytical_measures") or []
    if not filtered_dims and not filtered_meas:
        logger.warning(f"[{node_name}] No filtered dimensions/measures")
        return {}

    view_columns, _ = _view_columns_from_filtered(filtered_dims, filtered_meas)
    # Buckets: (view, frozenset(dims)) -> (measures, filters)
    buckets: Dict[Tuple[str, Tuple[str, ...]], Tuple[Set[str], List[Dict[str, Any]]]] = {}
    for view, (dims, meas) in view_columns.items():
        if not dims and not meas:
            continue
        # Simple flow: one dimension per API call, all measures (no chart preplan / financial planner)
        for dim in sorted(dims):
            key = (view, (dim,))
            if key not in buckets:
                buckets[key] = (set(meas), [])
            else:
                buckets[key][0].update(meas)
        # If we have measures but no dimensions, one "totals" bucket
        if meas and not dims:
            key = (view, ())
            buckets[key] = (set(meas), [])

    # Inject date filter from analytical_date_filter
    llm_date = state.get("analytical_date_filter")
    date_columns_by_view = _date_columns_by_view(filtered_dims)
    for (view, dim_tuple), (measures, filters) in list(buckets.items()):
        if filters:
            continue
        date_cols = date_columns_by_view.get(view, [])
        if not date_cols or not llm_date or not isinstance(llm_date, dict):
            continue
        date_col = (llm_date.get("date_column") or "").strip()
        start_str = (llm_date.get("start_date") or "").strip()
        end_str = (llm_date.get("end_date") or "").strip()
        if date_col and start_str and end_str and date_col in date_cols:
            filters.extend([
                {"column": date_col, "operator": "ge", "value": start_str, "odata_syntax": f"{date_col} ge {start_str}"},
                {"column": date_col, "operator": "le", "value": end_str, "odata_syntax": f"{date_col} le {end_str}"},
            ])
            logger.info(f"[{node_name}] Date filter on {date_col} for view {view}")

    # Value filters
    value_filters: List[Dict[str, Any]] = []
    if llm_date and isinstance(llm_date, dict):
        vf = llm_date.get("value_filters")
        if isinstance(vf, list):
            value_filters.extend(vf)
    fiscal = state.get("sap_fiscal_filter")
    if fiscal and isinstance(fiscal, dict):
        vf = fiscal.get("value_filters")
        if isinstance(vf, list):
            value_filters.extend(vf)
    if value_filters:
        odata_vf = _value_filters_to_odata(value_filters)
        for (view, _), (_, filters) in buckets.items():
            filters.extend(odata_vf)

    # Build instructions (same format as analytical_fetch_plan)
    original_plan = state.get("plan") or state.get("sap_fetch_plan") or {}
    views_seen = {view for (view, _) in buckets}
    plan_views = original_plan.get("views", {}) if isinstance(original_plan, dict) else {}
    if not plan_views:
        plan_views = {v: {} for v in views_seen}

    instructions: List[Dict[str, Any]] = []
    mapping: Dict[str, Dict[str, str]] = {"charts": {}, "metrics": {}}

    for (view, dim_tuple), (measures, filters) in buckets.items():
        dims_list = list(dim_tuple)
        if len(dims_list) > MAX_DIMENSIONS_PER_SELECT:
            dims_list = dims_list[:MAX_DIMENSIONS_PER_SELECT]
        dim_suffix = "_".join(sorted(dims_list)) if dims_list else "totals"
        fetch_id = f"{view}{ANALYTICAL_KEY_BY_PREFIX}{dim_suffix}" if dims_list else f"{view}__totals"
        select_columns = sorted(set(dims_list) | measures)
        orig_view = plan_views.get(view, {}) if isinstance(plan_views, dict) else {}
        input_params = orig_view.get("input_parameters") if isinstance(orig_view, dict) else None
        if not input_params and fiscal and isinstance(fiscal, dict):
            input_params = fiscal.get("input_parameters")

        instr = {
            "fetch_id": fetch_id,
            "source_view": view,
            "dimensions": dims_list,
            "dimension": dims_list[0] if len(dims_list) == 1 else (dims_list if dims_list else None),
            "measures": sorted(measures),
            "select_columns": select_columns,
            "filters": list(filters),
            "input_parameters": input_params,
            "chart_ids": [],
            "metric_ids": [],
        }
        instructions.append(instr)
        logger.info(f"[{node_name}] Instruction: {fetch_id} dims={dims_list} measures={len(measures)}")

    if not instructions:
        logger.warning(f"[{node_name}] No instructions built")
        return {}

    optimized_plan = {"views": {instr["fetch_id"]: {
        "columns": instr["select_columns"],
        "filters": instr.get("filters", []),
        "input_parameters": instr.get("input_parameters"),
        "source_view": instr["source_view"],
        "dimensions": instr.get("dimensions", []),
        "chart_ids": [],
        "metric_ids": [],
    } for instr in instructions}}

    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"[{node_name}] Built {len(instructions)} instruction(s) in {duration:.2f}s")

    return {
        "plan": optimized_plan,
        "sap_fetch_plan": optimized_plan,
        "analytical_fetch_instructions": instructions,
        "analytical_dataset_mapping": mapping,
    }
