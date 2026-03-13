"""Gantt preparation node — fetch data, group by process order and line, LLM plans start order.

Reads raw_dataframes from state (from sap_data_fetch). Uses hard-coded column mapping
for Process Order view (no LLM column mapping). Data is grouped by process order and
by line (machine); then an LLM is asked how to plan production — which process order
to start first per line. Jobs are reordered by that plan and emitted as gantt_data.
"""
from typing import Dict, Any, List, Optional, Set, Tuple
from datetime import datetime
import logging
import re
import json

import polars as pl

from ...llm.azure_openai import AzureOpenAIClient
from ..state import AnalyticsState
from ..data_models import DataResult
from ..prompts import PRODUCTION_PLAN_SYSTEM_PROMPT, get_production_plan_user_prompt
from ..utils import parse_json_response, save_llm_call_input, save_llm_call_output
from ..process_order_columns import (
    PROCESS_ORDER_DATE_COLUMN,
    PROCESS_ORDER_MAIN_COL,
)

logger = logging.getLogger(__name__)

_MAX_GANTT_CHARTS_BY_MEASURE = 8


def _coerce_date(value: Any) -> Optional[str]:
    """Best-effort conversion of a value to an ISO-8601 date string."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    from datetime import date as _date
    if isinstance(value, _date):
        return value.isoformat()
    s = str(value).strip()
    if not s or s.lower() in ("nan", "none", "null", "nat"):
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y/%m/%d", "%m/%d/%Y", "%d.%m.%Y"):
        try:
            return datetime.strptime(s[:19], fmt).date().isoformat()
        except (ValueError, IndexError):
            continue
    return s[:10]


def _coerce_progress(value: Any) -> int:
    """Convert a progress value to an integer percentage (0-100)."""
    if value is None:
        return 0
    try:
        num = float(value)
        if num < 0:
            return 0
        if num <= 1.0:
            return int(round(num * 100))
        return min(int(round(num)), 100)
    except (ValueError, TypeError):
        return 0


def _materialise_dataframe(raw: Any) -> Optional[pl.DataFrame]:
    """Materialise a raw_dataframes entry into a concrete Polars DataFrame."""
    if isinstance(raw, pl.DataFrame):
        return raw
    if isinstance(raw, pl.LazyFrame):
        return raw.collect()
    if isinstance(raw, DataResult):
        return raw.collect()
    if hasattr(raw, "collect"):
        try:
            return raw.collect()
        except Exception:
            pass
    return None


def _measure_column_name_safe(measure: str) -> str:
    """Return a short id-safe label for a measure column (for chart id/title)."""
    s = re.sub(r"[^a-zA-Z0-9_]", "_", measure).strip("_")[:48]
    return s or "measure"


def _detect_measure_columns(df: pl.DataFrame, used: Set[str]) -> List[str]:
    """Detect numeric columns that can be used as measures for additional Gantt charts."""
    measure_cols: List[str] = []
    numeric_dtypes = (pl.Float32, pl.Float64, pl.Int32, pl.Int64, pl.UInt32, pl.UInt64)
    for col in df.columns:
        if col in used:
            continue
        try:
            dtype = df.schema[col]
            if dtype in numeric_dtypes:
                measure_cols.append(col)
            else:
                dstr = str(dtype).lower()
                if "float" in dstr or "int" in dstr or "decimal" in dstr:
                    measure_cols.append(col)
        except Exception:
            continue
    return measure_cols[:_MAX_GANTT_CHARTS_BY_MEASURE]


def _resolve_column(val: Any, columns: List[str]) -> Optional[str]:
    """Resolve LLM column name to actual column (exact or case-insensitive)."""
    if not val or not isinstance(val, str) or str(val).lower() in ("null", "none", ""):
        return None
    if val in columns:
        return val
    case_map = {c.lower(): c for c in columns}
    return case_map.get(val.strip().lower())


def _hardcoded_process_order_mapping(columns: List[str]) -> Dict[str, Any]:
    """Hard-coded Gantt column mapping for Process Order view (no LLM).
    Line = machine, main process order = job_id, date = start/end, plant/material as attributes.
    """
    col_set = set(columns)
    case_map = {c.lower(): c for c in columns}

    def _pick(*candidates: str) -> Optional[str]:
        for c in candidates:
            if c in col_set:
                return c
            if case_map.get(c.lower()):
                return case_map[c.lower()]
        return None

    return {
        "machine_col": _pick("ZRESMRPC_Process_Order"),
        "job_id_col": _pick(PROCESS_ORDER_MAIN_COL, "0PRODORDD14"),
        "job_name_col": _pick("ZPRDORDL_Process_Order"),
        "start_col": _pick(PROCESS_ORDER_DATE_COLUMN, "ZPPBSTDT_Process_Order"),
        "end_col": _pick(PROCESS_ORDER_DATE_COLUMN, "ZPPBSTDT_Process_Order"),
        "progress_col": _pick("Target_Qty", "Target"),
        "plant_col": _pick("0PLANT_Process_Order"),
        "task_col": None,
        "material_col": _pick("MATERIAL_Process_Order"),
        "inventory_col": None,
        "chart_plan": {"plant_wise": True, "task_wise": False, "material_wise": True, "inventory_wise": False},
        "plan_reason": "Process Order hard-coded mapping (LLM column mapping commented out).",
    }


async def _llm_production_plan(
    line_id: str,
    jobs: List[Dict[str, Any]],
    llm_client: AzureOpenAIClient,
    model: str,
    query_id: Optional[str],
    node_name: str = "gantt_preparation",
) -> Tuple[List[Dict[str, Any]], str]:
    """Ask LLM which process order to start first on this line. Returns (reordered jobs, reason)."""
    if not jobs:
        return [], ""
    call_suffix = f"plan_{re.sub(r'[^a-zA-Z0-9]', '_', line_id)[:30]}"
    user_prompt = get_production_plan_user_prompt(line_id, jobs)
    save_llm_call_input(
        query_id=query_id,
        node_name=node_name,
        call_suffix=call_suffix,
        system_prompt=PRODUCTION_PLAN_SYSTEM_PROMPT,
        user_prompt=user_prompt,
    )
    try:
        response = await llm_client._call_llm_unified(
            model=model,
            system_prompt=PRODUCTION_PLAN_SYSTEM_PROMPT,
            user_prompt=user_prompt,
            node_name=node_name,
            query_id=query_id,
            temperature=0.0,
            use_json_mode=True,
        )
    except Exception as e:
        logger.warning(f"[{node_name}] Production plan LLM failed for line {line_id}: {e}")
        return jobs, ""
    raw = (response or "").strip()
    save_llm_call_output(node_name=node_name, query_id=query_id, raw_response=raw, call_suffix=call_suffix)
    parsed = parse_json_response(raw, expected_type=dict)
    if not parsed or not isinstance(parsed, dict):
        return jobs, ""
    order_ids = parsed.get("order")
    reason = (parsed.get("reason") or "")[:500] if isinstance(parsed.get("reason"), str) else ""
    if not isinstance(order_ids, list) or not order_ids:
        return jobs, reason
    id_to_job: Dict[str, Dict[str, Any]] = {str(j.get("id")): j for j in jobs}
    reordered: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    for oid in order_ids:
        j = id_to_job.get(str(oid))
        if j and str(j.get("id")) not in seen:
            reordered.append(j)
            seen.add(str(j.get("id")))
    for j in jobs:
        if str(j.get("id")) not in seen:
            reordered.append(j)
    return reordered if reordered else jobs, reason


# ---------------------------------------------------------------------------
# Main node
# ---------------------------------------------------------------------------

async def gantt_preparation_node(state: AnalyticsState, model: str = None) -> Dict[str, Any]:
    """Transform raw fetched DataFrames into a Gantt chart payload using LLM column mapping.

    Returns state update with *gantt_data* (Gantt JSON) and *status*.
    """
    start_time = datetime.now()
    node_name = "gantt_preparation"

    from ..node_timing_registry import get_node_timing_registry
    registry = get_node_timing_registry()
    if registry:
        registry.record_node_start(node_name, start_time)

    logger.info(f"[{node_name}] ========== Starting Gantt Preparation (LLM) ==========")

    raw_dataframes: Dict[str, Any] = state.get("raw_dataframes") or {}
    user_query: str = state.get("user_query", "")
    query_id: Optional[str] = state.get("query_id")

    if not raw_dataframes:
        logger.warning(f"[{node_name}] No raw_dataframes found in state — returning empty gantt_data")
        _record_completion(node_name, registry, start_time)
        return {
            "gantt_data": {
                "machines": [],
                "charts": [{"id": "default", "title": "Schedule", "measure": None, "machines": []}],
                "suggested_queries": [],
                "chart_plan": [],
                "plan_reason": "",
            },
            "status": "gantt_prepared",
        }

    logger.info(f"[{node_name}] Processing {len(raw_dataframes)} table(s) from raw_dataframes")

    llm_client = state.get("llm_client") or AzureOpenAIClient()
    from config.settings import settings
    model_name = model or getattr(settings, "analytics_gantt_preparation_model", "claude-haiku-4-5")

    all_machines: Dict[str, List[Dict[str, Any]]] = {}
    machine_names_seen: Set[str] = set()
    tables_processed = 0
    had_any_process_order_table = False
    measure_charts_per_table: List[Tuple[str, List[Dict[str, Any]]]] = []
    merged_chart_plan: Dict[str, bool] = {"plant_wise": False, "task_wise": False, "material_wise": False, "inventory_wise": False}
    plan_reason_global = ""

    for table_name, raw_value in raw_dataframes.items():
        logger.info(f"[{node_name}] Inspecting table '{table_name}'")

        df = _materialise_dataframe(raw_value)
        if df is None or df.is_empty():
            logger.warning(f"[{node_name}] Table '{table_name}' is empty or could not be materialised — skipping")
            continue

        columns = list(df.columns)
        logger.info(f"[{node_name}] Table '{table_name}': {len(df)} rows, columns={columns}")

        # Use hard-coded column mapping only (LLM column mapping removed)
        mapping = _hardcoded_process_order_mapping(list(columns))
        process_order_cols = {PROCESS_ORDER_DATE_COLUMN, "ZRESMRPC_Process_Order", PROCESS_ORDER_MAIN_COL}
        is_process_order_table = bool(process_order_cols & set(columns))
        if is_process_order_table:
            logger.info(f"[{node_name}] Process Order table: using hard-coded column mapping")

        machine_col = mapping.get("machine_col")
        job_id_col = mapping.get("job_id_col")
        job_name_col = mapping.get("job_name_col")
        start_col = mapping.get("start_col")
        end_col = mapping.get("end_col")
        progress_col = mapping.get("progress_col")
        plant_col = mapping.get("plant_col")
        task_col = mapping.get("task_col")
        material_col = mapping.get("material_col")
        inventory_col = mapping.get("inventory_col")
        chart_plan = mapping.get("chart_plan") or {}
        plan_reason = mapping.get("plan_reason") or ""

        logger.info(
            f"[{node_name}] Column mapping for '{table_name}': "
            f"machine={machine_col}, job_id={job_id_col}, job_name={job_name_col}, "
            f"start={start_col}, end={end_col}, progress={progress_col}; "
            f"plant={plant_col}, task={task_col}, material={material_col}, inventory={inventory_col}"
        )
        if chart_plan:
            logger.info(f"[{node_name}] Chart plan (all in sync): plant_wise={chart_plan.get('plant_wise')}, task_wise={chart_plan.get('task_wise')}, material_wise={chart_plan.get('material_wise')}, inventory_wise={chart_plan.get('inventory_wise')}")
        if plan_reason:
            logger.info(f"[{node_name}] Plan reason: {plan_reason[:120]}{'...' if len(plan_reason) > 120 else ''}")

        for k in merged_chart_plan:
            merged_chart_plan[k] = merged_chart_plan[k] or chart_plan.get(k, False)
        if plan_reason:
            plan_reason_global = plan_reason

        if not start_col and not end_col:
            logger.warning(
                f"[{node_name}] Table '{table_name}' has no recognisable start/end date columns — skipping"
            )
            continue

        if is_process_order_table:
            had_any_process_order_table = True

        used: Set[str] = set()
        for c in (machine_col, job_id_col, job_name_col, start_col, end_col, progress_col, plant_col, task_col, material_col, inventory_col):
            if c:
                used.add(c)

        measure_columns = _detect_measure_columns(df, used)
        if measure_columns:
            logger.info(f"[{node_name}] Table '{table_name}': building {len(measure_columns)} extra Gantt chart(s) by measure: {measure_columns[:5]}{'...' if len(measure_columns) > 5 else ''}")
        measure_machines: Dict[str, Dict[str, List[Dict[str, Any]]]] = {
            m: {} for m in measure_columns
        }

        def _str_attr(row: Dict, col: Optional[str]) -> str:
            if not col:
                return ""
            v = row.get(col)
            if v is None:
                return ""
            return str(v).strip() or ""

        rows = df.to_dicts()
        for row_idx, row in enumerate(rows):
            machine_id = str(row.get(machine_col, "Unassigned")) if machine_col else "Unassigned"
            if machine_id.lower() in ("none", "null", "nan", ""):
                machine_id = "Unassigned"

            job_id = str(row.get(job_id_col, f"ROW-{row_idx}")) if job_id_col else f"ROW-{row_idx}"
            job_name = str(row.get(job_name_col, job_id)) if job_name_col else job_id
            start_val = _coerce_date(row.get(start_col)) if start_col else None
            end_val = _coerce_date(row.get(end_col)) if end_col else None
            progress_val = _coerce_progress(row.get(progress_col)) if progress_col else 0

            if not start_val and not end_val:
                continue

            if not start_val:
                start_val = end_val
            if not end_val:
                end_val = start_val

            job_entry: Dict[str, Any] = {
                "id": job_id,
                "name": job_name,
                "start": start_val,
                "end": end_val,
                "progress": progress_val,
            }
            if plant_col:
                job_entry["plant"] = _str_attr(row, plant_col)
            if task_col:
                job_entry["task"] = _str_attr(row, task_col)
            if material_col:
                job_entry["material"] = _str_attr(row, material_col)
            if inventory_col:
                job_entry["inventory"] = _str_attr(row, inventory_col)

            all_machines.setdefault(machine_id, []).append(job_entry)
            machine_names_seen.add(machine_id)

            for mcol in measure_columns:
                m_progress = _coerce_progress(row.get(mcol))
                job_entry_m: Dict[str, Any] = {
                    "id": job_id,
                    "name": job_name,
                    "start": start_val,
                    "end": end_val,
                    "progress": m_progress,
                }
                if plant_col:
                    job_entry_m["plant"] = _str_attr(row, plant_col)
                if task_col:
                    job_entry_m["task"] = _str_attr(row, task_col)
                if material_col:
                    job_entry_m["material"] = _str_attr(row, material_col)
                if inventory_col:
                    job_entry_m["inventory"] = _str_attr(row, inventory_col)
                measure_machines[mcol].setdefault(machine_id, []).append(job_entry_m)

        tables_processed += 1

        for mcol in measure_columns:
            m_machines = measure_machines[mcol]
            m_list: List[Dict[str, Any]] = []
            for mid in sorted(m_machines.keys()):
                jobs = m_machines[mid]
                jobs.sort(key=lambda j: j.get("start") or "")
                m_list.append({"machineId": mid, "jobs": jobs})
            measure_charts_per_table.append((mcol, m_list))

    # Group by process order is implicit (each job has process order id); we have jobs per line (machine).
    # Ask LLM how to plan: which process order to start first per line, then reorder.
    if had_any_process_order_table and all_machines:
        logger.info(f"[{node_name}] Asking LLM for production plan (start order) per line")
        plan_reasons: List[str] = []
        for machine_id in sorted(all_machines.keys()):
            jobs = all_machines[machine_id]
            if not jobs:
                continue
            reordered, reason = await _llm_production_plan(
                machine_id, jobs, llm_client, model_name, query_id, node_name
            )
            all_machines[machine_id] = reordered
            if reason:
                plan_reasons.append(f"Line {machine_id}: {reason}")
        if plan_reasons:
            plan_reason_global = " ".join(plan_reasons)[:1000]
            logger.info(f"[{node_name}] Production plan reason(s): {plan_reason_global[:200]}...")

    machines_payload = []
    for machine_id in sorted(all_machines.keys()):
        jobs = all_machines[machine_id]
        jobs.sort(key=lambda j: j.get("start") or "")
        machines_payload.append({"machineId": machine_id, "jobs": jobs})

    total_jobs = sum(len(m["jobs"]) for m in machines_payload)
    logger.info(
        f"[{node_name}] Built Gantt payload: {len(machines_payload)} machine(s), {total_jobs} job(s) "
        f"from {tables_processed} table(s)"
    )

    # Build grouped charts (plant/task/material/inventory) — same jobs, different grouping, all in sync
    all_jobs_flat: List[Dict[str, Any]] = []
    for _mid, job_list in all_machines.items():
        all_jobs_flat.extend(job_list)

    def _build_grouped_chart(attr: str) -> List[Dict[str, Any]]:
        groups: Dict[str, List[Dict[str, Any]]] = {}
        for j in all_jobs_flat:
            val = (j.get(attr) or "").strip() or "Unassigned"
            groups.setdefault(val, []).append(j)
        out = []
        for group_id in sorted(groups.keys()):
            jobs_sorted = sorted(groups[group_id], key=lambda x: x.get("start") or "")
            out.append({"machineId": group_id, "jobs": jobs_sorted})
        return out

    chart_plan_list: List[Dict[str, str]] = []
    if merged_chart_plan.get("plant_wise"):
        chart_plan_list.append({"id": "by_plant", "title": "By Plant", "group_by": "plant"})
    if merged_chart_plan.get("task_wise"):
        chart_plan_list.append({"id": "by_task", "title": "By Task", "group_by": "task"})
    if merged_chart_plan.get("material_wise"):
        chart_plan_list.append({"id": "by_material", "title": "By Material", "group_by": "material"})
    if merged_chart_plan.get("inventory_wise"):
        chart_plan_list.append({"id": "by_inventory", "title": "By Inventory", "group_by": "inventory"})

    suggested_queries = _generate_suggested_queries(machine_names_seen, user_query)

    charts: List[Dict[str, Any]] = [
        {"id": "default", "title": "Schedule", "measure": None, "machines": machines_payload}
    ]
    if merged_chart_plan.get("plant_wise") and all_jobs_flat and any(j.get("plant") for j in all_jobs_flat):
        charts.append({"id": "by_plant", "title": "By Plant", "measure": None, "group_by": "plant", "machines": _build_grouped_chart("plant")})
    if merged_chart_plan.get("task_wise") and all_jobs_flat and any(j.get("task") for j in all_jobs_flat):
        charts.append({"id": "by_task", "title": "By Task", "measure": None, "group_by": "task", "machines": _build_grouped_chart("task")})
    if merged_chart_plan.get("material_wise") and all_jobs_flat and any(j.get("material") for j in all_jobs_flat):
        charts.append({"id": "by_material", "title": "By Material", "measure": None, "group_by": "material", "machines": _build_grouped_chart("material")})
    if merged_chart_plan.get("inventory_wise") and all_jobs_flat and any(j.get("inventory") for j in all_jobs_flat):
        charts.append({"id": "by_inventory", "title": "By Inventory", "measure": None, "group_by": "inventory", "machines": _build_grouped_chart("inventory")})
    for mcol, m_list in measure_charts_per_table:
        safe_id = _measure_column_name_safe(mcol)
        charts.append({
            "id": f"by_{safe_id}",
            "title": f"By {mcol}",
            "measure": mcol,
            "machines": m_list,
        })
    if len(charts) > 1:
        logger.info(f"[{node_name}] Emitting {len(charts)} Gantt chart(s) (1 default + {len(chart_plan_list)} breakdown(s) + by measure)")

    gantt_data: Dict[str, Any] = {
        "machines": machines_payload,
        "charts": charts,
        "suggested_queries": suggested_queries,
    }
    if chart_plan_list:
        gantt_data["chart_plan"] = chart_plan_list
    if plan_reason_global:
        gantt_data["plan_reason"] = plan_reason_global

    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"[{node_name}] ========== Gantt Preparation Complete ==========")
    logger.info(f"[{node_name}] Duration: {duration:.2f}s")

    _record_completion(node_name, registry, start_time)

    return {
        "gantt_data": gantt_data,
        "status": "gantt_prepared",
    }


# ---------------------------------------------------------------------------
# Suggested queries
# ---------------------------------------------------------------------------

def _generate_suggested_queries(machine_names: Set[str], user_query: str) -> List[str]:
    """Generate contextual follow-up query suggestions based on the data."""
    suggestions: List[str] = []

    suggestions.append("Show next week's schedule")
    suggestions.append("Show overdue jobs")

    concrete_machines = sorted(m for m in machine_names if m != "Unassigned")
    if concrete_machines:
        suggestions.append(f"Filter by machine {concrete_machines[0]}")
        if len(concrete_machines) > 1:
            suggestions.append(f"Compare machines {concrete_machines[0]} and {concrete_machines[1]}")

    suggestions.append("Show jobs with low progress")

    return suggestions[:5]


def _record_completion(node_name: str, registry: Any, start_time: datetime) -> None:
    """Record node completion in the timing registry."""
    if registry:
        registry.record_node_completion(node_name)
    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"[{node_name}] Node completed in {duration:.2f}s")
