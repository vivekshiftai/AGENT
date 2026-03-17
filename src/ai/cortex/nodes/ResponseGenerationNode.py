"""Prepare the final response returned to the API (message and tasks).
CHG enhancement: scheduling_summary + allergen_warnings in state."""
import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def _build_chg_summary(state: Dict[str, Any]) -> Dict[str, Any]:
    """Build scheduling_summary and allergen_warnings from CHG schedule data."""
    machine_schedules = state.get("machine_schedules_raw") or {}
    process_orders = state.get("process_orders") or []
    exceptions = state.get("scheduling_exceptions") or []

    scheduled_po_ids = set()
    for mid, schedule in machine_schedules.items():
        for block in schedule:
            po_id = block.get("process_order_id")
            if po_id and block.get("block_type") == "PRODUCTION":
                scheduled_po_ids.add(po_id)

    total_cleaning = sum(
        len([b for b in sched if b.get("block_type") == "CLEANING"])
        for sched in machine_schedules.values()
    )
    allergen_cip_count = sum(
        len([b for b in sched if b.get("clean_type") == "ALLERGEN_CIP"])
        for sched in machine_schedules.values()
    )

    scheduling_summary = {
        "total_process_orders": len(process_orders),
        "scheduled": len(scheduled_po_ids),
        "blocked_mrp": len([e for e in exceptions if e.get("type") == "MRP_BLOCK"]),
        "missed_date_risk": len([e for e in exceptions if e.get("type") in ("MISSED_DATE", "MISSED_DATE_RISK")]),
        "total_cleaning_blocks": total_cleaning,
        "allergen_cip_count": allergen_cip_count,
        "machines_scheduled": len(machine_schedules),
        "exceptions_total": len(exceptions),
        "critical_exceptions": len([e for e in exceptions if e.get("severity") == "CRITICAL"]),
    }

    allergen_warnings = []
    for mid, sched in machine_schedules.items():
        for b in sched:
            if b.get("block_type") == "CLEANING":
                allergen_warnings.append({
                    "machine_id": mid,
                    "from_allergens": b.get("from_allergens"),
                    "to_allergens": b.get("to_allergens"),
                    "clean_type": b.get("clean_type"),
                    "duration_min": b.get("duration_min"),
                    "atp_required": b.get("atp_swab_required", False),
                    "start": b.get("start_datetime"),
                })

    return scheduling_summary, allergen_warnings


def run(state: Dict[str, Any]) -> Dict[str, Any]:
    existing_response = state.get("response") or ""
    gantt_tasks = state.get("ganttTasks") or []
    data_fetch_error = state.get("dataFetchError")
    validation_error = state.get("validationError")
    is_chg = state.get("_is_chg", False)

    if data_fetch_error:
        message = "Data fetch issue: %s. Add datasources in the Data sources tab and try again." % data_fetch_error
    elif validation_error:
        message = "Validation failed: %s" % validation_error
    elif existing_response:
        message = existing_response
    elif is_chg:
        message = _build_chg_response_message(state, gantt_tasks)
    elif gantt_tasks:
        message = "Production plan generated successfully. %d task(s) in timeline." % len(gantt_tasks)
    else:
        normalized_count = len(state.get("normalizedData") or [])
        if normalized_count:
            message = "Processed %d record(s). No schedule generated." % normalized_count
        else:
            message = "I'm your production planning assistant. Ask for production demand between dates or request a production plan."

    updates: Dict[str, Any] = {
        "response": message,
        "ganttTasks": gantt_tasks,
    }

    if is_chg:
        scheduling_summary, allergen_warnings = _build_chg_summary(state)
        updates["scheduling_summary"] = scheduling_summary
        updates["allergen_warnings"] = allergen_warnings

    return {**state, **updates}


def _build_chg_response_message(state: Dict[str, Any], gantt_tasks: List) -> str:
    process_orders = state.get("process_orders") or []
    exceptions = state.get("scheduling_exceptions") or []
    machine_schedules = state.get("machine_schedules_raw") or {}
    validation = state.get("validationResult") or {}

    parts = []

    if gantt_tasks:
        prod_count = len([t for t in gantt_tasks if t.get("block_type") == "PRODUCTION"])
        clean_count = len([t for t in gantt_tasks if t.get("block_type") == "CLEANING"])
        parts.append(
            f"CHG production schedule generated: {prod_count} production blocks, "
            f"{clean_count} cleaning blocks across {len(machine_schedules)} machine(s)."
        )

    if process_orders:
        parts.append(f"Planning {len(process_orders)} process order(s) from sales orders.")

    critical_exceptions = [e for e in exceptions if e.get("severity") == "CRITICAL"]
    if critical_exceptions:
        parts.append(f"CRITICAL: {len(critical_exceptions)} critical issue(s) require attention.")

    missed_dates = [e for e in exceptions if e.get("type") in ("MISSED_DATE", "MISSED_DATE_RISK")]
    if missed_dates:
        parts.append(f"{len(missed_dates)} delivery date(s) at risk.")

    mrp_blocks = [e for e in exceptions if e.get("type") == "MRP_BLOCK"]
    if mrp_blocks:
        parts.append(f"{len(mrp_blocks)} process order(s) blocked by material shortages (MRP RED).")

    if not validation.get("valid", True):
        parts.append("Schedule has validation issues — review exceptions panel.")

    if not parts:
        parts.append("I'm your production planning assistant. Ask for production demand between dates or request a production plan.")

    return " ".join(parts)
