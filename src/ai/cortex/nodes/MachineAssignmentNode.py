"""Assign products to appropriate production lines/machines.
CHG enhancements: GF line constraint, MRP block checks, availability filtering."""
import logging
from typing import Any, Dict, List, Optional

from src.domain.entities.production_task import ProductionTask, TaskType
from src.domain.services.machine_assignment_service import MachineAssignmentService
from src.domain.services.recipe_service import RecipeService

logger = logging.getLogger(__name__)

MACHINE_TABLE_HINTS = ("machine", "equipment", "workcenter", "resource", "line", "production_line")
MACHINE_ID_KEYS = ("machine_id", "equipment_id", "workcenter_id", "resource_id", "machine")
MACHINE_NAME_KEYS = ("machine_name", "equipment_name", "name", "description")
MACHINE_TYPE_KEYS = ("machine_type", "type", "category", "equipment_type")
CAPACITY_KEYS = ("capacity", "capacity_per_hour", "throughput", "rate", "capacity_lbs_hr", "cap_lbs_hr")
PLANT_KEYS = ("plant", "plant_id", "site", "facility")
LINE_KEYS = ("line", "line_id", "production_line")

GF_DEDICATED_LINE = "LINE-DUN-B"


def _check_gf_constraint(machine_queues: Dict[str, List], machines: List[Dict]) -> List[Dict]:
    """LINE-DUN-B is dedicated GF — reject any WHEAT assignment to it."""
    violations = []
    dun_b_machines = {m["machine_id"] for m in machines if m.get("line_id") == GF_DEDICATED_LINE}
    for mid in dun_b_machines:
        if mid not in machine_queues:
            continue
        to_remove = []
        for i, job in enumerate(machine_queues[mid]):
            if "WHEAT" in (job.get("allergens") or []):
                violations.append({
                    "type": "GF_LINE_VIOLATION",
                    "severity": "CRITICAL",
                    "message": (
                        f"WHEAT product {job.get('product_name', '')} cannot run "
                        f"on GF-dedicated {GF_DEDICATED_LINE} (machine {mid})"
                    ),
                    "process_order_id": job.get("process_order_id"),
                })
                to_remove.append(i)
        for i in reversed(to_remove):
            machine_queues[mid].pop(i)
    return violations


def _check_mrp_blocks(machine_queues: Dict[str, List], mrp_alerts: List[Dict], open_pos: List[Dict]) -> List[Dict]:
    """Block process orders where MRP shows RED shortage with no PO coverage."""
    blocked = []
    red_alerts = {}
    for a in mrp_alerts:
        if a.get("risk_level") == "RED":
            pid = a.get("product_id")
            if pid:
                red_alerts.setdefault(pid, []).append(a)

    po_coverage = {p.get("ingredient_id"): p.get("expected_date") for p in open_pos}

    for mid, jobs in machine_queues.items():
        for job in jobs:
            alerts_for_product = red_alerts.get(job.get("product_id"), [])
            for alert in alerts_for_product:
                ing_id = alert.get("ingredient_id")
                po_eta = po_coverage.get(ing_id)
                blocked.append({
                    "type": "MRP_BLOCK",
                    "severity": "RED",
                    "message": (
                        f"PO {job.get('process_order_id')} blocked — "
                        f"{alert.get('ingredient_name', '?')} shortage "
                        f"({alert.get('shortage_lbs', '?')} lbs). "
                        f"PO ETA: {po_eta or 'NO PO PLACED'}"
                    ),
                    "process_order_id": job.get("process_order_id"),
                    "unblock_date": po_eta,
                })
    return blocked


def _filter_unavailable_machines(
    machine_queues: Dict[str, List],
    availability: List[Dict],
) -> List[Dict]:
    """Flag machines with no available shifts this planning week."""
    exceptions = []
    avail_by_machine: Dict[str, List] = {}
    for slot in availability:
        mid = slot.get("machine_id")
        if mid:
            avail_by_machine.setdefault(mid, []).append(slot)

    for mid in list(machine_queues.keys()):
        slots = avail_by_machine.get(mid, [])
        available_slots = [s for s in slots if s.get("status") not in ("MAINTENANCE", "BREAKDOWN", "OFFLINE") and float(s.get("available_hrs", 0) or 0) > 0]
        if slots and not available_slots:
            exceptions.append({
                "type": "MACHINE_UNAVAILABLE",
                "severity": "HIGH",
                "message": f"Machine {mid} has no available shifts this planning week",
                "affected_jobs": [j.get("process_order_id") for j in machine_queues[mid][:5]],
            })
    return exceptions


def run(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Assign production tasks to machines.
    CHG path: uses chg_machines from state and applies GF/MRP constraints.
    Generic path: scans rawData for machine tables.
    """
    if state.get("needsClarification") or state.get("rejected"):
        return {**state}

    is_chg = state.get("_is_chg", False)

    # ── CHG path ────────────────────────────────────────────────────
    if is_chg:
        machine_queues = dict(state.get("machine_queues") or {})
        chg_machines = state.get("chg_machines") or []
        mrp_alerts = state.get("chg_mrp_alerts") or []
        open_pos = state.get("chg_open_pos") or []
        availability = state.get("chg_machine_availability") or []
        exceptions = list(state.get("scheduling_exceptions") or [])

        gf_violations = _check_gf_constraint(machine_queues, chg_machines)
        exceptions.extend(gf_violations)

        mrp_blocks = _check_mrp_blocks(machine_queues, mrp_alerts, open_pos)
        exceptions.extend(mrp_blocks)

        avail_exceptions = _filter_unavailable_machines(machine_queues, availability)
        exceptions.extend(avail_exceptions)

        machines_dict = {m["machine_id"]: m for m in chg_machines}
        assignments_dict = {mid: [j.get("process_order_id") for j in jobs] for mid, jobs in machine_queues.items()}

        logger.info(
            "MachineAssignmentNode CHG: %d machines, %d queues, %d GF violations, %d MRP blocks",
            len(chg_machines), len(machine_queues), len(gf_violations), len(mrp_blocks),
        )

        return {
            **state,
            "machines": machines_dict,
            "machineAssignments": assignments_dict,
            "machine_queues": machine_queues,
            "scheduling_exceptions": exceptions,
        }

    # ── Generic path (unchanged) ────────────────────────────────────
    production_tasks = state.get("_production_tasks") or []
    if not production_tasks:
        task_dicts = state.get("productionTasks") or []
        for td in task_dicts:
            production_tasks.append(ProductionTask(
                task_id=td.get("task_id", ""),
                task_type=TaskType(td.get("task_type", "production")),
                product_id=td.get("product_id"),
                product_name=td.get("product_name"),
                quantity=td.get("quantity", 0),
                priority=td.get("priority", 0),
                allergens=td.get("allergens", []),
            ))

    raw_data = state.get("rawData") or {}
    recipe_service = state.get("_recipe_service")

    machine_records = []
    for source_name, tables in raw_data.items():
        if not isinstance(tables, dict):
            continue
        for table_name, df in tables.items():
            tn_lower = (table_name or "").lower()
            if not any(hint in tn_lower for hint in MACHINE_TABLE_HINTS):
                continue
            try:
                import pandas as pd
                if not isinstance(df, pd.DataFrame) or df.empty:
                    continue
                cols_lower = {c.lower(): c for c in df.columns}
                for _, row in df.iterrows():
                    row_dict = row.to_dict()
                    machine_id = None
                    for key in MACHINE_ID_KEYS:
                        real_col = cols_lower.get(key)
                        if real_col and row_dict.get(real_col):
                            machine_id = str(row_dict[real_col])
                            break
                    if not machine_id:
                        continue
                    machine_name = machine_id
                    for key in MACHINE_NAME_KEYS:
                        real_col = cols_lower.get(key)
                        if real_col and row_dict.get(real_col):
                            machine_name = str(row_dict[real_col])
                            break
                    machine_type = "general"
                    for key in MACHINE_TYPE_KEYS:
                        real_col = cols_lower.get(key)
                        if real_col and row_dict.get(real_col):
                            machine_type = str(row_dict[real_col])
                            break
                    capacity = 100.0
                    for key in CAPACITY_KEYS:
                        real_col = cols_lower.get(key)
                        if real_col and row_dict.get(real_col) is not None:
                            try:
                                capacity = float(row_dict[real_col])
                            except (TypeError, ValueError):
                                pass
                            break
                    plant_id = None
                    for key in PLANT_KEYS:
                        real_col = cols_lower.get(key)
                        if real_col and row_dict.get(real_col):
                            plant_id = str(row_dict[real_col])
                            break
                    line_id = None
                    for key in LINE_KEYS:
                        real_col = cols_lower.get(key)
                        if real_col and row_dict.get(real_col):
                            line_id = str(row_dict[real_col])
                            break
                    machine_records.append({
                        "machine_id": machine_id,
                        "machine_name": machine_name,
                        "machine_type": machine_type,
                        "capacity_per_hour": capacity,
                        "plant_id": plant_id,
                        "line_id": line_id,
                    })
            except Exception as e:
                logger.warning("MachineAssignmentNode: error reading %s.%s: %s", source_name, table_name, e)

    machine_service = MachineAssignmentService(machine_records, recipe_service)
    if not machine_records:
        machine_service.create_default_machine("Machine-1")
        logger.info("MachineAssignmentNode: no machine data found, created default machine")
    else:
        logger.info("MachineAssignmentNode: loaded %d machines", len(machine_records))

    machine_tasks = machine_service.assign_tasks_to_machines(production_tasks)
    machines_dict = {m.machine_id: m.to_dict() for m in machine_service.get_all_machines()}
    assignments_dict = {mid: [t.task_id for t in tasks] for mid, tasks in machine_tasks.items()}
    production_tasks_dicts = [t.to_dict() for t in production_tasks]

    logger.info("MachineAssignmentNode: assigned %d tasks to %d machines", len(production_tasks), len(assignments_dict))

    return {
        **state,
        "machines": machines_dict,
        "machineAssignments": assignments_dict,
        "productionTasks": production_tasks_dicts,
        "_production_tasks": production_tasks,
        "_machine_service": machine_service,
    }
