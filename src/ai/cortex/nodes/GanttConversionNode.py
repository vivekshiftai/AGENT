"""Convert production plan tasks into Gantt-ready format with type for coloring.
CHG enhancement: handles block types from per-machine LLM scheduling."""
import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

BLOCK_COLORS = {
    "PRODUCTION":  "#22c55e",
    "SETUP":       "#3b82f6",
    "CLEANING":    "#f97316",
    "HOLD":        "#ef4444",
    "PRE_COOL":    "#06b6d4",
    "MAINTENANCE": "#6b7280",
    "BLOCKED":     "#991b1b",
    "EXCEPTION":   "#fbbf24",
}

BLOCK_CSS_CLASS = {
    "PRODUCTION":  "bar-production",
    "SETUP":       "bar-setup",
    "CLEANING":    "bar-cleaning",
    "HOLD":        "bar-hold",
    "PRE_COOL":    "bar-precool",
    "MAINTENANCE": "bar-maintenance",
    "BLOCKED":     "bar-blocked",
    "EXCEPTION":   "bar-exception",
}


def _get_block_label(block: Dict) -> str:
    bt = block.get("block_type", "")
    if bt == "PRODUCTION":
        return f"{block.get('product_name', '?')} — Step {block.get('step_number', '?')}: {block.get('step_name', '')}"
    if bt == "CLEANING":
        return f"Clean: {block.get('clean_type', '')} ({block.get('duration_min', 0)} min)"
    if bt == "HOLD":
        return f"ATP Swab Hold ({block.get('duration_min', 0)} min)"
    if bt == "SETUP":
        return f"Setup: {block.get('product_name', '')}"
    if bt == "PRE_COOL":
        return f"IQF Pre-Cool ({block.get('duration_min', 90)} min)"
    if bt == "MAINTENANCE":
        return block.get("notes", "Maintenance")
    if bt == "BLOCKED":
        return f"BLOCKED — {block.get('ingredient_blocked', block.get('blocked_reason', 'shortage'))}"
    if bt == "EXCEPTION":
        return f"EXCEPTION: {block.get('notes', '')}"
    return block.get("block_type", "Task")


def _convert_chg_block(block: Dict, machine_id: str, machine_name: str) -> Dict[str, Any]:
    bt = block.get("block_type", "PRODUCTION")
    result = dict(block)
    result.update({
        "id": f"{machine_id}-{block.get('start_datetime', '')}-{block.get('step_number', '')}",
        "machine_id": machine_id,
        "machine_name": machine_name,
        "name": _get_block_label(block),
        "start": block.get("start_datetime", ""),
        "end": block.get("end_datetime", ""),
        "duration_min": block.get("duration_min", 0),
        "type": bt.lower(),
        "block_type": bt,
        "color": BLOCK_COLORS.get(bt, "#6b7280"),
        "custom_class": BLOCK_CSS_CLASS.get(bt, "bar-production"),
        "allergens": block.get("allergens", block.get("from_allergens", [])),
        "is_ccp": block.get("is_ccp", False),
        "atp_required": block.get("atp_swab_required", False),
        "progress": 0,
    })
    return result


def run(state: Dict[str, Any]) -> Dict[str, Any]:
    if state.get("needsClarification") or state.get("rejected"):
        return {**state, "ganttTasks": state.get("ganttTasks") or []}

    is_chg = state.get("_is_chg", False)

    # ── CHG path: convert machine_schedules_raw blocks ──────────────
    if is_chg:
        machine_schedules = state.get("machine_schedules_raw") or {}
        machines_dict = state.get("machines") or {}
        gantt_tasks = []

        for machine_id, blocks in machine_schedules.items():
            machine_info = machines_dict.get(machine_id, {})
            machine_name = machine_info.get("machine_name", machine_id)
            for block in blocks:
                if not isinstance(block, dict):
                    continue
                gantt_tasks.append(_convert_chg_block(block, machine_id, machine_name))

        gantt_tasks.sort(key=lambda t: t.get("start", ""))
        logger.info("GanttConversionNode CHG: %d gantt tasks from %d machines", len(gantt_tasks), len(machine_schedules))
        return {**state, "ganttTasks": gantt_tasks}

    # ── Generic path (unchanged) ────────────────────────────────────
    plan = state.get("productionPlan")
    tasks = []
    if isinstance(plan, dict) and plan.get("tasks"):
        tasks = list(plan["tasks"])
    elif isinstance(plan, list):
        tasks = list(plan)

    gantt_tasks = []
    for t in tasks:
        if not isinstance(t, dict):
            continue
        start = str(t.get("start", "2026-01-01"))[:10]
        end = str(t.get("end", "2026-01-02"))[:10]
        if hasattr(t.get("start"), "isoformat"):
            start = t["start"].isoformat()[:10]
        if hasattr(t.get("end"), "isoformat"):
            end = t["end"].isoformat()[:10]

        machine = t.get("machine") or ""
        plant = t.get("plant") or ""
        line = t.get("line") or ""
        product = t.get("product") or ""
        quantity = t.get("quantity")
        task_name = str(t.get("name", "Task"))
        task_type = str(t.get("type", "production"))

        gantt_tasks.append({
            "id": str(t.get("id", "task-%s" % len(gantt_tasks))),
            "name": task_name,
            "start": start,
            "end": end,
            "progress": int(t.get("progress", 0)) if t.get("progress") is not None else 0,
            "custom_class": "bar-cleaning" if task_type == "cleaning" else "bar-production",
            "type": task_type,
            "plant": plant,
            "line": line,
            "machine": machine,
            "product": product,
            "quantity": quantity,
        })

    logger.info("GanttConversionNode: %s gantt tasks", len(gantt_tasks))
    return {**state, "ganttTasks": gantt_tasks}
