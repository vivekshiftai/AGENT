"""
Agent state for the LangGraph planning workflow.

Core fields:
- userQuery, analysisResult, dateRange, products, requiredSources, rawData, normalizedData

Production planning workflow:
- salesOrders, productTargets, recipes, inventory, inventoryCheck
- materialPicks, machines, machineAssignments, schedulingResult
- productionTasks, validationResult

CHG-specific fields:
- chg_plants, chg_machines, chg_production_lines, chg_machine_availability
- chg_allergen_matrix, chg_mrp_alerts, chg_open_pos, chg_work_orders_existing
- process_orders, machine_queues, machine_schedules_raw
- allergen_clean_blocks, scheduling_exceptions
- planning_week_start, planning_week_end

Output:
- productionPlan, ganttTasks, response
- scheduling_summary, allergen_warnings
"""
from typing import Any, Dict, List, Optional

AgentState = Dict[str, Any]


def create_initial_state(
    user_query: str,
    user_id: Optional[str] = None,
    **kwargs: Any,
) -> AgentState:
    """Create initial graph state."""
    return {
        "userQuery": user_query,
        "user_id": user_id or "default",
        "analysisResult": None,
        "dateRange": None,
        "products": [],
        "requiredSources": [],
        "rawData": {},
        "normalizedData": [],
        # Production planning workflow fields
        "salesOrders": [],
        "productTargets": [],
        "recipes": {},
        "inventory": {},
        "inventoryCheck": {},
        "materialPicks": [],
        "machines": {},
        "machineAssignments": {},
        "schedulingResult": {},
        "productionTasks": [],
        "validationResult": {},
        # CHG-specific fields (fetched from ClickHouse)
        "chg_plants": [],
        "chg_machines": [],
        "chg_production_lines": [],
        "chg_machine_availability": [],
        "chg_allergen_matrix": [],
        "chg_mrp_alerts": [],
        "chg_open_pos": [],
        "chg_work_orders_existing": [],
        # Built during pipeline
        "process_orders": [],
        "machine_queues": {},
        "machine_schedules_raw": {},
        "allergen_clean_blocks": [],
        "scheduling_exceptions": [],
        "planning_week_start": "",
        "planning_week_end": "",
        # Output fields
        "productionPlan": None,
        "ganttTasks": [],
        "response": "",
        "needsClarification": False,
        "rejected": False,
        "messages": [{"role": "user", "content": user_query}],
        "scheduling_summary": {},
        "allergen_warnings": [],
        **kwargs,
    }
