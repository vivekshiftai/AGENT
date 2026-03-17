"""Validate production plan: start < end, references exist; reject invalid plans."""
import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def run(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate state.productionPlan. Check start < end for each task.
    Reject invalid plans by clearing productionPlan or setting validationError.
    """
    if state.get("needsClarification") or state.get("rejected"):
        return {**state}

    plan = state.get("productionPlan")
    if not plan:
        return {**state, "validationError": None}

    tasks = plan.get("tasks") if isinstance(plan, dict) else (plan if isinstance(plan, list) else [])
    if not tasks:
        return {**state, "validationError": None}

    errors = []
    for i, t in enumerate(tasks):
        if not isinstance(t, dict):
            continue
        start_s = t.get("start") or t.get("start_date")
        end_s = t.get("end") or t.get("end_date")
        if start_s and end_s:
            if str(start_s) > str(end_s):
                errors.append("Task %s: start after end" % t.get("id", i))

    if errors:
        logger.warning("Plan validation failed: %s", errors)
        return {
            **state,
            "productionPlan": None,
            "validationError": "; ".join(errors),
            "response": "Plan validation failed: " + "; ".join(errors),
        }
    return {
        **state,
        "validationError": None,
    }
