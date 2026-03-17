"""
Build production schedule: LLM prioritizes product order per machine,
code builds the actual Gantt tasks with cleaning slots.
"""
import json
import logging
import re
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from src.ai.llm.client_factory import get_llm_client
from src.ai.llm.prompts.planner import PLANNER_SYSTEM, build_planner_user_prompt
from src.core.config import settings

logger = logging.getLogger(__name__)

DEFAULT_CLEANING_HOURS = 4
DEFAULT_QTY_PER_DAY = 500


# ---------------------------------------------------------------------------
# 1. Aggregate data into machine → product groups
# ---------------------------------------------------------------------------

def _aggregate_by_machine(normalized: List[Dict[str, Any]]) -> Dict[str, Dict[str, Dict]]:
    """Group records: machine → product → { total_qty, dates, plant, line, cleaning_time }."""
    machines: Dict[str, Dict[str, Dict]] = defaultdict(lambda: defaultdict(lambda: {
        "total_qty": 0, "dates": [], "plant": None, "line": None, "cleaning_time": None,
    }))
    for r in normalized:
        if not isinstance(r, dict):
            continue
        machine = r.get("machine") or "Machine 1"
        product = str(r.get("product") or "Unknown").strip()
        entry = machines[machine][product]
        entry["total_qty"] += float(r.get("quantity") or 0)
        d = str(r.get("date") or "")[:10]
        if d:
            entry["dates"].append(d)
        entry["plant"] = entry["plant"] or r.get("plant") or "Default Plant"
        entry["line"] = entry["line"] or r.get("line") or "Line 1"
        if r.get("cleaning_time") is not None:
            entry["cleaning_time"] = r.get("cleaning_time")
    return dict(machines)


def _build_summary_for_llm(machines: Dict[str, Dict[str, Dict]]) -> str:
    """Build a text summary for the LLM from aggregated machine data."""
    lines = []
    for machine, products in sorted(machines.items()):
        plant = next(iter(products.values()))["plant"] if products else "Default Plant"
        line = next(iter(products.values()))["line"] if products else "Line 1"
        lines.append(f"\nMachine: {machine}  (Plant: {plant}, Line: {line})")
        for product, info in sorted(products.items(), key=lambda x: -x[1]["total_qty"]):
            dates = sorted(set(info["dates"]))[:5]
            ct = info.get("cleaning_time")
            ct_str = f", cleaning={ct}h" if ct else ""
            lines.append(f"  - {product}: qty={info['total_qty']:.0f}, orders={len(info['dates'])}, dates={', '.join(dates)}{ct_str}")
    return "\n".join(lines) if lines else "No data."


# ---------------------------------------------------------------------------
# 2. Ask LLM for product priority order per machine
# ---------------------------------------------------------------------------

def _parse_priority_response(text: str) -> Optional[Dict]:
    """Parse LLM JSON response with machine priority orders."""
    if not text or not text.strip():
        return None
    raw = text.strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```\s*$", "", raw)
    raw = raw.strip()
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
        return None
    except json.JSONDecodeError as e:
        logger.warning("ProductionPlanningNode: priority JSON parse failed: %s", e)
        return None


def _get_llm_priority(
    machines: Dict[str, Dict[str, Dict]],
    user_query: str,
    date_range_str: str,
) -> Dict[str, List[str]]:
    """
    Ask LLM for product processing order per machine.
    Returns { machine_name: [product_a, product_b, ...] }.
    Falls back to qty-based sorting if LLM fails.
    """
    summary = _build_summary_for_llm(machines)
    priority: Dict[str, List[str]] = {}

    try:
        llm = get_llm_client()
        model = settings.planning_planner_model or getattr(
            llm, "_default_model", lambda: "claude-sonnet-4-6"
        )()
        user_prompt = build_planner_user_prompt(user_query, date_range_str, summary)
        if hasattr(llm, "call_llm_unified"):
            response = llm.call_llm_unified(
                model=model,
                system_prompt=PLANNER_SYSTEM,
                user_prompt=user_prompt,
                use_json_mode=True,
                default_max_tokens=16384,
            )
        else:
            response = llm.invoke(
                [
                    {"role": "system", "content": PLANNER_SYSTEM},
                    {"role": "user", "content": user_prompt},
                ],
                model=model,
                max_tokens=16384,
            )
        parsed = _parse_priority_response(response)
        if parsed and "machines" in parsed:
            for m_name, m_info in parsed["machines"].items():
                if isinstance(m_info, dict) and "priority_order" in m_info:
                    priority[m_name] = m_info["priority_order"]
                elif isinstance(m_info, list):
                    priority[m_name] = m_info
        if priority:
            logger.info("ProductionPlanningNode: LLM prioritized %s machine(s)", len(priority))
        else:
            logger.warning("ProductionPlanningNode: LLM returned empty priority, using demand-based sort")
    except Exception as e:
        logger.warning("ProductionPlanningNode: LLM priority failed (%s), using demand-based sort", e)

    # Fallback: sort by total_qty descending
    for machine, products in machines.items():
        if machine not in priority:
            priority[machine] = [
                p for p, _ in sorted(products.items(), key=lambda x: -x[1]["total_qty"])
            ]

    return priority


# ---------------------------------------------------------------------------
# 3. Build the schedule from priority + data
# ---------------------------------------------------------------------------

def _build_schedule(
    machines: Dict[str, Dict[str, Dict]],
    priority: Dict[str, List[str]],
    date_range: Any,
) -> List[Dict[str, Any]]:
    """
    Build production + cleaning tasks per machine using the priority order.
    Cleaning tasks inserted when product changes on a machine.
    """
    base_date = datetime.today()
    horizon_end: Optional[datetime] = None
    if isinstance(date_range, dict):
        start_str = str(date_range.get("start") or "")[:10]
        end_str = str(date_range.get("end") or "")[:10]
        try:
            if start_str:
                base_date = datetime.strptime(start_str, "%Y-%m-%d")
        except Exception:
            pass
        try:
            if end_str:
                horizon_end = datetime.strptime(end_str, "%Y-%m-%d") + timedelta(days=1)
        except Exception:
            horizon_end = None
    # Default horizon: 30 days from base_date if no explicit end
    if horizon_end is None:
        horizon_end = base_date + timedelta(days=30)

    tasks: List[Dict[str, Any]] = []
    task_id = 0

    for machine in sorted(machines.keys()):
        products = machines[machine]
        order = priority.get(machine, list(products.keys()))
        # Include products not in priority (shouldn't happen, but safe)
        remaining = [p for p in products if p not in order]
        order = order + remaining

        current_date = base_date
        prev_product = None
        plant = next(iter(products.values()))["plant"] if products else "Default Plant"
        line = next(iter(products.values()))["line"] if products else "Line 1"

        for product in order:
            if product not in products:
                continue
            info = products[product]

            # Stop scheduling if horizon exceeded
            if current_date >= horizon_end:
                break

            # Insert cleaning task if product changes
            if prev_product is not None and prev_product != product:
                clean_hours = info.get("cleaning_time") or DEFAULT_CLEANING_HOURS
                task_id += 1
                clean_end = min(current_date + timedelta(hours=clean_hours), horizon_end)
                tasks.append({
                    "id": f"clean-{task_id}",
                    "name": f"Cleaning ({clean_hours}h)",
                    "type": "cleaning",
                    "plant": plant,
                    "line": line,
                    "machine": machine,
                    "product": product,
                    "start": current_date.strftime("%Y-%m-%d"),
                    "end": clean_end.strftime("%Y-%m-%d"),
                    "progress": 0,
                    "quantity": None,
                })
                current_date = clean_end

            # Production task
            qty = info["total_qty"]
            days_needed = max(1, int(qty / DEFAULT_QTY_PER_DAY) + (1 if qty % DEFAULT_QTY_PER_DAY else 0))
            task_id += 1
            end_date = min(current_date + timedelta(days=days_needed), horizon_end)
            tasks.append({
                "id": f"prod-{task_id}",
                "name": product,
                "type": "production",
                "plant": plant,
                "line": line,
                "machine": machine,
                "product": product,
                "start": current_date.strftime("%Y-%m-%d"),
                "end": end_date.strftime("%Y-%m-%d"),
                "progress": 0,
                "quantity": qty,
            })
            current_date = end_date
            prev_product = product

    return tasks


# ---------------------------------------------------------------------------
# 4. Main entry point
# ---------------------------------------------------------------------------

def run(state: Dict[str, Any]) -> Dict[str, Any]:
    if state.get("needsClarification") or state.get("rejected"):
        return {**state, "productionPlan": state.get("productionPlan")}

    normalized = state.get("normalizedData") or []
    if not normalized:
        return {**state, "productionPlan": {"tasks": [], "plan_id": "empty"}}

    user_query = state.get("userQuery") or "Generate production plan"
    date_range = state.get("dateRange") or (state.get("analysisResult") or {}).get("date_range")
    date_range_str = "Not specified"
    if isinstance(date_range, dict):
        date_range_str = "%s to %s" % (date_range.get("start", ""), date_range.get("end", ""))
    elif date_range:
        date_range_str = str(date_range)

    machines = _aggregate_by_machine(normalized)
    logger.info("ProductionPlanningNode: %s records across %s machine(s)", len(normalized), len(machines))

    priority = _get_llm_priority(machines, user_query, date_range_str)
    tasks = _build_schedule(machines, priority, date_range)

    logger.info("ProductionPlanningNode: built %s tasks (production + cleaning)", len(tasks))
    return {
        **state,
        "productionPlan": {"tasks": tasks, "plan_id": "optimized-plan"},
    }
