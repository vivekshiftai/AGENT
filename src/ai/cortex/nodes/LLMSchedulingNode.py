"""LLM-based scheduling optimization for multi-product machines.
CHG enhancement: per-machine prompt with allergen matrix, availability, MRP."""
import json
import logging
import re
from typing import Any, Dict, List

from src.ai.llm.client_factory import get_llm_client
from src.ai.llm.prompts.scheduling.system_prompt import SCHEDULING_SYSTEM_PROMPT
from src.ai.llm.prompts.scheduling.machine_prompt_builder import build_machine_prompt
from src.core.config import settings

logger = logging.getLogger(__name__)


def _parse_schedule_json(llm_response: str) -> list:
    text = llm_response.strip()
    text = re.sub(r'^```(?:json)?\s*', '', text)
    text = re.sub(r'\s*```$', '', text)
    match = re.search(r'\[[\s\S]*\]', text)
    if match:
        return json.loads(match.group())
    return json.loads(text)


def run(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Use LLM to optimize task scheduling per machine.
    CHG path: calls LLM once per machine with per-machine prompt.
    Generic path: uses existing LLMSchedulingService.
    """
    if state.get("needsClarification") or state.get("rejected"):
        return {**state}

    is_chg = state.get("_is_chg", False)

    # ── CHG path: per-machine LLM call ─────────────────────────────
    if is_chg:
        return _run_chg_scheduling(state)

    # ── Generic path (unchanged) ───────────────────────────────────
    return _run_generic_scheduling(state)


def _run_chg_scheduling(state: Dict[str, Any]) -> Dict[str, Any]:
    machine_queues = state.get("machine_queues") or {}
    availability = state.get("chg_machine_availability") or []
    allergen_matrix = state.get("chg_allergen_matrix") or []
    machines_info = state.get("chg_machines") or []
    mrp_alerts = state.get("chg_mrp_alerts") or []
    existing_wos = state.get("chg_work_orders_existing") or []

    machines_dict = {m["machine_id"]: m for m in machines_info}

    avail_by_machine: Dict[str, List] = {}
    for slot in availability:
        mid = slot.get("machine_id")
        if mid:
            avail_by_machine.setdefault(mid, []).append(slot)

    matrix_by_machine: Dict[str, List] = {}
    for row in allergen_matrix:
        mid = row.get("machine_id")
        if mid:
            matrix_by_machine.setdefault(mid, []).append(row)

    llm = get_llm_client()
    all_machine_schedules: Dict[str, list] = {}
    all_exceptions = list(state.get("scheduling_exceptions") or [])

    for machine_id, job_queue in machine_queues.items():
        if not job_queue:
            continue

        machine_info = machines_dict.get(machine_id, {"machine_id": machine_id, "machine_name": machine_id})
        machine_avail = avail_by_machine.get(machine_id, [])
        machine_allergen_rules = matrix_by_machine.get(machine_id, [])
        machine_mrp = [a for a in mrp_alerts if any(j.get("product_id") == a.get("product_id") for j in job_queue)]
        machine_existing_wos = [w for w in existing_wos if w.get("line_id") == machine_info.get("line_id")]

        available_slots = [s for s in machine_avail if s.get("status") not in ("MAINTENANCE", "BREAKDOWN", "OFFLINE") and float(s.get("available_hrs", 0) or 0) > 0]
        if machine_avail and not available_slots:
            all_exceptions.append({
                "type": "MACHINE_UNAVAILABLE",
                "severity": "HIGH",
                "message": f"Machine {machine_id} has no available shifts this week",
                "affected_jobs": [j.get("process_order_id") for j in job_queue[:3]],
            })
            continue

        prompt = build_machine_prompt(
            machine_info, job_queue, machine_avail,
            machine_allergen_rules, machine_mrp, machine_existing_wos,
        )

        try:
            model = settings.anthropic_model or "claude-sonnet-4-6"
            llm_response = llm.call_llm_unified(
                model=model,
                system_prompt=SCHEDULING_SYSTEM_PROMPT,
                user_prompt=prompt,
                node_name="LLMSchedulingNode",
                max_tokens=4096,
                temperature=0.1,
            )
            schedule = _parse_schedule_json(llm_response)
            all_machine_schedules[machine_id] = schedule

            for block in schedule:
                if block.get("block_type") == "EXCEPTION":
                    all_exceptions.append({
                        "type": block.get("exception_type", "UNKNOWN"),
                        "severity": block.get("severity", "MEDIUM"),
                        "message": block.get("notes", ""),
                        "machine_id": machine_id,
                        "process_order_id": block.get("process_order_id"),
                    })

        except Exception as e:
            logger.exception("LLMSchedulingNode CHG: failed to schedule machine %s", machine_id)
            all_exceptions.append({
                "type": "SCHEDULING_ERROR",
                "severity": "HIGH",
                "message": f"Failed to schedule machine {machine_id}: {str(e)}",
                "machine_id": machine_id,
            })

    logger.info(
        "LLMSchedulingNode CHG: scheduled %d machines, %d total blocks, %d exceptions",
        len(all_machine_schedules),
        sum(len(s) for s in all_machine_schedules.values()),
        len(all_exceptions),
    )

    return {
        **state,
        "machine_schedules_raw": all_machine_schedules,
        "scheduling_exceptions": all_exceptions,
    }


def _run_generic_scheduling(state: Dict[str, Any]) -> Dict[str, Any]:
    """Original generic scheduling for non-CHG datasources."""
    from src.domain.entities.machine import Machine, AllergenCleaningTime
    from src.domain.entities.production_task import ProductionTask, TaskType
    from src.domain.services.llm_scheduling_service import LLMSchedulingService

    production_tasks = state.get("_production_tasks") or []
    if not production_tasks:
        task_dicts = state.get("productionTasks") or []
        for td in task_dicts:
            from datetime import date
            due_date = None
            if td.get("delivery_target_date"):
                try:
                    due_date = date.fromisoformat(td["delivery_target_date"])
                except (ValueError, TypeError):
                    pass
            production_tasks.append(ProductionTask(
                task_id=td.get("task_id", ""),
                task_type=TaskType(td.get("task_type", "production")),
                product_id=td.get("product_id"),
                product_name=td.get("product_name"),
                quantity=td.get("quantity", 0),
                machine_id=td.get("machine_id"),
                machine_name=td.get("machine_name"),
                plant_id=td.get("plant_id"),
                line_id=td.get("line_id"),
                priority=td.get("priority", 0),
                allergens=td.get("allergens", []),
                delivery_target_date=due_date,
                estimated_duration_minutes=td.get("estimated_duration_minutes", 0),
            ))

    machines_dict = state.get("machines") or {}
    machine_assignments = state.get("machineAssignments") or {}
    date_range = state.get("dateRange") or (state.get("analysisResult") or {}).get("date_range")
    user_query = state.get("userQuery") or "Optimize production schedule"

    machines = []
    for mid, mdata in machines_dict.items():
        allergen_cleaning = []
        for act in mdata.get("allergen_cleaning_times", []):
            if isinstance(act, dict):
                allergen_cleaning.append(AllergenCleaningTime(
                    from_allergen=act.get("from_allergen", ""),
                    to_allergen=act.get("to_allergen", ""),
                    cleaning_minutes=act.get("cleaning_minutes", 60),
                ))
        machine = Machine(
            machine_id=mid,
            machine_name=mdata.get("machine_name", mid),
            machine_type=mdata.get("machine_type", "general"),
            plant_id=mdata.get("plant_id"),
            line_id=mdata.get("line_id"),
            capacity_per_hour=mdata.get("capacity_per_hour", 100),
            available_hours_per_day=mdata.get("available_hours_per_day", 8),
            changeover_time_minutes=mdata.get("changeover_time_minutes", 30),
            default_cleaning_time_minutes=mdata.get("default_cleaning_time_minutes", 60),
            allergen_cleaning_times=allergen_cleaning,
        )
        machines.append(machine)

    task_by_id = {t.task_id: t for t in production_tasks}
    scheduling_service = LLMSchedulingService()
    scheduling_result = {
        "machines": {},
        "cleaning_schedule": [],
        "risk_assessment": {"at_risk_deliveries": [], "overall_risk_level": "low"},
    }

    for machine in machines:
        task_ids = machine_assignments.get(machine.machine_id, [])
        if len(task_ids) <= 1:
            scheduling_result["machines"][machine.machine_id] = {
                "task_sequence": task_ids,
                "reasoning": "Single task, no optimization needed",
                "cleaning_events": [],
            }
            continue
        machine_tasks = [task_by_id[tid] for tid in task_ids if tid in task_by_id]
        if not machine_tasks:
            continue
        result = scheduling_service.optimize_machine_schedule(
            machine=machine, tasks=machine_tasks,
            user_query=user_query, date_range=date_range,
        )
        scheduling_result["machines"][machine.machine_id] = result
        if result.get("cleaning_events"):
            for ce in result["cleaning_events"]:
                ce["machine_id"] = machine.machine_id
                scheduling_result["cleaning_schedule"].append(ce)
        if result.get("risk_assessment", {}).get("at_risk_tasks"):
            for task_id in result["risk_assessment"]["at_risk_tasks"]:
                task = task_by_id.get(task_id)
                if task:
                    scheduling_result["risk_assessment"]["at_risk_deliveries"].append({
                        "task_id": task_id,
                        "product_name": task.product_name,
                        "delivery_date": task.delivery_target_date.isoformat() if task.delivery_target_date else None,
                    })

    if scheduling_result["risk_assessment"]["at_risk_deliveries"]:
        scheduling_result["risk_assessment"]["overall_risk_level"] = "medium"
        if len(scheduling_result["risk_assessment"]["at_risk_deliveries"]) > len(production_tasks) * 0.3:
            scheduling_result["risk_assessment"]["overall_risk_level"] = "high"

    logger.info(
        "LLMSchedulingNode generic: optimized %d machines, %d cleaning events, risk: %s",
        len(scheduling_result["machines"]),
        len(scheduling_result["cleaning_schedule"]),
        scheduling_result["risk_assessment"]["overall_risk_level"],
    )

    return {
        **state,
        "schedulingResult": scheduling_result,
        "_production_tasks": production_tasks,
    }
