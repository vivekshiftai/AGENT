"""LLM-based scheduling optimization service."""
import json
import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from src.ai.llm.client_factory import get_llm_client
from src.ai.llm.prompts.generic_scheduling import (
    SCHEDULING_OPTIMIZER_SYSTEM,
    MULTI_MACHINE_SCHEDULING_SYSTEM,
    build_scheduling_user_prompt,
    build_multi_machine_prompt,
)
from src.core.config import settings
from src.domain.entities.machine import Machine
from src.domain.entities.production_task import ProductionTask, TaskType, TaskStatus

logger = logging.getLogger(__name__)


def _parse_llm_response(text: str) -> Optional[Dict[str, Any]]:
    """Parse JSON from LLM response, stripping markdown if present."""
    if not text or not text.strip():
        return None
    raw = text.strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```\s*$", "", raw)
    raw = raw.strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        logger.warning("LLM scheduling response parse failed: %s", e)
        return None


class LLMSchedulingService:
    """Service for LLM-based production scheduling optimization."""

    def __init__(self):
        self._llm = None

    def _get_llm(self):
        if self._llm is None:
            self._llm = get_llm_client()
        return self._llm

    def optimize_machine_schedule(
        self,
        machine: Machine,
        tasks: List[ProductionTask],
        user_query: str = "",
        date_range: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Optimize task sequence for a single machine using LLM.
        Returns optimized sequence with cleaning events and risk assessment.
        """
        if not tasks:
            return {
                "machine_id": machine.machine_id,
                "task_sequence": [],
                "cleaning_events": [],
                "risk_assessment": {"at_risk_tasks": [], "risk_notes": "No tasks to schedule"},
            }

        date_range_str = "Not specified"
        if date_range:
            date_range_str = f"{date_range.get('start', '')} to {date_range.get('end', '')}"

        allergen_cleaning_str = self._format_allergen_cleaning_times(machine)
        tasks_json = self._format_tasks_for_llm(tasks)

        user_prompt = build_scheduling_user_prompt(
            user_query=user_query,
            date_range=date_range_str,
            machine_id=machine.machine_id,
            machine_type=machine.machine_type,
            capacity_per_hour=machine.capacity_per_hour,
            available_hours=machine.available_hours_per_day,
            changeover_time=machine.changeover_time_minutes,
            default_cleaning_time=machine.default_cleaning_time_minutes,
            allergen_cleaning_times=allergen_cleaning_str,
            tasks_json=tasks_json,
        )

        try:
            llm = self._get_llm()
            model = settings.planning_planner_model or "claude-sonnet-4-6"
            
            if hasattr(llm, "call_llm_unified"):
                response = llm.call_llm_unified(
                    model=model,
                    system_prompt=SCHEDULING_OPTIMIZER_SYSTEM,
                    user_prompt=user_prompt,
                    use_json_mode=True,
                    default_max_tokens=16384,
                )
            else:
                response = llm.invoke(
                    [
                        {"role": "system", "content": SCHEDULING_OPTIMIZER_SYSTEM + "\n\nRespond with valid JSON only."},
                        {"role": "user", "content": user_prompt},
                    ],
                    model=model,
                    max_tokens=16384,
                )

            parsed = _parse_llm_response(response)
            if parsed and "machines" in parsed:
                machine_result = parsed["machines"].get(machine.machine_id, {})
                return {
                    "machine_id": machine.machine_id,
                    "task_sequence": machine_result.get("task_sequence", [t.task_id for t in tasks]),
                    "reasoning": machine_result.get("reasoning", ""),
                    "estimated_total_time_minutes": machine_result.get("estimated_total_time_minutes", 0),
                    "cleaning_events": machine_result.get("cleaning_events", []),
                    "risk_assessment": machine_result.get("risk_assessment", {}),
                    "overall_summary": parsed.get("overall_summary", {}),
                }
        except Exception as e:
            logger.warning("LLM scheduling optimization failed: %s, using fallback", e)

        return self._fallback_schedule(machine, tasks)

    def optimize_multi_machine_schedule(
        self,
        machines: List[Machine],
        tasks: List[ProductionTask],
        user_query: str = "",
        date_range: Optional[Dict[str, str]] = None,
        optimization_goals: Optional[Dict[str, bool]] = None,
    ) -> Dict[str, Any]:
        """
        Optimize schedule across multiple machines using LLM.
        """
        if not machines or not tasks:
            return {
                "schedule": {},
                "cleaning_schedule": [],
                "risk_assessment": {"at_risk_deliveries": [], "overall_risk_level": "low"},
            }

        date_range_str = "Not specified"
        if date_range:
            date_range_str = f"{date_range.get('start', '')} to {date_range.get('end', '')}"

        goals = optimization_goals or {}
        machines_json = json.dumps([self._machine_to_dict(m) for m in machines], indent=2)
        tasks_json = self._format_tasks_for_llm(tasks)
        constraints_json = json.dumps(self._build_constraints(machines), indent=2)

        user_prompt = build_multi_machine_prompt(
            user_query=user_query,
            date_range=date_range_str,
            machines_json=machines_json,
            tasks_json=tasks_json,
            constraints_json=constraints_json,
            maximize_production=goals.get("maximize_production", True),
            minimize_delivery_delays=goals.get("minimize_delivery_delays", True),
            minimize_changeover_time=goals.get("minimize_changeover_time", True),
            balance_workload=goals.get("balance_workload", True),
        )

        try:
            llm = self._get_llm()
            model = settings.planning_planner_model or "claude-sonnet-4-6"
            
            if hasattr(llm, "call_llm_unified"):
                response = llm.call_llm_unified(
                    model=model,
                    system_prompt=MULTI_MACHINE_SCHEDULING_SYSTEM,
                    user_prompt=user_prompt,
                    use_json_mode=True,
                    default_max_tokens=16384,
                )
            else:
                response = llm.invoke(
                    [
                        {"role": "system", "content": MULTI_MACHINE_SCHEDULING_SYSTEM + "\n\nRespond with valid JSON only."},
                        {"role": "user", "content": user_prompt},
                    ],
                    model=model,
                    max_tokens=16384,
                )

            parsed = _parse_llm_response(response)
            if parsed:
                return {
                    "schedule": parsed.get("schedule", {}),
                    "cleaning_schedule": parsed.get("cleaning_schedule", []),
                    "risk_assessment": parsed.get("risk_assessment", {}),
                    "alternative_sequences": parsed.get("alternative_sequences", []),
                }
        except Exception as e:
            logger.warning("LLM multi-machine scheduling failed: %s, using fallback", e)

        return self._fallback_multi_machine_schedule(machines, tasks)

    def _format_allergen_cleaning_times(self, machine: Machine) -> str:
        """Format allergen cleaning times for LLM prompt."""
        if not machine.allergen_cleaning_times:
            return f"Default cleaning time: {machine.default_cleaning_time_minutes} minutes for any allergen change"
        
        lines = []
        for act in machine.allergen_cleaning_times:
            lines.append(f"- {act.from_allergen} -> {act.to_allergen}: {act.cleaning_minutes} minutes")
        return "\n".join(lines)

    def _format_tasks_for_llm(self, tasks: List[ProductionTask]) -> str:
        """Format tasks as JSON for LLM prompt."""
        task_dicts = []
        for task in tasks:
            task_dicts.append({
                "task_id": task.task_id,
                "product_name": task.product_name,
                "product_id": task.product_id,
                "quantity": task.quantity,
                "delivery_date": task.delivery_target_date.isoformat() if task.delivery_target_date else None,
                "priority": task.priority,
                "estimated_duration_minutes": task.estimated_duration_minutes,
                "allergens": task.allergens,
                "recipe_requirements": {
                    m.material_id: m.required_quantity 
                    for m in task.material_requirements
                } if task.material_requirements else {},
            })
        return json.dumps(task_dicts, indent=2)

    def _machine_to_dict(self, machine: Machine) -> Dict[str, Any]:
        """Convert machine to dict for LLM prompt."""
        return {
            "machine_id": machine.machine_id,
            "machine_name": machine.machine_name,
            "machine_type": machine.machine_type,
            "capacity_per_hour": machine.capacity_per_hour,
            "available_hours_per_day": machine.available_hours_per_day,
            "changeover_time_minutes": machine.changeover_time_minutes,
            "default_cleaning_time_minutes": machine.default_cleaning_time_minutes,
            "compatible_products": machine.compatible_products,
            "current_allergens": machine.current_allergens,
        }

    def _build_constraints(self, machines: List[Machine]) -> Dict[str, Any]:
        """Build constraints dict for LLM prompt."""
        constraints = {
            "machines": {},
            "global": {
                "max_overtime_hours": 2,
                "prefer_no_weekend_work": True,
            }
        }
        for machine in machines:
            constraints["machines"][machine.machine_id] = {
                "allergen_cleaning_times": {
                    f"{act.from_allergen}->{act.to_allergen}": act.cleaning_minutes
                    for act in machine.allergen_cleaning_times
                },
                "changeover_time": machine.changeover_time_minutes,
                "max_capacity": machine.capacity_per_hour * machine.available_hours_per_day,
                "available_hours": machine.available_hours_per_day,
            }
        return constraints

    def _fallback_schedule(
        self,
        machine: Machine,
        tasks: List[ProductionTask],
    ) -> Dict[str, Any]:
        """Fallback scheduling when LLM fails - sort by priority and delivery date."""
        sorted_tasks = sorted(
            tasks,
            key=lambda t: (
                -t.priority,
                t.delivery_target_date or datetime.max.date(),
                -t.quantity,
            )
        )

        allergen_groups: Dict[str, List[ProductionTask]] = {}
        for task in sorted_tasks:
            allergen_key = ",".join(sorted(task.allergens)) if task.allergens else "none"
            if allergen_key not in allergen_groups:
                allergen_groups[allergen_key] = []
            allergen_groups[allergen_key].append(task)

        optimized_sequence = []
        for allergen_key in sorted(allergen_groups.keys()):
            optimized_sequence.extend(allergen_groups[allergen_key])

        cleaning_events = []
        prev_allergens = set()
        for i, task in enumerate(optimized_sequence):
            current_allergens = set(task.allergens) if task.allergens else set()
            if prev_allergens and current_allergens != prev_allergens:
                cleaning_minutes = machine.get_cleaning_time(
                    list(prev_allergens), list(current_allergens)
                )
                if cleaning_minutes > 0:
                    cleaning_events.append({
                        "after_task": optimized_sequence[i-1].task_id if i > 0 else None,
                        "before_task": task.task_id,
                        "cleaning_minutes": cleaning_minutes,
                        "reason": f"allergen change from {prev_allergens} to {current_allergens}",
                    })
            prev_allergens = current_allergens

        return {
            "machine_id": machine.machine_id,
            "task_sequence": [t.task_id for t in optimized_sequence],
            "reasoning": "Fallback: sorted by priority, delivery date, grouped by allergens",
            "cleaning_events": cleaning_events,
            "risk_assessment": {
                "at_risk_tasks": [],
                "risk_notes": "Fallback schedule - manual review recommended",
            },
        }

    def _fallback_multi_machine_schedule(
        self,
        machines: List[Machine],
        tasks: List[ProductionTask],
    ) -> Dict[str, Any]:
        """Fallback multi-machine scheduling - round-robin assignment."""
        schedule = {m.machine_id: {"task_sequence": [], "reasoning": "Fallback round-robin"} for m in machines}
        
        sorted_tasks = sorted(
            tasks,
            key=lambda t: (
                -t.priority,
                t.delivery_target_date or datetime.max.date(),
            )
        )
        
        machine_ids = [m.machine_id for m in machines]
        for i, task in enumerate(sorted_tasks):
            machine_id = machine_ids[i % len(machine_ids)]
            schedule[machine_id]["task_sequence"].append(task.task_id)

        return {
            "schedule": schedule,
            "cleaning_schedule": [],
            "risk_assessment": {
                "at_risk_deliveries": [],
                "overall_risk_level": "medium",
                "notes": "Fallback schedule - manual review recommended",
            },
        }
