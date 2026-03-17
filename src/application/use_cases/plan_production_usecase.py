"""Use case: create a production plan (tasks for Gantt)."""
import logging
from typing import Any, Dict, List

from src.application.dto.planning_request import PlanningRequest
from src.domain.entities.production_plan import ProductionPlan
from src.domain.services.production_planner import ProductionPlanner

logger = logging.getLogger(__name__)


class PlanProductionUseCase:
    """Creates a production plan via ProductionPlanner (frePPLe or mock)."""

    def __init__(self, production_planner: ProductionPlanner):
        self._planner = production_planner

    def execute(self, request: PlanningRequest) -> Dict[str, Any]:
        """Return plan as dict with tasks in Gantt format."""
        try:
            plan: ProductionPlan = self._planner.create_plan(
                plan_id=request.plan_id,
                input_data=request.input_data,
                use_frepple=request.use_frepple,
            )
            tasks: List[Dict[str, Any]] = plan.to_gantt_tasks()
            return {"plan_id": plan.plan_id, "tasks": tasks, "error": None}
        except Exception as e:
            logger.exception("PlanProductionUseCase failed: %s", e)
            return {"plan_id": request.plan_id, "tasks": [], "error": str(e)}
