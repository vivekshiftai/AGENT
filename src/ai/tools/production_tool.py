"""Tool: create production plan (for agent use)."""
import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def production_tool(
    plan_id: str,
    production_planner: Any,
    input_data: Dict[str, Any] = None,
    use_frepple: bool = False,
) -> Dict[str, Any]:
    """Return production plan tasks in Gantt format."""
    from src.application.dto.planning_request import PlanningRequest
    from src.application.use_cases.plan_production_usecase import PlanProductionUseCase

    use_case = PlanProductionUseCase(production_planner)
    request = PlanningRequest(
        plan_id=plan_id,
        input_data=input_data or {},
        use_frepple=use_frepple,
    )
    return use_case.execute(request)
