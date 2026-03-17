"""API controller: chat, plan, datasources; orchestrator with use cases."""
import logging
from typing import Any, Dict, List, Optional

from src.ai.cortex.orchestrator import PlanningOrchestrator
from src.application.dto.planning_request import PlanningRequest
from src.application.use_cases.analyze_query_usecase import AnalyzeQueryUseCase
from src.application.use_cases.manage_datasource_usecase import ManageDatasourceUseCase
from src.application.use_cases.plan_production_usecase import PlanProductionUseCase
from src.domain.repositories.datasource_repository import IDatasourceRepository
from src.domain.services.production_planner import ProductionPlanner
from src.infrastructure.database.datasource_repository_impl import DatasourceRepositoryImpl

logger = logging.getLogger(__name__)

_production_planner: Optional[ProductionPlanner] = None
_orchestrator: Optional[PlanningOrchestrator] = None
_datasource_repo: Optional[IDatasourceRepository] = None
_manage_datasource_uc: Optional[ManageDatasourceUseCase] = None


def get_datasource_repository() -> IDatasourceRepository:
    global _datasource_repo
    if _datasource_repo is None:
        _datasource_repo = DatasourceRepositoryImpl()
    return _datasource_repo


def get_production_planner() -> ProductionPlanner:
    global _production_planner
    if _production_planner is None:
        try:
            from src.infrastructure.external_services.frepple_service import run_frepple
            _production_planner = ProductionPlanner(frepple_runner=run_frepple)
        except Exception:
            _production_planner = ProductionPlanner()
    return _production_planner


def get_manage_datasource_use_case() -> ManageDatasourceUseCase:
    global _manage_datasource_uc
    if _manage_datasource_uc is None:
        _manage_datasource_uc = ManageDatasourceUseCase(get_datasource_repository())
    return _manage_datasource_uc


def get_orchestrator() -> PlanningOrchestrator:
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = PlanningOrchestrator(
            datasource_repository=get_datasource_repository(),
            analyze_query_use_case=AnalyzeQueryUseCase(),
            plan_production_use_case=PlanProductionUseCase(get_production_planner()),
        )
    return _orchestrator


def chat(
    user_message: str,
    user_id: Optional[str] = None,
    datasource_ids: Optional[List[int]] = None,
) -> Dict[str, Any]:
    """Handle /chat: run orchestrator and return comprehensive planning response."""
    orch = get_orchestrator()
    repo = get_datasource_repository()
    datasource_configs = None
    if datasource_ids and len(datasource_ids) > 0:
        configs = []
        for did in datasource_ids:
            try:
                entity = repo.get_by_id(did)
                if entity:
                    configs.append(entity.to_connection_config())
            except Exception as e:
                logger.warning("Skip datasource id %s: %s", did, e)
        datasource_configs = configs if configs else None
        logger.info("Chat: using %s selected datasource(s) (ids=%s)", len(configs or []), datasource_ids)
    else:
        logger.info("Chat: using all datasources (no selection)")
    result = orch.run(
        user_message=user_message,
        user_id=user_id,
        datasource_configs=datasource_configs,
    )
    
    plan_tasks = result.get("plan_tasks") or []
    sales_orders_by_material = result.get("sales_orders_by_material") or []
    
    product_targets = result.get("product_targets") or []
    
    material_shortages = result.get("material_shortages") or []
    inventory_check = result.get("inventory_check") or {}
    if not material_shortages and inventory_check.get("shortages"):
        material_shortages = inventory_check["shortages"]
    
    machine_schedules = []
    scheduling_result = result.get("scheduling_result") or {}
    machines = result.get("machines") or {}
    for machine_id, schedule in scheduling_result.get("machines", {}).items():
        machine_data = machines.get(machine_id, {})
        machine_schedules.append({
            "machine_id": machine_id,
            "machine_name": machine_data.get("machine_name", machine_id),
            "task_sequence": schedule.get("task_sequence", []),
            "reasoning": schedule.get("reasoning", ""),
            "cleaning_events": schedule.get("cleaning_events", []),
        })
    
    validation_issues = []
    validation_result = result.get("validation_result") or {}
    for issue in validation_result.get("issues", []):
        validation_issues.append({
            "issue_type": issue.get("type", "unknown"),
            "severity": "error",
            "message": _format_validation_issue(issue),
            "task_id": issue.get("task_id"),
            "details": issue,
        })
    for warning in validation_result.get("warnings", []):
        validation_issues.append({
            "issue_type": warning.get("type", "unknown"),
            "severity": "warning",
            "message": _format_validation_issue(warning),
            "task_id": warning.get("task_id"),
            "details": warning,
        })
    
    risk_level = scheduling_result.get("risk_assessment", {}).get("overall_risk_level", "low")
    
    inventory_summary = None
    if inventory_check:
        inventory_summary = {
            "total_materials": len(inventory_check.get("materials", {})),
            "sufficient_count": inventory_check.get("sufficient_count", 0),
            "shortage_count": inventory_check.get("shortage_count", 0),
        }
    
    scheduling_summary = None
    if scheduling_result:
        scheduling_summary = {
            "machines_scheduled": len(scheduling_result.get("machines", {})),
            "total_cleaning_events": len(scheduling_result.get("cleaning_schedule", [])),
            "at_risk_deliveries": len(scheduling_result.get("risk_assessment", {}).get("at_risk_deliveries", [])),
        }
    
    allergen_warnings = result.get("allergen_warnings") or []
    scheduling_exceptions = result.get("scheduling_exceptions") or []

    logger.info("Chat: response ready, plan_tasks=%s, risk=%s, chg=%s", len(plan_tasks), risk_level, result.get("is_chg", False))
    return {
        "response": result["response"],
        "plan_tasks": plan_tasks,
        "sales_orders_by_material": sales_orders_by_material,
        "product_targets": product_targets,
        "material_shortages": material_shortages,
        "machine_schedules": machine_schedules,
        "validation_issues": validation_issues,
        "risk_level": risk_level,
        "inventory_summary": inventory_summary,
        "scheduling_summary": scheduling_summary,
        "allergen_warnings": allergen_warnings,
        "scheduling_exceptions": scheduling_exceptions,
        # Detail panel data
        "sales_orders": result.get("sales_orders") or [],
        "recipes": result.get("recipes") or [],
        "machines_info": result.get("machines_info") or [],
        "cip_procedures": result.get("cip_procedures") or [],
    }


def _format_validation_issue(issue: Dict[str, Any]) -> str:
    """Format a validation issue as a human-readable message."""
    issue_type = issue.get("type", "unknown")
    
    if issue_type == "delivery_risk":
        return f"Delivery at risk for {issue.get('product', 'product')}: scheduled {issue.get('scheduled_end', '')} vs target {issue.get('delivery_date', '')}"
    elif issue_type == "material_shortage":
        return f"Material shortage: {issue.get('material_id', '')} needs {issue.get('required', 0)}, available {issue.get('available', 0)}"
    elif issue_type == "capacity_exceeded":
        return f"Machine {issue.get('machine_id', '')} over capacity at {issue.get('utilization_percent', 0):.1f}%"
    elif issue_type == "tight_timeline":
        return f"Tight timeline for task with only {issue.get('buffer_days', 0)} day(s) buffer"
    elif issue_type == "high_utilization":
        return f"High utilization on {issue.get('machine_id', '')}: {issue.get('utilization_percent', 0):.1f}%"
    else:
        return str(issue)


def plan(
    plan_id: Optional[str] = None,
    input_data: Optional[Dict[str, Any]] = None,
    use_frepple: bool = False,
) -> Dict[str, Any]:
    """Handle /plan: return production plan tasks for Gantt."""
    planner = get_production_planner()
    use_case = PlanProductionUseCase(planner)
    request = PlanningRequest(
        plan_id=plan_id or "plan-1",
        input_data=input_data or {},
        use_frepple=use_frepple,
    )
    return use_case.execute(request)


def add_datasource(
    name: str,
    type: str,
    host: Optional[str] = None,
    port: Optional[int] = None,
    database: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    extra_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Add datasource after testing connection. Returns { success, datasource? } or { success, error }."""
    return get_manage_datasource_use_case().add_datasource(
        name=name, type=type, host=host, port=port, database=database,
        username=username, password=password, extra_config=extra_config,
    )


def list_datasources() -> List[Dict[str, Any]]:
    """List all datasources (password masked)."""
    return get_manage_datasource_use_case().list_datasources()


def delete_datasource(id: int) -> Dict[str, Any]:
    """Delete datasource by id."""
    return get_manage_datasource_use_case().delete_datasource(id)


def test_datasource_connection(config: Dict[str, Any]) -> Dict[str, Any]:
    """Test connection without saving. Returns { success, error? }."""
    return get_manage_datasource_use_case().test_connection(config)
