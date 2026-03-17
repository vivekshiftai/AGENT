"""Orchestrator: runs the planning graph with optional datasources and use cases."""
import logging
from typing import Any, Dict, List, Optional

from src.ai.cortex.graphs.PlanningGraph import run as run_planning_graph
from src.ai.cortex.state.AgentState import create_initial_state
from src.ai.llm.client_factory import get_llm_client

logger = logging.getLogger(__name__)


class PlanningOrchestrator:
    """Runs the cortex planning graph and returns comprehensive planning response."""

    def __init__(
        self,
        llm=None,
        datasource_repository=None,
        analyze_query_use_case=None,
        plan_production_use_case=None,
    ):
        self._llm = llm if llm is not None else get_llm_client()
        self._datasource_repo = datasource_repository
        self._analyze_query_uc = analyze_query_use_case
        self._plan_production_uc = plan_production_use_case

    def run(
        self,
        user_message: str,
        user_id: Optional[str] = None,
        plan_tasks: Optional[List[Dict[str, Any]]] = None,
        datasource_configs: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        state = create_initial_state(user_message, user_id=user_id)
        if plan_tasks is not None:
            state["ganttTasks"] = plan_tasks

        configs = datasource_configs
        if configs is None and self._datasource_repo:
            try:
                all_ds = self._datasource_repo.get_all()
                configs = [d.to_connection_config() for d in all_ds]
                logger.info("Orchestrator: loaded %s datasource(s) from repository", len(configs))
            except Exception as e:
                logger.warning("Could not load datasources: %s", e)
                configs = []
        else:
            logger.info("Orchestrator: using %s datasource config(s) from request", len(configs or []))

        final = run_planning_graph(
            state,
            datasource_configs=configs or [],
            datasource_repository=self._datasource_repo,
            analyze_query_use_case=self._analyze_query_uc,
            plan_production_use_case=self._plan_production_uc,
        )

        recipes_raw = final.get("recipes") or {}
        recipes_list = list(recipes_raw.values()) if isinstance(recipes_raw, dict) else list(recipes_raw)

        cip_procedures = self._extract_cip_procedures(final)

        return {
            "response": final.get("response") or "",
            "plan_tasks": final.get("ganttTasks") or [],
            "sales_orders_by_material": final.get("salesOrdersByMaterial") or [],
            "product_targets": final.get("productTargets") or [],
            "material_shortages": final.get("materialShortages") or [],
            "machines": final.get("machines") or {},
            "scheduling_result": final.get("schedulingResult") or {},
            "validation_result": final.get("validationResult") or {},
            "inventory_check": final.get("inventoryCheck") or {},
            "material_picks": final.get("materialPicks") or [],
            "procurement_list": final.get("procurementList") or [],
            # CHG-specific outputs
            "scheduling_summary": final.get("scheduling_summary") or {},
            "allergen_warnings": final.get("allergen_warnings") or [],
            "scheduling_exceptions": final.get("scheduling_exceptions") or [],
            "process_orders": final.get("process_orders") or [],
            "machine_schedules_raw": final.get("machine_schedules_raw") or {},
            "is_chg": final.get("_is_chg", False),
            # Detail panel data
            "sales_orders": final.get("salesOrders") or [],
            "recipes": recipes_list,
            "machines_info": final.get("chg_machines") or [],
            "cip_procedures": cip_procedures,
        }

    @staticmethod
    def _extract_cip_procedures(final: Dict[str, Any]) -> list:
        """Pull CIP procedure rows from rawData if available."""
        raw_data = final.get("rawData") or {}
        for source_data in raw_data.values():
            if not isinstance(source_data, dict):
                continue
            for table_name, df in source_data.items():
                if "cip" not in str(table_name).lower():
                    continue
                try:
                    import pandas as pd
                    if isinstance(df, pd.DataFrame) and not df.empty:
                        return df.to_dict(orient="records")
                except Exception:
                    pass
        return []
