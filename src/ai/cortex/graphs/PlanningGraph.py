"""
LangGraph planning workflow: register nodes and run in order.

Comprehensive Production Planning Flow:
  1. QueryAnalysisNode -> ClarificationNode -> [if not needsClarification]
  2. DataFetchNode (uses user-selected _datasource_configs; CHG queries if detected)
  3. DataNormalizationNode
  4. SalesOrderProcessingNode - Extract sales orders and product targets
  5. RecipeFetchNode - Get recipes/BOMs for products (CHG: step-level detail)
  6. ProcessOrderBuildNode (CHG only) - Explode SO items into process orders + machine queues
  7. InventoryCheckNode - Check material availability
  8. MaterialPickingNode - Create pick lists, flag shortages
  9. MachineAssignmentNode - Assign products to machines (CHG: GF constraint, MRP blocks)
  10. LLMSchedulingNode - Per-machine LLM scheduling with allergen awareness
  11. ScheduleValidationNode - Validate against constraints
  12. GanttConversionNode - Convert blocks to Gantt tasks
  13. ResponseGenerationNode - Build response + scheduling summary
"""
import logging
from typing import Any, Dict, List

from src.ai.cortex.nodes.QueryAnalysisNode import run as run_query_analysis
from src.ai.cortex.nodes.ClarificationNode import run as run_clarification
from src.ai.cortex.nodes.DataFetchNode import run as run_data_fetch
from src.ai.cortex.nodes.DataNormalizationNode import run as run_data_normalization
from src.ai.cortex.nodes.SalesOrderProcessingNode import run as run_sales_order_processing
from src.ai.cortex.nodes.RecipeFetchNode import run as run_recipe_fetch
from src.ai.cortex.nodes.ProcessOrderBuildNode import run as run_process_order_build
from src.ai.cortex.nodes.InventoryCheckNode import run as run_inventory_check
from src.ai.cortex.nodes.MaterialPickingNode import run as run_material_picking
from src.ai.cortex.nodes.MachineAssignmentNode import run as run_machine_assignment
from src.ai.cortex.nodes.LLMSchedulingNode import run as run_llm_scheduling
from src.ai.cortex.nodes.ScheduleValidationNode import run as run_schedule_validation
from src.ai.cortex.nodes.GanttConversionNode import run as run_gantt_conversion
from src.ai.cortex.nodes.ResponseGenerationNode import run as run_response_generation

logger = logging.getLogger(__name__)


def run(
    state: Dict[str, Any],
    *,
    datasource_configs: List[Dict[str, Any]] = None,
    datasource_repository=None,
    analyze_query_use_case=None,
    plan_production_use_case=None,
) -> Dict[str, Any]:
    """
    Execute the comprehensive production planning graph.
    Detects CHG datasource automatically via DataFetchNode and adapts the pipeline.
    """
    configs = datasource_configs or []
    logger.info(
        "PlanningGraph: start query=%s datasource_configs=%s",
        (state.get("userQuery") or "")[:80],
        len(configs),
    )
    s = dict(state)
    s["_datasource_configs"] = configs
    s["_datasource_repository"] = datasource_repository
    s["_analyze_query_use_case"] = analyze_query_use_case
    s["_plan_production_use_case"] = plan_production_use_case

    # Step 1: Query Analysis
    s = run_query_analysis(s)
    logger.debug("PlanningGraph: after QueryAnalysis action=%s", (s.get("analysisResult") or {}).get("action"))
    s = run_clarification(s)

    if s.get("needsClarification") or s.get("rejected"):
        logger.info("PlanningGraph: early exit (clarification/rejected), skipping data/plan pipeline")
        s = run_response_generation(s)
        return s

    # Step 2: Data Fetching (also populates CHG fields if CHG datasource detected)
    s["requiredSources"] = s.get("_datasource_configs") or []
    logger.info("PlanningGraph: using %s user-selected datasource(s)", len(s["requiredSources"]))
    s = run_data_fetch(s)
    is_chg = s.get("_is_chg", False)
    logger.info("PlanningGraph: rawData keys=%s, CHG=%s", list((s.get("rawData") or {}).keys()), is_chg)

    # Step 3: Data Normalization
    s = run_data_normalization(s)
    logger.info("PlanningGraph: normalizedData count=%s", len(s.get("normalizedData") or []))

    # Step 4: Sales Order Processing
    s = run_sales_order_processing(s)
    logger.info("PlanningGraph: salesOrders=%s, productTargets=%s",
                len(s.get("salesOrders") or []), len(s.get("productTargets") or []))

    # Step 5: Recipe/BOM Fetch (CHG: step-level detail)
    s = run_recipe_fetch(s)
    logger.info("PlanningGraph: recipes=%s", len(s.get("recipes") or {}))

    # Step 6: Process Order Build (CHG only — batch explosion + machine queues)
    if is_chg:
        s = run_process_order_build(s)
        logger.info("PlanningGraph: process_orders=%s, machine_queues=%s",
                     len(s.get("process_orders") or []), len(s.get("machine_queues") or {}))

    # Step 7: Inventory Check
    s = run_inventory_check(s)
    inventory_check = s.get("inventoryCheck") or {}
    logger.info("PlanningGraph: inventory items=%s, shortages=%s",
                len(s.get("inventory") or {}), inventory_check.get("shortage_count", 0))

    # Step 8: Material Picking
    s = run_material_picking(s)
    logger.info("PlanningGraph: materialPicks=%s, productionTasks=%s",
                len(s.get("materialPicks") or []), len(s.get("productionTasks") or []))

    # Step 9: Machine Assignment (CHG: GF constraint + MRP blocks)
    s = run_machine_assignment(s)
    logger.info("PlanningGraph: machines=%s, assignments=%s",
                len(s.get("machines") or {}), len(s.get("machineAssignments") or {}))

    # Step 10: LLM Scheduling Optimization (CHG: per-machine prompt)
    s = run_llm_scheduling(s)
    if is_chg:
        logger.info("PlanningGraph: CHG scheduled %s machines, %s exceptions",
                     len(s.get("machine_schedules_raw") or {}),
                     len(s.get("scheduling_exceptions") or []))
    else:
        scheduling_result = s.get("schedulingResult") or {}
        logger.info("PlanningGraph: scheduling optimized for %s machines, risk=%s",
                     len(scheduling_result.get("machines") or {}),
                     scheduling_result.get("risk_assessment", {}).get("overall_risk_level", "unknown"))

    # Step 11: Schedule Validation (CHG: missed dates + allergen gaps)
    s = run_schedule_validation(s)
    validation = s.get("validationResult") or {}
    logger.info("PlanningGraph: validation valid=%s, issues=%s",
                validation.get("valid"), len(validation.get("issues") or []))

    # Step 12: Convert to Gantt format (CHG: block type colors)
    s = run_gantt_conversion(s)
    logger.info("PlanningGraph: ganttTasks count=%s", len(s.get("ganttTasks") or []))

    # Step 13: Generate response (CHG: scheduling_summary + allergen_warnings)
    s = run_response_generation(s)

    return s
