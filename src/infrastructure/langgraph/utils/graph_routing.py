"""Routing and inline graph nodes for the data analysis LangGraph workflow."""
import logging
from typing import Any, Dict

from ..state import AnalyticsState

logger = logging.getLogger(__name__)


def is_sap_datasphere(state: AnalyticsState) -> bool:
    """Check if the data source is SAP Datasphere."""
    data_source_config = state.get("data_source_config", {})
    data_source_type = (
        data_source_config.get("type", "").lower() if data_source_config else ""
    )
    return data_source_type in ("sap", "sap_datasphere")


def route_after_get_schema(state: AnalyticsState) -> str:
    """
    Route after get_schema: SAP uses analytical path only (no sap_fetch_plan LLM).
    Non-SAP -> sql_plan_synthesis.
    SAP + simple flow -> get_schema_simple_sink (do not start moderate pipeline; simple path uses prepare_analytical_schema only).
    SAP + moderate -> sap_analytical_passthrough -> analytical_fetch_plan.
    """
    if not is_sap_datasphere(state):
        logger.info("[graph] Routing to SQL plan synthesis (non-SAP data source)")
        return "sql_plan_synthesis"
    decision = (state.get("orchestrator_decision") or "").strip().lower()
    if decision == "simple":
        logger.info(
            "[graph] SAP + simple flow: get_schema -> get_schema_simple_sink (skip moderate pipeline)"
        )
        return "get_schema_simple_sink"
    logger.info(
        "[graph] SAP Datasphere - skipping sap_fetch_plan, using analytical path (sap_analytical_passthrough)"
    )
    return "sap_analytical_passthrough"


def route_after_get_schema_moderate(state: AnalyticsState) -> str:
    """
    Used only inside moderate_workflow. After get_schema, starts the data path (in parallel
    with get_schema → prepare_analytical_schema). Same logic as route_after_get_schema but
    never returns get_schema_simple_sink. Returns sap_analytical_passthrough or
    sap_fetch_plan for SAP, sql_plan_synthesis for non-SAP.
    """
    if not is_sap_datasphere(state):
        logger.info("[graph] Moderate workflow: routing to SQL plan synthesis (non-SAP)")
        return "sql_plan_synthesis"
    # SAP: use analytical passthrough (no LLM plan) for consistency with main route_after_get_schema
    logger.info("[graph] Moderate workflow: SAP -> sap_analytical_passthrough")
    return "sap_analytical_passthrough"


def route_after_sql_plan(state: AnalyticsState) -> str:
    """Route after SQL plan (non-SAP only): sql_plan_synthesis -> sql_generation."""
    logger.info("[graph] Routing to SQL generation (non-SAP data source)")
    return "sql_generation"


def route_after_analytical_fetch_plan(state: AnalyticsState) -> str:
    """
    Route after analytical_fetch_plan: proceed to sap_data_fetch only when
    instructions were written and we have not already triggered sap_data_fetch.

    Routes directly to sap_data_fetch (no intermediate trigger node) so that
    the data fetch starts in the very next superstep.  The one-shot guard flag
    (_analytical_fetch_triggered) is now set by sap_data_fetch itself.
    """
    if not state.get("analytical_fetch_instructions"):
        return "analytical_fetch_plan_sink"
    if state.get("_analytical_fetch_triggered"):
        return "analytical_fetch_plan_sink"
    return "sap_data_fetch"


def route_after_db_execution_ready(state: AnalyticsState) -> str:
    """
    Join after operation_specification and sql_generation (non-SAP).
    Proceed to db_execution only when BOTH operation_plan and SQL generation
    output are in state (so computation_engine will have a plan when data arrives).
    """
    has_operation_plan = bool(state.get("operation_plan")) and isinstance(
        state.get("operation_plan"), dict
    )
    has_sql_ready = bool(state.get("generated_queries")) or bool(state.get("plan"))
    if has_operation_plan and has_sql_ready:
        logger.info("[graph] db_execution_ready: operation_plan and SQL ready -> db_execution")
        return "db_execution"
    return "db_execution_ready_sink"


def analytical_fetch_plan_sink_node(state: AnalyticsState) -> Dict[str, Any]:
    """No-op sink when analytical_fetch_plan did not yet write (waiting for other triggers)."""
    return {}


def sap_data_fetch_trigger_node(state: AnalyticsState) -> Dict[str, Any]:
    """One-shot gate: set flag so we only trigger sap_data_fetch once.

    NOTE: kept for backwards compatibility but no longer wired into the graph.
    The flag is now set directly by sap_data_fetch_simple_node.
    """
    return {"_analytical_fetch_triggered": True}


def db_execution_ready_node(state: AnalyticsState) -> Dict[str, Any]:
    """No-op join: run db_execution only when both operation_specification and sql_generation have completed (non-SAP)."""
    return {}


def db_execution_ready_sink_node(state: AnalyticsState) -> Dict[str, Any]:
    """Sink when db_execution_ready is invoked before both predecessors are ready (wait for other branch)."""
    return {}


def sap_analytical_passthrough_node(state: AnalyticsState) -> Dict[str, Any]:
    """No-op when using analytical view path: skip sap_fetch_plan."""
    return {}


def route_after_orchestration(state: AnalyticsState) -> str:
    """
    After orchestration_agent: clarification -> end; simple or moderate -> query_analysis (parse_query).
    query_analysis then routes to simple_workflow or moderate_workflow.
    """
    decision = (state.get("orchestrator_decision") or "").strip().lower()
    if decision == "clarification":
        logger.info("[graph] Orchestrator: clarification — ending flow")
        return "end"
    logger.info(f"[graph] Orchestrator: {decision} — entering query_analysis (parse_query), then workflow")
    return "query_analysis"


def route_after_query_analysis(state: AnalyticsState) -> str:
    """
    After query_analysis (parse_query): route to simple_workflow or moderate_workflow by orchestrator_decision.
    """
    decision = (state.get("orchestrator_decision") or "").strip().lower()
    if decision == "simple":
        logger.info("[graph] After query_analysis: simple — entering simple_workflow")
        return "simple_workflow"
    logger.info("[graph] After query_analysis: moderate — entering moderate_workflow")
    return "moderate_workflow"


def route_after_prepare_analytical_schema(state: AnalyticsState) -> str:
    """
    After prepare_analytical_schema: simple flow -> simple_column_selection (one dim + query measures);
    moderate flow -> analytical_column_selection (full LLM selection for chart pre-plan).
    """
    decision = (state.get("orchestrator_decision") or "").strip().lower()
    if decision == "simple":
        logger.info("[graph] Simple flow: prepare_analytical_schema -> simple_column_selection")
        return "simple_column_selection"
    logger.info("[graph] Moderate flow: prepare_analytical_schema -> analytical_column_selection")
    return "analytical_column_selection"


def route_after_simple_column_selection(state: AnalyticsState) -> str:
    """
    After simple_column_selection: if no related data for user query (data_sufficiency_result.can_answer false),
    end the flow so we can show message + suggested_queries. Otherwise continue to fetch plan.
    """
    ds = state.get("data_sufficiency_result")
    if isinstance(ds, dict) and ds.get("can_answer") is False:
        logger.info("[graph] Simple flow: no related data for query — ending flow (show message + suggestions)")
        return "end"
    logger.info("[graph] Simple flow: simple_column_selection -> simple_analytical_fetch_plan")
    return "simple_analytical_fetch_plan"


def route_after_analytical_column_selection(state: AnalyticsState) -> str:
    """
    After analytical_column_selection (moderate flow only): if no related data for user query
    (data_sufficiency_result.can_answer false), end the flow. Otherwise -> analytical_fetch_plan.
    Simple flow uses simple_column_selection -> simple_analytical_fetch_plan instead.
    """
    ds = state.get("data_sufficiency_result")
    if isinstance(ds, dict) and ds.get("can_answer") is False:
        logger.info("[graph] Moderate flow: no related data for query — ending flow (show message + suggestions)")
        return "end"
    decision = (state.get("orchestrator_decision") or "").strip().lower()
    if decision == "simple":
        logger.info("[graph] Simple flow: analytical_column_selection -> simple_analytical_fetch_plan (fallback)")
        return "simple_analytical_fetch_plan"
    logger.info("[graph] Moderate flow: analytical_column_selection -> analytical_fetch_plan")
    return "analytical_fetch_plan"


def route_after_sap_data_fetch(state: AnalyticsState) -> str:
    """
    After sap_data_fetch: simple flow -> analytical_summary; moderate -> moderate_post_fetch (then chart_preparation + computation_engine).
    """
    decision = (state.get("orchestrator_decision") or "").strip().lower()
    if decision == "simple":
        logger.info("[graph] Simple flow: sap_data_fetch -> analytical_summary")
        return "analytical_summary"
    logger.info("[graph] Moderate flow: sap_data_fetch -> moderate_post_fetch")
    return "moderate_post_fetch"


def moderate_post_fetch_node(state: AnalyticsState) -> Dict[str, Any]:
    """No-op fork: after sap_data_fetch on moderate flow, fan out to chart_preparation and computation_engine."""
    return {}


# ---------------------------------------------------------------------------
# Join nodes: run downstream only when required predecessors are done
# ---------------------------------------------------------------------------


def route_computation_engine_ready(state: AnalyticsState) -> str:
    """
    Run computation_engine when data fetch (raw_dataframes) is done.
    operation_plan can be derived from recommended_charts inside computation_engine when absent.
    """
    has_data = bool(state.get("raw_dataframes"))
    if has_data:
        logger.info("[graph] computation_engine_ready: data present -> computation_engine")
        return "computation_engine"
    return "computation_engine_ready_sink"


def computation_engine_ready_node(state: AnalyticsState) -> Dict[str, Any]:
    """No-op join: invoked by db_execution, sap_data_fetch, or operation_specification_sink; routes when both data and plan are ready."""
    return {}


def computation_engine_ready_sink_node(state: AnalyticsState) -> Dict[str, Any]:
    """Sink when computation_engine_ready is invoked before both data fetch and operation_specification are done."""
    return {}


