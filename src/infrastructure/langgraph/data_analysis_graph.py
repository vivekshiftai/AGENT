"""Data Analysis LangGraph workflow - Planning-First Architecture.

CRITICAL ARCHITECTURE RULES:
1. Planning nodes must NEVER access full data
2. fetch_data must run ONLY after planning
3. computation_engine is the ONLY node allowed to touch data
4. Use Polars LazyFrame end-to-end
5. Call .collect() EXACTLY ONCE
"""
import datetime
import logging
import traceback
import uuid
from functools import partial
from typing import Any, Dict, Optional

from langgraph.graph import END, StateGraph

from config.settings import settings

from .state import AnalyticsState
from .nodes.analytical_column_selection import analytical_column_selection_node
from .nodes.analytical_fetch_plan import analytical_fetch_plan_node
from .nodes.analytical_summary import analytical_summary_node
from .nodes.computation_engine import computation_engine_node
from .nodes.fetch_data import fetch_data_node
from .nodes.gantt_preparation import gantt_preparation_node
from .nodes.generate_sql import generate_sql_node
from .nodes.get_schema import get_schema_node
from .nodes.load_data import load_data_node
from .nodes.parse_query import parse_query_node
from .nodes.prepare_analytical_schema import prepare_analytical_schema_node
from .nodes.orchestration_agent import orchestration_agent_node
from .nodes.simple_analytical_fetch_plan import simple_analytical_fetch_plan_node
from .nodes.simple_column_selection import simple_column_selection_node
from .nodes.sap_data_fetch_simple import sap_data_fetch_simple_node
from .nodes.sap_fetch_plan import sap_fetch_plan_node
from .nodes.select_tables import select_tables_node
from .nodes.sql_plan import sql_plan_node
from .utils import (
    analytical_fetch_plan_sink_node,
    computation_engine_ready_node,
    computation_engine_ready_sink_node,
    extract_all_partial_data_fields,
    extract_step_details,
    db_execution_ready_node,
    db_execution_ready_sink_node,
    moderate_post_fetch_node,
    route_after_analytical_column_selection,
    route_after_analytical_fetch_plan,
    route_after_db_execution_ready,
    route_after_get_schema,
    route_after_get_schema_moderate,
    route_after_orchestration,
    route_after_query_analysis,
    route_after_sap_data_fetch,
    route_after_simple_column_selection,
    route_after_sql_plan,
    route_computation_engine_ready,
    sap_analytical_passthrough_node,
    select_revenue_priority_metrics,
)

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Shared partial-send helpers (used by stream loop and moderate_workflow)
# -----------------------------------------------------------------------------


def _filter_metrics_by_suggested(computation_results: list, suggested_metrics: list) -> list:
    """Filter computation_results by suggested_metrics (normalized name matching). Fallback to select_revenue_priority_metrics if no match."""
    if not computation_results or not isinstance(computation_results, list):
        return []
    if not suggested_metrics or len(suggested_metrics) == 0:
        return select_revenue_priority_metrics(computation_results, max_count=15, metric_key="metric")

    def normalize_metric_name(name: str) -> str:
        if not name:
            return ""
        return name.lower().replace("_", "").replace(" ", "").replace("-", "")

    suggested_normalized = {normalize_metric_name(m): m for m in suggested_metrics}
    suggested_set_lower = {m.lower() for m in suggested_metrics}
    filtered_results = []
    for r in computation_results:
        if not isinstance(r, dict):
            continue
        metric_id = r.get("metric", "")
        if not metric_id:
            continue
        metric_lower = metric_id.lower()
        metric_norm = normalize_metric_name(metric_id)
        if metric_lower in suggested_set_lower or metric_norm in suggested_normalized:
            filtered_results.append(r)
            continue
        for suggested in suggested_metrics:
            if suggested.lower() in metric_lower or metric_lower in suggested.lower():
                filtered_results.append(r)
                break
    if len(filtered_results) == 0:
        return select_revenue_priority_metrics(computation_results, max_count=15, metric_key="metric")
    return filtered_results


async def _send_metrics_partial_from_state(ws_manager, current_state: Dict[str, Any], partial_data_sent: Dict[str, bool], source_label: str = "") -> bool:
    """Build and send metrics_ready from state (normal mode: filter by suggested_metrics). Returns True if sent."""
    if partial_data_sent.get("metrics"):
        return False
    computation_results = current_state.get("computation_results", [])
    if not computation_results or len(computation_results) == 0:
        return False
    analysis_mode = current_state.get("analysis_mode", "normal")
    user_query = current_state.get("user_query", "")
    suggested_metrics = (current_state.get("suggested_metrics") or []) if analysis_mode != "deep_research" else []
    if analysis_mode == "deep_research":
        filtered_results = computation_results
    else:
        if not suggested_metrics and isinstance(current_state.get("analysis_summary"), dict):
            suggested_metrics = current_state.get("analysis_summary", {}).get("metrics_to_display") or []
        filtered_results = _filter_metrics_by_suggested(computation_results, suggested_metrics)
    statistics = {r.get("metric"): r.get("value") for r in filtered_results if isinstance(r, dict) and r.get("metric") is not None and r.get("value") is not None}
    computation_metrics = current_state.get("computation_metrics", {})
    message_parts = [f"{len(filtered_results)} key metrics"]
    if computation_metrics and computation_metrics.get("aggregations_executed") is not None:
        message_parts.append(f"{computation_metrics['aggregations_executed']} aggregations")
    partial_data = {
        "type": "metrics_ready",
        "query": user_query,
        "statistics": statistics or None,
        "computation_results": filtered_results,
        "status": "metrics_ready",
        "message": " | ".join(message_parts),
    }
    _max_rows = 25
    additional_fields = extract_all_partial_data_fields(
        current_state, include_chart_plan=False, include_operation_plan=True, max_rows=_max_rows
    )
    if additional_fields:
        partial_data = {**partial_data, **additional_fields}
    source_data = partial_data.get("source_data") or {}
    partial_data["sample_data"] = {t: (rows[:_max_rows] if isinstance(rows, list) else rows) for t, rows in source_data.items()} if source_data else {}
    success = await ws_manager.send_partial_data("metrics", partial_data)
    if success:
        partial_data_sent["metrics"] = True
        logger.info(f"🌐 [Graph] ✅ Sent metrics (metrics_ready)" + (f" from {source_label}" if source_label else ""))
    else:
        logger.warning(f"🌐 [Graph] ⚠️ Failed to send metrics" + (f" from {source_label}" if source_label else ""))
    return success


async def _send_summary_partial_from_state(ws_manager, current_state: Dict[str, Any], partial_data_sent: Dict[str, bool], source_label: str = "") -> bool:
    """Build and send summary partial (summary_ready or normal_text). Returns True if sent.
    
    NOTE: Real-time token streaming now happens at the LLM layer (azure_openai.py)
    for every node automatically. By the time this function runs the user has
    already seen the tokens stream in. This function sends the full structured
    payload so the frontend can render charts, metrics, and metadata.
    """
    if partial_data_sent.get("summary"):
        return False
    analysis_summary = current_state.get("analysis_summary") or {}
    if not isinstance(analysis_summary, dict):
        return False
    main_text = (analysis_summary.get("summary_text") or "").strip()
    if not main_text:
        return False

    user_query = current_state.get("user_query", "")
    summary_status = "insufficient_data" if current_state.get("no_data_available") else "summary_ready"
    is_message_only = current_state.get("no_data_available") or (current_state.get("orchestrator_decision") or "").strip().lower() == "simple"
    payload_type = "normal_text" if is_message_only else "summary_ready"
    suggested_follow_ups = (analysis_summary.get("suggested_follow_up_queries") if isinstance(analysis_summary.get("suggested_follow_up_queries"), list) else []) if is_message_only else []
    summary_data = {
        "type": payload_type,
        "query": user_query,
        "normal_text": main_text,
        "analysis_summary": analysis_summary,
        "status": summary_status,
        "suggested_queries": suggested_follow_ups,
    }
    if current_state.get("query_id"):
        summary_data["query_id"] = current_state["query_id"]
        summary_data["botMessageId"] = current_state["query_id"]
    success = await ws_manager.send_partial_data("normal_text" if is_message_only else "summary", summary_data)
    if success:
        partial_data_sent["summary"] = True
        logger.info(f"🌐 [Graph] ✅ Sent {payload_type}" + (f" from {source_label}" if source_label else ""))
    else:
        logger.warning(f"🌐 [Graph] ⚠️ Failed to send summary" + (f" from {source_label}" if source_label else ""))
    return success


# -----------------------------------------------------------------------------
# Graph builder and execution
# -----------------------------------------------------------------------------


class DataAnalysisGraphBuilder:
    """
    Builder for Production Planning LangGraph workflow.
    
    ARCHITECTURE:
    
    Phase I: Context & Strategy
    - User query → orchestration_agent → query_analysis (parse_query) when simple/moderate
    - If moderate: moderate_workflow entry = table_identification
    - load_data → get_schema → prepare_analytical_schema
    
    Phase II: Data Source Routing (Parallel paths after get_schema)
    After get_schema:
    ├─ Path A: prepare_analytical_schema → analytical_column_selection
    │   → [SAP: analytical_fetch_plan → sap_data_fetch | non-SAP: sink]
    └─ Path B: [SAP: sap_analytical_passthrough → sink | non-SAP: sql_plan_synthesis → sql_generation → db_execution]
    
    Phase III: Execution
    - db_execution / sap_data_fetch → computation_engine_ready → computation_engine
    - computation_engine → gantt_preparation → analytical_summary → END
    
    All nodes send partial updates directly to frontend via WebSocket.
    """
    
    def __init__(self):
        """Initialize the data analysis graph builder with node-specific models."""
        # Load node-specific models from settings (see config/settings.py for all analytics_*_model)
        self.parse_query_model = settings.analytics_parse_query_model
        self.orchestration_agent_model = getattr(settings, "analytics_orchestration_agent_model", None) or settings.analytics_parse_query_model
        self.select_tables_model = settings.analytics_select_tables_model
        self.analytical_column_selection_model = settings.analytics_analytical_column_selection_model
        self.sql_plan_model = settings.analytics_sql_plan_model
        self.sap_fetch_plan_model = settings.analytics_sap_fetch_plan_model
        self.generate_sql_model = settings.analytics_generate_sql_model
        self.analytical_summary_model = settings.analytics_analytical_summary_model
        self.gantt_preparation_model = getattr(settings, "analytics_gantt_preparation_model", "claude-haiku-4-5")
        
        self.graph: StateGraph = None
        self._build_graph()

    def _build_simple_workflow(self):
        """
        Simple flow only: linear chain. No moderate nodes. No conditional routing.
        table_identification → load_data → get_schema → prepare_analytical_schema
        → simple_column_selection → simple_analytical_fetch_plan → sap_data_fetch → analytical_summary → END.
        """
        w = StateGraph(AnalyticsState)
        w.add_node("table_identification", partial(select_tables_node, model=self.select_tables_model))
        w.add_node("load_data", load_data_node)
        w.add_node("get_schema", get_schema_node)
        w.add_node("prepare_analytical_schema", prepare_analytical_schema_node)
        w.add_node("simple_column_selection", partial(simple_column_selection_node, model=self.analytical_column_selection_model))
        w.add_node("simple_analytical_fetch_plan", simple_analytical_fetch_plan_node)
        w.add_node("sap_data_fetch", sap_data_fetch_simple_node)
        w.add_node("analytical_summary", partial(analytical_summary_node, model=self.analytical_summary_model))
        w.set_entry_point("table_identification")
        w.add_edge("table_identification", "load_data")
        w.add_edge("load_data", "get_schema")
        w.add_edge("get_schema", "prepare_analytical_schema")
        w.add_edge("prepare_analytical_schema", "simple_column_selection")
        w.add_conditional_edges(
            "simple_column_selection",
            route_after_simple_column_selection,
            {"end": END, "simple_analytical_fetch_plan": "simple_analytical_fetch_plan"},
        )
        w.add_edge("simple_analytical_fetch_plan", "sap_data_fetch")
        w.add_edge("sap_data_fetch", "analytical_summary")
        w.add_edge("analytical_summary", END)
        return w.compile()

    def _build_moderate_workflow(self):
        """
        Moderate/analysis flow for production planning.

        table_identification → load_data → get_schema
        After get_schema (parallel):
          Path A: prepare_analytical_schema → analytical_column_selection
                  → [SAP: analytical_fetch_plan → sap_data_fetch | non-SAP: sink]
          Path B: [SAP: sap_analytical_passthrough → sink | non-SAP: sql_plan_synthesis → sql_generation → db_execution]
        After data fetch → computation_engine_ready → computation_engine
        → gantt_preparation → analytical_summary → END
        """
        w = StateGraph(AnalyticsState)
        w.add_node("table_identification", partial(select_tables_node, model=self.select_tables_model))
        w.add_node("load_data", load_data_node)
        w.add_node("get_schema", get_schema_node)
        w.add_node("prepare_analytical_schema", prepare_analytical_schema_node)
        w.add_node("analytical_column_selection", partial(analytical_column_selection_node, model=self.analytical_column_selection_model))
        w.add_node("sql_plan_synthesis", partial(sql_plan_node, model=self.sql_plan_model))
        w.add_node("sap_analytical_passthrough", sap_analytical_passthrough_node)
        w.add_node("sap_fetch_plan", partial(sap_fetch_plan_node, model=self.sap_fetch_plan_model))
        w.add_node("analytical_fetch_plan", analytical_fetch_plan_node)
        w.add_node("analytical_fetch_plan_sink", analytical_fetch_plan_sink_node)
        w.add_node("sql_generation", partial(generate_sql_node, model=self.generate_sql_model))
        w.add_node("db_execution", fetch_data_node)
        w.add_node("computation_engine", computation_engine_node)
        w.add_node("gantt_preparation", partial(gantt_preparation_node, model=self.gantt_preparation_model))
        w.add_node("analytical_summary", partial(analytical_summary_node, model=self.analytical_summary_model))
        w.add_node("sap_data_fetch", sap_data_fetch_simple_node)
        w.add_node("computation_engine_ready", computation_engine_ready_node)
        w.add_node("computation_engine_ready_sink", computation_engine_ready_sink_node)
        w.set_entry_point("table_identification")

        w.add_edge("table_identification", "load_data")
        w.add_edge("load_data", "get_schema")

        # After get_schema: two parallel paths (schema analysis + data fetch routing)
        w.add_edge("get_schema", "prepare_analytical_schema")
        w.add_conditional_edges(
            "get_schema",
            route_after_get_schema_moderate,
            {
                "sql_plan_synthesis": "sql_plan_synthesis",
                "sap_analytical_passthrough": "sap_analytical_passthrough",
                "sap_fetch_plan": "sap_fetch_plan",
            },
        )

        # Path A: schema analysis → column selection → fetch plan (SAP) or sink (non-SAP)
        w.add_edge("prepare_analytical_schema", "analytical_column_selection")
        w.add_conditional_edges(
            "analytical_column_selection",
            route_after_analytical_column_selection,
            {
                "end": END,
                "analytical_fetch_plan": "analytical_fetch_plan",
            },
        )
        w.add_conditional_edges(
            "analytical_fetch_plan",
            route_after_analytical_fetch_plan,
            {
                "sap_data_fetch": "sap_data_fetch",
                "analytical_fetch_plan_sink": "analytical_fetch_plan_sink",
            },
        )
        w.add_edge("analytical_fetch_plan_sink", END)

        # Path B: data source routing
        w.add_edge("sap_analytical_passthrough", "analytical_fetch_plan_sink")
        w.add_edge("sap_fetch_plan", "analytical_fetch_plan_sink")
        w.add_conditional_edges("sql_plan_synthesis", route_after_sql_plan, {"sql_generation": "sql_generation"})
        w.add_edge("sql_generation", "db_execution")

        # Join: data fetch → computation engine
        w.add_edge("db_execution", "computation_engine_ready")
        w.add_edge("sap_data_fetch", "computation_engine_ready")
        w.add_conditional_edges(
            "computation_engine_ready",
            route_computation_engine_ready,
            {
                "computation_engine": "computation_engine",
                "computation_engine_ready_sink": "computation_engine_ready_sink",
            },
        )
        w.add_edge("computation_engine_ready_sink", END)

        # Post-computation: gantt → summary → END
        w.add_edge("computation_engine", "gantt_preparation")
        w.add_edge("gantt_preparation", "analytical_summary")
        w.add_edge("analytical_summary", END)

        return w.compile()

    def _build_graph(self):
        """
        Main graph: entry = orchestration_agent. Then query_analysis (parse_query) when simple/moderate, then workflow.
        - clarification → END (return to user).
        - simple or moderate → query_analysis (parse_query) → simple_workflow or moderate_workflow.
        """
        workflow = StateGraph(AnalyticsState)
        workflow.add_node("orchestration_agent", partial(orchestration_agent_node, model=self.orchestration_agent_model))
        workflow.add_node("query_analysis", partial(parse_query_node, model=self.parse_query_model))
        simple_workflow = self._build_simple_workflow()
        moderate_workflow = self._build_moderate_workflow()
        workflow.add_node("simple_workflow", simple_workflow)
        workflow.add_node("moderate_workflow", moderate_workflow)
        workflow.set_entry_point("orchestration_agent")
        workflow.add_conditional_edges(
            "orchestration_agent",
            route_after_orchestration,
            {
                "end": END,
                "query_analysis": "query_analysis",
            },
        )
        workflow.add_conditional_edges(
            "query_analysis",
            route_after_query_analysis,
            {
                "simple_workflow": "simple_workflow",
                "moderate_workflow": "moderate_workflow",
            },
        )
        workflow.add_edge("simple_workflow", END)
        workflow.add_edge("moderate_workflow", END)
        self.graph = workflow.compile()
    
    async def execute(self, initial_state: Dict[str, Any], progress_callback=None, ws_manager=None) -> Dict[str, Any]:
        """
        Execute the data analysis workflow.
        
        Args:
            initial_state: Initial state dictionary
            progress_callback: Optional callback function(node_name, status, message) for progress updates
            
        Returns:
            Final state dictionary with metrics, Gantt data, and summary
        """
        # Set query_id and user_id in request context so every log line shows them for traceability (RequestIdFilter uses get_request_id/get_user_id)
        query_id = (initial_state.get("query_id") or "").strip()
        user_id = (initial_state.get("user_id") or "").strip()
        if not query_id:
            query_id = str(uuid.uuid4())
            initial_state["query_id"] = query_id
            logger.info(f"[ORCHESTRATOR] No query_id in state - generated for traceability: {query_id[:16]}...")
        try:
            from shared.request_context import set_request_context
            set_request_context(query_id=query_id, user_id=user_id or None)
            log_q = query_id[:16] + "..." if len(query_id) > 16 else (query_id or "-")
            log_u = (user_id[:16] + "...") if user_id and len(user_id) > 16 else (user_id or "-")
            logger.info(f"[ORCHESTRATOR] Log identification: query_id={log_q} user_id={log_u}")
        except Exception as e:
            _q = (query_id[:16] + "...") if query_id and len(query_id) > 16 else (query_id or "None")
            _u = (user_id[:16] + "...") if user_id and len(user_id) > 16 else (user_id or "None")
            logger.warning(
                "[ORCHESTRATOR] Failed to set request context (query_id/user_id) for logging - "
                "logs may show query_id=- user_id=-. Reason: %s (%s). Values attempted: query_id=%s user_id=%s",
                type(e).__name__, str(e), _q, _u
            )
            logger.warning("[ORCHESTRATOR] set_request_context traceback:\n%s", traceback.format_exc())

        # Ensure datetime is always accessible (avoid local variable shadowing issues)
        # Store a reference to datetime.datetime at the start to prevent shadowing
        # This ensures datetime is accessible even in exception handlers
        _datetime = datetime.datetime
        start_time = _datetime.now()

        # Store ws_manager in state so nodes can access it
        if ws_manager:
            initial_state["ws_manager"] = ws_manager

        # Single LLM client per request (reuse across nodes to avoid repeated init)
        if initial_state.get("llm_client") is None:
            try:
                from ...llm.azure_openai import AzureOpenAIClient
                initial_state["llm_client"] = AzureOpenAIClient()
            except Exception as e:
                logger.debug(f"[ORCHESTRATOR] Could not pre-create LLM client: {e}")

        # Node display names for progress updates
        node_display_names = {
            "initializing": "Initializing the analysis pipeline and preparing resources",
            "query_analysis": "Understanding your question and extracting intent and entities",
            "orchestration_agent": "Deciding whether to run a quick or full analysis based on your request",
            "simple_workflow": "Running a streamlined analysis path for straightforward questions",
            "moderate_workflow": "Running a full analysis with metrics, Gantt schedule, and summary",
            "simple_analytical_fetch_plan": "Planning which data to fetch and how to retrieve it",
            "moderate_post_fetch": "Preparing results after data has been retrieved",
            "table_identification": "Identifying and selecting the most relevant data tables for your query",
            "load_data": "Loading and optimizing the data pipeline for your selected tables",
            "get_schema": "Reading table structures and building a unified schema blueprint",
            "sql_plan_synthesis": "Designing the data retrieval strategy and query plan",
            "sap_fetch_plan": "Planning how to fetch data from SAP using the analytical view",
            "sap_analytical_passthrough": "Using the analytical view path for SAP (skipping fetch plan)",
            "sql_generation": "Generating and validating the database queries from the plan",
            "db_execution": "Executing queries and retrieving data from the database",
            "db_execution_ready": "Preparing to run the data fetch once the plan is ready",
            "db_execution_ready_sink": "Waiting for the query plan and operation spec before fetching",
            "computation_engine": "Computing production metrics and utilization data",
            "gantt_preparation": "Building Gantt chart from production schedule data",
            "analytical_summary": "Writing a narrative summary of production planning findings",
            "analytical_fetch_plan": "Building the SAP fetch plan from analytical column selection",
            "analytical_fetch_plan_sink": "Waiting for column selection to complete",
            "get_schema_simple_sink": "Schema loaded; continuing on the simple analysis path",
            "computation_engine_ready": "Checking that data is ready for metrics computation",
            "computation_engine_ready_sink": "Waiting for data fetch to complete",
        }

        try:
            if progress_callback:
                # Use streaming to get progress updates
                final_state = None
                last_state = initial_state  # Track the last state we've seen
                
                try:
                    # Track which critical nodes have completed
                    nodes_completed = set()
                    end_event_received = False
                    end_state = None
                    
                    # Track which partial data has already been sent to prevent duplicate sends
                    # This is important because LangGraph might send duplicate events for nodes
                    # in complex graph structures with multiple paths
                    partial_data_sent = {
                        "metrics": False,
                        "summary": False,
                        "gantt": False,
                    }
                    
                    # Stream with subgraphs=True so we get every node (including inside simple_workflow/moderate_workflow)
                    async for chunk in self.graph.astream(
                        initial_state,
                        stream_mode="updates",
                        subgraphs=True,
                    ):
                        # With subgraphs=True, chunk can be tuple (namespace, event) or dict event
                        if isinstance(chunk, tuple) and len(chunk) == 2:
                            _namespace, event = chunk
                        elif isinstance(chunk, dict):
                            event = chunk
                        else:
                            continue
                        if not event:
                            continue
                        # Check if this is the end event first
                        if "__end__" in event:
                            end_state = event["__end__"]
                            end_event_received = True
                            logger.info("📥 Received __end__ event - checking if workflow is truly complete")
                            
                            # CRITICAL: Check if we have meaningful results before ending
                            # This prevents premature termination when planning nodes complete before data fetch
                            has_data = bool(end_state.get("raw_dataframes") or end_state.get("fetched_data"))
                            has_metrics = bool(end_state.get("computation_results") or end_state.get("computation_metrics"))
                            has_gantt = bool(end_state.get("gantt_data"))
                            queries_generated = bool(end_state.get("generated_queries"))
                            
                            if has_data or has_metrics or has_gantt or queries_generated:
                                logger.info("✅ Stream completed - valid results found")
                                logger.info(f"   - Has data: {has_data}, Has metrics: {has_metrics}, Has gantt: {has_gantt}, Queries generated: {queries_generated}")
                                final_state = end_state
                                break
                            else:
                                logger.warning("⚠️ Received __end__ but no results yet - workflow may have ended prematurely")
                                logger.warning(f"   - Queries generated: {queries_generated}, Has data: {has_data}, Has metrics: {has_metrics}, Has gantt: {has_gantt}")
                                logger.warning(f"   - Completed nodes: {sorted(nodes_completed)}")
                                # Use the end state anyway since LangGraph has determined the graph is complete
                                # This might indicate an error condition that needs investigation
                                final_state = end_state
                                break
                        
                        # Process each node completion event
                        # Each event contains node_name -> node_output (which is the updated state)
                        # Note: During parallel execution, multiple nodes may complete in the same event
                        for node_name, node_output in event.items():
                            if node_name != "__end__":
                                # node_output is the node's return value, not the full state
                                # LangGraph automatically merges node_output into the state internally,
                                # but in the stream event we only get the node's return value.
                                # We need to merge it with last_state to get the full current state.
                                # CRITICAL: Preserve table_dataframes and other important fields from last_state
                                
                                # Handle case where node_output might be None or not a dict
                                if node_output is None:
                                    logger.warning(f"⚠️ [{node_name}] node_output is None - skipping state merge")
                                    continue
                                
                                if not isinstance(node_output, dict):
                                    logger.warning(f"⚠️ [{node_name}] node_output is not a dict (type: {type(node_output)}) - converting to dict")
                                    node_output = {} if node_output is None else {"node_output": node_output}
                                
                                # Debug logging for plan field (critical for SAP flow)
                                if node_name == "sap_fetch_plan":
                                    logger.info(f"🔍 [STATE_MERGE] sap_fetch_plan node_output keys: {list(node_output.keys())}")
                                    if "plan" in node_output:
                                        plan = node_output.get("plan")
                                        logger.info(f"🔍 [STATE_MERGE] plan type: {type(plan)}, has views: {bool(plan and isinstance(plan, dict) and plan.get('views'))}")
                                        if plan and isinstance(plan, dict):
                                            logger.info(f"🔍 [STATE_MERGE] plan views count: {len(plan.get('views', {}))}")
                                
                                if last_state:
                                    # Merge node_output into last_state to get full current state
                                    # This ensures table_dataframes and other fields from previous nodes are preserved
                                    current_state = {**last_state, **node_output}
                                    
                                    # Debug logging after merge
                                    if node_name == "sap_fetch_plan":
                                        merged_plan = current_state.get("plan")
                                        logger.info(f"🔍 [STATE_MERGE] After merge - plan in current_state: {bool(merged_plan)}, type: {type(merged_plan)}")
                                        if merged_plan and isinstance(merged_plan, dict):
                                            logger.info(f"🔍 [STATE_MERGE] Merged plan has {len(merged_plan.get('views', {}))} view(s)")
                                    # CRITICAL: Preserve all important state fields that subsequent nodes need
                                    # This ensures state is properly maintained across parallel execution paths
                                    critical_fields = [
                                        # Data and schema
                                        "raw_dataframes", "table_data", "fetched_data", "fetched_data_columns",
                                        "unified_schema", "schema_context", "datasource_info",
                                        # Planning and queries
                                        "plan", "generated_queries",
                                        "selected_tables", "query_id", "user_query",
                                        # Metrics and computation
                                        "operation_plan", "computation_execution_log", "computation_metrics",
                                        "computation_results", "analysis_summary", "suggested_metrics",
                                        "aggregated_methodology_stack",
                                        # Gantt
                                        "gantt_data",
                                        # User context
                                        "parsed_intent", "identified_metrics", "org_context", "analysis_mode",
                                        # Data source config
                                        "data_source_config", "sap_datasphere_assets", "sap_view_schemas",
                                        # Other
                                        "available_date_ranges", "analytical_date_filter", "applied_date_filters", "errors", "status", "no_data_available", "sap_view_not_available_message",
                                    ]
                                    for key in critical_fields:
                                        if key in last_state and key not in node_output:
                                            current_state[key] = last_state[key]
                                else:
                                    current_state = node_output
                                last_state = current_state
                                
                                # Track which nodes have completed
                                nodes_completed.add(node_name)
                                
                                # Log when critical nodes complete
                                if node_name == "sql_generation":
                                    logger.info(f"🌐 [Graph] ✅ sql_generation completed - SQL queries generated")
                                elif node_name == "db_execution":
                                    logger.info(f"🌐 [Graph] ✅ db_execution completed - Data fetched")
                                    logger.info(f"🌐 [Graph] ✅ Data fetch complete - computation_engine can now proceed")
                                
                                display_name = node_display_names.get(node_name, node_name.replace("_", " ").title())
                                # Extract step details for user display
                                step_details = extract_step_details(node_name, node_output)
                                
                                # Always send progress for every node so loading state never stops
                                if progress_callback:
                                    try:
                                        if node_name == "computation_engine":
                                            data_fetch_completed = (
                                                "db_execution" in nodes_completed or "sap_data_fetch" in nodes_completed
                                            )
                                            if not data_fetch_completed:
                                                # Still send progress so UI keeps updating; use a waiting message
                                                await progress_callback(
                                                    node_name, "processing",
                                                    "Preparing to compute metrics...",
                                                    step_details,
                                                )
                                                logger.debug(f"🌐 [Graph] computation_engine: sent progress (waiting for data)")
                                            else:
                                                await progress_callback(node_name, "processing", display_name, step_details)
                                        else:
                                            await progress_callback(node_name, "processing", display_name, step_details)
                                    except Exception as e:
                                        logger.warning(f"Progress callback error (continuing anyway): {str(e)}")
                                
                                logger.info(f"✅ [{node_name}] Completed" + (f" - {step_details}" if step_details else ""))
                                
                                # Send partial data as soon as it's ready
                                # IMPORTANT: Track which nodes have already sent partial data to prevent
                                # duplicate sends if LangGraph sends the same event multiple times
                                if ws_manager:
                                    try:
                                        # When orchestration_agent ends with clarification, send summary partial so frontend can show message immediately
                                        if node_name == "orchestration_agent":
                                            decision = (current_state.get("orchestrator_decision") or "").strip().lower()
                                            if decision == "clarification":
                                                if partial_data_sent["summary"]:
                                                    logger.info("🌐 [Graph] orchestration_agent clarification event but summary already sent - skipping duplicate")
                                                else:
                                                    clarification_message = current_state.get("clarification_message") or "What would you like me to focus on?"
                                                    suggested_queries = current_state.get("clarification_suggestions") or []
                                                    user_query = current_state.get("user_query", "")
                                                    # Single source: normal_text (frontend uses it for display); suggested_queries = buttons (no copy/paste)
                                                    summary_data = {
                                                        "type": "normal_text",
                                                        "query": user_query,
                                                        "normal_text": clarification_message,
                                                        "status": "clarification",
                                                        "suggested_queries": suggested_queries,
                                                    }
                                                    if current_state.get("query_id"):
                                                        summary_data["query_id"] = current_state["query_id"]
                                                        summary_data["botMessageId"] = current_state["query_id"]
                                                    log_id = (current_state.get("query_id") or "")[:16]
                                                    logger.info(f"🌐 [Graph] 📤 Sending clarification (normal_text) query_id={log_id}")
                                                    summary_success = await ws_manager.send_partial_data("normal_text", summary_data)
                                                    if summary_success:
                                                        partial_data_sent["summary"] = True
                                                        logger.info("🌐 [Graph] ✅ Sent clarification partial to frontend")
                                                    else:
                                                        logger.warning("🌐 [Graph] ⚠️ Failed to send clarification partial")
                                        # When table_identification (select_tables) returns because SAP view is not in catalog, send user message (no LLM)
                                        elif node_name == "table_identification":
                                            msg = current_state.get("sap_view_not_available_message")
                                            if msg and not partial_data_sent["summary"]:
                                                user_query = current_state.get("user_query", "")
                                                summary_data = {
                                                    "type": "normal_text",
                                                    "query": user_query,
                                                    "normal_text": msg,
                                                    "status": "view_not_available",
                                                }
                                                if current_state.get("query_id"):
                                                    summary_data["query_id"] = current_state["query_id"]
                                                    summary_data["botMessageId"] = current_state["query_id"]
                                                logger.info("🌐 [Graph] 📤 Sending SAP view not available (normal_text)")
                                                summary_success = await ws_manager.send_partial_data("normal_text", summary_data)
                                                if summary_success:
                                                    partial_data_sent["summary"] = True
                                                    logger.info("🌐 [Graph] ✅ Sent view-not-available partial to frontend")
                                        # When simple_column_selection or analytical_column_selection ends with no related data, send message + suggestions (same UI as clarification)
                                        elif node_name in ("simple_column_selection", "analytical_column_selection"):
                                            ds = current_state.get("data_sufficiency_result")
                                            if isinstance(ds, dict) and ds.get("can_answer") is False:
                                                if partial_data_sent["summary"]:
                                                    logger.info("🌐 [Graph] %s no-related-data but summary already sent - skipping duplicate", node_name)
                                                else:
                                                    user_message = ds.get("user_message") or "There is no related data for your query in this data source. Try one of the suggestions below."
                                                    suggested_queries = ds.get("suggested_queries") or []
                                                    user_query = current_state.get("user_query", "")
                                                    summary_data = {
                                                        "type": "normal_text",
                                                        "query": user_query,
                                                        "normal_text": user_message,
                                                        "status": "insufficient_data",
                                                        "suggested_queries": suggested_queries,
                                                    }
                                                    if current_state.get("query_id"):
                                                        summary_data["query_id"] = current_state["query_id"]
                                                        summary_data["botMessageId"] = current_state["query_id"]
                                                    logger.info(f"🌐 [Graph] 📤 Sending no-related-data (normal_text) suggested_queries={len(suggested_queries)}")
                                                    summary_success = await ws_manager.send_partial_data("normal_text", summary_data)
                                                    if summary_success:
                                                        partial_data_sent["summary"] = True
                                                        logger.info("🌐 [Graph] ✅ Sent no-related-data partial to frontend")
                                                    else:
                                                        logger.warning("🌐 [Graph] ⚠️ Failed to send no-related-data partial")
                                        elif node_name == "moderate_workflow":
                                            logger.info(f"🌐 [Graph] moderate_workflow completed - sending partials from merged state (gantt, metrics, summary)")
                                            # Send gantt partial if not already sent
                                            if not partial_data_sent.get("gantt"):
                                                gantt_data = current_state.get("gantt_data", {})
                                                if gantt_data:
                                                    user_query = current_state.get("user_query", "")
                                                    gantt_partial_data = {
                                                        "type": "gantt_ready",
                                                        "query": user_query,
                                                        "gantt_data": gantt_data,
                                                        "status": "gantt_ready",
                                                        "message": "Production schedule ready",
                                                    }
                                                    success = await ws_manager.send_partial_data("gantt", gantt_partial_data)
                                                    if success:
                                                        partial_data_sent["gantt"] = True
                                                        logger.info("🌐 [Graph] ✅ Sent gantt (gantt_ready) from moderate_workflow")
                                            await _send_metrics_partial_from_state(ws_manager, current_state, partial_data_sent, "moderate_workflow")
                                            await _send_summary_partial_from_state(ws_manager, current_state, partial_data_sent, "moderate_workflow")
                                        # When simple_workflow subgraph completes, send normal_text if not already sent (shared helper)
                                        elif node_name == "simple_workflow":
                                            if partial_data_sent["summary"]:
                                                logger.info("🌐 [Graph] simple_workflow completed but summary already sent - skipping")
                                            elif (current_state.get("orchestrator_decision") or "").strip().lower() == "simple":
                                                await _send_summary_partial_from_state(ws_manager, current_state, partial_data_sent, "simple_workflow")
                                        # Send gantt partial when gantt_preparation completes
                                        elif node_name == "gantt_preparation":
                                            if partial_data_sent.get("gantt"):
                                                logger.info("🌐 [Graph] gantt_preparation event received but gantt already sent - skipping duplicate")
                                                continue
                                            gantt_data = current_state.get("gantt_data", {})
                                            if gantt_data:
                                                user_query = current_state.get("user_query", "")
                                                gantt_partial_data = {
                                                    "type": "gantt_ready",
                                                    "query": user_query,
                                                    "gantt_data": gantt_data,
                                                    "status": "gantt_ready",
                                                    "message": "Production schedule ready",
                                                }
                                                success = await ws_manager.send_partial_data("gantt", gantt_partial_data)
                                                if success:
                                                    partial_data_sent["gantt"] = True
                                                    logger.info("🌐 [Graph] ✅ Sent gantt (gantt_ready) from gantt_preparation")
                                                else:
                                                    logger.warning("🌐 [Graph] ⚠️ Failed to send gantt partial")
                                            else:
                                                logger.warning("🌐 [Graph] ⚠️ gantt_preparation completed but no gantt_data found")
                                        
                                        # Send metrics immediately when computation_engine completes
                                        elif node_name == "computation_engine":
                                            # Guard: Skip if metrics already sent (prevents duplicate sends in deep_research mode)
                                            if partial_data_sent["metrics"]:
                                                logger.info(f"🌐 [Graph] computation_engine event received but metrics already sent - skipping duplicate")
                                                continue
                                            
                                            computation_results = current_state.get("computation_results", [])
                                            user_query = current_state.get("user_query", "")
                                            analysis_mode = current_state.get("analysis_mode", "normal")  # Get analysis mode
                                            
                                            # Check if raw_dataframes is available in state
                                            raw_dataframes_check = current_state.get("raw_dataframes", {})
                                            logger.info(f"🌐 [Graph] computation_engine state check - raw_dataframes available: {bool(raw_dataframes_check)}, count: {len(raw_dataframes_check) if isinstance(raw_dataframes_check, dict) else 0}")
                                            
                                            if computation_results and len(computation_results) > 0:
                                                # Filter metrics based on analysis_mode
                                                # Deep research mode: Send ALL metrics immediately
                                                # Normal mode: Wait for analytical_summary to provide suggested_metrics
                                                if analysis_mode == "deep_research":
                                                    # Deep research mode: Show all metrics immediately
                                                    logger.info(f"🌐 [Graph] Deep research mode: sending all {len(computation_results)} metrics immediately")
                                                    filtered_results = computation_results
                                                    
                                                    # Build statistics from all results
                                                    statistics = {}
                                                    for result in filtered_results:
                                                        if isinstance(result, dict) and "metric" in result and "value" in result:
                                                            metric_name = result.get("metric", "")
                                                            metric_value = result.get("value")
                                                            if metric_name and metric_value is not None:
                                                                statistics[metric_name] = metric_value
                                                    
                                                    # Get computation metrics for message
                                                    computation_metrics = current_state.get("computation_metrics", {})
                                                    metrics_info = {}
                                                    if computation_metrics:
                                                        if "aggregations_executed" in computation_metrics:
                                                            metrics_info["aggregations"] = computation_metrics["aggregations_executed"]
                                                        if "total_duration_seconds" in computation_metrics:
                                                            metrics_info["duration_seconds"] = round(computation_metrics["total_duration_seconds"], 2)
                                                        if "initial_rows" in computation_metrics:
                                                            metrics_info["rows_processed"] = computation_metrics["initial_rows"]
                                                    
                                                    # Build detailed message
                                                    message_parts = [f"Calculated {len(filtered_results)} metrics"]
                                                    if metrics_info.get("aggregations"):
                                                        message_parts.append(f"{metrics_info['aggregations']} aggregations")
                                                    if metrics_info.get("rows_processed"):
                                                        message_parts.append(f"{metrics_info['rows_processed']:,} rows processed")
                                                    
                                                    # metrics_ready: statistics, computation_results (each with reasoning, column for UI); partial update is source of truth for these
                                                    partial_data = {
                                                        "type": "metrics_ready",
                                                        "query": user_query,
                                                        "statistics": statistics if statistics else None,
                                                        "computation_results": filtered_results,
                                                        "status": "metrics_ready",
                                                        "message": " | ".join(message_parts),
                                                    }
                                                    _METRICS_MAX_ROWS = 25
                                                    additional_fields = extract_all_partial_data_fields(
                                                        current_state, include_chart_plan=False, include_operation_plan=True, max_rows=_METRICS_MAX_ROWS
                                                    )
                                                    if additional_fields:
                                                        partial_data = {**partial_data, **additional_fields}
                                                        logger.info(f"🌐 [Graph] Including metrics payload (max_rows={_METRICS_MAX_ROWS}): {list(additional_fields.keys())}")
                                                    source_data = partial_data.get("source_data") or {}
                                                    if source_data:
                                                        sample_data = {t: (rows[:_METRICS_MAX_ROWS] if isinstance(rows, list) else rows) for t, rows in source_data.items()}
                                                        partial_data["sample_data"] = sample_data
                                                        sample_rows = sum(len(rows) if isinstance(rows, list) else 0 for rows in sample_data.values())
                                                        logger.info(f"🌐 [Graph] sample_data: {len(sample_data)} table(s), {sample_rows} rows (capped)")
                                                    else:
                                                        partial_data["sample_data"] = {}
                                                    logger.info(f"🌐 [Graph] Preparing to send metrics (metrics_ready: {len(filtered_results)} results + drill-down + sample_data)")
                                                    success = await ws_manager.send_partial_data("metrics", partial_data)
                                                    if success:
                                                        partial_data_sent["metrics"] = True  # Mark metrics as sent
                                                        logger.info(f"🌐 [Graph] ✅ Successfully sent {len(filtered_results)} metrics ({' | '.join(message_parts)}) immediately to frontend (deep_research mode)")
                                                    else:
                                                        logger.warning(f"🌐 [Graph] ⚠️ Failed to send metrics to frontend")
                                                else:
                                                    # Normal mode: Wait for analytical_summary to provide suggested_metrics
                                                    # Metrics will be sent when analytical_summary completes
                                                    logger.info(f"🌐 [Graph] Normal mode: deferring metric sending to analytical_summary (has {len(computation_results)} metrics)")
                                            else:
                                                logger.warning(f"🌐 [Graph] ⚠️ computation_engine completed but no computation_results found")
                                        
                                        # Send summary and metrics when analytical_summary completes (shared helpers)
                                        elif node_name == "analytical_summary":
                                            logger.info(f"🌐 [Graph] analytical_summary node completed - sending summary & metrics via helpers")
                                            if partial_data_sent["summary"]:
                                                logger.info(f"🌐 [Graph] analytical_summary event received but summary already sent - skipping duplicate")
                                                continue
                                            analysis_mode = current_state.get("analysis_mode", "normal")
                                            is_deep_research = (analysis_mode == "deep_research")
                                            if is_deep_research and partial_data_sent["metrics"]:
                                                logger.info(f"🌐 [Graph] Deep research mode: metrics already sent, will send summary separately")
                                            elif not is_deep_research and partial_data_sent["metrics"]:
                                                logger.info(f"🌐 [Graph] Normal mode: metrics already sent - skipping duplicate")
                                                continue
                                            analysis_summary = current_state.get("analysis_summary", {}) or {}
                                            if not isinstance(analysis_summary, dict):
                                                analysis_summary = {}
                                            suggested_metrics = current_state.get("suggested_metrics") or []
                                            if not suggested_metrics and isinstance(analysis_summary, dict) and analysis_summary.get("metrics_to_display"):
                                                suggested_metrics = analysis_summary.get("metrics_to_display", [])
                                                current_state["suggested_metrics"] = suggested_metrics
                                            insights = []
                                            if analysis_summary and (analysis_summary.get("summary_text") or "").strip():
                                                insights.append((analysis_summary.get("summary_text") or "").strip())
                                            if not analysis_summary or not analysis_summary.get("summary_text"):
                                                fallback_text = (insights[0] if insights else "").strip()
                                                analysis_summary = {
                                                    **(analysis_summary or {}),
                                                    "summary_text": fallback_text,
                                                    "confidence": (analysis_summary or {}).get("confidence", "medium"),
                                                    "confidence_reason": (analysis_summary or {}).get("confidence_reason", ""),
                                                }
                                                current_state["analysis_summary"] = analysis_summary
                                            await _send_summary_partial_from_state(ws_manager, current_state, partial_data_sent, "analytical_summary")
                                            computation_results = current_state.get("computation_results", [])
                                            if not is_deep_research and computation_results and len(computation_results) > 0:
                                                await _send_metrics_partial_from_state(ws_manager, current_state, partial_data_sent, "analytical_summary")
                                        
                                    except Exception as e:
                                        logger.warning(f"Failed to send partial data for {node_name}: {e}")
                        
                        if len(event) > 1:
                            logger.info(f"✅ PARALLEL EXECUTION: {len(event)} nodes completed simultaneously: {list(event.keys())}")
                    
                    # If we didn't get __end__, use last state
                    # This ensures we complete even if __end__ event is missed
                    if final_state is None:
                        if last_state != initial_state:
                            final_state = last_state
                            logger.info("✅ Stream completed, using last state from stream")
                        else:
                            logger.warning("⚠️ Stream completed but no state captured - will fallback to ainvoke")
                
                except Exception as stream_error:
                    # Safely format error message - avoid any datetime-related issues
                    # Include full traceback for debugging datetime issues
                    try:
                        error_msg = str(stream_error)
                        tb_str = traceback.format_exc()
                    except Exception:
                        # If str() fails (e.g., due to datetime issues), use a fallback
                        error_msg = f"Stream error: {type(stream_error).__name__}"
                        tb_str = "Could not format traceback"
                    logger.warning(f"Stream error (continuing with last state): {error_msg}")
                    logger.warning(f"Stream error traceback:\n{tb_str}")
                    # If streaming failed but we have a last state, use it
                    if last_state != initial_state:
                        final_state = last_state
                        logger.info("✅ Using last state from stream after error")
                
                # CRITICAL: Only fallback to ainvoke if we truly have no state
                # This ensures graph always completes, even if streaming fails
                if final_state is None:
                    logger.warning("⚠️ Stream did not return any state, falling back to ainvoke to ensure completion")
                    try:
                        final_state = await self.graph.ainvoke(initial_state)
                        logger.info("✅ Fallback ainvoke completed successfully - graph execution finished")
                    except Exception as ainvoke_error:
                        logger.error(f"❌ Fallback ainvoke also failed: {str(ainvoke_error)}")
                        raise RuntimeError(f"Graph execution failed completely: {str(ainvoke_error)}")
            else:
                # Run the graph without streaming
                final_state = await self.graph.ainvoke(initial_state)
            
            duration = (_datetime.now() - start_time).total_seconds()
            
            raw_dataframes = final_state.get("raw_dataframes", {})
            def _get_row_count(df):
                if df is None:
                    return 0
                if hasattr(df, 'height'):  # Polars DataFrame
                    return df.height
                if hasattr(df, '__len__'):  # Pandas DataFrame  
                    return len(df)
                return 0
            data_rows = sum(_get_row_count(df) for df in raw_dataframes.values()) if raw_dataframes else 0
            
            if final_state is None:
                raise RuntimeError("Graph execution completed but final_state is None - this should never happen")
            
            has_gantt = bool(final_state.get("gantt_data"))
            has_metrics = bool(final_state.get("computation_results"))
            status = final_state.get("status") or "success"
            
            logger.info(f"Workflow completed | Duration: {duration:.2f}s | Rows: {data_rows} | Gantt: {has_gantt} | Metrics: {has_metrics}")
            logger.info(f"Final state validation: status={status}")
            
            return final_state
            
        except Exception as e:
            # Safely calculate duration - ensure datetime is accessible
            try:
                # Use _datetime reference to avoid any potential shadowing issues
                end_time = _datetime.now()
                duration = (end_time - start_time).total_seconds()
            except (NameError, UnboundLocalError) as time_error:
                # If _datetime or start_time is not accessible, use a fallback
                logger.warning(f"Could not calculate duration: {time_error}")
                duration = 0.0
            logger.error(f"Workflow failed after {duration:.2f}s: {str(e)}", exc_info=True)
            raise
