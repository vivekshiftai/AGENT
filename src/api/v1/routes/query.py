"""Query endpoints.

Use the request/query ID from request context (set by middleware or by this module for WebSocket)
so the same ID is used for the whole request and for tracing; we do not generate new IDs.
"""
from fastapi import APIRouter, HTTPException, status, WebSocket, WebSocketDisconnect
import logging
import asyncio
from datetime import datetime
from typing import Dict, Any
import uuid
import json
from infrastructure.langgraph.graph_builder import AnalyticsGraphBuilder
from shared.request_context import get_request_id, set_request_id, set_request_context, clear_request_context
from infrastructure.langgraph.utils import build_aggregation_details, filter_charts_with_all_zeros
from application.dto.query_request import QueryRequest
from application.dto.dashboard_response import DashboardResponse
from shared.exceptions import AnalyticsException
from infrastructure.database.postgres_client_singleton import get_shared_postgres_client
from infrastructure.websocket.connection_manager import QueryWebSocketManager
from infrastructure.services import RefreshTokenExpiredError

router = APIRouter(prefix="/query", tags=["query"])
logger = logging.getLogger(__name__)


def _strip_reasoning_from_dashboard_payload(
    charts: list, computation_results: list | None
) -> tuple[list, list | None]:
    """
    Remove per-chart and per-metric reasoning from the final dashboard payload.
    These fields are sent only via partial updates (charts_ready, metrics_ready).
    """
    stripped_charts = []
    for c in charts:
        if isinstance(c, dict):
            c = {k: v for k, v in c.items() if k not in ("reasoning", "column_reasons")}
        stripped_charts.append(c)
    stripped_results = None
    if computation_results:
        stripped_results = []
        for r in computation_results:
            if isinstance(r, dict):
                r = {k: v for k, v in r.items() if k not in ("reasoning", "column")}
            stripped_results.append(r)
    return stripped_charts, stripped_results


def _json_serialize_for_size_check(obj: Any) -> str:
    """
    Serialize object to JSON string for size checking, handling non-serializable types.
    
    This function handles dates, datetimes, and other types that aren't JSON serializable
    by converting them to strings.
    """
    from datetime import date, datetime
    
    def default_serializer(o):
        if isinstance(o, (date, datetime)):
            return o.isoformat()
        elif hasattr(o, 'isoformat'):  # Other date/time types
            return o.isoformat()
        elif hasattr(o, 'item'):  # numpy types
            return o.item()
        elif hasattr(o, 'tolist'):  # numpy arrays
            return o.tolist()
        else:
            return str(o)
    
    return json.dumps(obj, default=default_serializer)


@router.post("", response_model=DashboardResponse, status_code=status.HTTP_200_OK)
async def process_query(request: QueryRequest) -> DashboardResponse:
    """
    Process an analytics query and return a dashboard.
    
    Args:
        request: Query request
        
    Returns:
        Dashboard response
        
    Raises:
        HTTPException: If processing fails
    """
    # Validate and set analysis_mode
    analysis_mode = request.analysis_mode or "normal"
    if analysis_mode not in ["normal", "deep_research"]:
        logger.warning(f"🌐 [API] Invalid analysis_mode '{analysis_mode}', defaulting to 'normal'")
        analysis_mode = "normal"
    
    logger.info("=" * 80)
    logger.info("🌐 [API] Received analytics query request")
    logger.info(f"🌐 [API] User: {request.user_id}")
    logger.info(f"🌐 [API] Query: '{request.query[:100]}{'...' if len(request.query) > 100 else ''}'")
    logger.info(f"🌐 [API] Analysis Mode: {analysis_mode}")
    if request.user_context:
        logger.info(f"🌐 [API] User Context: '{request.user_context[:200]}{'...' if len(request.user_context) > 200 else ''}'")
    if request.feedback_summary:
        logger.info(f"🌐 [API] Feedback Summary: '{request.feedback_summary[:200]}{'...' if len(request.feedback_summary) > 200 else ''}'")
    if request.org_context:
        logger.info(f"🌐 [API] Org Context: '{request.org_context[:200]}{'...' if len(request.org_context) > 200 else ''}'")
    logger.info("=" * 80)

    start_time = datetime.now()
    try:
        # Fetch active data source for the user (using async to prevent blocking)
        data_source_config = None
        try:
            postgres_client = get_shared_postgres_client(ensure_tables=False)
            active_sources = await postgres_client.execute_query_async(
                "SELECT * FROM data_source_config WHERE user_id = %s AND is_active = TRUE LIMIT 1",
                (request.user_id,)
            )
            if active_sources:
                source = active_sources[0]
                data_source_config = {
                    "type": source["type"],
                    "host": source.get("host"),
                    "port": source.get("port"),
                    "username": source.get("username"),
                    "password": source.get("password"),
                    "database_name": source.get("database_name"),
                    "file_path": source.get("file_path"),
                }
                logger.info(f"🌐 [API] Using active data source: {source['name']} ({source['type']})")
            else:
                logger.error(f"🌐 [API] No active data source found for user {request.user_id}")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="No active data source configured. Please configure and activate a data source through the Data Source Manager."
                )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"🌐 [API] Failed to fetch active data source: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve data source configuration: {str(e)}"
            )
        
        # Initialize LangGraph builder
        graph_builder = AnalyticsGraphBuilder()
        
        # Use request ID from context (set by middleware); do not generate a new one
        query_id = get_request_id()
        if not query_id:
            query_id = str(uuid.uuid4())
            set_request_id(query_id)
            logger.info(f"🌐 [API] No request_id in context, set query_id: {query_id}")
        
        # Initialize state with required fields
        initial_state: Dict[str, Any] = {
            # Phase I: Context & Strategy
            "query_id": query_id,
            "user_query": request.query,
            "user_id": request.user_id,
            "analysis_mode": analysis_mode,  # Analysis mode: "normal" or "deep_research"
            "user_context": request.user_context,  # User context to tune responses
            "feedback_summary": request.feedback_summary,  # Feedback summary to guide response tuning
            "org_context": request.org_context,  # Organization-level context (e.g., fiscal dates, org-specific settings)
            "timestamp": datetime.utcnow(),
            "data_source_config": data_source_config,
            "parsed_intent": None,
            "selected_methodologies": None,
            "required_tables": [],
            "selected_tables": [],
            "schema_context": None,
            "table_descriptions": None,
            "sql_plan": None,
            # Phase II: Construction
            "filters": None,
            "generated_sql": None,
            "fetched_data": None,
            "fetched_data_columns": None,
            "unified_schema": None,
            # Pipeline A: Analysis
            "aggregated_methodology_stack": None,
            "financial_calculations": None,
            "calculated_results": None,
            "validated_analysis_data": None,
            "data_quality_issues": [],
            "narrative_summary": None,
            "data_orchestration_summary": None,
            "analytical_output": None,
            # Pipeline A: New nodes
            "operation_plan": None,
            "processed_dataframe": None,
            "computation_execution_log": None,
            "computation_metrics": None,
            "analysis_summary": None,
            # Pipeline B: Visualization
            "data_shape_analysis": None,
            "chart_intent_analysis": None,
            "chart_plan": None,
            "recommended_charts": [],
            # Final Output
            "orchestrated_response": None,
            "dashboard_response": None,
            "prepared_charts": [],
            "errors": [],
            "status": "pending",
        }
        
        # Execute workflow
        final_state = await graph_builder.execute(initial_state, progress_callback=None)
        
        # Debug: Log what's in the final state
        logger.info(f"🔍 [API] Final state keys: {list(final_state.keys())}")
        logger.info(f"🔍 [API] prepared_charts count: {len(final_state.get('prepared_charts', []))}")
        logger.info(f"🔍 [API] dashboard_response present: {'dashboard_response' in final_state}")
        logger.info(f"🔍 [API] dashboard_response value: {final_state.get('dashboard_response') is not None}")
        
        # Handle orchestration outcomes: clarification or insufficient data
        orchestrator_decision = (final_state.get("orchestrator_decision") or "").strip().lower()
        if orchestrator_decision == "clarification":
            msg = final_state.get("clarification_message") or "What would you like me to focus on?"
            logger.info(f"🌐 [API] Returning clarification response: {msg[:80]}...")
            result = DashboardResponse(
                query=request.query,
                charts=[],
                insights=[msg],
                statistics=None,
                status="clarification",
                errors=[],
            )
            return result

        # Simple flow with no data: LLM agent response is in analysis_summary (no data_sufficiency_check node)
        if orchestrator_decision == "simple" and final_state.get("no_data_available"):
            analysis_summary = final_state.get("analysis_summary") or {}
            if isinstance(analysis_summary, dict) and analysis_summary.get("summary_text"):
                insight = analysis_summary.get("summary_text", "").strip()
                logger.info(f"🌐 [API] Returning simple-flow no-data response (LLM agent): {insight[:80]}...")
                return DashboardResponse(
                    query=request.query,
                    charts=[],
                    insights=[insight],
                    statistics=None,
                    status="insufficient_data",
                    errors=[],
                )

        # Get dashboard_response from response_orchestration node
        dashboard_response_dict = final_state.get("dashboard_response")

        # Extract reasoning fields from final state
        table_reasoning = final_state.get("table_reasoning", "")
        metric_reasoning = final_state.get("metric_reasoning", "")
        chart_reasoning = final_state.get("chart_reasoning", "")

        if dashboard_response_dict:
            # Use dashboard_response from LangGraph node
            # Filter out charts with all zeros before creating response
            charts = dashboard_response_dict.get('charts', [])
            filtered_charts = filter_charts_with_all_zeros(charts)
            # Reasoning/column_reasons sent only via partial updates; strip from final dashboard response
            stripped_charts, stripped_results = _strip_reasoning_from_dashboard_payload(
                filtered_charts, dashboard_response_dict.get("computation_results")
            )
            dashboard_response_dict = {**dashboard_response_dict, "charts": stripped_charts}
            if stripped_results is not None:
                dashboard_response_dict["computation_results"] = stripped_results

            logger.info(f"✅ [API] Using dashboard_response from response_orchestration node with {len(stripped_charts)} charts (filtered from {len(charts)})")
            # Add reasoning fields to the existing dashboard response
            dashboard_response_dict["table_reasoning"] = table_reasoning
            dashboard_response_dict["metric_reasoning"] = metric_reasoning
            dashboard_response_dict["chart_reasoning"] = chart_reasoning
            result = DashboardResponse(**dashboard_response_dict)
        else:
            # Fallback: build DashboardResponse from state (for backward compatibility)
            logger.warning("⚠️ [API] No dashboard_response found in state, building from components")
            logger.warning(f"⚠️ [API] State has prepared_charts: {len(final_state.get('prepared_charts', []))} charts")
            
            # Get prepared charts and filter out charts with all zeros; reasoning sent via partial updates only
            prepared_charts = final_state.get("prepared_charts", [])
            charts = filter_charts_with_all_zeros(prepared_charts) if prepared_charts else []
            charts, computation_results_raw = _strip_reasoning_from_dashboard_payload(charts, final_state.get("computation_results", []))
            
            # Get orchestrated_response for insights (handle None case)
            orchestrated_response = final_state.get("orchestrated_response")
            if orchestrated_response is None:
                orchestrated_response = {}
            data_summary = orchestrated_response.get("summary", "") if isinstance(orchestrated_response, dict) else ""
            key_insights = orchestrated_response.get("key_insights", []) if isinstance(orchestrated_response, dict) else []
            
            # Build insights (use analysis_summary for simple flow when no orchestrated_response)
            insights = []
            if data_summary:
                insights.append(data_summary)
            if not insights:
                analysis_summary = final_state.get("analysis_summary") or {}
                if isinstance(analysis_summary, dict) and analysis_summary.get("summary_text"):
                    insights.append(analysis_summary.get("summary_text", "").strip())
            if key_insights:
                for insight in key_insights:
                    if isinstance(insight, dict):
                        label = insight.get("label", "")
                        value = insight.get("value", "")
                        if label and value:
                            insights.append(f"• {label}: {value}")
                    elif isinstance(insight, str):
                        insights.append(f"• {insight}")
            
            # Get other fields from state (use stripped computation_results for final payload)
            computation_results = computation_results_raw if computation_results_raw is not None else final_state.get("computation_results", [])
            operation_plan = final_state.get("operation_plan", {})
            # State uses "plan" not "sql_plan" - check both for backward compatibility
            sql_plan = final_state.get("plan", final_state.get("sql_plan", {}))
            # State uses "generated_queries" not "generated_sql" - check both for backward compatibility
            generated_sql = final_state.get("generated_queries", final_state.get("generated_sql", ""))
            selected_tables = final_state.get("selected_tables", [])
            aggregation_details = build_aggregation_details(operation_plan, selected_tables)
            
            # Log field extraction for debugging
            logger.info(f"📋 [API] Extracted fields from state:")
            logger.info(f"📋 [API]   - sql_plan: {bool(sql_plan)} (type: {type(sql_plan).__name__})")
            logger.info(f"📋 [API]   - generated_sql: {bool(generated_sql)} (type: {type(generated_sql).__name__}, length: {len(str(generated_sql)) if generated_sql else 0})")
            logger.info(f"📋 [API]   - operation_plan: {bool(operation_plan)} (type: {type(operation_plan).__name__})")
            
            # Get source data - include all rows, not just first 100
            source_data = {}
            table_dataframes = final_state.get("table_dataframes", {})
            fetched_data = final_state.get("fetched_data", [])
            if table_dataframes:
                for table_name, table_rows in table_dataframes.items():
                    try:
                        # Check if the DataFrame is empty
                        is_empty = False
                        if hasattr(table_rows, 'is_empty'):  # Polars DataFrame
                            is_empty = table_rows.is_empty()
                        elif hasattr(table_rows, 'empty'):  # Pandas DataFrame
                            is_empty = table_rows.empty
                        elif hasattr(table_rows, '__len__'):  # List or other
                            is_empty = len(table_rows) == 0
                        
                        if not is_empty:
                            # Convert DataFrame to list of dicts for JSON serialization
                            if hasattr(table_rows, 'to_dicts'):  # Polars DataFrame
                                source_data[table_name] = table_rows.to_dicts()
                            elif hasattr(table_rows, 'to_dict'):  # Pandas DataFrame
                                source_data[table_name] = table_rows.to_dict('records')
                            else:  # already a list of dicts
                                source_data[table_name] = table_rows
                    except Exception as df_error:
                        logger.warning(f"⚠️ [API] Failed to convert {table_name} to dict: {str(df_error)}")
            elif fetched_data:
                source_data["data"] = fetched_data
            
            # Build statistics
            statistics = {}
            if computation_results:
                for result in computation_results:
                    if isinstance(result, dict) and "metric" in result and "value" in result:
                        metric_name = result.get("metric", "")
                        metric_value = result.get("value")
                        if metric_name and metric_value is not None:
                            statistics[metric_name] = metric_value
            
            result = DashboardResponse(
                query=request.query,
                charts=charts,
                insights=insights,
                statistics=statistics if statistics else None,
                status=final_state.get("status", "success"),
                errors=final_state.get("errors", []),
                date_grouping=None,
                computation_results=computation_results if computation_results else None,
                # Removed global operation_plan and aggregation_details as they're now included in individual components
                sql_plan=sql_plan if sql_plan else None,
                generated_sql=generated_sql if generated_sql else None,
                selected_tables=selected_tables if selected_tables else None,
                source_data=source_data if source_data else None,
                table_reasoning=table_reasoning,
                metric_reasoning=metric_reasoning,
                chart_reasoning=chart_reasoning,
            )

        duration = (datetime.now() - start_time).total_seconds()
        logger.info("✅ [API] Analytics query processed successfully")
        logger.info(f"✅ [API] Response charts: {len(result.charts)}")
        logger.info(f"✅ [API] Response insights: {len(result.insights)}")
        logger.info(f"✅ [API] Total API duration: {duration:.2f}s")

        return result
        
    except HTTPException:
        # Re-raise HTTPExceptions (like "No active data source") to preserve status codes
        raise
    except RefreshTokenExpiredError as e:
        # Refresh token expired - user needs to re-authenticate
        error_message = str(e) if str(e) else "Your SAP Datasphere credentials have expired. Please login again."
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"❌ [API] Refresh token expired after {duration:.2f}s: {error_message}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=error_message
        )
    except AnalyticsException as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"❌ [API] Analytics exception after {duration:.2f}s: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except Exception as e:
        import traceback
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"❌ [API] Unexpected error after {duration:.2f}s: {str(e)}\n{traceback.format_exc()}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error",
        )


@router.websocket("/ws")
async def websocket_query(websocket: WebSocket):
    """
    WebSocket endpoint for query processing with real-time progress updates.
    
    Expected message format:
    {
        "query": "user query string",
        "user_id": "user identifier"
    }
    """
    ws_manager = QueryWebSocketManager(websocket)
    
    try:
        # Accept the WebSocket connection
        if not await ws_manager.connect():
            logger.error("🌐 [WS] Failed to accept WebSocket connection")
            return
        
        # Start keep-alive pings
        await ws_manager.start_keep_alive()
        
        # Wait for initial query request
        try:
            data = await websocket.receive_text()
            request_data = json.loads(data)
            
            query = request_data.get("query", "").strip()
            user_id = request_data.get("user_id", "ab3c79ee-28ec-42ec-9347-604f0a584219")
            analysis_mode = request_data.get("analysis_mode", "normal")  # Default to "normal" if not provided
            user_context = request_data.get("user_context")  # User context to tune responses
            feedback_summary = request_data.get("feedback_summary")  # Feedback summary to guide response tuning
            org_context = request_data.get("org_context")  # Organization-level context (e.g., fiscal dates, org-specific settings)
            
            # Validate analysis_mode
            if analysis_mode not in ["normal", "deep_research"]:
                logger.warning(f"🌐 [WS] Invalid analysis_mode '{analysis_mode}', defaulting to 'normal'")
                analysis_mode = "normal"
            
            if not query:
                await ws_manager.send_error(
                    message="Query is required. Please provide a valid query.",
                    error_code="QUERY_REQUIRED",
                    error_category="VALIDATION"
                )
                await ws_manager.close()
                return
            
            # Set query_id and user_id in request context immediately for log traceability (before any further logs)
            query_id = get_request_id()
            if not query_id:
                query_id = str(uuid.uuid4())
            try:
                set_request_context(query_id=query_id, user_id=user_id)
            except Exception as e:
                import traceback
                logger.error(
                    "🌐 [WS] Failed to set query_id/user_id in request context - logs may show query_id=- user_id=-. "
                    "Reason: %s (%s). query_id=%s user_id=%s",
                    type(e).__name__, str(e), query_id, user_id
                )
                logger.error("🌐 [WS] set_request_context traceback:\n%s", traceback.format_exc())
                # Continue anyway so the query can run; only logging context is affected

            logger.info("=" * 80)
            logger.info("🌐 [WS] Received WebSocket query request")
            logger.info(f"🌐 [WS] User: {user_id}")
            logger.info(f"🌐 [WS] Query: '{query[:100]}{'...' if len(query) > 100 else ''}'")
            logger.info(f"🌐 [WS] Analysis Mode: {analysis_mode}")
            if user_context:
                logger.info(f"🌐 [WS] User Context: '{user_context[:200]}{'...' if len(user_context) > 200 else ''}'")
            if feedback_summary:
                logger.info(f"🌐 [WS] Feedback Summary: '{feedback_summary[:200]}{'...' if len(feedback_summary) > 200 else ''}'")
            if org_context:
                logger.info(f"🌐 [WS] Org Context: '{org_context[:200]}{'...' if len(org_context) > 200 else ''}'")
            logger.info("=" * 80)
            
        except json.JSONDecodeError:
            await ws_manager.send_error(
                message="Invalid request format. Please send valid JSON.",
                error_code="INVALID_JSON",
                error_category="VALIDATION"
            )
            await ws_manager.close()
            return
        except WebSocketDisconnect:
            logger.info("🌐 [WS] Client disconnected before sending query")
            return
        
        # Fetch active data source for the user (using async to prevent blocking)
        data_source_config = None
        try:
            postgres_client = get_shared_postgres_client(ensure_tables=False)
            active_sources = await postgres_client.execute_query_async(
                "SELECT * FROM data_source_config WHERE user_id = %s AND is_active = TRUE LIMIT 1",
                (user_id,)
            )
            if active_sources:
                source = active_sources[0]
                data_source_config = {
                    "type": source["type"],
                    "host": source.get("host"),
                    "port": source.get("port"),
                    "username": source.get("username"),
                    "password": source.get("password"),
                    "database_name": source.get("database_name"),
                    "file_path": source.get("file_path"),
                }
                logger.info(f"🌐 [WS] Using active data source: {source['name']} ({source['type']})")
            else:
                logger.error(f"🌐 [WS] No active data source found for user {user_id}")
                await ws_manager.send_error(
                    message="No active data source configured. Please configure and activate a data source through the Data Source Manager.",
                    error_code="NO_DATA_SOURCE",
                    error_category="DATA_SOURCE",
                    details="No active data source found for this user. Please configure a data source before running queries."
                )
                await ws_manager.close()
                return
        except Exception as e:
            logger.error(f"🌐 [WS] Failed to fetch active data source: {str(e)}")
            await ws_manager.send_error(
                message="Failed to retrieve data source configuration. Please try again or contact support.",
                error_code="DATA_SOURCE_FETCH_ERROR",
                error_category="DATA_SOURCE",
                details=str(e)
            )
            await ws_manager.close()
            return
        
        # Initialize LangGraph builder
        graph_builder = AnalyticsGraphBuilder()
        
        # query_id already set above for traceability; ensure WebSocket manager has it
        logger.info(f"🌐 [WS] Set query_id for WebSocket session: {query_id}")
        
        # Set query_id in WebSocket manager (initializes token usage registry)
        ws_manager.set_query_id(query_id, query)
        
        # Create progress callback
        progress_callback = ws_manager.create_progress_callback()
        
        # Initialize state with required fields
        initial_state: Dict[str, Any] = {
            # Phase I: Context & Strategy
            "query_id": query_id,
            "user_query": query,
            "user_id": user_id,
            "analysis_mode": analysis_mode,  # Analysis mode: "normal" or "deep_research"
            "user_context": user_context,  # User context to tune responses
            "feedback_summary": feedback_summary,  # Feedback summary to guide response tuning
            "org_context": org_context,  # Organization-level context (e.g., fiscal dates, org-specific settings)
            "timestamp": datetime.utcnow(),
            "data_source_config": data_source_config,
            "parsed_intent": None,
            "selected_methodologies": None,
            "required_tables": [],
            "selected_tables": [],
            "schema_context": None,
            "table_descriptions": None,
            "sql_plan": None,
            # Phase II: Construction
            "filters": None,
            "generated_sql": None,
            "fetched_data": None,
            "fetched_data_columns": None,
            "unified_schema": None,
            # Pipeline A: Analysis
            "aggregated_methodology_stack": None,
            "financial_calculations": None,
            "calculated_results": None,
            "validated_analysis_data": None,
            "data_quality_issues": [],
            "narrative_summary": None,
            "data_orchestration_summary": None,
            "analytical_output": None,
            # Pipeline A: New nodes
            "operation_plan": None,
            "processed_dataframe": None,
            "computation_execution_log": None,
            "computation_metrics": None,
            "analysis_summary": None,
            # Pipeline B: Visualization
            "data_shape_analysis": None,
            "chart_intent_analysis": None,
            "chart_plan": None,
            "recommended_charts": [],
            # Final Output
            "orchestrated_response": None,
            "dashboard_response": None,
            "prepared_charts": [],
            "errors": [],
            "status": "pending",
        }
        
        # Execute workflow with progress callback
        # CRITICAL: Run query in background task to keep event loop responsive
        # This ensures heartbeats continue during long-running queries
        try:
            # Create a background task for query execution
            # This prevents blocking the event loop, allowing heartbeats to continue
            # Pass ws_manager to enable incremental data sending
            query_task = asyncio.create_task(
                graph_builder.execute(initial_state, progress_callback=progress_callback, ws_manager=ws_manager)
            )
            
            # Wait for query to complete while keeping event loop responsive
            # Heartbeats continue automatically via the keep-alive task
            final_state = await query_task
            
            # Get dashboard_response from response_orchestration node (if available)
            # Note: Partial updates already sent charts, metrics, intelligence, and source_data
            # This is mainly for the final completion message
            dashboard_response_dict = final_state.get("dashboard_response")
            orchestrator_decision = (final_state.get("orchestrator_decision") or "").strip().lower()
            
            # Extract reasoning fields from final state
            table_reasoning = final_state.get("table_reasoning", "")
            metric_reasoning = final_state.get("metric_reasoning", "")
            chart_reasoning = final_state.get("chart_reasoning", "")

            # No related data: flow ended at column selection (simple or analytical) with data_sufficiency_result.can_answer false; same UI as clarification
            data_sufficiency = final_state.get("data_sufficiency_result")
            if isinstance(data_sufficiency, dict) and data_sufficiency.get("can_answer") is False:
                user_message = data_sufficiency.get("user_message") or "There is no related data for your query in this data source. Try one of the suggestions below."
                suggested_queries = data_sufficiency.get("suggested_queries") or []
                logger.info(f"🌐 [WS] Sending no-related-data completion: {user_message[:80]}... suggested_queries={len(suggested_queries)}")
                result = DashboardResponse(
                    query=query,
                    charts=[],
                    insights=[user_message],
                    statistics=None,
                    status="insufficient_data",
                    errors=[],
                    normal_text=user_message,
                    suggested_queries=suggested_queries if suggested_queries else None,
                    table_reasoning=table_reasoning,
                    metric_reasoning=metric_reasoning,
                    chart_reasoning=chart_reasoning,
                )
            # Clarification: flow ended at orchestration_agent; send clarification message + suggested_queries (UI buttons)
            elif orchestrator_decision == "clarification":
                clarification_message = final_state.get("clarification_message") or "What would you like me to focus on?"
                suggested_queries = final_state.get("clarification_suggestions") or []
                logger.info(f"🌐 [WS] Sending clarification completion: {clarification_message[:80]}... suggested_queries={len(suggested_queries)}")
                result = DashboardResponse(
                    query=query,
                    charts=[],
                    insights=[clarification_message],
                    statistics=None,
                    status="clarification",
                    errors=[],
                    normal_text=clarification_message,
                    suggested_queries=suggested_queries if suggested_queries else None,
                    table_reasoning=table_reasoning,
                    metric_reasoning=metric_reasoning,
                    chart_reasoning=chart_reasoning,
                )
            # Simple flow: always return normal_text from analytical_summary (with or without data)
            elif orchestrator_decision == "simple":
                analysis_summary = final_state.get("analysis_summary") or {}
                summary_text = (analysis_summary.get("summary_text") or "").strip() if isinstance(analysis_summary, dict) else ""
                no_data = final_state.get("no_data_available")
                status_simple = "insufficient_data" if no_data else "summary_ready"
                if no_data and not summary_text:
                    fallback_msg = "There isn't enough data to answer your question. You can try rephrasing your request or asking about a different period or scope."
                    normal_text = fallback_msg
                    insights_list = [fallback_msg]
                else:
                    normal_text = summary_text or ""
                    insights_list = [summary_text] if summary_text else []
                logger.info(f"🌐 [WS] Sending simple flow completion (normal_text): {status_simple} len={len(normal_text)}")
                result = DashboardResponse(
                    query=query,
                    charts=[],
                    insights=insights_list,
                    statistics=None,
                    status=status_simple,
                    errors=[],
                    normal_text=normal_text,
                    table_reasoning=table_reasoning,
                    metric_reasoning=metric_reasoning,
                    chart_reasoning=chart_reasoning,
                )
            elif dashboard_response_dict:
                # Use dashboard_response from LangGraph node (simplified - partial updates already sent everything)
                # Filter out charts with all zeros; reasoning/column_reasons sent only via partial updates
                charts = dashboard_response_dict.get('charts', [])
                filtered_charts = filter_charts_with_all_zeros(charts) if charts else []
                stripped_charts, stripped_results = _strip_reasoning_from_dashboard_payload(
                    filtered_charts, dashboard_response_dict.get("computation_results")
                )
                dashboard_response_dict = {**dashboard_response_dict, "charts": stripped_charts}
                if stripped_results is not None:
                    dashboard_response_dict["computation_results"] = stripped_results
                logger.info(f"✅ [WS] Using dashboard_response from response_orchestration node for completion message")
                result = DashboardResponse(**dashboard_response_dict)
            else:
                # Partial updates already sent everything, so we can send a minimal completion message
                # Build minimal response just for completion signal
                logger.info("✅ [WS] Partial updates already sent all data, sending minimal completion message")
                result = DashboardResponse(
                    query=query,
                    charts=[],
                    insights=[],
                    statistics=None,
                    status=final_state.get("status", "completed"),
                    errors=final_state.get("errors", []),
                    table_reasoning=table_reasoning,
                    metric_reasoning=metric_reasoning,
                    chart_reasoning=chart_reasoning,
                )
            
            # Convert DashboardResponse to dict for WebSocket transmission
            result_dict = result.model_dump() if hasattr(result, 'model_dump') else result.dict()
            
            # Ensure query_id is included for node timing tracking
            result_dict['query_id'] = query_id
            
            # NOTE: source_data is NOT sent via WebSocket - client fetches data via export endpoints
            # This improves performance and allows clients to download data on-demand
            # Uncomment below if you want to send source_data chunks via WebSocket instead of endpoints:

            # # Extract source_data from final_state.table_dataframes for chunked transmission
            # # This ensures we send prepared data (charts, metrics, intelligence) FIRST, then source_data
            # source_data_to_send = None
            # table_dataframes = final_state.get("table_dataframes", {})
            #
            # if table_dataframes:
            #     # Import the extraction function
            #     from ....infrastructure.langgraph.data_analysis_graph import _extract_full_source_data
            #
            #     try:
            #         logger.info(f"🌐 [WS] Extracting full source_data from table_dataframes for chunked transmission")
            #         full_source_data = _extract_full_source_data(table_dataframes)
            #
            #         if full_source_data:
            #             source_data_size = len(_json_serialize_for_size_check(full_source_data))
            #             table_count = len(full_source_data)
            #             total_rows = sum(len(records) for records in full_source_data.values())
            #             logger.info(f"🌐 [WS] Extracted source_data: {table_count} table(s), {total_rows:,} rows, size: {source_data_size / (1024*1024):.1f}MB")
            #
            #             # Always send source_data in chunks (even if small) to ensure connection stays open
            #             # This allows client to receive all data before connection closes
            #             source_data_to_send = full_source_data
            #         result_dict['source_data'] = None  # Indicate it will be sent separately
            #         result_dict['source_data_chunks_coming'] = True  # Signal that chunks will follow
            #             logger.info(f"🌐 [WS] source_data will be sent in chunks after completion message")
            #     else:
            #             logger.warning(f"🌐 [WS] No source_data extracted from table_dataframes")
            #     except Exception as e:
            #         logger.error(f"🌐 [WS] Failed to extract source_data from table_dataframes: {e}", exc_info=True)
            # else:
            #     logger.info(f"🌐 [WS] No table_dataframes in final_state, skipping source_data extraction")
            
            # CRITICAL: Add delay before completion message to ensure all partial data
            # (charts, metrics, intelligence) has been fully transmitted and processed by the client.
            # Without this delay, the completion message might arrive before partial data is processed,
            # causing the frontend to potentially clear/reset already-received data.
            await asyncio.sleep(0.3)
            
            # Send completion message (partial updates already sent all data, this is just a signal)
            send_success = await ws_manager.send_complete(result_dict)
            
            if send_success:
                # Source data is available via export endpoints, not sent via WebSocket
                logger.info(f"✅ [WS] Query processed successfully via WebSocket | Source data available via export endpoints")
                
                # NOTE: source_data chunks are NOT sent via WebSocket - client fetches data via export endpoints
                # This improves performance and allows clients to download data on-demand
                # Uncomment below if you want to send source_data chunks via WebSocket instead of endpoints:

                # # Send source_data chunks AFTER all prepared data (charts, metrics, intelligence) is sent
                # # This ensures prepared data is sent first and connection stays open until all data is transferred
                # if source_data_to_send and not ws_manager._source_data_chunks_sent:
                #     # Add delay to ensure complete message is fully processed by client
                #     await asyncio.sleep(0.3)  # Give client time to process complete message
                #
                #     # Verify connection is still open before sending chunks
                #     if not ws_manager.is_connected or ws_manager.is_closed:
                #         logger.warning(f"⚠️ [WS] Connection closed before sending chunks (is_connected={ws_manager.is_connected}, is_closed={ws_manager.is_closed})")
                #     else:
                #         table_count = len(source_data_to_send)
                #         total_rows = sum(len(records) for records in source_data_to_send.values())
                #         logger.info(f"🌐 [WS] Sending source_data chunks AFTER prepared data: {table_count} table(s), {total_rows:,} rows")
                #         # Use smaller chunk size (1MB) to prevent client disconnections
                #         chunks_sent = await ws_manager.send_source_data_chunks(source_data_to_send, max_chunk_size=1048576)
                #         if chunks_sent:
                #             ws_manager._source_data_chunks_sent = True
                #             logger.info(f"✅ [WS] All source_data chunks sent successfully ({table_count} tables)")
                #         else:
                #             logger.warning(f"⚠️ [WS] Some source_data chunks may not have been sent ({table_count} tables)")
                # elif source_data_to_send and ws_manager._source_data_chunks_sent:
                #     logger.info(f"🌐 [WS] Source_data chunks already sent, skipping duplicate send")

                # Standard delay to ensure completion message is fully processed
                await asyncio.sleep(0.5)
            else:
                logger.warning("⚠️ [WS] Failed to send completion message - client may have disconnected")
            
        except RefreshTokenExpiredError as e:
            # Refresh token expired - user needs to re-authenticate
            error_message = str(e) if str(e) else "Your SAP Datasphere credentials have expired. Please login again."
            logger.error(f"❌ [WS] Refresh token expired - user needs to re-authenticate: {error_message}")
            await ws_manager.send_error(
                message=error_message,
                error_code="REFRESH_TOKEN_EXPIRED",
                error_category="AUTHENTICATION",
                details="Your SAP Datasphere refresh token has expired. You need to re-authenticate to continue using the service."
            )
        except AnalyticsException as e:
            logger.error(f"❌ [WS] Analytics exception: {str(e)}")
            await ws_manager.send_error(
                message=str(e),
                error_code="ANALYTICS_ERROR",
                error_category="ANALYTICS",
                details="An error occurred during query processing. Please check your query and try again."
            )
        except Exception as e:
            import traceback
            error_trace = traceback.format_exc()
            logger.error(f"❌ [WS] Unexpected error: {str(e)}\n{error_trace}")
            await ws_manager.send_error(
                message="An unexpected error occurred during query processing. Please try again or contact support if the issue persists.",
                error_code="INTERNAL_SERVER_ERROR",
                error_category="SYSTEM",
                details=str(e)
            )
        
        finally:
            # Clear request context (query_id, user_id) so it does not leak to other connections
            clear_request_context()
            # Close the WebSocket connection
            await ws_manager.close()
            
    except WebSocketDisconnect:
        logger.info("🌐 [WS] Client disconnected")
    except RefreshTokenExpiredError as e:
        # Refresh token expired - user needs to re-authenticate
        error_message = str(e) if str(e) else "Your SAP Datasphere credentials have expired. Please login again."
        logger.error(f"❌ [WS] Refresh token expired (outer handler): {error_message}")
        try:
            await ws_manager.send_error(
                message=error_message,
                error_code="REFRESH_TOKEN_EXPIRED",
                error_category="AUTHENTICATION",
                details="Your SAP Datasphere refresh token has expired. You need to re-authenticate to continue using the service."
            )
            await ws_manager.close()
        except:
            pass  # Ignore errors during cleanup
    except Exception as e:
        logger.error(f"❌ [WS] WebSocket error: {str(e)}")
        try:
            await ws_manager.send_error(
                message="WebSocket connection error. Please refresh the page and try again.",
                error_code="WEBSOCKET_ERROR",
                error_category="CONNECTION",
                details=str(e)
            )
            await ws_manager.close()
        except:
            pass  # Ignore errors during cleanup



