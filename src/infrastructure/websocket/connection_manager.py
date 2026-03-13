"""
WebSocket Connection Manager for handling query progress streaming.

This module provides a clean, centralized connection manager for WebSocket
connections during query processing. It handles:
- Connection lifecycle (connect, disconnect, cleanup)
- Message sending with error handling
- Keep-alive ping management
- Graceful connection closure
"""
import asyncio
import logging
from typing import Optional, Dict, Any, Callable
from datetime import datetime
from fastapi import WebSocket, WebSocketDisconnect
from enum import Enum

logger = logging.getLogger(__name__)


class MessageType(str, Enum):
    """WebSocket message types."""
    PROGRESS = "progress"
    COMPLETE = "complete"
    ERROR = "error"
    PING = "ping"
    NODE_START = "node_start"
    NODE_COMPLETE = "node_complete"
    PIPELINE_START = "pipeline_start"
    DATA_CHUNK = "data_chunk"
    DATA_COMPLETE = "data_complete"
    CHARTS_READY = "charts_ready"
    METRICS_READY = "metrics_ready"
    GANTT_READY = "gantt_ready"  # Gantt chart data (partial update from gantt_preparation)
    SUMMARY_READY = "summary_ready"
    NORMAL_TEXT = "normal_text"  # Clarification and simple message-only responses (same format)
    INTELLIGENCE_READY = "intelligence_ready"
    INTELLIGENCE_PHASE1_READY = "intelligence_phase1_ready"  # Partial: levels 1–2 only, sent once after Phase 1
    STEPS_UPDATE = "steps_update"  # Processing nodes / step progress (partial update for frontend stepper)
    LLM_STREAM = "llm_stream"  # Token-by-token LLM text streaming chunk
    LLM_STREAM_END = "llm_stream_end"  # Signal that LLM streaming is complete
    LLM_THINKING_STREAM = "llm_thinking_stream"  # Token-by-token LLM thinking/reasoning chunk
    LLM_THINKING_END = "llm_thinking_end"  # Signal that LLM thinking stream is complete


class QueryWebSocketManager:
    """
    Manages a WebSocket connection for a single query execution.
    
    Provides:
    - Automatic keep-alive pings
    - Structured progress messages
    - Graceful error handling
    - Clean connection closure
    """
    
    def __init__(self, websocket: WebSocket):
        self.websocket = websocket
        self.is_connected = False
        self.is_closed = False
        self._ping_task: Optional[asyncio.Task] = None
        self._ping_count = 0
        self._start_time: Optional[datetime] = None
        self._completed_nodes: set = set()
        # JSON structure to accumulate all node timings for batch database update (single source of truth)
        self._node_timings_json: Dict[str, Dict[str, Any]] = {}
        self._last_node_end_time: Optional[datetime] = None  # Track last node's end time for sequential nodes
        # Token usage registry for batch database update
        self._token_usage_registry: Optional[Any] = None
        # Node timing registry for tracking actual start times
        self._node_timing_registry: Optional[Any] = None
        self._query_id: Optional[str] = None  # Store query_id for LLM usage tracking
        self._query_text: Optional[str] = None  # Store user query text
        self._analytics_saved: bool = False  # Flag to prevent duplicate saves
        # Track which partial data has been sent to avoid duplicates in complete message
        self._charts_sent: bool = False
        self._metrics_sent: bool = False
        self._summary_sent: bool = False
        self._intelligence_sent: bool = False
        self._source_data_chunks_sent: bool = False  # Track if source_data chunks have been sent
        
        # Node configuration - User-friendly display names (every node we may stream to the user)
        self.node_display_names = {
            "initializing": "Starting Analysis",
            "orchestration_agent": "Choosing Analysis Path",
            "query_analysis": "Understanding Your Request",
            "simple_workflow": "Running Simple Analysis",
            "moderate_workflow": "Running Full Analysis",
            "table_identification": "Finding Relevant Data Tables",
            "load_data": "Loading Data",
            "get_schema": "Loading Table Structure",
            "prepare_analytical_schema": "Preparing Schema",
            "simple_column_selection": "Selecting Columns",
            "simple_analytical_fetch_plan": "Planning Data Fetch",
            "analytical_column_selection": "Selecting Analysis Columns",
            "sql_plan_synthesis": "Planning Data Retrieval",
            "sap_analytical_passthrough": "Using Analytical View",
            "sap_fetch_plan": "Planning SAP Fetch",
            "analytical_fetch_plan": "Planning Fetch",
            "analytical_fetch_plan_sink": "Waiting for Plan",
            "financial_analyst_planner": "Analyzing Financial Patterns",
            "operation_specification": "Defining Calculations",
            "sql_generation": "Building Queries",
            "db_execution": "Retrieving Data",
            "sap_data_fetch": "Fetching Data",
            "computation_engine": "Computing Key Metrics",
            "gantt_preparation": "Building Gantt Chart",
            "analytical_summary": "Summarizing Findings",
        }
        
        # Pipeline structure: all nodes (main + inner) for progress when streaming with subgraphs=True
        self.main_pipeline_nodes = [
            "initializing",
            "orchestration_agent",
            "query_analysis",
            "simple_workflow",
            "moderate_workflow",
            "table_identification",
            "load_data",
            "get_schema",
            "prepare_analytical_schema",
            "simple_column_selection",
            "simple_analytical_fetch_plan",
            "analytical_column_selection",
            "sql_plan_synthesis",
            "sap_analytical_passthrough",
            "sap_fetch_plan",
            "analytical_fetch_plan",
            "financial_analyst_planner",
            "operation_specification",
            "sql_generation",
            "db_execution",
            "sap_data_fetch",
            "computation_engine",
            "gantt_preparation",
            "analytical_summary",
        ]
        self.pipeline_a_nodes = []
        self.pipeline_b_nodes = []
        
        # Graph structure: every node so we can show next_nodes and track steps for each (subgraphs=True sends every node)
        self.node_sequence = {
            "initializing": ["orchestration_agent"],
            "orchestration_agent": ["query_analysis"],
            "query_analysis": ["simple_workflow", "moderate_workflow"],
            "simple_workflow": [],
            "moderate_workflow": [],
            "table_identification": ["load_data", "prepare_analytical_schema"],
            "load_data": ["get_schema"],
            "get_schema": ["sql_plan_synthesis", "sap_fetch_plan", "sap_analytical_passthrough"],
            "prepare_analytical_schema": ["analytical_column_selection", "simple_column_selection"],
            "simple_column_selection": ["simple_analytical_fetch_plan"],
            "simple_analytical_fetch_plan": ["sap_data_fetch"],
            "analytical_column_selection": ["sap_fetch_plan", "financial_analyst_planner"],
            "sql_plan_synthesis": ["financial_analyst_planner", "sql_generation"],
            "sap_analytical_passthrough": ["analytical_fetch_plan"],
            "sap_fetch_plan": ["analytical_fetch_plan", "financial_analyst_planner"],
            "analytical_fetch_plan": ["sap_data_fetch", "analytical_fetch_plan_sink"],
            "analytical_fetch_plan_sink": [],
            "financial_analyst_planner": ["operation_specification"],
            "operation_specification": ["computation_engine"],
            "sql_generation": ["db_execution"],
            "db_execution": ["computation_engine", "operation_specification"],
            "sap_data_fetch": ["operation_specification"],
            "computation_engine": ["gantt_preparation"],
            "gantt_preparation": ["analytical_summary"],
            "analytical_summary": [],
        }
        
        # All trackable nodes for step counting (every node we may receive with subgraphs=True)
        self._all_trackable_nodes = list(self.node_sequence.keys())
        self._total_steps = len(self._all_trackable_nodes)
    
    def _get_node_display_name(self, node_name: str, fallback: Optional[str] = None) -> str:
        """Return a display name for every node; use dummy for unknown until next node is triggered."""
        if not node_name:
            return fallback or "Processing..."
        display = self.node_display_names.get(node_name)
        if display and str(display).strip():
            return str(display).strip()
        # Dummy for any node we don't have a label for (shown until next node triggers)
        return fallback or "Processing..."
    
    def _get_step_info(self) -> Dict[str, Any]:
        """Get step progress info for WebSocket messages: step_index, total_steps, completed_steps.
        
        Returns dict with:
        - step_index: Current step number (1-based when starting, equals completed when finishing)
        - total_steps: Total number of steps in the pipeline
        - completed_steps_count: Number of completed steps
        - completed_steps: List of {node, display_name} for completed steps (for frontend display)
        """
        completed_count = len(self._completed_nodes)
        completed_steps = [
            {"node": n, "display_name": self._get_node_display_name(n)}
            for n in self._all_trackable_nodes
            if n in self._completed_nodes
        ]
        return {
            "step_index": completed_count,
            "total_steps": self._total_steps,
            "completed_steps_count": completed_count,
            "completed_steps": completed_steps,
        }
    
    def _get_next_nodes(self, current_node: str) -> list:
        """Get the next node(s) in the sequence after the current node."""
        return self.node_sequence.get(current_node, [])
    
    async def connect(self) -> bool:
        """Accept the WebSocket connection."""
        try:
            await self.websocket.accept()
            self.is_connected = True
            self._start_time = datetime.now()
            
            # Track "initializing" node start time (query starts when connection is accepted)
            init_start = datetime.now()
            self._start_time = init_start  # Also set as overall start time
            pipeline = self._get_pipeline_info("initializing")
            self._node_timings_json["initializing"] = {
                "node_name": "initializing",
                "pipeline": pipeline or "main",
                "start_time": init_start.isoformat(),
                "status": "running",
            }
            self._last_node_end_time = init_start  # Initialize last end time
            
            logger.info("🌐 [WS Manager] Connection accepted")
            return True
        except Exception as e:
            logger.error(f"🌐 [WS Manager] Failed to accept connection: {e}")
            return False
    
    def set_query_id(self, query_id: str, query_text: str = "") -> None:
        """Set query_id and query_text, and initialize registries for batch updates."""
        self._query_id = query_id
        self._query_text = query_text
        # Reset analytics saved flag for new query
        self._analytics_saved = False
        
        # Initialize node timing registry for tracking actual start times
        from ..langgraph.node_timing_registry import NodeTimingRegistry, set_node_timing_registry
        self._node_timing_registry = NodeTimingRegistry(query_id)
        set_node_timing_registry(self._node_timing_registry)
        
        # Initialize token usage registry for this query
        from ..llm.token_usage_registry import TokenUsageRegistry, set_token_usage_registry
        self._token_usage_registry = TokenUsageRegistry(query_id)
        set_token_usage_registry(self._token_usage_registry)
        
        # Register this ws_manager so LLM calls can stream tokens to the UI
        from .ws_streaming_registry import set_ws_streaming_manager
        set_ws_streaming_manager(self)
        
        logger.info(f"🌐 [WS Manager] Initialized registries for query {query_id}")
    
    async def _save_analytics_data(self, query_id: str, status: str = "completed") -> None:
        """
        Save both node timings and token usage to database in batch.
        Called on query completion (success or error) to ensure data is saved.
        Prevents duplicate saves using _analytics_saved flag.
        
        Made async to prevent blocking intelligence partial updates.
        """
        # Prevent duplicate saves
        if self._analytics_saved:
            logger.debug(f"🌐 [WS Manager] Analytics data already saved for query {query_id}, skipping duplicate save")
            return
        
        # Save node timings (extract durations from JSON structure)
        node_timings_dict = {
            k: v.get("duration_seconds", 0) 
            for k, v in self._node_timings_json.items() 
            if v.get("duration_seconds") is not None and v.get("duration_seconds", 0) > 0
        }
        
        # Save node timings to database (run in background to avoid blocking)
        if node_timings_dict:
            try:
                # Run database save in background to avoid blocking intelligence partial updates
                # Use create_task directly - we're in an async context
                asyncio.create_task(self._save_node_timings_async(query_id, node_timings_dict, status))
            except Exception as e:
                logger.warning(f"Failed to schedule node timings save: {e}", exc_info=True)
        
        # Save token usage to database (run in background to avoid blocking)
        if self._token_usage_registry:
            try:
                # Run database save in background to avoid blocking intelligence partial updates
                # Use create_task directly - we're in an async context
                asyncio.create_task(self._save_token_usage_async(query_id))
            except Exception as e:
                logger.warning(f"Failed to schedule token usage save: {e}", exc_info=True)
        
        # Mark analytics as saved to prevent duplicate saves (immediately, before async saves complete)
        self._analytics_saved = True
        logger.debug(f"🌐 [WS Manager] Marked analytics data as saved for query {query_id}")
    
    async def _save_node_timings_async(self, query_id: str, node_timings_dict: dict, status: str) -> None:
        """Save node timings to database asynchronously."""
        try:
            from ..database.node_timing_repository import NodeTimingRepository
            timing_repo = NodeTimingRepository()
            
            # Create pipeline mapping
            pipeline_mapping = {}
            for node_name, timing_data in self._node_timings_json.items():
                if node_name in node_timings_dict:
                    pipeline_mapping[node_name] = timing_data.get("pipeline", "") or self._get_pipeline_info(node_name) or ""
            
            for node_name in node_timings_dict.keys():
                if node_name not in pipeline_mapping:
                    pipeline_mapping[node_name] = self._get_pipeline_info(node_name) or ""
            
            logger.info(f"🌐 [WS Manager] Saving {len(node_timings_dict)} node timings to database for query {query_id}")
            timing_repo.insert_batch_timings(
                query_id=query_id,
                query_text=self._query_text or "",
                node_timings=node_timings_dict,
                pipeline_mapping=pipeline_mapping,
                status=status,
            )
            logger.info(f"🌐 [WS Manager] Successfully saved {len(node_timings_dict)} node timings to database")
        except Exception as e:
            logger.warning(f"Failed to save node timings to database: {e}", exc_info=True)
    
    async def _save_token_usage_async(self, query_id: str) -> None:
        """Save token usage to database asynchronously."""
        try:
            if not self._token_usage_registry:
                return
                
            usage_records = self._token_usage_registry.get_all_records()
            if usage_records:
                from ..database.llm_usage_repository import LLMUsageRepository
                usage_repo = LLMUsageRepository()
                
                logger.info(f"🌐 [WS Manager] Saving {len(usage_records)} token usage records to database for query {query_id}")
                usage_repo.insert_batch_usage(
                    query_id=query_id,
                    query_text=self._query_text or "",
                    usage_records=usage_records,
                )
                logger.info(f"🌐 [WS Manager] Successfully saved {len(usage_records)} token usage records to database")
            else:
                logger.debug(f"🌐 [WS Manager] No token usage records to save for query {query_id}")
            
            # Clear registry after saving
            from ..llm.token_usage_registry import clear_token_usage_registry
            clear_token_usage_registry()
            self._token_usage_registry = None
        except Exception as e:
            logger.warning(f"Failed to save token usage to database: {e}", exc_info=True)
    
    async def start_keep_alive(self, interval_seconds: float = 25.0):
        """Start the keep-alive heartbeat task to prevent WebSocket disconnections.
        
        CRITICAL: This heartbeat MUST run continuously and independently of query execution.
        The heartbeat keeps the connection alive by sending messages every 25 seconds.
        
        Why 25 seconds:
        - Keeps connection alive during long queries (30-60+ minutes)
        - Prevents browser/proxy/Nginx from assuming connection is dead
        - Independent of query completion - runs until connection closes
        
        The heartbeat continues even while heavy query processing is happening.
        """
        if self._ping_task is not None:
            return
        
        async def ping_loop():
            consecutive_failures = 0
            max_failures = 10  # Increased from 3 to 10 - be more resilient to temporary failures
            last_successful_ping = datetime.now()
            
            try:
                while True:  # Continue until explicitly stopped
                    await asyncio.sleep(interval_seconds)
                    
                    # Check if we should stop (only if explicitly closed)
                    if self.is_closed:
                        logger.debug("🌐 [WS Manager] Heartbeat loop stopping - connection explicitly closed")
                        break
                    
                    # Don't check is_connected here - continue trying even if it's False
                    # The connection might recover
                    
                    try:
                        self._ping_count += 1
                        # Send heartbeat message - this keeps the connection alive
                        # CRITICAL: This message prevents the connection from going silent
                        # Browser/proxy/Nginx will close silent connections, so we must keep talking
                        # Use force_send=True to attempt sending even if state appears closed (allows recovery)
                        ping_sent = await self._send_raw({
                            "type": MessageType.PING,
                            "count": self._ping_count,
                            "message": "Connection alive - query processing...",
                            "timestamp": datetime.now().isoformat()
                        }, force_send=True)
                        if ping_sent:
                            consecutive_failures = 0  # Reset on successful ping
                            last_successful_ping = datetime.now()
                            self.is_connected = True  # Mark as connected on successful ping
                            # Log every 6th ping (every ~2.5 minutes) to show connection is alive
                            if self._ping_count % 6 == 0:
                                elapsed = (datetime.now() - self._start_time).total_seconds() if self._start_time else 0
                                logger.info(f"🌐 [WS Manager] Heartbeat #{self._ping_count} - connection alive ({elapsed:.0f}s elapsed)")
                        else:
                            # Log every failed heartbeat attempt (not just every 5th) for debugging
                            elapsed = (datetime.now() - self._start_time).total_seconds() if self._start_time else 0
                            logger.warning(f"🌐 [WS Manager] Heartbeat #{self._ping_count} FAILED to send ({elapsed:.0f}s elapsed) - connection may be closing")
                            consecutive_failures += 1
                            # Only log warnings, don't stop - keep trying
                            if consecutive_failures % 5 == 0:  # Log every 5th failure
                                logger.warning(f"🌐 [WS Manager] Heartbeat send failed ({consecutive_failures} consecutive failures) - continuing to retry...")
                    except WebSocketDisconnect:
                        # Client disconnected - stop heartbeat
                        logger.info("🌐 [WS Manager] Client disconnected - stopping heartbeat")
                        self.is_connected = False
                        break
                    except Exception as e:
                        consecutive_failures += 1
                        # Only stop if we've had many failures AND it's been a long time since last success
                        time_since_success = (datetime.now() - last_successful_ping).total_seconds()
                        if consecutive_failures >= max_failures and time_since_success > 300:  # 5 minutes
                            logger.error(f"🌐 [WS Manager] Heartbeat failed {consecutive_failures} times over {time_since_success:.0f}s - stopping heartbeat loop: {e}")
                            self.is_connected = False
                            break
                        elif consecutive_failures % 5 == 0:  # Log every 5th failure
                            logger.warning(f"🌐 [WS Manager] Heartbeat exception ({consecutive_failures} failures, {time_since_success:.0f}s since last success): {e} - continuing...")
            except asyncio.CancelledError:
                logger.debug(f"🌐 [WS Manager] Heartbeat task cancelled after {self._ping_count} heartbeats")
            except Exception as e:
                # Don't stop on outer exception - log and continue if possible
                logger.error(f"🌐 [WS Manager] Heartbeat loop outer exception: {e}", exc_info=True)
                # Only mark as disconnected if it's a critical error
                if "connection" in str(e).lower() or "closed" in str(e).lower():
                    self.is_connected = False
                # Continue the loop - don't break on outer exceptions
        
        self._ping_task = asyncio.create_task(ping_loop())
        logger.debug("🌐 [WS Manager] Keep-alive task started")
    
    async def stop_keep_alive(self):
        """Stop the keep-alive ping task."""
        if self._ping_task:
            self._ping_task.cancel()
            try:
                await self._ping_task
            except asyncio.CancelledError:
                pass
            self._ping_task = None
            logger.debug("🌐 [WS Manager] Keep-alive task stopped")
    
    def _prepare_for_json(self, data: Any) -> Any:
        """Recursively prepare data for JSON serialization."""
        import json
        
        if isinstance(data, dict):
            return {k: self._prepare_for_json(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._prepare_for_json(item) for item in data]
        elif isinstance(data, tuple):
            return [self._prepare_for_json(item) for item in data]
        elif isinstance(data, datetime):
            return data.isoformat()
        elif hasattr(data, 'isoformat'):  # date, time objects
            return data.isoformat()
        elif hasattr(data, 'item'):  # numpy types
            return data.item()
        elif hasattr(data, 'tolist'):  # numpy arrays
            return data.tolist()
        elif isinstance(data, (int, float, str, bool, type(None))):
            return data
        else:
            # Try to serialize, if fails convert to string
            try:
                json.dumps(data)
                return data
            except (TypeError, ValueError):
                return str(data)
    
    def _check_websocket_state(self) -> tuple[bool, str]:
        """Check the actual WebSocket connection state."""
        try:
            # Check WebSocket client state (if available)
            client_state = getattr(self.websocket, 'client_state', None)
            if client_state is not None:
                # WebSocket states: 0=CONNECTING, 1=OPEN, 2=CLOSING, 3=CLOSED
                if client_state.value == 3:  # CLOSED
                    return False, "WebSocket is CLOSED"
                elif client_state.value == 2:  # CLOSING
                    return False, "WebSocket is CLOSING"
                elif client_state.value != 1:  # Not OPEN
                    return False, f"WebSocket state is {client_state.name}"
        except Exception as e:
            logger.debug(f"🌐 [WS Manager] Could not check WebSocket state: {e}")
        
        # Fallback to our internal state
        if not self.is_connected:
            return False, "Internal state: not connected"
        if self.is_closed:
            return False, "Internal state: closed"
        
        return True, "OK"
    
    async def _send_raw(self, data: Dict[str, Any], force_send: bool = False) -> bool:
        """Send raw JSON data to the WebSocket.
        
        Args:
            data: Data to send
            force_send: If True, attempt to send even if connection state appears closed
                       (useful for heartbeat messages to recover connections)
        """
        # For heartbeat messages, be more lenient - try to send even if state appears closed
        # This allows recovery if the connection is still alive but state was incorrectly marked as closed
        if not force_send:
            # Check both internal state and WebSocket state
            ws_ok, ws_msg = self._check_websocket_state()
            if not ws_ok:
                logger.debug(f"🌐 [WS Manager] Cannot send - {ws_msg}")
                return False
        elif self.is_closed:
            # Don't try to send if explicitly closed
            return False
        
        try:
            # Prepare data to handle non-serializable types
            prepared_data = self._prepare_for_json(data)
            
            # Log message type and size for debugging
            msg_type = data.get("type", "unknown")
            import json
            json_str = json.dumps(prepared_data)
            msg_size = len(json_str)
            msg_size_kb = msg_size / 1024
            
            if msg_size > 100000:  # > 100KB
                logger.info(f"🌐 [WS Manager] Sending large {msg_type} message ({msg_size_kb:.1f}KB)")
            else:
                logger.debug(f"🌐 [WS Manager] Sending {msg_type} message ({msg_size_kb:.1f}KB)")
            
            # Send the JSON data
            await self.websocket.send_text(json_str)
            
            # For large messages, add a delay to ensure full transmission
            # Adaptive delays based on message size to prevent client disconnections
            if msg_size > 1000000:  # > 1MB
                import asyncio
                await asyncio.sleep(0.5)  # Longer delay for very large messages (>1MB)
                logger.debug(f"🌐 [WS Manager] Very large message ({msg_size_kb:.1f}KB) sent successfully")
            elif msg_size > 100000:  # > 100KB
                import asyncio
                await asyncio.sleep(0.3)  # Medium delay for large messages
                logger.debug(f"🌐 [WS Manager] Large message ({msg_size_kb:.1f}KB) sent successfully")
            elif msg_size > 10000:  # > 10KB
                import asyncio
                await asyncio.sleep(0.1)
                logger.debug(f"🌐 [WS Manager] Medium message sent successfully")
            
            # Mark as connected on successful send (especially important for force_send)
            if force_send:
                self.is_connected = True
            return True
        except WebSocketDisconnect:
            self.is_connected = False
            logger.info("🌐 [WS Manager] Client disconnected during send")
            return False
        except Exception as e:
            error_msg = str(e).lower()
            # Don't log 1005 errors (normal closure) as warnings
            if "1005" in str(e):
                self.is_connected = False
                logger.debug(f"🌐 [WS Manager] Connection closed normally: {e}")
            elif "1011" in str(e) or "timeout" in error_msg or "closed" in error_msg:
                self.is_connected = False
                logger.warning(f"🌐 [WS Manager] Connection closed during send: {e}")
            else:
                logger.warning(f"🌐 [WS Manager] Send failed: {e}")
            return False
    
    def _calculate_progress(self) -> int:
        """Calculate overall progress based on completed nodes."""
        # Weight calculation: main pipeline is required, then max of pipeline A/B
        main_completed = sum(1 for n in self.main_pipeline_nodes if n in self._completed_nodes)
        if not self.main_pipeline_nodes:
            return 0
        # Main graph only: progress = completed / total (cap at 95% until complete message)
        progress = int((main_completed / len(self.main_pipeline_nodes)) * 95)
        return min(progress, 95)
    
    def _get_pipeline_info(self, node_name: str) -> Optional[str]:
        """Get which pipeline a node belongs to."""
        if node_name in self.main_pipeline_nodes:
            return "main"
        elif node_name in self.pipeline_a_nodes:
            return "analysis"
        elif node_name in self.pipeline_b_nodes:
            return "visualization"
        return None
    
    async def send_node_start(self, node_name: str) -> bool:
        """Send a node start notification and track start time."""
        start_time = datetime.now()
        
        # Track in JSON structure for batch database update (single source of truth)
        pipeline = self._get_pipeline_info(node_name)
        self._node_timings_json[node_name] = {
            "node_name": node_name,
            "pipeline": pipeline or "",
            "start_time": start_time.isoformat(),
            "status": "running",
        }
        
        display_name = self._get_node_display_name(node_name)
        step_info = self._get_step_info()
        
        return await self._send_raw({
            "type": MessageType.NODE_START,
            "node": node_name,
            "message": display_name,
            "pipeline": pipeline,
            "progress": self._calculate_progress(),
            "timestamp": start_time.isoformat(),
            # Step progress info for frontend
            "step_index": step_info["completed_steps_count"] + 1,  # 1-based: we're starting this step
            "total_steps": step_info["total_steps"],
            "completed_steps_count": step_info["completed_steps_count"],
            "completed_steps": step_info["completed_steps"],
        })
    
    async def send_pipeline_start(self, pipeline_name: str, nodes: list) -> bool:
        """Send a pipeline start notification."""
        step_info = self._get_step_info()
        return await self._send_raw({
            "type": MessageType.PIPELINE_START,
            "pipeline": pipeline_name,
            "nodes": nodes,
            "progress": self._calculate_progress(),
            "timestamp": datetime.now().isoformat(),
            "step_index": step_info["completed_steps_count"] + 1,
            "total_steps": step_info["total_steps"],
            "completed_steps_count": step_info["completed_steps_count"],
            "completed_steps": step_info["completed_steps"],
        })
    
    async def send_progress(self, node_name: str, message: str, status: str = "processing", details: Optional[str] = None, data: Optional[Dict[str, Any]] = None) -> bool:
        """Send a progress update with optional step details.
        
        When a node completes, also sends the next node(s) as active/loading.
        """
        if status == "processing":
            self._completed_nodes.add(node_name)
            
            # Calculate and store node duration when node completes
            end_time = datetime.now()
            
            # Try to get ACTUAL start time from registry (recorded by node itself)
            actual_start_time = None
            if self._node_timing_registry:
                actual_start_time = self._node_timing_registry.get_node_start(node_name)
            
            # Fallback to estimated start time if registry doesn't have it (backward compatibility)
            if not actual_start_time and node_name in self._node_timings_json:
                start_time_str = self._node_timings_json[node_name].get("start_time")
                if start_time_str:
                    try:
                        actual_start_time = datetime.fromisoformat(start_time_str)
                    except (ValueError, TypeError):
                        pass
            
            if actual_start_time:
                duration = (end_time - actual_start_time).total_seconds()
                
                # Ensure duration is positive (should always be, but safety check)
                if duration < 0:
                    logger.warning(f"🌐 [WS Manager] Negative duration for {node_name}: {duration}s, using 0.001s")
                    duration = 0.001
                
                self._last_node_end_time = end_time  # Update last node end time
                
                # Update JSON structure for batch database update (single source of truth)
                if node_name in self._node_timings_json:
                    self._node_timings_json[node_name].update({
                        "duration_seconds": duration,
                        "start_time": actual_start_time.isoformat(),
                        "end_time": end_time.isoformat(),
                        "status": "completed",
                    })
                else:
                    # If node wasn't tracked in JSON (shouldn't happen, but handle gracefully)
                    pipeline = self._get_pipeline_info(node_name)
                    self._node_timings_json[node_name] = {
                        "node_name": node_name,
                        "pipeline": pipeline or "",
                        "start_time": actual_start_time.isoformat(),
                        "end_time": end_time.isoformat(),
                        "duration_seconds": duration,
                        "status": "completed",
                    }
                
                logger.debug(f"🌐 [WS Manager] Node {node_name} completed: {duration:.3f}s (start: {actual_start_time.isoformat()}, end: {end_time.isoformat()})")
        
        display_name = self._get_node_display_name(node_name, fallback=message)
        pipeline = self._get_pipeline_info(node_name)
        step_info = self._get_step_info()
        
        # Get duration if available
        duration = None
        if node_name in self._node_timings_json:
            duration = self._node_timings_json[node_name].get("duration_seconds")
        
        # Prepare the main progress message
        progress_data = {
            "type": MessageType.PROGRESS,
            "node": node_name,
            "message": display_name,
            "details": details,  # User-friendly summary of what was done
            "pipeline": pipeline,
            "status": status,
            "progress": self._calculate_progress(),
            "timestamp": datetime.now().isoformat(),
            # Step progress info for frontend
            "step_index": step_info["step_index"],
            "total_steps": step_info["total_steps"],
            "completed_steps_count": step_info["completed_steps_count"],
            "completed_steps": step_info["completed_steps"],
        }
        
        # Add duration if available
        if duration is not None:
            progress_data["duration"] = duration
        
        # Add structured data if provided
        if data is not None:
            progress_data["data"] = data
        
        # If node is completing, also send next node(s) as active
        if status == "processing":
            next_nodes = self._get_next_nodes(node_name)
            if next_nodes:
                progress_data["next_nodes"] = []
                for next_node in next_nodes:
                    next_display_name = self._get_node_display_name(next_node)
                    next_pipeline = self._get_pipeline_info(next_node)
                    progress_data["next_nodes"].append({
                        "node": next_node,
                        "message": next_display_name,
                        "pipeline": next_pipeline,
                        "status": "running",
                    })
        
        return await self._send_raw(progress_data)
    
    async def send_complete(self, data: Dict[str, Any]) -> bool:
        """Send the completion message with final response.
        
        NOTE: We use partial data messages (charts_ready, metrics_ready, intelligence_ready) as the main
        way to send data incrementally. The complete message now only sends status/metadata, not the full data.
        """
        import json
        import time
        
        total_duration = None
        if self._start_time:
            total_duration = (datetime.now() - self._start_time).total_seconds()
        
        # Save both node timings and token usage to database in batch (best-effort, non-blocking)
        # Note: Node timings are saved to database but not sent to frontend (available via API)
        # Run asynchronously to avoid blocking the completion message
        query_id = data.get("query_id") or self._query_id or ""
        if query_id:
            # Don't await - let it run in background so intelligence can be sent immediately
            asyncio.create_task(self._save_analytics_data(query_id, status="completed"))
        else:
            logger.warning(f"🌐 [WS Manager] No query_id available, skipping analytics data save")
        
        # COMMENTED OUT: We use partial data messages (charts_ready, metrics_ready, intelligence_ready) 
        # as the main way to send data incrementally. No need to send full data again in complete message.
        # All data (charts, metrics, intelligence) is already sent via partial messages.
        
        # # Create a copy of data to avoid modifying the original
        # complete_data = data.copy()
        # 
        # # Remove charts/metrics/intelligence if they were already sent via partial data
        # # This prevents duplicate data transmission
        # if self._charts_sent:
        #     if "charts" in complete_data:
        #         charts_count = len(complete_data.get("charts", [])) if isinstance(complete_data.get("charts"), list) else 0
        #         logger.info(f"🌐 [WS Manager] Charts already sent ({charts_count} charts), excluding from complete message")
        #         complete_data.pop("charts", None)
        # 
        # if self._metrics_sent:
        #     if "insights" in complete_data:
        #         insights_count = len(complete_data.get("insights", [])) if isinstance(complete_data.get("insights"), list) else 0
        #         logger.info(f"🌐 [WS Manager] Metrics/insights already sent ({insights_count} insights), excluding from complete message")
        #         complete_data.pop("insights", None)
        #     if "statistics" in complete_data:
        #         stats_count = len(complete_data.get("statistics", {})) if isinstance(complete_data.get("statistics"), dict) else 0
        #         logger.info(f"🌐 [WS Manager] Statistics already sent ({stats_count} stats), excluding from complete message")
        #         complete_data.pop("statistics", None)
        #     if "computation_results" in complete_data:
        #         logger.info(f"🌐 [WS Manager] Computation results already sent, excluding from complete message")
        #         complete_data.pop("computation_results", None)
        # 
        # if self._intelligence_sent:
        #     if "intelligence_analysis" in complete_data:
        #         logger.info(f"🌐 [WS Manager] Intelligence analysis already sent, excluding from complete message")
        #         complete_data.pop("intelligence_analysis", None)
        
        step_info = self._get_step_info()
        # Send only completion status/metadata, not the full data (already sent via partial messages)
        # Include flags to inform frontend which partial data was already sent (should NOT be overwritten)
        complete_msg = {
            "type": MessageType.COMPLETE,
            "progress": 100,
            "data": {
                "query": data.get("query", ""),
                "status": data.get("status", "completed"),
                "query_id": query_id,
                # Only include source_data if it's small (large source_data is sent via data_chunk messages)
                "source_data": data.get("source_data") if data.get("source_data") and len(str(data.get("source_data"))) < 1000 else None,
            },
            # IMPORTANT: Flags to inform frontend which data was already sent via partial updates
            # Frontend should NOT overwrite/clear this data when receiving the complete message
            "partial_data_sent": {
                "charts": self._charts_sent,
                "metrics": self._metrics_sent,  # computation_results, statistics
                "summary": self._summary_sent,  # insights, analysis_summary
                "intelligence": self._intelligence_sent,
            },
            "total_duration": total_duration,
            "timestamp": datetime.now().isoformat(),
            # Step progress - all steps done
            "step_index": step_info["total_steps"],
            "total_steps": step_info["total_steps"],
            "completed_steps_count": step_info["completed_steps_count"],
            "completed_steps": step_info["completed_steps"],
        }
        
        # Log partial data flags
        logger.info(f"🌐 [WS Manager] Partial data flags: charts={self._charts_sent}, metrics={self._metrics_sent}, summary={self._summary_sent}, intelligence={self._intelligence_sent}")
        
        # Log timing for serialization and sending
        start_time = time.time()
        prepared_data = self._prepare_for_json(complete_msg)
        prep_time = time.time() - start_time
        
        start_time = time.time()
        json_str = json.dumps(prepared_data)
        serialize_time = time.time() - start_time
        
        msg_size_kb = len(json_str) / 1024
        logger.info(f"🌐 [WS Manager] Complete message (status only): {msg_size_kb:.1f}KB (prep: {prep_time:.2f}s, serialize: {serialize_time:.2f}s)")
        
        # Check actual WebSocket state (not just internal flags) - catches Nginx closing connection
        ws_ok, ws_msg = self._check_websocket_state()
        if not ws_ok:
            logger.warning(f"🌐 [WS Manager] Cannot send complete - {ws_msg} (internal: connected={self.is_connected}, closed={self.is_closed})")
            # Update internal state to match actual WebSocket state
            self.is_connected = False
            return False
        
        try:
            start_time = time.time()
            await self.websocket.send_text(json_str)
            send_time = time.time() - start_time
            logger.info(f"🌐 [WS Manager] Complete message (status only) sent in {send_time:.2f}s")
            return True
        except Exception as e:
            logger.error(f"🌐 [WS Manager] Failed to send complete message: {e}")
            # Update state on error
            self.is_connected = False
            # Verify WebSocket is actually closed
            try:
                ws_ok, ws_msg = self._check_websocket_state()
                if not ws_ok:
                    logger.debug(f"🌐 [WS Manager] WebSocket state after send error: {ws_msg}")
            except:
                pass
            return False
    
    async def send_partial_data(self, data_type: str, data: Dict[str, Any]) -> bool:
        """Send partial data as it becomes available (charts, metrics, intelligence).
        
        Args:
            data_type: Type of partial data - 'charts', 'metrics', or 'intelligence'
            data: Partial data dictionary to send
            
        Returns:
            True if sent successfully, False otherwise
        """
        import json
        import time
        
        # Map data_type to message type
        message_type_map = {
            'charts': MessageType.CHARTS_READY,
            'metrics': MessageType.METRICS_READY,
            'gantt': MessageType.GANTT_READY,
            'summary': MessageType.SUMMARY_READY,
            'insights': MessageType.SUMMARY_READY,  # Alias for summary
            'normal_text': MessageType.NORMAL_TEXT,  # Clarification and simple message-only (display as normal text)
            'intelligence': MessageType.INTELLIGENCE_READY,
            'intelligence_phase1': MessageType.INTELLIGENCE_PHASE1_READY,  # Partial update once after Phase 1
            'steps': MessageType.STEPS_UPDATE,  # Processing nodes / step progress (partial update)
        }
        
        message_type = message_type_map.get(data_type)
        if not message_type:
            logger.error(f"🌐 [WS Manager] Unknown partial data type: {data_type}")
            return False
        
        # Check WebSocket state
        ws_ok, ws_msg = self._check_websocket_state()
        if not ws_ok:
            logger.warning(f"🌐 [WS Manager] Cannot send {data_type} - {ws_msg}")
            self.is_connected = False
            return False
        
        # Log what we're sending
        source_data = data.get("source_data")
        source_data_tables = len(source_data) if source_data and isinstance(source_data, dict) else 0
        
        if data_type == 'charts':
            charts_count = len(data.get("charts", [])) if isinstance(data.get("charts"), list) else 0
            logger.info(f"🌐 [WS Manager] Preparing to send {charts_count} charts to frontend" + (f" with {source_data_tables} source_data tables" if source_data_tables > 0 else ""))
        elif data_type == 'metrics':
            stats_count = len(data.get("statistics", {})) if isinstance(data.get("statistics"), dict) else 0
            results_count = len(data.get("computation_results", [])) if isinstance(data.get("computation_results"), list) else 0
            logger.info(f"🌐 [WS Manager] Preparing to send metrics: {results_count} computation results, {stats_count} statistics" + (f" with {source_data_tables} source_data tables" if source_data_tables > 0 else ""))
        elif data_type == 'gantt':
            gantt_data = data.get("gantt_data") or {}
            machines_count = len(gantt_data.get("machines", [])) if isinstance(gantt_data.get("machines"), list) else 0
            logger.info(f"🌐 [WS Manager] Preparing to send gantt: {machines_count} machine(s)" + (f" with {source_data_tables} source_data tables" if source_data_tables > 0 else ""))
        elif data_type in ('summary', 'insights'):
            insights_count = len(data.get("insights", [])) if isinstance(data.get("insights"), list) else 0
            summary_present = bool(data.get("analysis_summary"))
            logger.info(f"🌐 [WS Manager] Preparing to send summary: {insights_count} insights, summary present: {summary_present}" + (f" with {source_data_tables} source_data tables" if source_data_tables > 0 else ""))
        elif data_type == 'normal_text':
            normal_len = len(data.get("normal_text", "") or "")
            logger.info(f"🌐 [WS Manager] Preparing to send normal_text: {normal_len} chars" + (f" with {source_data_tables} source_data tables" if source_data_tables > 0 else ""))
        elif data_type == 'intelligence':
            logger.info(f"🌐 [WS Manager] Preparing to send intelligence analysis" + (f" with {source_data_tables} source_data tables" if source_data_tables > 0 else ""))
        elif data_type == 'intelligence_phase1':
            logger.info(f"🌐 [WS Manager] Preparing to send intelligence Phase 1 partial (levels 1–2, once after Phase 1)")
        elif data_type == 'steps':
            step_idx = data.get("step_index")
            total = data.get("total_steps")
            logger.info(f"🌐 [WS Manager] Preparing to send steps update: step {step_idx}/{total}" + (f" (node: {data.get('current_node', '')})" if data.get("current_node") else ""))
        
        # Add timeout protection for large messages
        import asyncio
        
        step_info = self._get_step_info()
        partial_msg = {
            "type": message_type,
            "data": data,
            "timestamp": datetime.now().isoformat(),
            # Step progress for frontend (charts/metrics/intelligence ready = major milestones)
            "step_index": step_info["step_index"],
            "total_steps": step_info["total_steps"],
            "completed_steps_count": step_info["completed_steps_count"],
            "completed_steps": step_info["completed_steps"],
        }
        
        try:
            import asyncio
            start_time = time.time()
            prepared_data = self._prepare_for_json(partial_msg)
            json_str = json.dumps(prepared_data)
            msg_size_kb = len(json_str) / 1024
            msg_size_mb = msg_size_kb / 1024
            
            # Warn if message is very large
            if msg_size_mb > 5.0:
                logger.warning(f"🌐 [WS Manager] ⚠️ Large message detected ({msg_size_mb:.2f}MB) - may take time to send")
            
            # Send with timeout to prevent hanging
            try:
                await asyncio.wait_for(
                    self.websocket.send_text(json_str),
                    timeout=30.0  # 30 second timeout
                )
            except asyncio.TimeoutError:
                logger.error(f"🌐 [WS Manager] ⚠️ Timeout sending {data_type} message ({msg_size_mb:.2f}MB) - message too large or connection slow")
                self.is_connected = False
                return False
            
            send_time = time.time() - start_time
            
            # Mark this data type as sent (steps_update is not tracked as a "slot", no duplicate check)
            if data_type == 'charts':
                self._charts_sent = True
            elif data_type == 'metrics':
                self._metrics_sent = True
            elif data_type in ('summary', 'insights'):
                self._summary_sent = True
            elif data_type == 'normal_text':
                self._summary_sent = True  # Normal text uses same UI slot as summary
            elif data_type == 'intelligence':
                self._intelligence_sent = True
            # data_type == 'steps' does not set any _*_sent flag
            
            logger.info(f"🌐 [WS Manager] ✅ {data_type.capitalize()} message sent successfully ({msg_size_kb:.1f}KB, {send_time:.2f}s)")
            return True
        except Exception as e:
            logger.error(f"🌐 [WS Manager] ❌ Failed to send {data_type} message: {e}", exc_info=True)
            self.is_connected = False
            return False
    
    async def send_source_data_chunks(self, source_data: Dict[str, Any], max_chunk_size: int = 78643200) -> bool:
        """Send source_data in chunks - splitting large tables when needed.

        Tables smaller than max_chunk_size are sent as complete units.
        Tables larger than max_chunk_size are split into smaller chunks.
        This ensures data integrity and prevents browser memory issues.

        Args:
            source_data: Dictionary of table_name -> list of records
            max_chunk_size: Maximum size in bytes for each chunk (default: 75MB)

        Returns:
            True if all chunks were sent successfully, False otherwise
        """
        if not source_data:
            return True

        # IMPORTANT:
        # We intentionally do NOT stream raw table data over WebSocket by default.
        # Clients should fetch table data via the export endpoints on-demand.
        # To re-enable chunk streaming for debugging/experiments, set:
        #   WS_SEND_SOURCE_DATA_CHUNKS=true
        import os
        send_chunks = os.getenv("WS_SEND_SOURCE_DATA_CHUNKS", "false").strip().lower() in ("1", "true", "yes", "y", "on")
        if not send_chunks:
            table_count = len(source_data) if isinstance(source_data, dict) else 0
            try:
                total_rows = sum(len(records) for records in source_data.values()) if isinstance(source_data, dict) else 0
            except Exception:
                total_rows = 0
            logger.info(
                "🌐 [WS Manager] Skipping source_data chunk streaming over WebSocket "
                f"(tables={table_count}, rows={total_rows:,}). "
                "Use export endpoints to fetch raw table data."
            )
            return True
            
        import json
        from datetime import date, datetime
        
        # Helper function to serialize with date handling
        def _json_serialize_with_dates(obj):
            def default_serializer(o):
                if isinstance(o, (date, datetime)):
                    return o.isoformat()
                elif hasattr(o, 'isoformat'):
                    return o.isoformat()
                elif hasattr(o, 'item'):
                    return o.item()
                elif hasattr(o, 'tolist'):
                    return o.tolist()
                else:
                    return str(o)
            return json.dumps(obj, default=default_serializer)
        
        # Verify connection is still open before starting
        ws_ok, ws_msg = self._check_websocket_state()
        if not ws_ok:
            logger.warning(f"🌐 [WS Manager] Cannot send chunks - {ws_msg}")
            return False
        
        # Calculate total size for logging
        prepared_data = self._prepare_for_json(source_data)
        total_size = len(_json_serialize_with_dates(prepared_data))
        total_tables = len(source_data)
        
        logger.info(f"🌐 [WS Manager] Sending source_data: {total_tables} table(s) (total: {total_size / (1024*1024):.1f}MB)")
        
        # Calculate total chunks needed (some tables may need multiple chunks)
        total_chunks = 0
        chunk_plan = []  # List of (table_name, start_index, end_index, chunk_size)

        for table_name, table_records in source_data.items():
            table_data = {table_name: table_records}
            prepared_table_data = self._prepare_for_json(table_data)
            table_size = len(_json_serialize_with_dates(prepared_table_data))

            if table_size <= max_chunk_size:
                # Table fits in one chunk
                total_chunks += 1
                chunk_plan.append((table_name, 0, len(table_records), table_size))
            else:
                # Table needs to be split - estimate chunks needed
                # Split records into roughly equal-sized chunks
                records_per_chunk = max(1, len(table_records) // ((table_size // max_chunk_size) + 1))
                num_chunks_for_table = (len(table_records) + records_per_chunk - 1) // records_per_chunk

                for i in range(num_chunks_for_table):
                    start_idx = i * records_per_chunk
                    end_idx = min((i + 1) * records_per_chunk, len(table_records))

                    # Create a subset for size estimation
                    subset_data = {table_name: table_records[start_idx:end_idx]}
                    prepared_subset = self._prepare_for_json(subset_data)
                    subset_size = len(_json_serialize_with_dates(prepared_subset))

                    total_chunks += 1
                    chunk_plan.append((table_name, start_idx, end_idx, subset_size))

        logger.info(f"🌐 [WS Manager] Will send {total_chunks} chunks total across {total_tables} tables")

        # Send all chunks
        chunks_sent = 0
        for table_name, start_idx, end_idx, estimated_size in chunk_plan:
            # Check connection state with detailed logging
            ws_ok, ws_msg = self._check_websocket_state()
            if not ws_ok:
                logger.warning(f"🌐 [WS Manager] {ws_msg} while sending chunks (sent {chunks_sent}/{total_chunks})")
                return False

            # Get the table records
            table_records = source_data[table_name]
            chunk_records = table_records[start_idx:end_idx]

            # Prepare chunk data
            chunk_data = {table_name: chunk_records}
            is_last_chunk = (chunks_sent == total_chunks - 1)

            # Add chunk metadata for split tables
            chunk_info = ""
            if end_idx - start_idx < len(table_records):
                # This table was split
                chunk_info = f" (records {start_idx}-{end_idx-1} of {len(table_records)})"

            size_display = f"{estimated_size / (1024*1024):.1f}MB" if estimated_size > 1024*1024 else f"{estimated_size / 1024:.1f}KB"
            logger.info(f"🌐 [WS Manager] Sending chunk {chunks_sent + 1}/{total_chunks}: table '{table_name}' ({size_display}){chunk_info}")

            step_info = self._get_step_info()
            chunk_sent = await self._send_raw({
                "type": MessageType.DATA_CHUNK,
                "chunk_index": chunks_sent,
                "total_chunks": total_chunks,
                "table_name": table_name,
                "record_chunk_index": chunks_sent if end_idx - start_idx < len(table_records) else None,
                "total_record_chunks": len([p for p in chunk_plan if p[0] == table_name]) if end_idx - start_idx < len(table_records) else None,
                "data": chunk_data,
                "is_complete": is_last_chunk,
                "timestamp": datetime.now().isoformat(),
                "step_index": step_info["step_index"],
                "total_steps": step_info["total_steps"],
                "completed_steps_count": step_info["completed_steps_count"],
                "completed_steps": step_info["completed_steps"],
            })

            if not chunk_sent:
                # Check if client disconnected
                if not self.is_connected or self.is_closed:
                    logger.warning(f"🌐 [WS Manager] Client disconnected while sending chunk {chunks_sent + 1}/{total_chunks} for table '{table_name}'")
                    logger.warning(f"🌐 [WS Manager] Successfully sent {chunks_sent}/{total_chunks} chunks before disconnection")
                    return False
                else:
                    logger.warning(
                        f"🌐 [WS Manager] Failed to send chunk {chunks_sent + 1}/{total_chunks} for table '{table_name}' "
                        f"(is_connected={self.is_connected}, is_closed={self.is_closed})"
                    )
                    return False

            chunks_sent += 1
            # Adaptive delay between chunks based on size to prevent overwhelming the connection
            # Larger chunks need more time to be processed by the client
            if estimated_size > 500000:  # > 500KB
                await asyncio.sleep(0.3)  # Longer delay for large chunks
            elif estimated_size > 100000:  # > 100KB
                await asyncio.sleep(0.2)  # Medium delay
            else:
                await asyncio.sleep(0.1)  # Short delay for small chunks
        
        # Send completion message
        step_info = self._get_step_info()
        return await self._send_raw({
            "type": MessageType.DATA_COMPLETE,
            "total_chunks_sent": chunks_sent,
            "total_tables": total_tables,
            "timestamp": datetime.now().isoformat(),
            "step_index": step_info["step_index"],
            "total_steps": step_info["total_steps"],
            "completed_steps_count": step_info["completed_steps_count"],
            "completed_steps": step_info["completed_steps"],
        })
    
    async def send_error(
        self, 
        message: str, 
        details: Optional[str] = None,
        error_code: Optional[str] = None,
        error_category: Optional[str] = None
    ) -> bool:
        """
        Send an error message with structured error information.
        
        Args:
            message: User-friendly error message
            details: Optional detailed error information for debugging
            error_code: Optional error code for programmatic handling (e.g., "REFRESH_TOKEN_EXPIRED")
            error_category: Optional error category (e.g., "AUTHENTICATION", "DATA_SOURCE", "VALIDATION")
        """
        # Save analytics data even on error (best-effort, non-blocking)
        query_id = self._query_id or ""
        if query_id:
            logger.info(f"🌐 [WS Manager] Query failed, saving analytics data before error response")
            # Don't await - let it run in background
            import asyncio
            asyncio.create_task(self._save_analytics_data(query_id, status="failed"))
        
        step_info = self._get_step_info()
        error_payload = {
            "type": MessageType.ERROR,
            "message": message,
            "progress": self._calculate_progress(),
            "timestamp": datetime.now().isoformat(),
            "step_index": step_info["step_index"],
            "total_steps": step_info["total_steps"],
            "completed_steps_count": step_info["completed_steps_count"],
            "completed_steps": step_info["completed_steps"],
        }
        
        # Add optional fields if provided
        if details:
            error_payload["details"] = details
        if error_code:
            error_payload["error_code"] = error_code
        if error_category:
            error_payload["error_category"] = error_category
        
        return await self._send_raw(error_payload)
    
    async def send_llm_stream_chunk(self, text: str, node_name: str = "", chunk_index: int = 0) -> bool:
        """Send a single LLM text chunk for real-time streaming display in the UI."""
        return await self._send_raw({
            "type": MessageType.LLM_STREAM,
            "text": text,
            "node": node_name,
            "chunk_index": chunk_index,
            "timestamp": datetime.now().isoformat(),
        })

    async def send_llm_stream_end(self, node_name: str = "", full_text: str = "") -> bool:
        """Signal that LLM streaming is complete. Includes the full accumulated text for reconciliation."""
        return await self._send_raw({
            "type": MessageType.LLM_STREAM_END,
            "node": node_name,
            "full_text": full_text,
            "timestamp": datetime.now().isoformat(),
        })

    async def send_llm_thinking_chunk(self, text: str, node_name: str = "", chunk_index: int = 0) -> bool:
        """Send a single LLM thinking/reasoning chunk for real-time display."""
        return await self._send_raw({
            "type": MessageType.LLM_THINKING_STREAM,
            "text": text,
            "node": node_name,
            "chunk_index": chunk_index,
            "timestamp": datetime.now().isoformat(),
        })

    async def send_llm_thinking_end(self, node_name: str = "", full_thinking: str = "") -> bool:
        """Signal that LLM thinking/reasoning stream is complete."""
        return await self._send_raw({
            "type": MessageType.LLM_THINKING_END,
            "node": node_name,
            "full_thinking": full_thinking,
            "timestamp": datetime.now().isoformat(),
        })

    async def stream_text_simulated(self, text: str, node_name: str = "", words_per_chunk: int = 3, delay_seconds: float = 0.03) -> bool:
        """
        Simulated streaming: send already-available text word-by-word over WebSocket.
        Gives the user a typewriter effect without changing any LLM call logic.
        
        Args:
            text: Full text to stream (e.g. summary_text from analytical_summary)
            node_name: Source node for the UI to identify the stream
            words_per_chunk: Number of words to send per chunk
            delay_seconds: Delay between chunks (controls typing speed)
        """
        if not text or not text.strip():
            return True
        
        words = text.split()
        if not words:
            return True
        
        chunk_index = 0
        for i in range(0, len(words), words_per_chunk):
            word_group = " ".join(words[i:i + words_per_chunk])
            if i > 0:
                word_group = " " + word_group
            sent = await self.send_llm_stream_chunk(word_group, node_name, chunk_index)
            if not sent:
                logger.warning(f"🌐 [WS Manager] Stream interrupted at chunk {chunk_index} for {node_name}")
                return False
            chunk_index += 1
            await asyncio.sleep(delay_seconds)
        
        await self.send_llm_stream_end(node_name, text)
        logger.info(f"🌐 [WS Manager] ✅ Simulated stream complete for {node_name}: {len(words)} words in {chunk_index} chunks")
        return True

    async def stream_from_llm_generator(self, async_generator, node_name: str = "") -> str:
        """
        Stream real LLM tokens from an async generator to the WebSocket.
        Returns the full accumulated text.
        
        Usage:
            gen = llm_client.stream_llm_response(model=..., system_prompt=..., user_prompt=...)
            full_text = await ws_manager.stream_from_llm_generator(gen, node_name="analytical_summary")
        """
        full_text = ""
        chunk_index = 0
        try:
            async for chunk in async_generator:
                full_text += chunk
                await self.send_llm_stream_chunk(chunk, node_name, chunk_index)
                chunk_index += 1
        except Exception as e:
            logger.error(f"🌐 [WS Manager] Error during LLM streaming for {node_name}: {e}", exc_info=True)
        finally:
            await self.send_llm_stream_end(node_name, full_text)
            logger.info(f"🌐 [WS Manager] ✅ LLM stream complete for {node_name}: {len(full_text)} chars in {chunk_index} chunks")
        return full_text

    async def close(self):
        """Gracefully close the WebSocket connection."""
        if self.is_closed:
            return
        
        # Save analytics data before closing (in case it wasn't saved earlier)
        # Run asynchronously but wait briefly to ensure it starts
        query_id = self._query_id or ""
        if query_id and (self._node_timings_json or (self._token_usage_registry and self._token_usage_registry.get_all_records())):
            logger.info(f"🌐 [WS Manager] Saving analytics data before connection close")
            # Don't await - let it run in background, connection will close anyway
            import asyncio
            try:
                asyncio.create_task(self._save_analytics_data(query_id, status="completed"))
            except Exception as e:
                logger.warning(f"Failed to schedule analytics save on close: {e}")
        
        self.is_closed = True
        await self.stop_keep_alive()
        
        if self.is_connected:
            try:
                await self.websocket.close()
                logger.info("🌐 [WS Manager] Connection closed gracefully")
            except Exception as e:
                logger.debug(f"🌐 [WS Manager] Error during close: {e}")
        
        self.is_connected = False
        
        # Clear all registries
        from ..llm.token_usage_registry import clear_token_usage_registry
        clear_token_usage_registry()
        from ..langgraph.node_timing_registry import clear_node_timing_registry
        clear_node_timing_registry()
        from .ws_streaming_registry import clear_ws_streaming_manager
        clear_ws_streaming_manager()
    
    def create_progress_callback(self) -> Callable:
        """Create a progress callback function for the LangGraph executor."""
        _started_nodes = set()  # Track which nodes have started
        _node_start_times: Dict[str, datetime] = {}  # Track actual start times from LangGraph
        
        async def callback(node_name: str, status: str, message: str, details: Optional[str] = None):
            if not self.is_connected or self.is_closed:
                return
            
            # Track pipeline starts
            if node_name == "pipeline_a" and status == "starting":
                await self.send_pipeline_start("analysis", self.pipeline_a_nodes)
            elif node_name == "pipeline_b" and status == "starting":
                await self.send_pipeline_start("visualization", self.pipeline_b_nodes)
            else:
                # Complete "initializing" node when first real node starts
                if "initializing" in self._node_timings_json and "initializing" not in self._completed_nodes:
                    init_end = datetime.now()
                    init_start_str = self._node_timings_json["initializing"].get("start_time")
                    if init_start_str:
                        try:
                            init_start = datetime.fromisoformat(init_start_str)
                            init_duration = (init_end - init_start).total_seconds()
                            self._last_node_end_time = init_end

                            self._node_timings_json["initializing"].update({
                                "duration_seconds": init_duration,
                                "end_time": init_end.isoformat(),
                                "status": "completed",
                            })
                            self._completed_nodes.add("initializing")
                            logger.debug(f"🌐 [WS Manager] Initializing node completed: {init_duration:.3f}s")
                        except Exception as e:
                            logger.warning(f"🌐 [WS Manager] Failed to parse initializing node timing: {e}")
                            self._completed_nodes.add("initializing")
                
                # For regular nodes, track start time when we first see them
                # Since LangGraph callback is called AFTER node completes, we use the previous node's end time
                # as the start time for sequential nodes, or current time minus a small buffer for parallel nodes
                if node_name not in _started_nodes and node_name not in ["pipeline_a", "pipeline_b"]:
                    _started_nodes.add(node_name)
                    
                    # Use last node's end time as start time (for sequential execution)
                    # Or use a small time before now if this is a parallel node
                    if self._last_node_end_time:
                        start_time = self._last_node_end_time
                    else:
                        # Fallback: use current time minus a small buffer (node just completed)
                        # This is not perfect but better than using completion time
                        from datetime import timedelta
                        start_time = datetime.now() - timedelta(seconds=0.1)
                    
                    _node_start_times[node_name] = start_time
                    
                    # Track in our timing structure (single source of truth)
                    pipeline = self._get_pipeline_info(node_name)
                    self._node_timings_json[node_name] = {
                        "node_name": node_name,
                        "pipeline": pipeline or "",
                        "start_time": start_time.isoformat(),
                        "status": "running",
                    }
                    
                    logger.debug(f"🌐 [WS Manager] Tracking node {node_name} start: {start_time.isoformat()}")
                    
                    # Send node_start notification (with step info)
                    display_name = self._get_node_display_name(node_name)
                    step_info = self._get_step_info()
                    await self._send_raw({
                        "type": MessageType.NODE_START,
                        "node": node_name,
                        "message": display_name,
                        "pipeline": pipeline,
                        "progress": self._calculate_progress(),
                        "timestamp": start_time.isoformat(),
                        "step_index": step_info["completed_steps_count"] + 1,
                        "total_steps": step_info["total_steps"],
                        "completed_steps_count": step_info["completed_steps_count"],
                        "completed_steps": step_info["completed_steps"],
                    })
                
                # Send progress update (which also marks node as completed and calculates duration)
                await self.send_progress(node_name, message, status, details)
                
                # Send processing nodes / step progress as a partial update (frontend stepper)
                # Include next_nodes (up to 2 for parallel flow) so UI doesn't stick waiting
                step_info = self._get_step_info()
                display_name = self._get_node_display_name(node_name, fallback=message)
                next_node_names = self._get_next_nodes(node_name)
                next_nodes_list = []
                for next_node in (next_node_names or [])[:2]:  # Keep next 2 nodes for each update (parallel flow)
                    next_display_name = self._get_node_display_name(next_node)
                    next_pipeline = self._get_pipeline_info(next_node)
                    next_nodes_list.append({
                        "node": next_node,
                        "message": next_display_name,
                        "pipeline": next_pipeline,
                        "status": "pending",
                    })
                steps_data = {
                    "step_index": step_info["step_index"],
                    "total_steps": step_info["total_steps"],
                    "completed_steps_count": step_info["completed_steps_count"],
                    "completed_steps": step_info["completed_steps"],
                    "current_node": node_name,
                    "message": message or display_name,
                    "status": status,
                    "next_nodes": next_nodes_list,
                }
                try:
                    await self.send_partial_data("steps", steps_data)
                except Exception as e:
                    logger.warning(f"🌐 [WS Manager] Failed to send steps partial: {e}")
        
        return callback
