"""Main LangGraph workflow orchestrator."""
from typing import Dict, Any
import logging
from datetime import datetime
from .data_analysis_graph import DataAnalysisGraphBuilder

logger = logging.getLogger(__name__)


class AnalyticsGraphBuilder:
    """Orchestrator for Data Analysis LangGraph workflow."""
    
    def __init__(self):
        """Initialize the graph builder."""
        self.data_analysis_graph = DataAnalysisGraphBuilder()
        logger.info("Analytics Graph Builder initialized")
    
    async def execute(self, initial_state: Dict[str, Any], progress_callback=None, ws_manager=None) -> Dict[str, Any]:
        """
        Execute the analytics workflow:
        Query Analysis → Select Tables → Get Schema → SQL Plan → SQL Generation → Data Fetch → Chart Generation

        Args:
            initial_state: Initial state dictionary
            progress_callback: Optional callback function(node_name, status, message) for progress updates
            ws_manager: Optional WebSocket manager for sending incremental updates

        Returns:
            Final state dictionary with SQL and charts
        """
        logger.info("🎯 [ORCHESTRATOR] Starting Analytics Orchestrator")
        logger.info(f"🎯 [ORCHESTRATOR] Processing query: '{initial_state.get('user_query', '')[:80]}{'...' if len(initial_state.get('user_query', '')) > 80 else ''}'")

        start_time = datetime.now()
        try:
            # Execute Data Analysis Pipeline (includes chart generation)
            # Pass ws_manager to enable incremental data sending
            final_state = await self.data_analysis_graph.execute(initial_state, progress_callback, ws_manager)

            duration = (datetime.now() - start_time).total_seconds()
            has_sql = final_state.get("generated_sql") is not None
            
            # Get chart count from the correct location (prepared_charts or dashboard_response)
            prepared_charts = final_state.get("prepared_charts", [])
            dashboard_response = final_state.get("dashboard_response", {})
            dashboard_charts = dashboard_response.get("charts", []) if isinstance(dashboard_response, dict) else []
            chart_count = len(prepared_charts) if prepared_charts else len(dashboard_charts)
            
            # Get status from dashboard_response if top-level status is None
            status_val = final_state.get("status")
            if not status_val and dashboard_response:
                dashboard_status = dashboard_response.get("status") if isinstance(dashboard_response, dict) else None
                status_val = dashboard_status or "success"
            else:
                status_val = status_val or "success"
            
            raw_dataframes = final_state.get("raw_dataframes", {})
            data_fetch_status = final_state.get("data_fetch_status") or {}
            # Prefer total_actual_rows from fetch when available (consistent with metrics computed from same fetch)
            data_rows = data_fetch_status.get("total_actual_rows") or 0
            if data_rows == 0 and raw_dataframes:
                def get_row_count(df):
                    if df is None:
                        return 0
                    if hasattr(df, "height"):  # Polars DataFrame (collected)
                        return df.height
                    if hasattr(df, "__len__"):
                        return len(df)
                    return 0
                data_rows = sum(get_row_count(df) for df in raw_dataframes.values())

            logger.info("=" * 80)
            logger.info("🎯 [ORCHESTRATOR] Analytics Workflow Complete")
            logger.info("=" * 80)
            logger.info(f"Total Duration: {duration:.2f}s")
            logger.info(f"Final Status: {status_val}")
            logger.info(f"SQL Generated: {'✅' if has_sql else '❌'}")
            logger.info(f"Data Rows Fetched: {data_rows}")
            logger.info(f"Charts Generated: {chart_count}")
            logger.info("=" * 80)

            return final_state

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"❌ [ORCHESTRATOR] Workflow execution failed after {duration:.2f}s")
            logger.error(f"❌ [ORCHESTRATOR] Error: {str(e)}")
            raise

