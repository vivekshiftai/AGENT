"""SAP data fetch node — delegates to relational or analytical fetch services.

- Analytical path: when analytical_fetch_instructions are present, calls
  sap_analytical_fetch_service.execute_analytical_fetch (no $count, $top/$skip pagination).
- Relational path: builds OData queries from plan and calls
  sap_relational_fetch_service.fetch_view_data per view (count-first + parallel chunks).
"""
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

import polars as pl

from ..state import AnalyticsState
from ..utils.sap_fetch_helpers import (
    api_urls_to_generated_queries,
    build_date_filter_expression,
    clean_odata_select,
    filter_columns_for_api_call,
    get_allowed_columns_for_view,
)
from ...services.datasphere_service import get_datasphere_service
from ...services.sap_relational_fetch_service import fetch_view_data as relational_fetch_view_data
from ...services.sap_analytical_fetch_service import execute_analytical_fetch

logger = logging.getLogger(__name__)


async def sap_data_fetch_simple_node(state: AnalyticsState) -> Dict[str, Any]:
    """SAP data fetch node — supports both relational and analytical views.
    
    For analytical views (when analytical_fetch_instructions are present):
    - Uses direct $select/$filter/$orderby without $count (analytical views 
      don't support $count)
    - Each instruction fetches one dimension + measures per API call
    - Results are tagged with dimension suffixes (e.g. ViewName__by_Plant)
    
    For relational views (standard path):
    - Uses count-first + parallel $top/$skip chunking
    - Results keyed by view name
    
    Args:
        state: Current analytics state
            
    Returns:
        Updated state with raw_dataframes
    """
    start_time = datetime.now()
    node_name = "sap_data_fetch_simple"
    
    logger.info(f"[{node_name}] ========== Starting SAP Data Fetch ==========")
    
    # Avoid double fetch when invoked from both analytical_fetch_plan and operation_specification
    if state.get("_analytical_fetch_triggered") and state.get("raw_dataframes"):
        logger.info(f"[{node_name}] Already fetched (raw_dataframes present); skipping duplicate invocation")
        return {}
    
    try:
        # ── Check for analytical fetch instructions (dimension-based fetch) ──
        analytical_instructions = state.get("analytical_fetch_instructions")
        if analytical_instructions and isinstance(analytical_instructions, list) and len(analytical_instructions) > 0:
            logger.info(f"[{node_name}] ✅ Found {len(analytical_instructions)} analytical fetch instruction(s) — using analytical fetch path (no $count)")
            return await execute_analytical_fetch(state, analytical_instructions, start_time, node_name)
        
        logger.info(f"[{node_name}] No analytical fetch instructions — using standard relational fetch path")
        
        # Get plan from state (check both keys for compatibility)
        plan = state.get("plan") or state.get("sap_fetch_plan", {})
        if not plan or not isinstance(plan, dict):
            logger.error(f"[{node_name}] ❌ No plan found in state")
            return {"errors": ["No plan found"], "status": "error"}
        
        views = plan.get("views", {})
        if not views:
            logger.error(f"[{node_name}] ❌ No views in plan")
            return {"errors": ["No views in plan"], "status": "error"}
        
        # Get date columns from state (extracted in sap_fetch_plan)
        sap_date_columns_by_view = state.get("sap_date_columns_by_view", {})
        
        # Build OData queries from plan
        odata_queries = []
        for view_key, view_plan in views.items():
            if not isinstance(view_plan, dict):
                continue
            
            # Support dimension-based fetch: source_view overrides key for URL construction
            view_name = view_plan.get("source_view") or view_key
            
            # Build $select from plan columns — restrict to selected columns (filtered_analytical_* or schema)
            plan_columns = view_plan.get("columns", [])
            filters = view_plan.get("filters", [])
            allowed_set = get_allowed_columns_for_view(state, view_name)
            if allowed_set:
                plan_columns, filters = filter_columns_for_api_call(
                    plan_columns or [], filters, allowed_set, view_name, node_name
                )
                logger.info("[%s] Fetching only selected columns for '%s': $select has %s column(s)", node_name, view_name, len(plan_columns or []))
            select_str = clean_odata_select(",".join(plan_columns) if plan_columns else None)
            
            # Extract filters and convert to OData filter string
            
            # Separate date filters from non-date filters
            # For source_view-based queries, also check date columns under the real view name
            view_date_columns = sap_date_columns_by_view.get(view_key, []) or sap_date_columns_by_view.get(view_name, [])
            date_filters = []  # List of filter dicts for date columns
            non_date_filters = []  # List of filter dicts for non-date columns
            
            for filter_dict in filters:
                if not isinstance(filter_dict, dict):
                    continue
                
                column = filter_dict.get("column", "")
                # Skip Calendar_Year
                if column and column.lower() == "calendar_year":
                    logger.info(f"[{node_name}] Skipping Calendar_Year filter for '{view_name}' - not a date column")
                    continue
                
                # Categorize filters
                if column and column in view_date_columns:
                    date_filters.append(filter_dict)
                else:
                    non_date_filters.append(filter_dict)
            
            # Build filter expression
            filter_expr = None
            
            # Process date filters: group by ranges and combine with OR
            date_filter_expr = build_date_filter_expression(date_filters, view_name, node_name)
            
            # Process non-date filters: combine with AND
            non_date_filter_parts = []
            for filter_dict in non_date_filters:
                column = filter_dict.get("column", "")
                odata_syntax = filter_dict.get("odata_syntax")
                if odata_syntax:
                    non_date_filter_parts.append(odata_syntax)
                else:
                    # Build OData syntax from filter dict
                    operator = filter_dict.get("operator", "=")
                    value = filter_dict.get("value", "")
                    
                    # Map operator
                    op_map = {
                        "=": "eq", "!=": "ne", "<>": "ne",
                        ">": "gt", ">=": "ge", "<": "lt", "<=": "le"
                    }
                    odata_op = op_map.get(operator.lower(), "eq")
                    
                    # Format value
                    date_pattern = r'^\d{4}-\d{2}-\d{2}'
                    if isinstance(value, str) and re.match(date_pattern, value):
                        non_date_filter_parts.append(f"{column} {odata_op} {value}")
                    elif isinstance(value, (int, float)):
                        non_date_filter_parts.append(f"{column} {odata_op} {value}")
                    else:
                        non_date_filter_parts.append(f"{column} {odata_op} '{value}'")
            
            non_date_filter_expr = " and ".join(non_date_filter_parts) if non_date_filter_parts else None
            
            # Combine date and non-date filters with AND
            filter_parts_combined = []
            if date_filter_expr:
                filter_parts_combined.append(date_filter_expr)
            if non_date_filter_expr:
                filter_parts_combined.append(non_date_filter_expr)
            
            filter_expr = " and ".join(filter_parts_combined) if filter_parts_combined else None
            
            # Log the final constructed filter for verification
            if filter_expr:
                logger.info(
                    f"[{node_name}] ✅ Final filter for '{view_name}': {filter_expr[:400]}{'...' if len(filter_expr) > 400 else ''}"
                )
            
            # Get orderby from date columns
            orderby = None
            if view_date_columns:
                orderby = view_date_columns[0]  # Use first date column
                logger.info(f"[{node_name}] Using date column '{orderby}' for $orderby on '{view_name}'")
            
            odata_queries.append({
                "view_name": view_name,           # Real SAP view name (for URL construction)
                "dataset_key": view_key,           # Key for raw_dataframes storage (may be tagged)
                "filter": filter_expr,
                "orderby": orderby,
                "select": select_str,              # $select columns (None = all columns)
            })
        
        if not odata_queries:
            logger.error(f"[{node_name}] ❌ No valid queries built from plan")
            return {"errors": ["No valid queries built from plan"], "status": "error"}
        
        logger.info(f"[{node_name}] ✅ Built {len(odata_queries)} OData query/queries from plan")
        
        # Get service and token
        datasphere_service = get_datasphere_service()
        user_id = state.get("user_id", "unknown")
        token = state.get("sap_access_token")
        
        # Get WebSocket manager for progress updates
        ws_manager = state.get("ws_manager")
        
        if not token:
            logger.error(f"[{node_name}] ❌ No SAP access token found")
            return {"errors": ["No SAP access token"], "status": "error"}
        
        # Get asset information and schemas
        sap_datasphere_assets = state.get("sap_datasphere_assets", {})
        sap_view_schemas = state.get("sap_view_schemas", {})
        
        # Extract assets dict - sap_datasphere_assets has structure: {"view_names": [...], "assets": {...}}
        if isinstance(sap_datasphere_assets, dict):
            assets_dict = sap_datasphere_assets.get("assets", {})
        else:
            assets_dict = {}
        
        logger.info(f"[{node_name}] 📋 Processing {len(odata_queries)} query/queries")
        logger.info(f"[{node_name}] 📋 Available assets: {len(assets_dict)}")
        logger.info(f"[{node_name}] 📋 Available schemas: {len(sap_view_schemas)}")
        
        # Log available asset keys for debugging
        if assets_dict:
            asset_keys = list(assets_dict.keys())[:10]  # First 10 keys
            logger.info(f"[{node_name}] 📋 Asset keys (first 10): {asset_keys}")
        
        # Fetch data for each view (supports multiple views)
        # Check if there's existing raw_dataframes in state to avoid duplicate fetches
        existing_raw_dataframes = state.get("raw_dataframes", {})
        raw_dataframes = existing_raw_dataframes.copy() if existing_raw_dataframes else {}
        table_data = {}
        
        # Track statistics for summary
        view_stats = []  # List of dicts with view_name, rows, columns, status
        # Track fetch status per view for partial-fetch / error reporting in summary
        data_fetch_status = {"by_view": {}, "has_partial_fetch": False, "total_planned_rows": 0, "total_actual_rows": 0}
        
        logger.info(f"[{node_name}] 🔄 Starting fetch for {len(odata_queries)} view(s)...")
        logger.info(f"[{node_name}] 📋 Found {len(raw_dataframes)} existing view(s) in state")
        
        for query_idx, query in enumerate(odata_queries, 1):
            if not isinstance(query, dict):
                logger.warning(f"[{node_name}] ⚠️ Query {query_idx}: Skipping invalid query (not a dict)")
                continue
            
            view_name = query.get("view_name", "")
            dataset_key = query.get("dataset_key", view_name)  # Tagged key for raw_dataframes
            if not view_name:
                logger.warning(f"[{node_name}] ⚠️ Query {query_idx}: Skipping query with no view_name")
                continue
            
            # CRITICAL: Check if dataset has already been fetched - skip to avoid duplicate fetches
            if dataset_key in raw_dataframes:
                logger.info(
                    f"[{node_name}] ⏭️ Query {query_idx}/{len(odata_queries)}: Dataset '{dataset_key}' already exists in raw_dataframes - skipping fetch"
                )
                # Still add to stats to track that it was processed
                try:
                    existing_df = raw_dataframes[dataset_key]
                    if hasattr(existing_df, 'select'):
                        row_count = existing_df.select(pl.len()).collect().item()
                    else:
                        row_count = 0
                    
                    if hasattr(existing_df, 'collect_schema'):
                        column_count = len(existing_df.collect_schema().names())
                    elif hasattr(existing_df, 'columns'):
                        column_count = len(existing_df.columns)
                    else:
                        column_count = 0
                    
                    view_stats.append({
                        "view_name": dataset_key,
                        "rows": row_count,
                        "columns": column_count,
                        "status": "skipped (already exists)"
                    })
                except Exception as e:
                    logger.debug(f"[{node_name}] Could not get stats for existing dataset '{dataset_key}': {e}")
                    view_stats.append({
                        "view_name": dataset_key,
                        "rows": 0,
                        "columns": 0,
                        "status": "skipped (already exists)"
                    })
                continue
            
            filter_expr = query.get("filter")
            orderby = query.get("orderby")
            select_str = clean_odata_select(query.get("select"))  # $select: comma-separated, no spaces
            
            logger.info(f"[{node_name}] ========== Processing Query {query_idx}/{len(odata_queries)} ==========")
            logger.info(f"[{node_name}] 📋 View: '{view_name}' (dataset_key: '{dataset_key}')")
            logger.info(f"[{node_name}] 📋 Filter: {filter_expr[:100] + '...' if filter_expr and len(filter_expr) > 100 else (filter_expr or 'none')}")
            logger.info(f"[{node_name}] 📋 OrderBy: {orderby or 'none (will use date column from schema)'}")
            if select_str:
                logger.info(f"[{node_name}] 📋 Select: {select_str[:200]}{'...' if len(select_str) > 200 else ''}")
            
            # Get asset info - handle both dict and DatasphereAsset object
            asset_info = assets_dict.get(view_name)
            
            # Handle different asset info formats
            if asset_info is None:
                logger.error(f"[{node_name}] ❌ Query {query_idx}: Asset '{view_name}' not found in assets_dict")
                logger.error(f"[{node_name}]    Available asset keys: {list(assets_dict.keys())[:20]}")
                continue
            
            # Extract data_url and space_id - handle both dict and object formats
            if isinstance(asset_info, dict):
                data_url = asset_info.get("data_url")
                space_id = asset_info.get("space_id")
            else:
                # Try to get as object attributes (DatasphereAsset)
                data_url = getattr(asset_info, "data_url", None)
                space_id = getattr(asset_info, "space_id", None)
                # If it has to_dict method, use it
                if hasattr(asset_info, "to_dict"):
                    asset_dict = asset_info.to_dict()
                    data_url = asset_dict.get("data_url")
                    space_id = asset_dict.get("space_id")
            
            if not data_url:
                logger.error(f"[{node_name}] ❌ Query {query_idx}: No data_url found for '{view_name}'")
                logger.error(f"[{node_name}]    Asset info type: {type(asset_info)}")
                logger.error(f"[{node_name}]    Asset info: {str(asset_info)[:200]}")
                continue
            
            logger.info(f"[{node_name}] 📋 Data URL: {data_url}")
            logger.info(f"[{node_name}] 📋 Space ID: {space_id}")
            
            # Fetch data
            df_lazy, total_api_calls, api_url, fetch_status = await relational_fetch_view_data(
                datasphere_service,
                user_id,
                view_name,
                filter_expr,
                orderby,
                data_url,
                space_id,
                token,
                sap_view_schemas,  # Pass schemas for orderby fallback
                select=select_str,  # $select for dimension-based fetch
            )
            
            # Accumulate fetch status for summary (partial fetch / errors)
            if fetch_status:
                data_fetch_status["by_view"][dataset_key] = fetch_status
                if fetch_status.get("message"):
                    data_fetch_status["has_partial_fetch"] = True
                data_fetch_status["total_planned_rows"] += fetch_status.get("planned_rows") or 0
                data_fetch_status["total_actual_rows"] += fetch_status.get("actual_rows") or 0
            
            # Store API URL per view in state
            if "api_urls_by_view" not in state:
                state["api_urls_by_view"] = {}
            if api_url:
                state["api_urls_by_view"][dataset_key] = api_url
                logger.info(f"[{node_name}] 📋 Stored API URL for '{dataset_key}': {api_url[:100]}...")
            
            if df_lazy is not None:
                # Store LazyFrame using dataset_key (supports dimension-based tagged keys)
                raw_dataframes[dataset_key] = df_lazy
                table_data[dataset_key] = []  # Empty for memory efficiency
                
                logger.info(
                    f"[{node_name}] ✅ Dataset '{dataset_key}' stored in raw_dataframes "
                    f"(total datasets stored: {len(raw_dataframes)})"
                )
                
                # Collect statistics for this view
                try:
                    # Get row count efficiently using pl.len() (counts rows, not column values)
                    # This is efficient as it only collects the count, not full data
                    row_count = df_lazy.select(pl.len()).collect().item()
                    
                    # Get column count using collect_schema() to avoid PerformanceWarning
                    if hasattr(df_lazy, 'collect_schema'):
                        column_count = len(df_lazy.collect_schema().names())
                    elif hasattr(df_lazy, 'columns'):
                        column_count = len(df_lazy.columns)
                    else:
                        column_count = 0
                    
                    view_stats.append({
                        "view_name": dataset_key,
                        "rows": row_count,
                        "columns": column_count,
                        "status": "success"
                    })
                    
                    logger.info(
                        f"[{node_name}] ✅ Query {query_idx}/{len(odata_queries)} Complete: '{dataset_key}' - "
                        f"Data fetched and combined successfully ({row_count:,} rows, {column_count} columns)"
                    )
                except Exception as e:
                    # Truncate error message to 200 chars to avoid full schema dumps
                    error_msg = str(e)
                    if len(error_msg) > 200:
                        error_msg = error_msg[:200] + "..."
                    logger.warning(
                        f"[{node_name}] ⚠️ Could not get statistics for '{dataset_key}': {error_msg}"
                    )
                    view_stats.append({
                        "view_name": dataset_key,
                        "rows": 0,
                        "columns": 0,
                        "status": "success (stats unavailable)"
                    })
            else:
                logger.error(f"[{node_name}] ❌ Query {query_idx}/{len(odata_queries)} Failed: '{dataset_key}' - Failed to fetch data")
                view_stats.append({
                    "view_name": dataset_key,
                    "rows": 0,
                    "columns": 0,
                    "status": "failed"
                })
        
        # Verify all views were processed
        logger.info(
            f"[{node_name}] ✅ Completed processing {len(odata_queries)} view(s): "
            f"{len(raw_dataframes)} view(s) successfully fetched and stored"
        )
        
        # Calculate summary statistics
        duration = (datetime.now() - start_time).total_seconds()
        total_views = len(odata_queries)
        successful_views = len([s for s in view_stats if s["status"] == "success"])
        failed_views = len([s for s in view_stats if s["status"] == "failed"])
        total_rows = sum(s["rows"] for s in view_stats)
        total_columns_sum = sum(s["columns"] for s in view_stats)
        avg_columns = total_columns_sum / successful_views if successful_views > 0 else 0
        
        # Build summary message for frontend
        summary_parts = [
            f"Fetched data from {total_views} view(s)",
            f"{total_rows:,} total rows",
            f"{successful_views} successful"
        ]
        if failed_views > 0:
            summary_parts.append(f"{failed_views} failed")
        summary_message = ", ".join(summary_parts)
        
        # Build detailed summary for frontend details
        details_parts = [f"Total Views: {total_views}"]
        details_parts.append(f"✅ Successful: {successful_views}")
        if failed_views > 0:
            details_parts.append(f"❌ Failed: {failed_views}")
        details_parts.append(f"Total Rows: {total_rows:,}")
        if successful_views > 0:
            details_parts.append(f"Avg Columns: {avg_columns:.1f}")
        details_parts.append("")
        details_parts.append("View Details:")
        for stat in view_stats:
            status_icon = "✅" if stat["status"] == "success" else "❌"
            details_parts.append(
                f"{status_icon} {stat['view_name']}: {stat['rows']:,} rows, {stat['columns']} columns"
            )
        summary_details = "\n".join(details_parts)
        
        # Log comprehensive summary
        logger.info("")
        logger.info("=" * 80)
        logger.info(f"[{node_name}] 📊 FETCH SUMMARY")
        logger.info("=" * 80)
        logger.info(f"[{node_name}] Total Views Processed: {total_views}")
        logger.info(f"[{node_name}] ✅ Successful: {successful_views}")
        logger.info(f"[{node_name}] ❌ Failed: {failed_views}")
        logger.info(f"[{node_name}] Total Rows Fetched: {total_rows:,}")
        if successful_views > 0:
            logger.info(f"[{node_name}] Average Columns per View: {avg_columns:.1f}")
        logger.info("")
        logger.info(f"[{node_name}] View Details:")
        for stat in view_stats:
            status_icon = "✅" if stat["status"] == "success" else "❌"
            logger.info(
                f"[{node_name}]   {status_icon} {stat['view_name']}: "
                f"{stat['rows']:,} rows, {stat['columns']} columns - {stat['status']}"
            )
        logger.info("=" * 80)
        logger.info(
            f"[{node_name}] ========== Simple SAP Data Fetch Complete ========== "
            f"({duration:.2f}s)"
        )
        logger.info("")
        
        # Send summary to frontend via WebSocket with structured data
        if ws_manager:
            try:
                # Prepare structured summary data for WebSocket
                summary_data = {
                    "fetch_summary": {
                        "total_views_processed": total_views,
                        "successful_views": successful_views,
                        "failed_views": failed_views,
                        "total_rows_fetched": total_rows,
                        "average_columns_per_view": round(avg_columns, 1) if successful_views > 0 else 0,
                        "duration_seconds": round(duration, 2),
                        "view_details": [
                            {
                                "view_name": stat["view_name"],
                                "rows": stat["rows"],
                                "columns": stat["columns"],
                                "status": stat["status"]
                            }
                            for stat in view_stats
                        ]
                    }
                }
                
                await ws_manager.send_progress(
                    node_name=node_name,
                    message=summary_message,
                    status="complete",
                    details=summary_details,
                    data=summary_data
                )
                logger.info(f"[{node_name}] ✅ Sent fetch summary to frontend with structured data")
            except Exception as e:
                logger.warning(f"[{node_name}] ⚠️ Failed to send summary to frontend: {e}")
        
        # Store SAP API URLs per table for export (re-fetch on download)
        query_id = state.get("query_id")
        api_urls = state.get("api_urls_by_view") or {}
        if query_id and api_urls:
            try:
                from ...cache.data_cache import get_query_cache
                get_query_cache().save_sap_api_urls(
                    query_id, api_urls, state.get("data_source_config")
                )
            except Exception as cache_err:
                logger.warning(f"[{node_name}] Could not save SAP API URLs for export: {cache_err}")

        # Same format as SQL flow: frontend shows these as "SQL queries" (endpoint only, no base URL)
        generated_queries = api_urls_to_generated_queries(api_urls)

        # If total_rows is zero across all views, signal no_data_available so downstream nodes
        # (analytical_summary, charts, etc.) can return a normal-text "no data for this query" message.
        no_data_flag = total_rows == 0

        return {
            "raw_dataframes": raw_dataframes,
            "table_data": table_data,
            "status": "data_fetched",
            "data_fetch_status": data_fetch_status if data_fetch_status.get("by_view") else None,
            "generated_queries": generated_queries,
            "_analytical_fetch_triggered": True,
            "no_data_available": no_data_flag,
        }
        
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"[{node_name}] ❌ Failed after {duration:.2f}s: {e}", exc_info=True)
        return {
            "errors": state.get("errors", []) + [f"SAP data fetch failed: {str(e)}"],
            "status": "error",
        }

