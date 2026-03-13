"""SAP Datasphere fetch plan generation node - creates SAP-specific data fetch plan directly from user query and schemas.

This node generates a SAP Datasphere-specific plan directly (without requiring sql_plan) that includes:
- Column selection based on user query and identified metrics
- OData query structure
- Column batching strategy
- Date chunking strategy
- Input parameters mapping
- Filter extraction and conversion to OData syntax

For SAP flows, this node runs after get_schema and replaces sql_plan_synthesis.

Simple plan generation - no column batching (API returns all columns).
"""
from typing import Dict, Any, List, Optional
import logging
import json
import asyncio
from datetime import datetime
from ...llm.azure_openai import AzureOpenAIClient
from ..state import AnalyticsState
from ..utils import log_llm_invalid_columns, parse_json_response, save_llm_call_input, save_llm_call_output
from ..utils.sap_fetch_helpers import extract_date_columns_by_view
from ..prompts import (
    SAP_FETCH_PLAN_SYSTEM_PROMPT,
    get_sap_fetch_plan_user_prompt,
)
from config.settings import settings

logger = logging.getLogger(__name__)


def _validate_plan_filters_against_schema(
    plan: Dict[str, Any],
    view_schemas: Dict[str, Any],
    node_name: str
) -> Dict[str, Any]:
    """
    Validate filter columns in plan against schema and remove filters with invalid columns.
    
    Args:
        plan: SAP fetch plan with "views" key
        view_schemas: Dict mapping view_name to schema dict with columns
        node_name: Node name for logging
        
    Returns:
        Updated plan with invalid filter columns removed
    """
    if not view_schemas or not plan:
        return plan
    
    if not isinstance(plan, dict):
        return plan
    
    views = plan.get("views", {})
    if not views:
        return plan
    
    updated_plan = plan.copy()
    updated_views = {}
    
    for view_name, view_plan in views.items():
        if not isinstance(view_plan, dict):
            updated_views[view_name] = view_plan
            continue
        
        # Get schema for this view
        schema_info = view_schemas.get(view_name)
        if not schema_info or not isinstance(schema_info, dict):
            updated_views[view_name] = view_plan
            continue
        
        # Get valid column names from schema
        schema_columns = schema_info.get("columns", [])
        valid_column_names = set()
        for col in schema_columns:
            if isinstance(col, dict):
                col_name = col.get("name", "")
                if col_name:
                    valid_column_names.add(col_name)
            elif isinstance(col, str):
                valid_column_names.add(col)
        
        if not valid_column_names:
            updated_views[view_name] = view_plan
            continue
        
        # Validate filter columns
        filters = view_plan.get("filters", [])
        if isinstance(filters, list):
            valid_filters = []
            for filter_dict in filters:
                if isinstance(filter_dict, dict):
                    filter_column = filter_dict.get("column", "")
                    if filter_column and filter_column in valid_column_names:
                        valid_filters.append(filter_dict)
                    elif filter_column:
                        logger.warning(
                            f"[{node_name}] ⚠️ '{view_name}': Removing filter with invalid column "
                            f"'{filter_column}' (not in schema)"
                        )
                else:
                    valid_filters.append(filter_dict)
            
            if len(valid_filters) < len(filters):
                logger.info(
                    f"[{node_name}] ✅ '{view_name}': Keeping {len(valid_filters)} valid filter(s) "
                    f"from {len(filters)} total"
                )
                
                # Update view plan with only valid filters
                updated_view_plan = view_plan.copy()
                updated_view_plan["filters"] = valid_filters
                updated_views[view_name] = updated_view_plan
            else:
                updated_views[view_name] = view_plan
        else:
            updated_views[view_name] = view_plan
    
    updated_plan["views"] = updated_views
    return updated_plan


def _validate_plan_columns_against_schema(
    plan: Dict[str, Any],
    view_schemas: Dict[str, Any],
    node_name: str
) -> Dict[str, Any]:
    """
    Validate and filter out columns from plan that don't exist in the schema.
    
    Args:
        plan: SAP fetch plan with "views" key
        view_schemas: Dict mapping view_name to schema dict with columns
        node_name: Node name for logging
        
    Returns:
        Updated plan with invalid columns removed
    """
    if not view_schemas or not plan:
        return plan
    
    if not isinstance(plan, dict):
        return plan
    
    views = plan.get("views", {})
    if not views:
        return plan
    
    updated_plan = plan.copy()
    updated_views = {}
    
    for view_name, view_plan in views.items():
        if not isinstance(view_plan, dict):
            updated_views[view_name] = view_plan
            continue
        
        # Get schema for this view
        schema_info = view_schemas.get(view_name)
        if not schema_info or not isinstance(schema_info, dict):
            logger.warning(f"[{node_name}] No schema found for '{view_name}' - cannot validate columns")
            updated_views[view_name] = view_plan
            continue
        
        # Get valid column names from schema
        schema_columns = schema_info.get("columns", [])
        valid_column_names = set()
        for col in schema_columns:
            if isinstance(col, dict):
                col_name = col.get("name", "")
                if col_name:
                    valid_column_names.add(col_name)
            elif isinstance(col, str):
                valid_column_names.add(col)
        
        if not valid_column_names:
            logger.warning(f"[{node_name}] Schema for '{view_name}' has no columns - cannot validate")
            updated_views[view_name] = view_plan
            continue
        
        # Validate columns in plan
        columns = view_plan.get("columns", [])
        if isinstance(columns, list):
            valid_columns = [col for col in columns if col in valid_column_names]
            invalid_columns = [col for col in columns if col not in valid_column_names]
            
            if invalid_columns:
                log_llm_invalid_columns(
                    source="sap_fetch_plan.columns",
                    node_name=node_name,
                    invalid_columns=invalid_columns,
                    available_sample=list(valid_column_names)[:20],
                    context=f"view_name={view_name}",
                )
                logger.warning(
                    f"[{node_name}] ⚠️ '{view_name}': Removing {len(invalid_columns)} invalid column(s) "
                    f"not in schema: {invalid_columns[:10]}{'...' if len(invalid_columns) > 10 else ''}"
                )
                logger.info(
                    f"[{node_name}] ✅ '{view_name}': Keeping {len(valid_columns)} valid column(s) "
                    f"from {len(columns)} total"
                )
                
                # Update view plan with only valid columns
                updated_view_plan = view_plan.copy()
                updated_view_plan["columns"] = valid_columns
                updated_views[view_name] = updated_view_plan
            else:
                updated_views[view_name] = view_plan
        else:
            updated_views[view_name] = view_plan
    
    updated_plan["views"] = updated_views
    return updated_plan


async def sap_fetch_plan_node(state: AnalyticsState, model: str = None) -> Dict[str, Any]:
    """
    Create SAP Datasphere-specific fetch plan directly from user query, schemas, and intent.
    
    This node:
    1. Takes user query, parsed intent, identified metrics, and table schemas
    2. Generates SAP-specific fetch plan:
       - Column selection based on query and metrics
       - OData filter syntax
       - Column batching strategy
       - Date chunking requirements
       - Input parameters
    3. Creates a structured plan for OData query generation
    
    Args:
        state: Current analytics state containing:
            - user_query: Original user query
            - parsed_intent: User query and intent explanation
            - identified_metrics: Metrics with metric_name, data_needed, formula
            - unified_schema: Structured schema + sample data
            - schema_context: Schema information
            - datasource_info: Column descriptions, usage suggestions
            - sap_view_schemas: SAP view schemas
            - sap_datasphere_assets: SAP asset information
            - selected_tables: List of selected table/view names
        model: Optional model name override
        
    Returns:
        Updated state dictionary with:
            - sap_fetch_plan: SAP-specific fetch plan
            - status: "sap_fetch_plan_generated" on success
    """
    start_time = datetime.now()
    node_name = "sap_fetch_plan"
    
    # Record timing
    from ..node_timing_registry import get_node_timing_registry
    registry = get_node_timing_registry()
    if registry:
        registry.record_node_start(node_name, start_time)
    
    logger.info(f"[{node_name}] ========== Starting SAP Fetch Plan Generation ==========")
    
    # Initialize date_columns_by_view to ensure it's always defined
    date_columns_by_view = {}
    
    # Check if errors exist
    if state.get("errors"):
        logger.warning(f"[{node_name}] Errors detected in state - skipping SAP fetch plan")
        return {
            "sap_date_columns_by_view": date_columns_by_view,  # Ensure it's always in state
        }
    
    # Get required state
    user_message = state.get("user_query", "")
    selected_tables = state.get("selected_tables", [])
    parsed_intent = state.get("parsed_intent", {})
    
    # SAP-specific state
    sap_view_schemas = state.get("sap_view_schemas", {})
    sap_datasphere_assets = state.get("sap_datasphere_assets", {})
    
    # Validate we have required data
    if not selected_tables:
        logger.error(f"[{node_name}] Missing selected tables - cannot generate SAP fetch plan")
        return {
            "plan": {},
            "sap_date_columns_by_view": date_columns_by_view,
            "errors": state.get("errors", []) + ["SAP fetch plan failed: missing selected tables"],
            "status": "error",
        }
    
    # Check if we have schema information
    has_sap_view_schemas = sap_view_schemas and len(sap_view_schemas) > 0
    
    # CRITICAL: Verify sap_view_schemas is fetched and has correct structure
    if not has_sap_view_schemas:
        logger.error(f"[{node_name}] CRITICAL: sap_view_schemas is missing or empty!")
        logger.error(f"[{node_name}]   - sap_view_schemas type: {type(sap_view_schemas)}")
        logger.error(f"[{node_name}]   - sap_view_schemas value: {sap_view_schemas}")
        logger.error(f"[{node_name}]   - selected_tables: {selected_tables}")
        logger.error(f"[{node_name}]   - This means schemas were not fetched in select_tables node")
        logger.error(f"[{node_name}]   - Cannot generate plan without schema - will attempt to fetch schemas now")
        
        # Try to fetch schemas if we have assets and user_id
        if sap_datasphere_assets and selected_tables:
            try:
                user_id = state.get("user_id")
                if user_id:
                    logger.info(f"[{node_name}] Attempting to fetch schemas for {len(selected_tables)} view(s)")
                    from ...services.datasphere_service import get_datasphere_service, DatasphereAsset
                    datasphere_service = get_datasphere_service()
                    
                    # Get assets dict
                    assets_dict_raw = sap_datasphere_assets.get("assets", {})
                    assets_dict = {}
                    for view_name, asset_info in assets_dict_raw.items():
                        if isinstance(asset_info, dict) and view_name in selected_tables:
                            assets_dict[view_name] = DatasphereAsset(
                                name=asset_info.get("name", view_name),
                                label=asset_info.get("label"),
                                data_url=asset_info.get("data_url"),
                                metadata_url=asset_info.get("metadata_url"),
                            )
                    
                    # Fetch schemas
                    schemas_result = await datasphere_service.get_multiple_view_schemas(
                        user_id=user_id,
                        view_names=selected_tables,
                        assets=assets_dict,
                        max_concurrent=5,
                        state=state
                    )
                    
                    # Convert to dict format
                    for view_name, schema in schemas_result.items():
                        sap_view_schemas[view_name] = schema.to_dict()
                    
                    logger.info(f"[{node_name}] Successfully fetched {len(sap_view_schemas)} schema(s)")
                    has_sap_view_schemas = len(sap_view_schemas) > 0
                else:
                    logger.error(f"[{node_name}] Cannot fetch schemas - user_id is missing")
            except Exception as e:
                logger.error(f"[{node_name}] Failed to fetch schemas: {e}", exc_info=True)
    
    # Filter schemas to selected tables only
    if sap_view_schemas and selected_tables:
        filtered_schemas = {k: v for k, v in sap_view_schemas.items() if k in selected_tables}
        sap_view_schemas = filtered_schemas
    
    logger.info(f"[{node_name}] Processing {len(selected_tables)} view(s): {selected_tables}")
    logger.info(f"[{node_name}] Available schemas: {len(sap_view_schemas)}")
    
    # Log schema details for debugging - CRITICAL for verifying correct column names
    if sap_view_schemas:
        for view_name in selected_tables:
            if view_name in sap_view_schemas:
                schema_info = sap_view_schemas[view_name]
                if isinstance(schema_info, dict):
                    columns = schema_info.get("columns", [])
                    col_names = [c.get("name", "") for c in columns if isinstance(c, dict) and c.get("name")]
                    logger.info(f"[{node_name}] Schema for '{view_name}': {len(col_names)} columns")
                    if col_names:
                        # Log count only; avoid printing full column list in plan
                        if len(col_names) <= 10:
                            logger.debug(f"[{node_name}]   Columns: {col_names}")
                        else:
                            logger.debug(f"[{node_name}]   First 5: {col_names[:5]} ... last 5: {col_names[-5:]}")
                    else:
                        logger.warning(f"[{node_name}]   WARNING: Schema for '{view_name}' has no columns!")
                else:
                    logger.warning(f"[{node_name}]   WARNING: Schema for '{view_name}' is not a dict: {type(schema_info)}")
            else:
                logger.warning(f"[{node_name}]   WARNING: No schema found for selected view '{view_name}' in sap_view_schemas")
                logger.warning(f"[{node_name}]   Available schema keys: {list(sap_view_schemas.keys())}")
    else:
        logger.error(f"[{node_name}] CRITICAL: sap_view_schemas is empty - cannot generate plan with correct column names!")
    
    
    try:
        # Initialize LLM client
        llm_client = state.get("llm_client") or AzureOpenAIClient()
        model_name = model or settings.analytics_sap_fetch_plan_model
        query_id = state.get("query_id")
        
        # Extract date columns from schemas - CRITICAL for filtering
        date_columns_by_view = extract_date_columns_by_view(sap_view_schemas)
        if date_columns_by_view:
            logger.info(
                f"[{node_name}] ✅ Extracted date columns from schemas: "
                f"{', '.join([f'{v}: {len(cols)} cols' for v, cols in date_columns_by_view.items()])}"
            )
        else:
            logger.warning(f"[{node_name}] ⚠️ No date columns found in schemas - date filtering may fail")
        
        # Calculate total columns across all views for summary message
        total_columns = 0
        for view_name in selected_tables:
            if view_name in sap_view_schemas:
                schema_info = sap_view_schemas[view_name]
                if isinstance(schema_info, dict):
                    columns = schema_info.get("columns", [])
                    col_count = len([c for c in columns if isinstance(c, dict) and c.get("name")])
                    total_columns += col_count
                    logger.info(f"[{node_name}] View '{view_name}': {col_count} columns")
        
        # Create summary message
        views_summary = f"**SUMMARY:** {len(selected_tables)} view(s) selected with {total_columns} total columns."
        logger.info(f"[{node_name}] {views_summary}")
        
        # CRITICAL: No column batching - simple single LLM call with all columns
        # API returns all columns, so no need to split queries
        logger.info(f"[{node_name}] 🚀 Generating single plan with ALL columns (no batching needed)")
        
        # Extract date columns for selected views
        date_columns_for_prompt = {
            view_name: date_columns_by_view.get(view_name, [])
            for view_name in selected_tables
            if view_name in date_columns_by_view
        }
        
        user_prompt = get_sap_fetch_plan_user_prompt(
            user_message=user_message,
            selected_tables=selected_tables,
            parsed_intent=parsed_intent,
            view_schemas=sap_view_schemas,
            date_columns_by_view=date_columns_for_prompt,
            views_summary=views_summary,
        )
        
        logger.info(f"[{node_name}] Calling LLM ({model_name}) to generate SAP fetch plan")
        save_llm_call_input(
            node_name=node_name,
            query_id=query_id,
            system_prompt=SAP_FETCH_PLAN_SYSTEM_PROMPT,
            user_prompt=user_prompt,
            extra={"model": model_name},
        )
        response = await llm_client._call_llm_unified(
            model=model_name,
            system_prompt=SAP_FETCH_PLAN_SYSTEM_PROMPT,
            user_prompt=user_prompt,
            node_name=node_name,
            query_id=query_id,
            temperature=0.0,
            use_json_mode=True,
        )
        
        logger.info(f"[{node_name}] LLM response received - parsing SAP fetch plan")
        
        # Parse the SAP fetch plan from response
        sap_fetch_plan = parse_json_response(response, expected_type=dict) if isinstance(response, str) else response
        save_llm_call_output(
            node_name=node_name,
            query_id=query_id,
            raw_response=response,
            parsed=sap_fetch_plan,
        )
        # Validate structure
        if not isinstance(sap_fetch_plan, dict):
            raise ValueError("SAP fetch plan must be a dictionary")
        
        # Ensure it has the expected structure
        if "views" not in sap_fetch_plan:
            logger.warning(f"[{node_name}] SAP fetch plan missing 'views' key - adding empty structure")
            sap_fetch_plan["views"] = {}
        
        logger.info(f"[{node_name}] Parsed SAP fetch plan with {len(sap_fetch_plan.get('views', {}))} view(s)")
        
        # Log plan details
        for view_name, view_plan in sap_fetch_plan.get("views", {}).items():
            if isinstance(view_plan, dict):
                columns = view_plan.get("columns", [])
                filters = view_plan.get("filters", [])
                logger.info(
                    f"[{node_name}]   View '{view_name}': {len(columns)} columns, "
                    f"{len(filters)} filters"
                )
    
    except json.JSONDecodeError as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"[{node_name}] Failed to parse SAP fetch plan as JSON after {duration:.2f}s: {e}")
        if 'response' in locals():
            logger.error(f"[{node_name}] Response: {response[:1000]}")
        return {
            "plan": {},
            "sap_date_columns_by_view": date_columns_by_view,  # Ensure it's always in state
            "errors": state.get("errors", []) + [f"SAP fetch plan parsing failed: {str(e)}"],
            "status": "error",
        }
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(
            f"[{node_name}] SAP fetch plan generation failed after {duration:.2f}s: {str(e)}",
            exc_info=True
        )
        return {
            "plan": {},
            "sap_date_columns_by_view": date_columns_by_view,  # Ensure it's always in state
            "errors": state.get("errors", []) + [f"SAP fetch plan generation failed: {str(e)}"],
            "status": "error",
        }
    
    # Success path - return the plan
    # Ensure sap_fetch_plan is defined
    if 'sap_fetch_plan' not in locals():
        logger.error(f"[{node_name}] sap_fetch_plan variable not defined - this should not happen")
        return {
            "plan": {},
            "sap_date_columns_by_view": date_columns_by_view,  # Ensure it's always in state
            "errors": state.get("errors", []) + ["SAP fetch plan variable not defined"],
            "status": "error",
        }
    
    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"[{node_name}] SAP fetch plan generation completed in {duration:.2f}s")
    
    # CRITICAL: Validate columns and filters against schema - remove invalid entries
    if sap_view_schemas and sap_fetch_plan:
        logger.info(f"[{node_name}] Validating plan columns and filters against schema...")
        sap_fetch_plan = _validate_plan_columns_against_schema(sap_fetch_plan, sap_view_schemas, node_name)
        sap_fetch_plan = _validate_plan_filters_against_schema(sap_fetch_plan, sap_view_schemas, node_name)
        
        # Log validated plan summary
        for view_name, view_plan in sap_fetch_plan.get("views", {}).items():
            if isinstance(view_plan, dict):
                columns = view_plan.get("columns", [])
                filters = view_plan.get("filters", [])
                logger.info(
                    f"[{node_name}] ✅ Validated plan for '{view_name}': "
                    f"{len(columns)} valid columns, {len(filters)} valid filters"
                )
    
    views_count = len(sap_fetch_plan.get('views', {}))
    logger.info(f"[{node_name}] Returning plan with {views_count} view(s)")
    
    logger.info(f"[{node_name}] ========== SAP Fetch Plan Generation Complete ==========")
    
    # Store validated plan in both 'plan' and 'sap_fetch_plan' keys for compatibility
    result = {
        "plan": sap_fetch_plan,
        "sap_fetch_plan": sap_fetch_plan,
        "sap_date_columns_by_view": date_columns_by_view,
        "status": "sap_fetch_plan_generated",
    }
    
    return result

