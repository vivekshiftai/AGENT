"""Intelligent table selection node - selects optimal tables based on user intent and analytical depth."""
from typing import Dict, Any, List, Optional
import logging
import json
from datetime import datetime
from ...llm.azure_openai import AzureOpenAIClient
from ...database.clickhouse import ClickHouseClient
from ...database.data_source_gateway import DataSourceGateway
from ...database.postgres_client_singleton import get_shared_postgres_client
from ..state import AnalyticsState
from ..prompts import TABLE_SELECTION_SYSTEM_PROMPT, get_table_selection_user_prompt
from ..utils import parse_json_response, save_llm_call_input, save_llm_call_output
from ...services.datasphere_service import get_datasphere_service
from shared.exceptions import DatabaseException

logger = logging.getLogger(__name__)

# SAP view to use for all SAP flows (required view, not a fallback).
# Production planning analytical view — no input parameters, call directly.
# Date filtering: uses Edm.Date columns if available, otherwise fiscal period columns.
SAP_VIEW = "AM_Production_Analysis_v2"

# User-facing message when the hardcoded SAP view is not in catalog/assets (no LLM, simple logic).
SAP_VIEW_NOT_AVAILABLE_MESSAGE = (
    "The production analysis view (AM_Production_Analysis_v2) is currently not available. "
    "Please try again later or contact your SAP administrator."
)


def _resolve_sap_view(available_tables: List[str]) -> Optional[str]:
    """
    Resolve which view to use for SAP/Datasphere. Returns the view name if found
    (exact, or with leading underscore, or case-insensitive, or name ending with SAP_VIEW).
    Returns None if not found — caller should then return SAP_VIEW_NOT_AVAILABLE_MESSAGE to user.
    """
    if not available_tables:
        return None
    want = SAP_VIEW.strip()
    alt = f"_{want}"
    want_upper = want.upper()
    if want in available_tables:
        return want
    if alt in available_tables:
        return alt
    for t in available_tables:
        if (t or "").strip().upper() == want_upper:
            return t
    for t in available_tables:
        if (t or "").strip().upper().endswith(want_upper) or (t or "").strip().endswith(want):
            return t
    logger.warning(
        "[select_tables] SAP_VIEW '%s' not found in catalog (%s views). Available (first 10): %s",
        SAP_VIEW,
        len(available_tables),
        available_tables[:10],
    )
    return None


def _extract_analytical_depth(parsed_intent: Optional[Dict[str, Any]]) -> Optional[str]:
    """
    Extract analytical depth from parsed intent.
    
    Args:
        parsed_intent: Parsed intent dictionary from query analysis node
        
    Returns:
        Analytical depth string (simple, summary, deep) or None
    """
    if not parsed_intent:
        return None
    
    intent_analysis = parsed_intent.get("intent_analysis", {})
    if isinstance(intent_analysis, dict):
        return intent_analysis.get("analytical_depth")
    
    return None


async def _fetch_datasource_info(data_source_id: int, selected_tables: List[str]) -> Dict[str, Any]:
    """
    Fetch column descriptions, usage suggestions, and unique values for selected tables.
    
    Args:
        data_source_id: Data source ID
        selected_tables: List of selected table names
        
    Returns:
        Dictionary with structure: {table_name: {column_name: {description, usage_suggestions, unique_values, data_type}}}
    """
    if not selected_tables or not data_source_id:
        return {}
    
    try:
        postgres_client = get_shared_postgres_client(ensure_tables=False)
        
        # Get the latest completed analysis for this data source
        analysis_results = await postgres_client.execute_query_async(
            """
            SELECT id FROM data_source_analysis 
            WHERE data_source_id = %s AND status = 'completed'
            ORDER BY completed_at DESC LIMIT 1
            """,
            (data_source_id,)
        )
        
        if not analysis_results:
            logger.debug(f"No completed analysis found for data source {data_source_id}")
            return {}
        
        analysis_id = analysis_results[0]['id']
        
        # Build query to get column descriptions for selected tables
        placeholders = ','.join(['%s'] * len(selected_tables))
        column_results = await postgres_client.execute_query_async(
            f"""
            SELECT table_name, column_name, data_type, unique_values, description, usage_suggestions
            FROM column_descriptions 
            WHERE analysis_id = %s AND table_name IN ({placeholders})
            ORDER BY table_name, column_name
            """,
            (analysis_id, *selected_tables)
        )
        
        # Organize by table and column
        datasource_info = {}
        for row in column_results:
            table_name = row['table_name']
            column_name = row['column_name']
            
            if table_name not in datasource_info:
                datasource_info[table_name] = {}
            
            # Parse unique_values if it's a JSON string
            unique_values = row.get('unique_values')
            if isinstance(unique_values, str):
                try:
                    unique_values = json.loads(unique_values)
                except json.JSONDecodeError:
                    unique_values = []
            elif unique_values is None:
                unique_values = []
            
            datasource_info[table_name][column_name] = {
                "description": row.get('description', ''),
                "usage_suggestions": row.get('usage_suggestions', ''),
                "unique_values": unique_values,
                "data_type": row.get('data_type', 'Unknown')
            }
        
        logger.info(f"Fetched datasource info for {len(datasource_info)} tables with column descriptions")
        return datasource_info
        
    except Exception as e:
        logger.warning(f"Failed to fetch datasource info: {str(e)}")
        return {}


def _extract_selected_tables(parsed: Any, available_tables: List[str]) -> List[str]:
    """
    Extract and validate selected tables from LLM response.
    
    Args:
        parsed: Parsed JSON response from LLM
        available_tables: List of available table names
        
    Returns:
        List of valid selected table names
    """
    selected_tables = []
    
    if isinstance(parsed, dict):
        selected_tables = parsed.get("selected_tables", [])
    elif isinstance(parsed, list):
        if all(isinstance(item, str) for item in parsed):
            selected_tables = parsed
        elif all(isinstance(item, dict) for item in parsed):
            for item in parsed:
                table_name = item.get("table") or item.get("table_name") or item.get("name")
                if table_name:
                    selected_tables.append(table_name)
            if not selected_tables and parsed:
                selected_tables = parsed[0].get("selected_tables", [])
        else:
            selected_tables = [str(item) for item in parsed]
    
    # Normalize to list
    if not isinstance(selected_tables, list):
        selected_tables = [selected_tables] if isinstance(selected_tables, str) else []
    
    # Validate against available tables
    valid_tables = [t for t in selected_tables if t in available_tables]
    return valid_tables


async def select_tables_node(state: AnalyticsState, model: str = None) -> Dict[str, Any]:
    """
    Select optimal database tables based on metrics from Node 2.
    
    This node's responsibility is to:
    - Take identified_metrics from Node 2 (metric analysis)
    - Use the metrics to identify which tables contain the data needed
    - Select the optimal tables from the available tables list
    - Return only the list of selected table names
    
    Args:
        state: Current analytics state containing:
            - user_query: Original user query
            - parsed_intent: User query and intent explanation from Node 1
            - identified_metrics: Metrics from Node 2 with data_needed and formula
        model: Optional model name override (from graph builder)
        
    Returns:
        Updated state dictionary with:
            - selected_tables: List of selected table names
            - status: "tables_selected" on success, "error" on failure
    """
    start_time = datetime.now()
    node_name = "table_identification"  # Graph uses "table_identification" as the node name
    
    # Record actual start time in registry for accurate timing
    from ..node_timing_registry import get_node_timing_registry
    registry = get_node_timing_registry()
    if registry:
        registry.record_node_start(node_name, start_time)
    
    logger.info(f"[{node_name}] Starting Phase 1 Step 3: Table Selection")
    
    try:
        # Get data source configuration from state
        data_source_config = state.get("data_source_config")
        
        if not data_source_config:
            logger.error(f"[{node_name}] No data source config found in state")
            raise DatabaseException(
                "No active data source configured. Please configure and activate a data source through the Data Source Manager."
            )
        
        data_source_type = data_source_config.get('type', 'unknown').lower()
        logger.info(f"[{node_name}] Data Source Type: {data_source_type.upper()}")
        
        # Check if SAP Datasphere - fetch catalog assets
        # Support both "sap" and "sap_datasphere" type names
        sap_datasphere_assets = None
        available_tables = []
        sap_token = None  # Token retrieved for SAP Datasphere (stored in state)
        
        if data_source_type in ("sap", "sap_datasphere"):
            logger.info(f"[{node_name}] 🔷 SAP Datasphere - Fetching catalog")
            
            user_id = state.get("user_id")
            if not user_id:
                logger.error(f"[{node_name}] ❌ Missing user_id for SAP Datasphere")
                return {
                    "selected_tables": [],
                    "errors": state.get("errors", []) + ["User ID required for SAP Datasphere"],
                    "status": "error",
                }
            
            try:
                datasphere_service = get_datasphere_service()
                
                # Retrieve and refresh token if needed (45-minute check), then store in state
                logger.info(f"[{node_name}] 🔑 Retrieving SAP Datasphere token (with 45-minute refresh check)")
                try:
                    sap_token = await datasphere_service.refresh_user_token(user_id)
                    logger.info(f"[{node_name}] ✅ Token retrieved and will be stored in state")
                except Exception as token_error:
                    logger.error(f"[{node_name}] ❌ Failed to retrieve token: {token_error}")
                    return {
                        "selected_tables": [],
                        "errors": state.get("errors", []) + [f"Failed to retrieve SAP token: {str(token_error)}"],
                        "status": "error",
                    }
                
                # Use token for catalog fetch
                assets_result = await datasphere_service.list_catalog_assets(user_id, token=sap_token)
                
                # Store simplified assets in state (name, label, data_url, metadata_url only)
                sap_datasphere_assets = {
                    "view_names": assets_result.view_names,
                    "assets": {name: asset.to_dict() for name, asset in assets_result.assets.items()}
                }
                
                available_tables = assets_result.view_names
                
                # Store token in state for reuse throughout the query
                logger.info(f"[{node_name}] 💾 Storing SAP access token in state for reuse")
                
                # Clean log: total views and first few names
                logger.info(f"[{node_name}] ✅ SAP Catalog: {len(available_tables)} views available")
                if available_tables:
                    sample = available_tables[:5]
                    logger.info(f"[{node_name}]    Sample: {', '.join(sample)}{'...' if len(available_tables) > 5 else ''}")
                
            except Exception as e:
                logger.error(f"[{node_name}] ❌ SAP Catalog fetch failed: {e}")
                return {
                    "selected_tables": [],
                    "errors": state.get("errors", []) + [f"Failed to fetch SAP Datasphere catalog: {str(e)}"],
                    "status": "error",
                }
        else:
            # Regular database - use DataSourceGateway
            logger.info(f"[{node_name}] Retrieving available tables from {data_source_type} database")
            db_client = DataSourceGateway(data_source_config)
            
            # Get available tables
            available_tables = await db_client.list_tables()
        if not available_tables:
            logger.error(f"[{node_name}] No tables found in database - cannot proceed")
            return {
                "selected_tables": [],
                "errors": state.get("errors", []) + ["No tables found in database"],
                "status": "error",
            }
        
        logger.info(f"[{node_name}] Found {len(available_tables)} available tables/views")

        # SAP/Datasphere: if the hardcoded view is not in catalog/assets, return user message (no LLM)
        if data_source_type in ("sap", "sap_datasphere"):
            resolved = _resolve_sap_view(available_tables)
            if resolved is None:
                logger.info(f"[{node_name}] SAP view '{SAP_VIEW}' not in catalog — returning user message")
                return {
                    "selected_tables": [],
                    "errors": state.get("errors", []) + [f"SAP view '{SAP_VIEW}' not available in catalog"],
                    "status": "error",
                    "sap_view_not_available_message": SAP_VIEW_NOT_AVAILABLE_MESSAGE,
                }
        
        # Extract table schema information
        table_info = {}
        table_descriptions = {}
        
        if data_source_type in ("sap", "sap_datasphere"):
            # For SAP Datasphere, we'll fetch schemas after table selection
            # For now, just use view names (schemas will be fetched later)
            logger.info(f"[{node_name}] 🔷 SAP Datasphere - will fetch schemas after table selection")
            for view_name in available_tables:
                table_info[view_name] = {
                    "table_name": view_name,
                    "columns": [],  # Will be populated after selection
                }
                table_descriptions[view_name] = {"columns": []}
        else:
            # Regular database - extract schemas now
            db_client = DataSourceGateway(data_source_config)
            for table_name in available_tables:
                try:
                    schema_str = await db_client.get_table_schema(table_name)
                    columns = []
                    for line in schema_str.split("\n"):
                        if "  - " in line:
                            col_name = line.split("  - ")[1].split(":")[0].strip()
                            if col_name:
                                columns.append(col_name)
                    
                    table_info[table_name] = {
                        "table_name": table_name,
                        "columns": columns,
                    }
                except Exception:
                    continue
            
            # Prepare table descriptions for prompt
            table_descriptions = {
                name: {"columns": info.get("columns", [])}
                for name, info in table_info.items()
            }
        
        # Reuse LLM client from state when available (single instance per request)
        llm_client = state.get("llm_client") or AzureOpenAIClient()

        # Extract data from Node 1 and Node 2
        parsed_intent = state.get("parsed_intent", {})
        user_query = state.get("user_query", "")
        identified_metrics = state.get("identified_metrics", [])
        
        logger.info(f"[{node_name}] Using {len(identified_metrics)} metric(s) from Node 2 to identify tables")
        
        # Generate prompts with metrics from Node 2
        system_prompt = TABLE_SELECTION_SYSTEM_PROMPT
        user_prompt = get_table_selection_user_prompt(
            user_message=user_query,
            available_tables=available_tables,
            table_descriptions=table_descriptions,
            parsed_intent=parsed_intent,
            identified_metrics=identified_metrics
        )
        
        # Call LLM for intelligent table selection
        from config.settings import settings
        model_name = model or settings.analytics_select_tables_model

        logger.info(f"[{node_name}] Calling LLM ({model_name}) to select optimal tables based on identified metrics")

        query_id = state.get("query_id")
        save_llm_call_input(
            node_name=node_name,
            query_id=query_id,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            extra={"model": model_name},
        )
        response = await llm_client._call_llm_unified(
            model=model_name,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            node_name=node_name,
            query_id=query_id,
            temperature=0.0,
            use_json_mode=True
        )
        
        logger.debug(f"[{node_name}] LLM response received - parsing table selection results")
        # Parse and validate response
        try:
            parsed = parse_json_response(response, expected_type=None)
            save_llm_call_output(
                node_name=node_name,
                query_id=query_id,
                raw_response=response,
                parsed=parsed,
            )
            valid_tables = _extract_selected_tables(parsed, available_tables)

            # Extract reasoning from LLM response
            # Extract reasoning from LLM response
            table_reasoning = parsed.get("table_reasoning") or parsed.get("reasoning", "") if isinstance(parsed, dict) else ""
            
            # Fallback only when LLM returned no valid tables (never use other views for SAP — hardcoded view only)
            if not valid_tables and data_source_type not in ("sap", "sap_datasphere"):
                fallback_tables = state.get("required_tables", [])
                valid_tables = [t for t in fallback_tables if t in available_tables]
                if not valid_tables and available_tables:
                    valid_tables = [available_tables[0]]

            # SAP: use only the hardcoded view (no fallback to other views)
            if data_source_type in ("sap", "sap_datasphere") and available_tables:
                resolved = _resolve_sap_view(available_tables)
                if resolved is None:
                    return {
                        "selected_tables": [],
                        "errors": state.get("errors", []) + [f"SAP view '{SAP_VIEW}' not available in catalog"],
                        "status": "error",
                        "sap_view_not_available_message": SAP_VIEW_NOT_AVAILABLE_MESSAGE,
                    }
                valid_tables = [resolved]
                logger.info(f"[{node_name}] Using SAP view: {valid_tables[0]}")

            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"[{node_name}] Table selection completed | Selected Tables: {valid_tables} | Count: {len(valid_tables)} | Duration: {duration:.2f}s")
            
            # Send summary to frontend via WebSocket with structured data
            ws_manager = state.get("ws_manager")
            if ws_manager:
                try:
                    total_columns = sum(len(schema.get("columns", [])) for schema in sap_view_schemas.values()) if sap_view_schemas else 0
                    summary_data = {
                        "table_selection_summary": {
                            "tables_selected": len(valid_tables),
                            "table_names": valid_tables,
                            "data_source_type": data_source_type,
                            "has_datasource_info": bool(datasource_info),
                            "sap_schemas_fetched": len(sap_view_schemas) if sap_view_schemas else 0,
                            "total_columns": total_columns,
                            "duration_seconds": round(duration, 2)
                        }
                    }
                    
                    await ws_manager.send_progress(
                        node_name=node_name,
                        message="Table selection complete",
                        status="complete",
                        details=f"Selected {len(valid_tables)} table(s)",
                        data=summary_data
                    )
                    logger.info(f"[{node_name}] ✅ Sent table selection summary to frontend")
                except Exception as e:
                    logger.warning(f"[{node_name}] ⚠️ Failed to send summary to frontend: {e}")
            
            # Fetch datasource info (column descriptions, usage suggestions, unique values) for selected tables
            datasource_info = {}
            if valid_tables and data_source_config:
                data_source_id = data_source_config.get('id')
                if data_source_id:
                    logger.info(f"[{node_name}] Fetching datasource info for {len(valid_tables)} selected tables")
                    datasource_info = await _fetch_datasource_info(data_source_id, valid_tables)
                    if datasource_info:
                        logger.info(f"[{node_name}] Fetched datasource info for {len(datasource_info)} tables")
                    else:
                        logger.debug(f"[{node_name}] No datasource info available for selected tables")
            
            # For SAP Datasphere, fetch view schemas after table selection (in parallel)
            sap_view_schemas = {}
            if data_source_type in ("sap", "sap_datasphere") and valid_tables and sap_datasphere_assets:
                logger.info(f"[{node_name}] 🔷 SAP Schema Fetch: {len(valid_tables)} views")
                try:
                    user_id = state.get("user_id")
                    datasphere_service = get_datasphere_service()
                    
                    # Get assets dict for metadata URLs (simplified structure)
                    assets_dict_raw = sap_datasphere_assets.get("assets", {})
                    
                    # Convert dict of dicts to dict of DatasphereAsset objects
                    from ...services.datasphere_service import DatasphereAsset
                    assets_dict = {}
                    for view_name, asset_info in assets_dict_raw.items():
                        if isinstance(asset_info, dict):
                            assets_dict[view_name] = DatasphereAsset(
                                name=asset_info.get("name", view_name),
                                label=asset_info.get("label"),
                                data_url=asset_info.get("data_url"),
                                metadata_url=asset_info.get("metadata_url"),
                            )
                    
                    # Fetch schemas in parallel (pass state so token is retrieved from state, not Key Vault)
                    schemas_result = await datasphere_service.get_multiple_view_schemas(
                        user_id=user_id,
                        view_names=valid_tables,
                        assets=assets_dict,
                        max_concurrent=5,
                        state=state
                    )
                    
                    # Store schemas with column count summary
                    total_columns = 0
                    for view_name, schema in schemas_result.items():
                        sap_view_schemas[view_name] = schema.to_dict()
                        total_columns += len(schema.columns)
                    
                    # Log summary message - prominent display
                    logger.info(
                        f"[{node_name}] 📊 SCHEMA FETCH SUMMARY: "
                        f"{len(valid_tables)} view(s) selected with {total_columns} total columns"
                    )
                    for view_name, schema in schemas_result.items():
                        logger.debug(f"[{node_name}]    {view_name}: {len(schema.columns)} columns")
                        
                except Exception as e:
                    logger.error(f"[{node_name}] ❌ SAP Schema fetch failed: {e}")
            
            logger.info(f"[{node_name}] Phase 1 Step 3 completed - proceeding to schema retrieval")

            # Prepare full output
            output = {
                "selected_tables": valid_tables,
                "datasource_info": datasource_info,
                "table_reasoning": table_reasoning,
                "status": "tables_selected",
            }
            
            # Add SAP Datasphere specific state
            if sap_datasphere_assets:
                output["sap_datasphere_assets"] = sap_datasphere_assets
            if sap_view_schemas:
                output["sap_view_schemas"] = sap_view_schemas
            
            # Store token in state if we retrieved it (for SAP Datasphere)
            if data_source_type in ("sap", "sap_datasphere") and sap_token:
                output["sap_access_token"] = sap_token

            # Log the full output from this node
            logger.info(f"[{node_name}] ========== FULL OUTPUT FROM NODE ==========")
            logger.info(f"[{node_name}] Full output (JSON formatted, first 200 chars):")
            try:
                output_json = json.dumps(output, indent=2, ensure_ascii=False, default=str)
                truncated_json = output_json[:200] + "..." if len(output_json) > 200 else output_json
                logger.info(f"[{node_name}]\n{truncated_json}")
            except Exception as json_error:
                logger.warning(f"[{node_name}] Could not format output as JSON: {json_error}")
                output_str = str(output)
                truncated_str = output_str[:200] + "..." if len(output_str) > 200 else output_str
                logger.info(f"[{node_name}] Full output (string representation, first 200 chars): {truncated_str}")
            logger.info(f"[{node_name}] ==========================================")

            return output
            
        except (json.JSONDecodeError, ValueError, KeyError) as e:
            logger.warning(f"[{node_name}] Parse failed: {e} - using fallback tables")

            # Fallback when LLM parse failed (never use other views for SAP — hardcoded view only)
            valid_tables = []
            if data_source_type not in ("sap", "sap_datasphere"):
                fallback_tables = state.get("required_tables", [])
                valid_tables = [t for t in fallback_tables if t in available_tables]
                if not valid_tables and available_tables:
                    valid_tables = [available_tables[0]]
            # SAP: use only the hardcoded view (no fallback to other views)
            if data_source_type in ("sap", "sap_datasphere"):
                if not available_tables:
                    return {
                        "selected_tables": [],
                        "errors": state.get("errors", []) + [f"SAP view '{SAP_VIEW}' not available in catalog"],
                        "status": "error",
                        "sap_view_not_available_message": SAP_VIEW_NOT_AVAILABLE_MESSAGE,
                    }
                resolved = _resolve_sap_view(available_tables)
                if resolved is None:
                    return {
                        "selected_tables": [],
                        "errors": state.get("errors", []) + [f"SAP view '{SAP_VIEW}' not available in catalog"],
                        "status": "error",
                        "sap_view_not_available_message": SAP_VIEW_NOT_AVAILABLE_MESSAGE,
                    }
                valid_tables = [resolved]
                logger.info(f"[{node_name}] Using SAP view: {valid_tables[0]}")

            # Fetch datasource info for fallback case too
            datasource_info = {}
            if valid_tables and data_source_config:
                data_source_id = data_source_config.get('id')
                if data_source_id:
                    datasource_info = await _fetch_datasource_info(data_source_id, valid_tables)

            # Prepare full output
            output = {
                "selected_tables": valid_tables,
                "datasource_info": datasource_info,
                "status": "tables_selected",
            }

            # Log the full output from this node
            logger.info(f"[{node_name}] ========== FULL OUTPUT FROM NODE ==========")
            logger.info(f"[{node_name}] Full output (JSON formatted, first 200 chars):")
            try:
                output_json = json.dumps(output, indent=2, ensure_ascii=False, default=str)
                truncated_json = output_json[:200] + "..." if len(output_json) > 200 else output_json
                logger.info(f"[{node_name}]\n{truncated_json}")
            except Exception as json_error:
                logger.warning(f"[{node_name}] Could not format output as JSON: {json_error}")
                output_str = str(output)
                truncated_str = output_str[:200] + "..." if len(output_str) > 200 else output_str
                logger.info(f"[{node_name}] Full output (string representation, first 200 chars): {truncated_str}")
            logger.info(f"[{node_name}] ==========================================")

            return output
        
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"Table selection failed after {duration:.2f}s: {str(e)}", exc_info=True)
        
        # Fallback (never use other views for SAP — hardcoded view only, or return user message)
        try:
            data_source_config = state.get("data_source_config")
            if data_source_config:
                db_client = DataSourceGateway(data_source_config)
                available_tables = await db_client.list_tables()
            else:
                logger.error(f"[{node_name}] No data source config available for fallback")
                available_tables = []
            valid_tables = []
            ds_type = (data_source_config or {}).get("type", "")
            if ds_type not in ("sap", "sap_datasphere"):
                fallback_tables = state.get("required_tables", [])
                valid_tables = [t for t in fallback_tables if t in available_tables]
                if not valid_tables and available_tables:
                    valid_tables = [available_tables[0]]
            # SAP: use only the hardcoded view (no fallback to other views)
            if ds_type in ("sap", "sap_datasphere"):
                if not available_tables:
                    return {
                        "selected_tables": [],
                        "errors": state.get("errors", []) + [f"SAP view '{SAP_VIEW}' not available in catalog"],
                        "status": "error",
                        "sap_view_not_available_message": SAP_VIEW_NOT_AVAILABLE_MESSAGE,
                    }
                resolved = _resolve_sap_view(available_tables)
                if resolved is None:
                    return {
                        "selected_tables": [],
                        "errors": state.get("errors", []) + [f"SAP view '{SAP_VIEW}' not available in catalog"],
                        "status": "error",
                        "sap_view_not_available_message": SAP_VIEW_NOT_AVAILABLE_MESSAGE,
                    }
                valid_tables = [resolved]
            
            # Fetch datasource info for exception case too
            datasource_info = {}
            if valid_tables and data_source_config:
                data_source_id = data_source_config.get('id')
                if data_source_id:
                    datasource_info = await _fetch_datasource_info(data_source_id, valid_tables)
            
            # Prepare full output
            output = {
                "selected_tables": valid_tables,
                "datasource_info": datasource_info,
                "status": "tables_selected",
            }

            # Log the full output from this node
            logger.info(f"[{node_name}] ========== FULL OUTPUT FROM NODE ==========")
            logger.info(f"[{node_name}] Full output (JSON formatted, first 200 chars):")
            try:
                output_json = json.dumps(output, indent=2, ensure_ascii=False, default=str)
                truncated_json = output_json[:200] + "..." if len(output_json) > 200 else output_json
                logger.info(f"[{node_name}]\n{truncated_json}")
            except Exception as json_error:
                logger.warning(f"[{node_name}] Could not format output as JSON: {json_error}")
                output_str = str(output)
                truncated_str = output_str[:200] + "..." if len(output_str) > 200 else output_str
                logger.info(f"[{node_name}] Full output (string representation, first 200 chars): {truncated_str}")
            logger.info(f"[{node_name}] ==========================================")

            return output
        except Exception:
            # Prepare error output
            error_output = {
                "selected_tables": [],
                "errors": state.get("errors", []) + [f"Table selection failed: {str(e)}"],
                "status": "error",
            }

            # Log the full output even on error
            logger.info(f"[{node_name}] ========== FULL OUTPUT FROM NODE (ERROR) ==========")
            logger.info(f"[{node_name}] Full output (JSON formatted):")
            try:
                output_json = json.dumps(error_output, indent=2, ensure_ascii=False, default=str)
                logger.info(f"[{node_name}]\n{output_json}")
            except Exception as json_error:
                logger.warning(f"[{node_name}] Could not format output as JSON: {json_error}")
                logger.info(f"[{node_name}] Full output (string representation): {error_output}")
            logger.info(f"[{node_name}] =================================================")

            return error_output

