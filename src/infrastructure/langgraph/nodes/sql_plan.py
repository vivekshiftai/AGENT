"""SQL plan generation node - creates structured SQL plan from user intent, query, and table schemas."""
from typing import Dict, Any, List, Optional
import logging
import json
import re
from datetime import datetime
from ...llm.azure_openai import AzureOpenAIClient
from ..state import AnalyticsState
from ..prompts import QUERY_AND_TABLE_ANALYSIS_SYSTEM_PROMPT, get_query_and_table_analysis_user_prompt
from ..utils import (
    normalize_sql_plan_to_tables_format,
    parse_json_response,
    save_llm_call_input,
    save_llm_call_output,
    try_repair_truncated_json,
)
from config.settings import settings

logger = logging.getLogger(__name__)


async def sql_plan_node(state: AnalyticsState, model: str = None) -> Dict[str, Any]:
    """
    Create comprehensive SQL plan based on:
    - Selected tables schema (from Node 3/4)
    - Identified metrics from Node 2
    - User query and intent explanation from Node 1
    
    This node generates a plan (not actual SQL query) that specifies:
    - Which columns to use
    - What filters to apply
    - What aggregations to perform
    - How to group data
    - How to order results
    
    Args:
        state: Current analytics state containing:
            - user_query: Original user query
            - parsed_intent: User query and intent explanation from Node 1
            - identified_metrics: Metrics from Node 2 with metric_name, data_needed, formula
            - schema_context: Schema information for selected tables
            - selected_tables: List of selected table names
        model: Optional model name override (from graph builder)
        
    Returns:
        Updated state dictionary with:
            - sql_plan: Structured SQL plan (not actual SQL query)
            - status: "sql_plan_generated" on success
    """
    start_time = datetime.now()
    node_name = "sql_plan_synthesis"  # Graph uses "sql_plan_synthesis" as the node name
    
    # Record actual start time in registry for accurate timing
    from ..node_timing_registry import get_node_timing_registry
    registry = get_node_timing_registry()
    if registry:
        registry.record_node_start(node_name, start_time)
    
    logger.info(f"[{node_name}] Starting Phase 1 Step 4: SQL Plan Generation")
    
    if state.get("errors"):
        logger.warning(f"[{node_name}] Errors detected in state - skipping SQL plan generation")
        return {}  # Return empty update if errors exist

    user_message = state.get("user_query", "")
    schema_context = state.get("schema_context", "")
    unified_schema = state.get("unified_schema", {})  # structured schema + sample data from get_schema node
    selected_tables = state.get("selected_tables", [])
    parsed_intent = state.get("parsed_intent", {})
    identified_metrics = state.get("identified_metrics", [])
    datasource_info = state.get("datasource_info", {})  # Column descriptions, usage suggestions, unique values
    
    logger.info(f"[{node_name}] Generating SQL plan for user query using {len(selected_tables)} table(s)")
    logger.debug(f"[{node_name}] Selected tables: {selected_tables}")
    logger.debug(f"[{node_name}] User query: {user_message[:100]}{'...' if len(user_message) > 100 else ''}")
    
    # Check if we have schema information
    has_unified_schema = unified_schema and isinstance(unified_schema, dict) and unified_schema.get("tables")
    has_datasource_info = datasource_info and len(datasource_info) > 0
    has_schema_context = schema_context and len(schema_context) > 0
    
    if not selected_tables:
        logger.warning(f"[{node_name}] Missing selected tables - skipping SQL plan generation")
        return {"plan": {}}
    
    if not has_unified_schema and not has_datasource_info and not has_schema_context:
        logger.warning(f"[{node_name}] Missing unified_schema, datasource_info, and schema_context - skipping SQL plan generation")
        return {"plan": {}}
    
    if has_unified_schema:
        logger.info(f"[{node_name}] Using unified_schema (complete schema) as primary source for ALL columns")
    if has_datasource_info:
        logger.info(f"[{node_name}] Using datasource_info (column descriptions from database) to enrich column information")
    if not has_unified_schema and has_schema_context:
        logger.info(f"[{node_name}] unified_schema not available - using schema_context as fallback")
    
    try:
        llm_client = state.get("llm_client") or AzureOpenAIClient()
        
        # Build table descriptions - use unified_schema for ALL columns, enrich with datasource_info descriptions
        table_descriptions = {}
        
        # CRITICAL: Use unified_schema to get ALL columns from the schema
        # This ensures we have the complete list of available columns
        logger.info(f"[{node_name}] Building table descriptions - using unified_schema for ALL columns")
        
        for table_name in selected_tables:
            table_desc = {
                "table_name": table_name,
                "schema_description": "",
                "columns": [],
                "sample_data": None
            }
            
            # FIRST: Get ALL columns from unified_schema (complete schema)
            columns_from_unified_schema = {}
            if isinstance(unified_schema, dict):
                table_schema_entry = unified_schema.get("tables", {}).get(table_name, {})
                if isinstance(table_schema_entry, dict):
                    columns_info = table_schema_entry.get("columns", {})
                    if isinstance(columns_info, dict):
                        # Get ALL columns from unified_schema
                        for col_name, col_info in columns_info.items():
                            if isinstance(col_info, dict):
                                columns_from_unified_schema[col_name] = {
                                    "name": col_name,
                                    "type": col_info.get("type", "Unknown"),
                                    "description": ""  # Will be enriched from datasource_info if available
                                }
                        logger.info(f"[{node_name}] Found {len(columns_from_unified_schema)} columns from unified_schema for '{table_name}'")
                    
                    # Get sample data and date hints
                    table_desc["sample_data"] = table_schema_entry.get("sample_data") or None
                    table_desc["date_hints"] = table_schema_entry.get("date_hints") or None
            
            # SECOND: Enrich with column descriptions from datasource_info (if available)
            if has_datasource_info and table_name in datasource_info:
                table_cols = datasource_info[table_name]
                if isinstance(table_cols, dict):
                    for col_name, col_info in table_cols.items():
                        if isinstance(col_info, dict):
                            # If column exists in unified_schema, enrich it with description
                            if col_name in columns_from_unified_schema:
                                columns_from_unified_schema[col_name]["description"] = col_info.get('description', '')
                            else:
                                # Column in datasource_info but not in unified_schema - add it anyway
                                col_type = col_info.get('data_type', 'Unknown')
                                columns_from_unified_schema[col_name] = {
                                    "name": col_name,
                                    "type": col_type,
                                    "description": col_info.get('description', '')
                                }
                    logger.info(f"[{node_name}] Enriched columns with descriptions from datasource_info for '{table_name}'")
            
            # Convert to list format
            table_desc["columns"] = list(columns_from_unified_schema.values())
            
            # If no columns from unified_schema, fallback to datasource_info only
            if not table_desc["columns"] and has_datasource_info:
                logger.warning(f"[{node_name}] No columns from unified_schema for '{table_name}', using datasource_info only")
                if table_name in datasource_info:
                    table_cols = datasource_info[table_name]
                    if isinstance(table_cols, dict):
                        for col_name, col_info in table_cols.items():
                            if isinstance(col_info, dict):
                                col_type = col_info.get('data_type', 'Unknown')
                                table_desc["columns"].append({
                                    "name": col_name,
                                    "type": col_type,
                                    "description": col_info.get('description', '')
                                })
            
            # If still no columns, try schema_context as last resort
            if not table_desc["columns"]:
                logger.warning(f"[{node_name}] No columns from unified_schema or datasource_info for '{table_name}', trying schema_context")
                if has_schema_context and table_name in schema_context:
                    # Extract columns from schema_context (existing fallback logic)
                    table_section_start = schema_context.find(f"Table: {table_name}")
                    if table_section_start != -1:
                        next_table_start = schema_context.find("Table:", table_section_start + 1)
                        if next_table_start == -1:
                            table_section = schema_context[table_section_start:]
                        else:
                            table_section = schema_context[table_section_start:next_table_start]
                        
                        column_lines = re.findall(r'^\s+-\s+(\w+):\s+([^\n]+)', table_section, re.MULTILINE)
                        for col_name, col_type_desc in column_lines:
                            col_type = col_type_desc.split('\n')[0].split('(')[0].strip()
                            table_desc["columns"].append({
                                "name": col_name,
                                "type": col_type,
                                "description": ""
                            })
            
            logger.info(f"[{node_name}] Final column count for '{table_name}': {len(table_desc['columns'])} columns")
            table_descriptions[table_name] = table_desc
        
        # Generate SQL plan using LLM with:
        # - User query and intent explanation from Node 1
        # - Identified metrics from Node 2
        # - Selected tables and schemas from Node 3/4
        # - Column descriptions from datasource_info (stored in database)
        # Org context used only in query analysis node; downstream uses parsed_intent/plan
        system_prompt = QUERY_AND_TABLE_ANALYSIS_SYSTEM_PROMPT
        user_prompt = get_query_and_table_analysis_user_prompt(
            user_message=user_message,
            table_descriptions=table_descriptions,
            selected_tables=selected_tables,
            parsed_intent=parsed_intent,
            identified_metrics=identified_metrics,
            datasource_info=datasource_info,  # Include column descriptions from database
        )
        
        if datasource_info:
            logger.info(f"[{node_name}] Including datasource_info (column descriptions) for {len(datasource_info)} tables")
        else:
            logger.debug(f"[{node_name}] No datasource_info available - using schema_context only")
        
        model_name = model or settings.analytics_sql_plan_model
        
        logger.info(f"[{node_name}] Calling LLM ({model_name}) to generate SQL plan from metrics and table schemas")
        logger.debug(f"[{node_name}] LLM call purpose: Create structured SQL plan (not actual SQL) based on identified metrics and table schemas")
        logger.debug(f"[{node_name}] About to call LLM with json_mode=True")

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
            temperature=0.0,  # Use temperature 0.0 for deterministic output
            use_json_mode=True  # JSON format for structured plan
        )
        logger.debug(f"[{node_name}] LLM response received - parsing SQL plan")
        logger.debug(f"[{node_name}] Raw LLM response (first 1000 chars): {response[:1000]}")
        logger.debug(f"[{node_name}] Full LLM response length: {len(response)} chars")
        
        # Quick check: does the response contain "tables"?
        if '"tables"' in response or "'tables'" in response:
            logger.debug(f"[{node_name}] Response contains 'tables' key - should parse correctly")
        else:
            logger.warning(f"[{node_name}] Response does NOT contain 'tables' key - may be incorrectly formatted")
        
        # Parse JSON response; if truncated, try repair util
        try:
            parsed = None
            if not response.rstrip().endswith("}") and not response.rstrip().endswith("]"):
                parsed = try_repair_truncated_json(response)
                if parsed is not None:
                    logger.info(f"[{node_name}] Successfully closed truncated JSON")
            if parsed is None:
                parsed = parse_json_response(response, expected_type=None)

            logger.debug(f"[{node_name}] Parsed response type: {type(parsed)}")
            if isinstance(parsed, dict) and "tables" in parsed:
                tables_data = parsed["tables"]
                if isinstance(tables_data, dict):
                    logger.debug(f"[{node_name}] Tables in plan: {list(tables_data.keys())}")

            sql_plan = normalize_sql_plan_to_tables_format(parsed, selected_tables)
            save_llm_call_output(
                node_name=node_name,
                query_id=query_id,
                raw_response=response,
                parsed=sql_plan,
            )
            plan_tables = set(sql_plan.get("tables", {}).keys())
            extra_tables = plan_tables - set(selected_tables)
            if extra_tables:
                logger.warning(f"[{node_name}] Extra tables in plan (not in selected): {extra_tables}. These will be kept but may not be used.")

            # Calculate table count for logging (always per-table format now)
            table_count = len(sql_plan.get('tables', {}))
            plan_type = "per-table"

            # Deterministic relative-date override:
            # If the user asks for "last month" (or similar), anchor it to the dataset's date_hints derived from sample rows.
            try:
                q_lower = (user_message or "").lower()
                wants_last_month = any(phrase in q_lower for phrase in ["last month", "previous month", "past month", "last 30 days"])
                if wants_last_month and isinstance(unified_schema, dict):
                    tables_schema = unified_schema.get("tables", {})
                    for t_name, t_plan in (sql_plan.get("tables") or {}).items():
                        t_schema = tables_schema.get(t_name, {}) if isinstance(tables_schema, dict) else {}
                        date_hints = (t_schema.get("date_hints") or {}) if isinstance(t_schema, dict) else {}
                        date_cols = (date_hints.get("date_columns") or {}) if isinstance(date_hints, dict) else {}
                        if not date_cols or not isinstance(t_plan, dict):
                            continue

                        # Prefer "Created On" if present, else first available date-like column
                        preferred = None
                        for col_name in date_cols.keys():
                            if isinstance(col_name, str) and col_name.lower().strip() in {"created on", "created_on", "created date", "posting date", "date"}:
                                preferred = col_name
                                break
                        if not preferred:
                            preferred = next(iter(date_cols.keys()))

                        hint = date_cols.get(preferred, {}) if isinstance(date_cols.get(preferred, {}), dict) else {}
                        start_iso = hint.get("relative_last_month_start")
                        end_excl_iso = hint.get("relative_last_month_end_exclusive")
                        if not start_iso or not end_excl_iso:
                            continue

                        # Remove any existing filters on this date column and replace with deterministic last-month window
                        filters = t_plan.get("filters") or []
                        if not isinstance(filters, list):
                            filters = []
                        filters = [f for f in filters if not (isinstance(f, dict) and f.get("column") == preferred)]
                        filters.extend([
                            {"column": preferred, "operator": ">=", "value": start_iso},
                            {"column": preferred, "operator": "<", "value": end_excl_iso},
                        ])
                        t_plan["filters"] = filters
                        logger.info(
                            f"[{node_name}] Applied deterministic 'last month' date window using date_hints for table '{t_name}': "
                            f"{preferred} >= {start_iso} AND {preferred} < {end_excl_iso}"
                        )
            except Exception as e:
                logger.warning(f"[{node_name}] Failed to apply deterministic relative-date override: {e}")
            
            duration = (datetime.now() - start_time).total_seconds()

            logger.info(f"[{node_name}] SQL plan generated | Type: {plan_type} | Tables: {table_count} | Duration: {duration:.2f}s")
            logger.debug(f"[{node_name}] SQL plan keys: {list(sql_plan.keys()) if isinstance(sql_plan, dict) else 'Not a dict'}")
            if "tables" in sql_plan:
                logger.debug(f"[{node_name}] Tables in plan: {list(sql_plan['tables'].keys())}")
            else:
                logger.debug(f"[{node_name}] Single table plan - {len(sql_plan.get('columns', []))} columns")
            logger.info(f"[{node_name}] Phase 2 Step 1 completed - proceeding to SQL generation")

            # Prepare full output
            output = {
                "plan": sql_plan,
                "status": "sql_plan_generated",
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
        except (json.JSONDecodeError, ValueError) as e:
            logger.error(f"[{node_name}] Failed to parse SQL plan response: {e}")
            # Return empty plan on parse error
            output = {
                "plan": {},
                "status": "sql_plan_generated",
            }
            return output
        
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"[{node_name}] SQL plan generation failed after {duration:.2f}s: {str(e)}", exc_info=True)
        
        # Prepare full output
        output = {
            "plan": {},
            "errors": state.get("errors", []) + [f"SQL plan generation failed: {str(e)}"],
            "status": "error",
        }

        # Log the full output from this node
        logger.info(f"[{node_name}] ========== FULL OUTPUT FROM NODE (ERROR) ==========")
        logger.info(f"[{node_name}] Full output (JSON formatted):")
        try:
            output_json = json.dumps(output, indent=2, ensure_ascii=False, default=str)
            logger.info(f"[{node_name}]\n{output_json}")
        except Exception as json_error:
            logger.warning(f"[{node_name}] Could not format output as JSON: {json_error}")
            logger.info(f"[{node_name}] Full output (string representation): {output}")
        logger.info(f"[{node_name}] =================================================")

        return output

