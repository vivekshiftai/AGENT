"""SQL query generation node - generates enriched, comprehensive SQL queries from SQL plans.

NOTE: This node handles only non-SAP data sources.
SAP Datasphere OData query generation is handled by sap_data_fetch_simple_node which builds queries directly from the plan.
"""
from typing import Dict, Any, Optional, List
import logging
import json
import re
from datetime import datetime
from ...llm.azure_openai import AzureOpenAIClient
from ...database.clickhouse import ClickHouseClient
from ...database.data_source_gateway import DataSourceGateway
from shared.exceptions import SQLGenerationException, DatabaseException
from ..state import AnalyticsState
from ..prompts import (
    SQL_GENERATION_SYSTEM_PROMPT, 
    get_sql_generation_user_prompt,
)
from ..utils import clean_sql_plan, _extract_queries_from_response, save_llm_call_input, save_llm_call_output
from config.settings import settings

logger = logging.getLogger(__name__)

_ISO_DATE_COMPARISON_PATTERN = re.compile(
    r'(?P<col>"[^"]+"|\b[a-zA-Z_][a-zA-Z0-9_]*\b)\s*'
    r'(?P<op>>=|<=|<>|!=|<|>|=)\s*'
    r"(?P<q>'|\")(?P<date>\d{4}-\d{2}-\d{2})(?P=q)",
    flags=re.IGNORECASE,
)


def _extract_date_columns_from_schema_context(schema_context: str) -> Dict[str, List[str]]:
    """
    Best-effort extraction of date/datetime/timestamp columns per table from schema_context.

    Expected schema_context format includes sections like:
      Table: my_table
        - Created On: datetime
        - posted_date: date
    """
    if not schema_context or not isinstance(schema_context, str):
        return {}

    tables: Dict[str, List[str]] = {}
    current_table: Optional[str] = None

    for raw_line in schema_context.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        if line.lower().startswith("table:"):
            # "Table: name ..." -> take the first token after "Table:"
            t = line.split(":", 1)[1].strip()
            # If it includes suffix like "(CSV File)" keep only first token-ish chunk before whitespace
            # but allow underscores etc.
            current_table = t.split()[0] if t else None
            if current_table:
                tables.setdefault(current_table, [])
            continue

        if not current_table:
            continue

        # "- col: dtype"
        m = re.match(r"^-+\s*([^\:]+)\:\s*(.+)$", line)
        if not m:
            continue

        col = m.group(1).strip()
        dtype = m.group(2).strip().lower()
        if any(k in dtype for k in ("datetime", "timestamp", "date")):
            tables[current_table].append(col)

    return tables


def _enforce_safe_date_filters_in_query(sql: str, date_columns: List[str]) -> str:
    """
    Guardrail for LLM-generated SQL: enforce DuckDB-safe date comparisons.
    
    Since date columns are already normalized to datetime64[ns] (TIMESTAMP in DuckDB),
    we can compare directly with DATE literals (no CAST needed).

    Rewrites:
      col >= 'YYYY-MM-DD'  -> col >= DATE 'YYYY-MM-DD'  (convert string to DATE literal)
    Only applies when col is in the provided date_columns list (case-insensitive).
    """
    if not sql or not date_columns:
        return sql

    date_cols_norm = {str(c).strip().lower() for c in date_columns if isinstance(c, str)}

    def repl(m: re.Match) -> str:
        col = m.group("col")
        op = m.group("op")
        date_str = m.group("date")

        col_name = col.strip('"').strip().lower()
        if col_name not in date_cols_norm:
            return m.group(0)

        # Already explicit cast or function-wrapped; leave it.
        if col.upper().startswith("CAST("):
            return m.group(0)

        # Convert string literal to DATE literal (no CAST needed - columns are already TIMESTAMP)
        return f"{col} {op} DATE '{date_str}'"

    return _ISO_DATE_COMPARISON_PATTERN.sub(repl, sql)


def _postprocess_generated_sql_with_date_guardrails(
    generated_sql_json: str,
    schema_context: str,
) -> str:
    """
    Apply deterministic, schema-aware guardrails to LLM-generated SQL before execution.
    """
    try:
        parsed = json.loads(generated_sql_json) if isinstance(generated_sql_json, str) else generated_sql_json
    except Exception:
        return generated_sql_json

    if not isinstance(parsed, dict) or "queries" not in parsed:
        return generated_sql_json

    queries = parsed.get("queries") or []
    if not isinstance(queries, list) or not queries:
        return generated_sql_json

    table_date_cols = _extract_date_columns_from_schema_context(schema_context)
    if not table_date_cols:
        return generated_sql_json

    rewritten_any = False
    rewritten_queries = []

    for q in queries:
        if not isinstance(q, str):
            rewritten_queries.append(q)
            continue

        q2 = q
        # Apply all known date columns (best-effort). This is safe because we only rewrite when
        # the column name matches the schema-derived date column list.
        for _, cols in table_date_cols.items():
            q2 = _enforce_safe_date_filters_in_query(q2, cols)

        if q2 != q:
            rewritten_any = True
        rewritten_queries.append(q2)

    if rewritten_any:
        logger.info("[sql_generation] Applied date-filter guardrails: enforced CAST(... AS DATE) with DATE literals")
        parsed["queries"] = rewritten_queries
        return json.dumps(parsed)

    return generated_sql_json


async def generate_sql_node(state: AnalyticsState, model: str = None) -> Dict[str, Any]:
    """
    Generate SQL queries from SQL plan - fetch raw data only, no aggregations.
    
    This node's ONLY responsibility is to:
    - Take the SQL plan from the SQL plan node
    - Generate simple SELECT queries that fetch RAW DATA only
    - NO aggregations (SUM, AVG, COUNT, etc.)
    - NO GROUP BY clauses
    - Just SELECT columns with WHERE filters, ORDER BY, and LIMIT
    
    Args:
        state: Current analytics state containing:
            - plan: Unified plan from sql_plan_node or sap_fetch_plan_node
            - schema_context: Complete schema information
            - selected_tables: List of selected table names
        model: Optional model name override
        
    Returns:
        Updated state dictionary with:
            - generated_queries: JSON string with queries array (raw data queries only)
            - status: "sql_generated" on success, "error" on failure
    """

    start_time = datetime.now()
    node_name = "sql_generation"
    
    # Record actual start time in registry for accurate timing
    from ..node_timing_registry import get_node_timing_registry
    registry = get_node_timing_registry()
    if registry:
        registry.record_node_start(node_name, start_time)
    
    logger.info(f"[{node_name}] Starting Phase 2 Step 2: SQL Query Generation")
    
    try:
        schema_context = state.get("schema_context", "")
        selected_tables = state.get("selected_tables", [])
        plan = state.get("plan", {})
        
        if not plan:
            logger.error(f"[{node_name}] Missing plan - cannot generate SQL queries")
            output = {
                "errors": state.get("errors", []) + ["SQL generation failed: missing plan"],
                "status": "error",
            }
            # Log the full output from this node
            logger.info(f"[sql_generation] ========== FULL OUTPUT FROM NODE ==========")
            logger.info(f"[sql_generation] Full output (JSON formatted, first 200 chars):")
            try:
                output_json = json.dumps(output, indent=2, ensure_ascii=False, default=str)
                truncated_json = output_json[:200] + "..." if len(output_json) > 200 else output_json
                logger.info(f"[sql_generation]\n{truncated_json}")
            except Exception as json_error:
                logger.warning(f"[sql_generation] Could not format output as JSON: {json_error}")
                output_str = str(output)
                truncated_str = output_str[:200] + "..." if len(output_str) > 200 else output_str
                logger.info(f"[sql_generation] Full output (string representation, first 200 chars): {truncated_str}")
            logger.info(f"[sql_generation] ==========================================")
            return output
        
        logger.info(f"[{node_name}] Generating SQL queries from SQL plan")
        
        # Check if this is SAP Datasphere - should be routed to sap_odata_generation_node instead
        data_source_config = state.get("data_source_config", {})
        data_source_type = data_source_config.get("type", "").lower() if data_source_config else ""
        if data_source_type in ("sap", "sap_datasphere"):
            logger.error(f"[{node_name}] ❌ SAP Datasphere detected - this should be handled by sap_data_fetch_simple_node")
            return {
                "errors": state.get("errors", []) + ["SQL generation: SAP Datasphere should use sap_data_fetch_simple_node"],
                "status": "error",
            }
        
        if not schema_context:
            logger.debug(f"[{node_name}] Schema context missing - retrieving from database")
            # Get data source configuration from state
            data_source_config = state.get("data_source_config")
            if data_source_config:
                db_client = DataSourceGateway(data_source_config)
            else:
                logger.error(f"[{node_name}] No data source config found in state")
                raise DatabaseException(
                    "No active data source configured. Please configure and activate a data source through the Data Source Manager."
                )
            schema_parts = []
            for table in selected_tables:
                try:
                    schema_str = await db_client.get_table_schema(table)
                    schema_parts.append(schema_str)
                except Exception:
                    continue
            schema_context = "\n\n".join(schema_parts)
            # Close gateway if it was created
            if data_source_config and hasattr(db_client, 'close'):
                try:
                    db_client.close()
                except Exception:
                    pass
        
        if not schema_context or not selected_tables:
            logger.error(f"[{node_name}] Missing schema context or selected tables - cannot generate SQL")
            output = {
                "errors": state.get("errors", []) + ["SQL generation failed: missing required context"],
                "status": "error",
            }
            # Log the full output from this node
            logger.info(f"[sql_generation] ========== FULL OUTPUT FROM NODE ==========")
            logger.info(f"[sql_generation] Full output (JSON formatted, first 200 chars):")
            try:
                output_json = json.dumps(output, indent=2, ensure_ascii=False, default=str)
                truncated_json = output_json[:200] + "..." if len(output_json) > 200 else output_json
                logger.info(f"[sql_generation]\n{truncated_json}")
            except Exception as json_error:
                logger.warning(f"[sql_generation] Could not format output as JSON: {json_error}")
                output_str = str(output)
                truncated_str = output_str[:200] + "..." if len(output_str) > 200 else output_str
                logger.info(f"[sql_generation] Full output (string representation, first 200 chars): {truncated_str}")
            logger.info(f"[sql_generation] ==========================================")
            return output
        
        llm_client = state.get("llm_client") or AzureOpenAIClient()
        system_prompt = SQL_GENERATION_SYSTEM_PROMPT
        has_per_table_plans = isinstance(plan, dict) and "tables" in plan
        generated_sql = None
        
        # Get unified_schema for sample data to show LLM actual column values
        unified_schema = state.get("unified_schema", {})
        
        if has_per_table_plans:
            # Generate SQL queries in batches of 4 tables to avoid LLM overload and schema errors
            join_keys = plan.get("join_keys", [])
            valid_tables = [t for t in selected_tables if t in plan.get("tables", {})]
            total_tables = len(valid_tables)

            # Process tables in batches of 4 to avoid overwhelming the LLM and schema errors
            batch_size = 4
            all_generated_queries = []

            logger.info(f"[{node_name}] Generating SQL queries for {total_tables} table(s) in batches of {batch_size}")

            # Split tables into batches
            for batch_idx, i in enumerate(range(0, total_tables, batch_size)):
                batch_tables = valid_tables[i:i + batch_size]
                batch_num = batch_idx + 1
                total_batches = (total_tables + batch_size - 1) // batch_size

                logger.info(f"[{node_name}] Processing batch {batch_num}/{total_batches}: {len(batch_tables)} table(s) - {batch_tables}")

                # Prepare table plans for this batch
                batch_table_plans = {}
                for table_name in batch_tables:
                    table_plan = clean_sql_plan(plan["tables"][table_name])

                    # Ensure join keys are included for relationship context ONLY if they exist in this table
                    if join_keys:
                        if "columns" not in table_plan:
                            table_plan["columns"] = []
                        # Check if this table actually has the join key columns by parsing the schema
                        table_schema_start = schema_context.find(f"Table: {table_name}")
                        if table_schema_start != -1:
                            table_schema_end = schema_context.find("Table:", table_schema_start + 1)
                            if table_schema_end == -1:
                                table_schema_end = len(schema_context)
                            table_schema_section = schema_context[table_schema_start:table_schema_end]

                            for join_key in join_keys:
                                if join_key not in table_plan["columns"] and f"  - {join_key}:" in table_schema_section:
                                    table_plan["columns"].append(join_key)

                    batch_table_plans[table_name] = table_plan
                    logger.debug(f"[{node_name}] Prepared plan for table '{table_name}': {len(table_plan.get('columns', []))} columns, {len(table_plan.get('filters', []))} filters")

                # Generate SQL for this batch of tables
                # Filter schema_context to only include tables in this batch to reduce prompt size
                batch_schema_context = ""
                if schema_context:
                    schema_parts = []
                    for table_name in batch_tables:
                        # Extract schema section for this table
                        table_schema_start = schema_context.find(f"Table: {table_name}")
                        if table_schema_start != -1:
                            # Find the end - look for next "Table:" marker
                            table_schema_end = len(schema_context)
                            for other_table in selected_tables:
                                if other_table != table_name:
                                    other_start = schema_context.find(f"Table: {other_table}", table_schema_start + 1)
                                    if other_start != -1 and other_start < table_schema_end:
                                        table_schema_end = other_start
                            
                            table_schema_section = schema_context[table_schema_start:table_schema_end].strip()
                            if table_schema_section:
                                schema_parts.append(table_schema_section)
                    
                    batch_schema_context = "\n\n".join(schema_parts)

                user_prompt = get_sql_generation_user_prompt(
                    sql_plan={"tables": batch_table_plans},  # Pass batch table plans (prompt expects sql_plan parameter name)
                    database_name=settings.clickhouse_database,
                    unified_schema=None,  # No longer using sample data
                    schema_context=batch_schema_context,  # Pass filtered schema context for column validation
                )

                model_name = model or settings.analytics_generate_sql_model

                logger.info(f"[{node_name}] Calling LLM ({model_name}) for batch {batch_num}/{total_batches} - {len(batch_tables)} table(s)")
                logger.debug(f"[{node_name}] LLM call purpose: Convert SQL plans for batch {batch_num} tables into executable ClickHouse SQL queries")
                logger.debug(f"[{node_name}] About to call LLM with json_mode=True for SQL generation")

                query_start_time = datetime.now()
                query_id = state.get("query_id")
                save_llm_call_input(
                    node_name=node_name,
                    query_id=query_id,
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    extra={"model": model_name, "batch": batch_num},
                    call_suffix=f"batch_{batch_num}",
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
                query_duration = (datetime.now() - query_start_time).total_seconds()

                logger.info(f"[{node_name}] LLM response received in {query_duration:.2f}s for batch {batch_num} - extracting SQL queries")

                # Extract queries from response for this batch
                query_json = json.loads(_extract_queries_from_response(response))
                save_llm_call_output(
                    node_name=node_name,
                    query_id=query_id,
                    raw_response=response,
                    parsed=query_json,
                    call_suffix=f"batch_{batch_num}",
                )
                if query_json.get("queries"):
                    batch_query_count = len(query_json["queries"])
                    all_generated_queries.extend(query_json["queries"])
                    logger.info(f"[{node_name}] ✓ Generated {batch_query_count} SQL query/queries for batch {batch_num}/{total_batches} ({len(batch_tables)} tables)")
                else:
                    logger.warning(f"[{node_name}] ⚠ No queries generated for batch {batch_num}/{total_batches} ({batch_tables})")

            # Combine all queries from all batches
            if all_generated_queries:
                generated_sql = json.dumps({"queries": all_generated_queries})
                total_query_count = len(all_generated_queries)
                logger.info(f"[{node_name}] ✓ Generated total of {total_query_count} SQL queries across {total_batches} batch(es) for {total_tables} table(s)")
            else:
                raise SQLGenerationException(f"No queries generated for any of the {total_tables} table(s) across {total_batches} batch(es)")
        
        else:
            # Single plan - generate SQL query from the plan
            if plan and isinstance(plan, dict):
                plan = clean_sql_plan(plan)
            else:
                logger.error(f"[{node_name}] Invalid plan - cannot generate SQL queries")
                output = {
                    "errors": state.get("errors", []) + ["SQL generation failed: invalid SQL plan"],
                    "status": "error",
                }
                # Log the full output from this node
                logger.info(f"[sql_generation] ========== FULL OUTPUT FROM NODE ==========")
                logger.info(f"[sql_generation] Full output (JSON formatted):")
                try:
                    output_json = json.dumps(output, indent=2, ensure_ascii=False, default=str)
                    logger.info(f"[sql_generation]\n{output_json}")
                except Exception as json_error:
                    logger.warning(f"[sql_generation] Could not format output as JSON: {json_error}")
                    logger.info(f"[sql_generation] Full output (string representation): {output}")
                logger.info(f"[sql_generation] ==========================================")
                return output
            
            # Generate SQL query from the plan
            # SQL plan already contains all necessary information (columns, filters)
            user_prompt = get_sql_generation_user_prompt(
                sql_plan=plan,
                database_name=settings.clickhouse_database,
                unified_schema=None,  # No longer using sample data
                schema_context=schema_context,  # Pass schema context for column validation
            )
            
            model_name = model or settings.analytics_generate_sql_model
            
            logger.info(f"[{node_name}] Generating SQL query from single plan - SEQUENTIAL MODE (single query)")
            logger.info(f"[{node_name}] Calling LLM ({model_name}) to generate SQL query from plan")
            logger.debug(f"[{node_name}] LLM call purpose: Convert SQL plan into executable ClickHouse SQL query")
            logger.debug(f"[{node_name}] About to call LLM with json_mode=True for SQL generation")

            query_start_time = datetime.now()
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
            query_duration = (datetime.now() - query_start_time).total_seconds()
            logger.info(f"[{node_name}] LLM response received in {query_duration:.2f}s - extracting SQL query")
            generated_sql = _extract_queries_from_response(response)
            try:
                parsed_sql = json.loads(generated_sql)
            except json.JSONDecodeError:
                parsed_sql = {"queries": [generated_sql]}
            save_llm_call_output(
                node_name=node_name,
                query_id=query_id,
                raw_response=response,
                parsed=parsed_sql,
            )
            
            # Validate final output
            try:
                sql_json = json.loads(generated_sql)
                if not isinstance(sql_json, dict) or "queries" not in sql_json:
                    generated_sql = json.dumps({"queries": [generated_sql]})
            except json.JSONDecodeError:
                generated_sql = json.dumps({"queries": [generated_sql]})
        
        duration = (datetime.now() - start_time).total_seconds()
        query_count = len(json.loads(generated_sql).get("queries", []))
        logger.info(f"[{node_name}] SQL generation completed | Queries Generated: {query_count} | Duration: {duration:.2f}s")
        logger.info(f"[{node_name}] Phase 2 Step 2 completed - proceeding to data fetch")

        # Post-processing guardrail: enforce safe date casting/literals based on schema
        try:
            generated_sql = _postprocess_generated_sql_with_date_guardrails(
                generated_sql_json=generated_sql,
                schema_context=schema_context,
            )
        except Exception as e:
            logger.warning(f"[{node_name}] Date guardrail post-processing failed (continuing): {e}")

        
        # Prepare full output
        output = {
            "generated_queries": generated_sql,
            "status": "sql_generated",
        }

        # Log the full output from this node
        logger.info(f"[sql_generation] ========== FULL OUTPUT FROM NODE ==========")
        logger.info(f"[sql_generation] Full output (JSON formatted):")
        try:
            # Safety check: ensure output is defined
            if 'output' not in locals():
                logger.warning(f"[sql_generation] Output variable not defined, creating default output")
                output = {
                    "errors": ["Output was not properly initialized"],
                    "status": "error",
                }
            output_json = json.dumps(output, indent=2, ensure_ascii=False, default=str)
            logger.info(f"[sql_generation]\n{output_json}")
        except Exception as json_error:
            logger.warning(f"[sql_generation] Could not format output as JSON: {json_error}")
            # Safety check before logging output
            if 'output' in locals():
                logger.info(f"[sql_generation] Full output (string representation): {output}")
            else:
                logger.warning(f"[sql_generation] Output variable not available for logging")
        logger.info(f"[sql_generation] ==========================================")

        return output
        
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"[{node_name}] SQL generation failed after {duration:.2f}s: {str(e)}", exc_info=True)
        
        # Prepare full output
        output = {
            "errors": state.get("errors", []) + [f"SQL generation failed: {str(e)}"],
            "status": "error",
        }

        # Log the full output from this node
        logger.info(f"[sql_generation] ========== FULL OUTPUT FROM NODE ==========")
        logger.info(f"[sql_generation] Full output (JSON formatted):")
        try:
            output_json = json.dumps(output, indent=2, ensure_ascii=False, default=str)
            logger.info(f"[sql_generation]\n{output_json}")
        except Exception as json_error:
            logger.warning(f"[sql_generation] Could not format output as JSON: {json_error}")
            logger.info(f"[sql_generation] Full output (string representation): {output}")
        logger.info(f"[sql_generation] ==========================================")

        return output
