"""Extract human-readable step details from graph node output for progress display."""
import json
import logging
from typing import Any, Dict, Optional

import polars as pl

logger = logging.getLogger(__name__)


def extract_step_details(node_name: str, state: Dict[str, Any]) -> Optional[str]:
    """Extract meaningful summary details from node output for user display."""
    try:
        if node_name == "query_analysis":
            intent = state.get("parsed_intent", {})
            if isinstance(intent, dict):
                query_type = intent.get("query_type") or intent.get("intent_type") or "analysis"
                time_range = intent.get("time_range") or intent.get("period")
                if time_range:
                    return f"Identified as {query_type} query for {time_range}"
                return f"Identified as {query_type} query"
            return "Query analyzed successfully"

        elif node_name == "table_identification":
            tables = state.get("selected_tables", []) or state.get("required_tables", [])
            if tables:
                table_names = [t.get("name", t) if isinstance(t, dict) else str(t) for t in tables[:3]]
                suffix = f" (+{len(tables)-3} more)" if len(tables) > 3 else ""
                return f"Selected {len(tables)} tables: {', '.join(table_names)}{suffix}"
            return "Tables identified successfully"

        elif node_name == "get_schema":
            schema = state.get("schema_context")
            if schema and isinstance(schema, dict):
                table_count = len(schema)
                col_count = sum(len(v.get("columns", [])) if isinstance(v, dict) else 0 for v in schema.values())
                return f"Retrieved {table_count} table schemas with {col_count} total columns"
            return "Schema retrieved successfully"

        elif node_name == "sql_plan_synthesis":
            plan = state.get("plan", {})
            if isinstance(plan, dict):
                steps = plan.get("steps", []) or plan.get("queries", [])
                insights = plan.get("insights", []) or plan.get("metrics", [])
                if steps and insights:
                    return f"Created execution plan with {len(steps)} queries targeting {len(insights)} insights"
                elif steps:
                    return f"Created execution plan with {len(steps)} SQL queries"
                elif insights:
                    return f"Created plan targeting {len(insights)} metrics/insights"
            return "SQL plan created successfully"

        elif node_name == "sql_generation":
            queries = state.get("generated_queries", {})
            if queries:
                query_count = 0
                if isinstance(queries, list):
                    query_count = len(queries)
                elif isinstance(queries, dict) and "queries" in queries:
                    query_count = len(queries["queries"])
                elif isinstance(queries, str):
                    try:
                        queries_json = json.loads(queries)
                        if isinstance(queries_json, dict) and "queries" in queries_json:
                            query_count = len(queries_json["queries"])
                        else:
                            query_count = queries.count(";") or 1
                    except (json.JSONDecodeError, TypeError):
                        query_count = queries.count(";") or 1
                if query_count > 0:
                    return f"Generated {query_count} SQL quer{'ies' if query_count != 1 else 'y'}"
            return "SQL generated successfully"

        elif node_name == "sap_data_fetch":
            raw_dataframes = state.get("raw_dataframes", {})
            if raw_dataframes and isinstance(raw_dataframes, dict):
                table_count = len(raw_dataframes)
                total_rows = 0
                total_columns = 0
                for df in raw_dataframes.values():
                    try:
                        if hasattr(df, "collect_schema"):
                            total_columns += len(df.collect_schema().names())
                        elif hasattr(df, "columns"):
                            total_columns += len(df.columns)
                        if hasattr(df, "select"):
                            row_count = df.select(pl.len()).collect().item()
                            total_rows += row_count
                        elif hasattr(df, "__len__"):
                            total_rows += len(df)
                    except Exception as e:
                        logger.debug("Could not get row/column count for frame: %s", e)
                if table_count > 0:
                    return f"Fetched data from {table_count} SAP views: {total_rows:,} rows, {total_columns} columns"
            return "SAP data fetched successfully"

        elif node_name == "db_execution":
            raw_dataframes = state.get("raw_dataframes", {})
            if raw_dataframes and isinstance(raw_dataframes, dict):
                table_count = len(raw_dataframes)
                total_rows = sum(len(df) for df in raw_dataframes.values() if hasattr(df, "__len__"))
                total_columns = sum(
                    len(df.columns) if hasattr(df, "columns") else 0 for df in raw_dataframes.values()
                )
                if table_count > 0:
                    return f"Fetched data from {table_count} tables: {total_rows:,} rows, {total_columns} columns"
            data = state.get("fetched_data")
            columns = state.get("fetched_data_columns", [])
            if data:
                if isinstance(data, list):
                    return f"Fetched {len(data):,} rows with {len(columns)} columns"
                if hasattr(data, "__len__"):
                    return f"Fetched {len(data):,} records"
            return "Data fetched successfully"

        elif node_name == "computation_engine":
            results = state.get("computation_metrics", {})
            if isinstance(results, dict) and results:
                metric_names = list(results.keys())[:3]
                suffix = f" (+{len(results)-3} more)" if len(results) > 3 else ""
                return f"Computed {len(results)} metrics: {', '.join(metric_names)}{suffix}"
            if isinstance(results, list) and results:
                return f"Computed {len(results)} metric results"
            return "Metrics computed successfully"

        elif node_name == "analytical_summary":
            summary = state.get("analysis_summary") or state.get("narrative_summary")
            if summary:
                if isinstance(summary, str):
                    word_count = len(summary.split())
                    sentence_count = summary.count(".") + summary.count("!") + summary.count("?")
                    return f"Generated summary with {sentence_count} insights ({word_count} words)"
                return "Analysis summary generated"
            return "Summary generated successfully"

        elif node_name == "gantt_preparation":
            gantt_data = state.get("gantt_data", {})
            if gantt_data and isinstance(gantt_data, dict):
                machines = gantt_data.get("machines", [])
                total_jobs = sum(len(m.get("jobs", [])) for m in machines if isinstance(m, dict))
                if machines:
                    return f"Built Gantt chart: {len(machines)} machine(s), {total_jobs} job(s)"
            return "Gantt chart prepared"

        return None
    except Exception as e:
        logger.debug("Error extracting step details for %s: %s", node_name, e)
        return None
