"""LangGraph state definition."""
from typing import TypedDict, List, Optional, Dict, Any, Annotated
from datetime import datetime
from operator import add


def error_reducer(left: List[str], right: List[str]) -> List[str]:
    """Reducer function for errors list to handle concurrent updates."""
    if not left:
        return right if right else []
    if not right:
        return left
    return left + right


def computation_results_reducer(left: List[Dict[str, Any]], right: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Reducer function for computation_results list to handle concurrent updates from Pipeline A and B."""
    # Ensure both inputs are lists
    if not isinstance(left, list):
        left = [left] if left else []
    if not isinstance(right, list):
        right = [right] if right else []
    
    if not left:
        return right if right else []
    if not right:
        return left
    # Merge lists, avoiding duplicate metrics (keep first occurrence)
    merged = list(left)
    existing_metrics = {result.get("metric") for result in left if isinstance(result, dict) and result.get("metric")}
    for result in right:
        if isinstance(result, dict):
            metric = result.get("metric")
            if metric and metric not in existing_metrics:
                merged.append(result)
                existing_metrics.add(metric)
            elif not metric:
                # If no metric name, append anyway (might be from different source)
                merged.append(result)
    return merged


def status_reducer(left: str, right: str) -> str:
    """Reducer for status: when multiple nodes write in the same step (e.g. parallel pipelines), take the right (latest) value."""
    return right if right else (left or "")


def computation_metrics_reducer(left: Dict[str, Any], right: Dict[str, Any]) -> Dict[str, Any]:
    """Reducer function for computation_metrics dict to handle concurrent updates from Pipeline A and B."""
    # Handle None or empty dicts
    if not left or not isinstance(left, dict):
        return right if right and isinstance(right, dict) else {}
    if not right or not isinstance(right, dict):
        return left if left and isinstance(left, dict) else {}
    
    # Merge dictionaries, combining values intelligently
    merged = dict(left)
    
    for key, value in right.items():
        if key not in merged:
            # New key, just add it
            merged[key] = value
        else:
            # Key exists in both - merge based on type
            left_value = merged[key]
            
            # For numeric values that represent totals (like duration), sum them
            if key in ["total_duration_seconds", "aggregations_executed", "charts_created"]:
                if isinstance(left_value, (int, float)) and isinstance(value, (int, float)):
                    merged[key] = left_value + value
                else:
                    # If types don't match, keep the larger value or right value
                    merged[key] = max(left_value, value) if isinstance(left_value, (int, float)) and isinstance(value, (int, float)) else value
            # For row counts, use the maximum (representing the largest dataset processed)
            elif key in ["initial_rows", "final_rows"]:
                if isinstance(left_value, (int, float)) and isinstance(value, (int, float)):
                    merged[key] = max(left_value, value)
                else:
                    merged[key] = value
            # For other keys, prefer the right value (Pipeline B) or merge if both are dicts
            elif isinstance(left_value, dict) and isinstance(value, dict):
                merged[key] = {**left_value, **value}
            else:
                # For conflicts, prefer Pipeline B's value (chart_preparation)
                merged[key] = value
    
    return merged


class AnalyticsState(TypedDict):
    """State schema for LangGraph analytics workflow."""
    # Phase I: Context & Strategy
    query_id: Optional[str]  # Unique identifier for this query session
    user_query: str
    user_id: Optional[str]
    analysis_mode: Optional[str]  # Analysis mode: "normal" or "deep_research"
    user_context: Optional[str]  # User context information to help tune the response
    feedback_summary: Optional[str]  # Summary of user feedback to guide response tuning
    org_context: Optional[str]  # Organization-level context (e.g., fiscal dates, org-specific settings) for SQL queries and analysis
    previous_queries_context: Optional[List[Dict[str, Any]]]  # Context from previous queries (list of {query, response, timestamp})
    feedback: Optional[Dict[str, Any]]  # User feedback for query refinement (e.g., {type: "correction", message: "..."})
    timestamp: datetime
    data_source_config: Optional[Dict[str, Any]]  # Active data source configuration
    parsed_intent: Optional[Dict[str, Any]]  # Query analysis result
    
    # SAP Datasphere specific state
    sap_datasphere_assets: Optional[Dict[str, Any]]  # Catalog assets from SAP Datasphere (view names, data URLs, metadata URLs)
    sap_view_schemas: Optional[Dict[str, Any]]  # Column schemas for selected SAP views (from $metadata endpoint)
    sap_access_token: Optional[str]  # SAP Datasphere access token (retrieved once at query start, reused throughout)
    sap_date_columns_by_view: Optional[Dict[str, List[str]]]  # Date columns (Edm.Date type) extracted from schemas, organized by view name - used for filtering and batch queries
    sap_common_columns_by_view: Optional[Dict[str, List[str]]]  # Common columns (filter/date columns) included in every query per view - used for efficient joining when combining split queries
    required_tables: List[str]
    selected_tables: List[str]  # Tables selected based on query
    schema_context: Optional[str]  # Schema with sample data
    datasource_info: Optional[Dict[str, Any]]  # Column descriptions, usage suggestions, and unique values for each table (from data source analysis)
    sql_plan: Optional[Dict[str, Any]]  # Structured SQL plan (columns, filters, aggregation, etc.)
    plan: Optional[Dict[str, Any]]  # Unified plan (can be SQL plan or SAP fetch plan)
    sap_fetch_plan: Optional[Dict[str, Any]]  # SAP-specific fetch plan (columns, filters, batching, etc.) - stored for backward compatibility
    
    # Phase II: Construction
    dataframes: Optional[Dict[str, Any]]  # Normalized pandas DataFrames per table (from load_data) - SINGLE SOURCE OF TRUTH
    available_date_ranges: Optional[Dict[str, Dict[str, Any]]]  # Available date ranges per table (from load_data) - {table_name: {min_date, max_date, date_columns}}
    analytical_date_filter: Optional[Dict[str, Any]]  # Date filter from analytical_column_selection (YTD/user intent): {date_column, start_date, end_date} — used for SAP fetch and synced to applied_date_filters
    analytical_date_filter_by_view: Optional[Dict[str, Dict[str, Any]]]  # Per-view analytical date filter (SAP multi-view): {view_name: {date_column, start_date, end_date, value_filters?}}
    sap_api_filter_by_view: Optional[Dict[str, Dict[str, Any]]]  # Per-view SAP API filter (SAP multi-view): {view_name: {date_column, start_date, end_date, value_filters?}}
    sap_fiscal_filter: Optional[Dict[str, Any]]  # Fiscal period filter when view has no Edm.Date columns: {fiscal_column, start_value (int), end_value (int), granularity, input_parameters}
    sap_fiscal_filter_by_view: Optional[Dict[str, Dict[str, Any]]]  # Per-view fiscal filter: {view_name: {fiscal_column, start_value, end_value, granularity, input_parameters}}
    applied_date_filters: Optional[Dict[str, Any]]  # Date filters actually used for data (for summary/intelligence): {date_range: {start_date, end_date, date_column}, filter_applied: bool, filter_source: str, time_period_description?: str}
    filters: Optional[Dict[str, Any]]
    generated_sql: Optional[str]  # Generated SQL query(ies)
    generated_queries: Optional[str]  # Generated queries in JSON format (can be SQL or OData queries)
    fetched_data: Optional[List[Dict[str, Any]]]  # Actual data fetched from SQL queries
    fetched_data_columns: Optional[List[str]]  # Column names from fetched data
    table_data: Optional[Dict[str, List[Dict[str, Any]]]]  # Data organized by table name (from fetch_data)
    raw_dataframes: Optional[Dict[str, Any]]  # Polars DataFrame or LazyFrame per table (from fetch_data or sap_data_fetch)
    data_fetch_status: Optional[Dict[str, Any]]  # When fetch is partial: planned_rows, actual_rows, by_view, message for summary
    unified_schema: Optional[Dict[str, Any]]  # Unified schema from multiple sources
    
    # Production Analysis
    operation_plan: Optional[Dict[str, Any]]  # JSON operation plan
    processed_dataframe: Optional[List[Dict[str, Any]]]  # Processed data from ComputationEngineNode
    computation_execution_log: Optional[List[Dict[str, Any]]]  # Execution log from ComputationEngineNode
    computation_metrics: Annotated[Optional[Dict[str, Any]], computation_metrics_reducer]  # Execution metrics
    computation_results: Annotated[List[Dict[str, Any]], computation_results_reducer]  # Aggregation results (metric, value, status)
    analysis_summary: Optional[Dict[str, Any]]  # Summary JSON from production summary node
    suggested_metrics: Optional[List[str]]  # List of metric IDs for display filtering
    
    # Gantt chart output (production scheduling)
    gantt_data: Optional[Dict[str, Any]]  # Gantt chart payload: {machines: [{machineId, jobs: [{id, name, start, end, progress}]}], suggested_queries: [...]}
    
    # Analytical Schema (dedicated for analytical flow - independent from existing schema processing)
    analytical_dimensions: Optional[List[Dict[str, Any]]]  # Extracted dimensions: [{name, label, data_type, view_name}]
    analytical_measures: Optional[List[Dict[str, Any]]]  # Extracted measures: [{name, label, data_type, view_name}]
    filtered_analytical_dimensions: Optional[List[Dict[str, Any]]]  # LLM-selected relevant dimensions (single list)
    # Group-wise columns (canonical): measures by LLM-chosen category. Flatten for backward compatibility.
    filtered_analytical_measures_by_group: Optional[Dict[str, List[Dict[str, Any]]]]  # Canonical: category_name -> list of measure dicts (each with "category" set). Source of truth for group-wise cols.
    filtered_analytical_measures: Optional[List[Dict[str, Any]]]  # Derived: flatten(filtered_analytical_measures_by_group values); each item has "category". Use for code that expects a flat list.
    category_priorities: Optional[Dict[str, int]]  # Priority for each category (0=highest, 9=lowest) from column selection
    dimension_priorities: Optional[Dict[str, int]]  # Priority for each dimension (0=highest, 9=lowest) from column selection
    
    # Analytical Fetch Planning (dimension-based SAP fetch optimization)
    analytical_fetch_instructions: Optional[List[Dict[str, Any]]]  # Dimension-based fetch instructions: [{fetch_id, source_view, dimension, measures, select_columns, filters, chart_ids, metric_ids}]
    analytical_dataset_mapping: Optional[Dict[str, Any]]  # Maps chart_ids/metric_ids to dataset keys: {charts: {chart_id: dataset_key}, metrics: {metric_id: dataset_key}}
    
    # Legacy visualization fields (kept for backward compatibility with partial updates)
    recommended_charts: List[Dict[str, Any]]  # Chart recommendations (unused in production planning; kept for state compat)
    prepared_charts: List[Dict[str, Any]]  # Prepared charts (unused; Gantt is in gantt_data)
    
    # Orchestrator and simple flow
    orchestrator_decision: Optional[str]  # "simple" | "moderate" | "clarification"
    clarification_message: Optional[str]  # Message to show user when asking for clarification
    clarification_suggestions: Optional[List[str]]  # Suggested queries as clickable options (UI buttons)
    data_sufficiency_result: Optional[Dict[str, Any]]  # { "sufficient": bool, "reason": str, "available_data": dict } — used by data_sufficiency_check and for API response when insufficient
    no_data_available: Optional[bool]  # Set when fetch returned no rows (simple or full flow)
    sap_view_not_available_message: Optional[str]  # When set, SAP hardcoded view was not in catalog; show this as normal_text to user

    # Final Output
    orchestrated_response: Optional[Dict[str, Any]]  # Unified response combining Pipeline A & B outputs
    dashboard_response: Optional[Dict[str, Any]]  # Final dashboard response for API (from response_orchestration node)
    errors: Annotated[List[str], error_reducer]  # Use reducer to handle concurrent updates
    status: Annotated[str, status_reducer]  # Use reducer when parallel nodes (e.g. Pipeline A & B) write status

