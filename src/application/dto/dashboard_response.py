"""Dashboard response DTO."""
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Dict, Any, Optional


class ChartResponse(BaseModel):
    """Chart response DTO. Kept for legacy compatibility; new flows use gantt_data on DashboardResponse."""
    title: str
    spec: Dict[str, Any]  # Minimal ECharts schema (chartType, xField, yField, groupBy, aggregation)
    data: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Pre-aggregated chart data"
    )  # Chart data (separate from schema)
    chart_type: str
    data_key: str
    alternative_types: Optional[List[str]] = None  # Alternative chart types user can switch to
    x_field: Optional[str] = None  # For chart type switching
    y_field: Optional[str] = None  # For chart type switching
    group_by: Optional[str] = None  # For chart type switching
    # Aggregation metadata fields
    data_row_count: Optional[int] = Field(
        default=None,
        description="Number of aggregated data rows"
    )
    aggregation_type: Optional[str] = Field(
        default="sum",
        description="Type of aggregation: sum, avg, count, min, max"
    )
    aggregation_status: Optional[str] = Field(
        default="completed",
        description="Status: completed, partial, failed"
    )
    aggregation_issues: Optional[List[str]] = Field(
        default_factory=list,
        description="Any warnings from aggregation"
    )
    field_validation: Optional[Dict[str, bool]] = Field(
        default_factory=dict,
        description="Which fields are valid: {field_name: true/false}"
    )
    original_row_count: Optional[int] = Field(
        default=None,
        description="Original row count before aggregation"
    )
    # Complete operational plan for this chart/metric
    operational_plan: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Complete operational plan showing all aggregations and derived calculations used for this chart/metric"
    )
    # Why the LLM chose this chart and these columns/aggregations (shown to user in UI)
    reasoning: Optional[str] = Field(
        default=None,
        description="Explanation of why this chart and these columns/aggregations were chosen for the user's question"
    )
    # Per-column reasoning: why we selected each column (metric vs group_by). For UI to show "why this column".
    column_reasons: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="List of { column, role (metric|group_by), reasoning } explaining why each column was selected"
    )

    model_config = ConfigDict(
        populate_by_name=True,
        validate_by_name=True
    )


class DashboardResponse(BaseModel):
    """
    Dashboard response DTO with segregated fields.
    
    Fields are organized into logical groups:
    - Core display: query, charts, insights, status, errors (always present)
    - Metrics data: statistics, computation_results (for KPI cards)
    - Drill-down data: sql_plan, generated_sql, selected_tables, source_data (optional, for detailed views)
    - UI configuration: date_grouping (optional, for UI features)
    
    Note: operation_plan is no longer at the top level - it's now attached per-chart/metric
    in their operational_plan field for better organization.
    """
    # Core display fields (always needed)
    query: str
    charts: List[ChartResponse]
    insights: List[str]
    status: str = Field(default="success")
    errors: List[str] = Field(default_factory=list)
    # Normal text: single message for clarification and simple responses (same format for UI)
    normal_text: Optional[str] = Field(
        default=None,
        description="Main message to display as normal text (clarification or simple summary); same format for both",
    )
    # Clarification: suggested queries as list (UI shows as buttons so user can click instead of copy/paste)
    suggested_queries: Optional[List[str]] = Field(
        default=None,
        description="When status=clarification, list of example queries the user can run with one click",
    )
    
    # Metrics data (for KPI cards)
    statistics: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Aggregated statistics for metrics display"
    )
    computation_results: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Computation results with metric, value, status, and operational_plan per metric"
    )
    
    # Drill-down data (optional, only included if available)
    sql_plan: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Structured SQL plan (columns, filters, aggregation, etc.) - only included if available"
    )
    generated_sql: Optional[str] = Field(
        default=None,
        description="Generated SQL query(ies) - only included if available"
    )
    selected_tables: Optional[List[str]] = Field(
        default=None,
        description="Tables used in the query - only included if available"
    )
    source_data: Optional[Dict[str, List[Dict[str, Any]]]] = Field(
        default=None,
        description="Source data for drill-down (table name -> list of data rows) - only included if available"
    )
    source_data_metadata: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description="Source data metadata (table name -> metadata with total_rows, preview_rows, is_truncated, columns, etc.) - only included if available"
    )
    
    # UI configuration (optional features)
    date_grouping: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Date grouping options for charts - only included if date columns detected"
    )
    
    # Gantt chart data (production planning)
    gantt_data: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Gantt data: machines (primary), charts (list of {id, title, measure, machines} for multiple Gantt charts by measure), suggested_queries"
    )

    # Planning data (optional, included in partial updates)
    operation_plan: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Operation plan from operation_specification node - included when sending metrics partial update"
    )

    # Reasoning fields (explain LLM decisions)
    table_reasoning: Optional[str] = Field(
        default=None,
        description="Explanation of why specific tables were selected for the analysis"
    )
    metric_reasoning: Optional[str] = Field(
        default=None,
        description="Explanation of why specific metrics were calculated"
    )

    # Deprecated fields (kept for backward compatibility but not populated)
    metadata: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Deprecated - kept for backward compatibility"
    )
    chart_plan: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Deprecated - chart_plan is no longer populated"
    )
    chart_reasoning: Optional[str] = Field(
        default=None,
        description="Deprecated - chart_reasoning is no longer populated"
    )
    intelligence_analysis: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Deprecated - intelligence_analysis is no longer populated"
    )

    model_config = ConfigDict(
        populate_by_name=True,
        validate_by_name=True
    )

