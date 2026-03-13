"""LangGraph nodes module.

Polars-first node implementations for high-performance analytics.

SAP Datasphere nodes are separate from non-SAP nodes:
- sap_fetch_plan_node: Generates SAP-specific fetch plan from SQL plan
- sap_data_fetch_simple_node: Simple data fetch - builds queries from plan, count first, then fetch with $top splitting

Analytical schema pipeline nodes (SAP-aware, no-op for non-SAP):
- prepare_analytical_schema_node: Parse SAP metadata XML, extract dimensions/measures/labels
- analytical_column_selection_node: LLM-driven selection of relevant columns
- analytical_fetch_plan_node: Converts analysis plans into optimized dimension-based
  SAP fetch instructions — one API call per (view, dimension) pair
"""
from .fetch_data import fetch_data_node
from .computation_engine import computation_engine_node, execute_aggregations
from .sap_fetch_plan import sap_fetch_plan_node
from .sap_data_fetch_simple import sap_data_fetch_simple_node
from .prepare_analytical_schema import prepare_analytical_schema_node
from .analytical_column_selection import analytical_column_selection_node
from .analytical_fetch_plan import analytical_fetch_plan_node
from .gantt_preparation import gantt_preparation_node

__all__ = [
    # Non-SAP nodes
    "fetch_data_node",
    "computation_engine_node",
    "execute_aggregations",
    # SAP Datasphere nodes
    "sap_fetch_plan_node",
    "sap_data_fetch_simple_node",
    # Analytical schema pipeline nodes
    "prepare_analytical_schema_node",
    "analytical_column_selection_node",
    "analytical_fetch_plan_node",
    # Gantt
    "gantt_preparation_node",
]
