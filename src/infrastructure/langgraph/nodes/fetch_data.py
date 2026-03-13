"""Data fetching node - Polars-first implementation for non-SAP data sources.

This node uses the Data Source Abstraction Layer (ConnectorFactory) to execute
queries and return data as Polars LazyFrames. Supported: clickhouse, excel, csv.
SAP Datasphere is handled by the separate sap_data_fetch node.
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
import json
import asyncio
import logging
import re

import polars as pl

from ..state import AnalyticsState
from ..data_models import DataResult, FetchIntent, pandas_to_polars
from ...datasources import ConnectorFactory
from config.settings import settings

logger = logging.getLogger(__name__)

# Maximum concurrent database queries (prevents OOM on large datasets)
MAX_CONCURRENT_QUERIES = 3


# =============================================================================
# Main Fetch Data Node
# =============================================================================

async def fetch_data_node(state: AnalyticsState) -> Dict[str, Any]:
    """Execute SQL queries and return data as Polars LazyFrames.
    
    This node handles non-SAP data sources:
    1. For Excel/CSV: Executes SQL queries on cached DataFrames
    2. For other sources: Executes SQL queries directly on the database
    3. Converts results directly to Polars LazyFrames
    4. Returns DataResults ready for lazy processing
    
    NOTE: SAP Datasphere data fetching is handled via direct API calls in sap_data_fetch_simple_node.
    
    Args:
        state: Current analytics state
            
    Returns:
        Updated state with raw_dataframes, table_data, etc.
    """
    start_time = datetime.now()
    node_name = "fetch_data"
    
    # Record timing
    from ..node_timing_registry import get_node_timing_registry
    registry = get_node_timing_registry()
    if registry:
        registry.record_node_start(node_name, start_time)
    
    logger.info(f"[{node_name}] ========== Starting Data Fetch ==========")
    
    # Proceed even if state has errors — when data is available we can get partial response
    # Get data source config
    data_source_config = state.get("data_source_config")
    if not data_source_config:
        logger.error(f"[{node_name}] ❌ No data source configuration found")
        return {"errors": ["No data source configuration"], "status": "error"}
    
    data_source_type = data_source_config.get("type", "").lower()
    logger.info(f"[{node_name}] 📊 Data Source Type: {data_source_type.upper()}")
    
    # SAP Datasphere should be routed to sap_data_fetch_node
    if data_source_type in ("sap", "sap_datasphere"):
        logger.error(f"[{node_name}] ❌ SAP Datasphere should use direct API calls via sap_data_fetch_simple_node")
        return {"errors": ["SAP data fetch should use dedicated node"], "status": "error"}
    
    # Get queries (SQL or OData)
    generated_queries = state.get("generated_queries")
    if not generated_queries:
        logger.warning(f"[{node_name}] ⚠️ No queries found in state")
        return {"errors": ["No queries generated"], "status": "error"}
    
    # Parse queries
    queries = _parse_queries(generated_queries)
    if not queries:
        logger.warning(f"[{node_name}] ⚠️ No valid queries found after parsing")
        return {"errors": ["No valid SQL queries found"], "status": "error"}
    
    logger.info(f"[{node_name}] 🔍 Parsed {len(queries)} SQL queries")
    
    # For Excel/CSV, get cached DataFrames (passed in query_plan to connector)
    cached_dataframes = None
    if data_source_type in ("excel", "csv"):
        cached_dataframes = state.get("dataframes", {})
        if not cached_dataframes:
            logger.error(f"[{node_name}] ❌ No cached DataFrames found for {data_source_type}")
            return {"errors": [f"No cached DataFrames for {data_source_type}"], "status": "error"}
        logger.info(f"[{node_name}] ✅ Found {len(cached_dataframes)} cached DataFrames")
    
    # Build query plan and execute via ConnectorFactory
    try:
        query_plan = {
            "queries": queries,
            "config": data_source_config,
            "cached_dataframes": cached_dataframes,
        }
        results = await _execute_via_connector(
            query_plan=query_plan,
            data_source_config=data_source_config,
            intent=FetchIntent.ANALYSIS,
        )
        
        if not results:
            logger.error(f"[{node_name}] ❌ All queries failed or returned no data")
            return {"errors": ["All queries failed"], "status": "error", "no_data_available": True}
        
        return await _build_fetch_result(results, cached_dataframes, node_name, start_time, state)
        
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"[{node_name}] Failed after {duration:.2f}s: {e}", exc_info=True)
        return {"errors": [f"Data fetch failed: {str(e)}"], "status": "error", "no_data_available": True}


def _parse_queries(generated_queries: Any) -> List[str]:
    """Parse queries from various formats.
    
    Args:
        generated_queries: Can be JSON string, list, dict, or single query string
            
    Returns:
        List of query strings
    """
    if not generated_queries:
        return []
    
    if isinstance(generated_queries, list):
        return [q for q in generated_queries if isinstance(q, str) and q.strip()]
    
    if isinstance(generated_queries, dict):
        return _parse_queries(generated_queries.get("queries", []))
    
    if isinstance(generated_queries, str):
        try:
            parsed = json.loads(generated_queries)
            return _parse_queries(parsed)
        except json.JSONDecodeError:
            if "SELECT" in generated_queries.upper():
                return [generated_queries]
    
    return []


async def _build_fetch_result(
    results: Dict[str, DataResult],
    cached_dataframes: Optional[Dict[str, Any]],
    node_name: str,
    start_time: datetime,
    state: AnalyticsState,
) -> Dict[str, Any]:
    """Build the final fetch result from DataResults.
    
    Args:
        results: Dict of table_name -> DataResult
        cached_dataframes: Optional cached dataframes for comparison
        node_name: Node name for logging
        start_time: Start time for duration calculation
        state: Analytics state to access ws_manager
        
    Returns:
        State update dictionary
    """
    # Check if all queries returned 0 rows
    total_rows = sum(data_result.row_count for data_result in results.values())
    
    if total_rows == 0:
        logger.error(f"[{node_name}] ❌ All queries returned 0 rows")
        return {
            "errors": ["No data available - all queries returned 0 rows"],
            "status": "error",
            "no_data_available": True,
            "table_dataframes": {},
            "table_data": {},
            "fetched_data": [],
            "fetched_data_columns": [],
        }
    
    # Process results
    table_dataframes = {}
    table_data = {}
    all_columns = set()
    
    for table_name, data_result in results.items():
        logger.info(f"[{node_name}] 📦 Processing result for table '{table_name}'...")
        
        df = data_result.collect()
        row_count = len(df)
        col_count = len(df.columns)
        
        table_dataframes[table_name] = df
        table_data[table_name] = df.to_dicts()
        all_columns.update(df.columns)
        
        # Log comparison with cached data
        if cached_dataframes and table_name in cached_dataframes:
            cached_row_count = len(cached_dataframes[table_name])
            if row_count < cached_row_count:
                reduction_pct = ((cached_row_count - row_count) / cached_row_count) * 100
                logger.info(
                    f"[{node_name}]   ✅ '{table_name}': {row_count:,} rows "
                    f"(filtered from {cached_row_count:,}, {reduction_pct:.1f}% removed)"
                )
            else:
                logger.info(f"[{node_name}]   ✅ '{table_name}': {row_count:,} rows")
        else:
            logger.info(f"[{node_name}]   ✅ '{table_name}': {row_count:,} rows, {col_count} columns")
        
        # Log sample data
        if row_count > 0:
            _log_sample_data(df, table_name, node_name)
    
    # Prepare legacy fields
    fetched_data = []
    fetched_data_columns = []
    
    if table_dataframes:
        first_table = list(table_dataframes.values())[0]
        fetched_data_columns = list(first_table.columns)
        fetched_data = first_table.to_dicts()
    
    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"[{node_name}] ========== Data Fetch Complete ==========")
    logger.info(f"[{node_name}] ⏱️  Duration: {duration:.2f}s")
    logger.info(
        f"[{node_name}] 📊 Summary: {len(table_dataframes)} table(s), "
        f"{total_rows:,} total rows, {len(all_columns)} unique columns"
    )
    
    # Send summary to frontend via WebSocket with structured data
    ws_manager = state.get("ws_manager")
    if ws_manager:
        try:
            # Build table details
            table_details = []
            for table_name, df in table_dataframes.items():
                table_details.append({
                    "table_name": table_name,
                    "rows": len(df),
                    "columns": len(df.columns),
                    "status": "success"
                })
            
            # Build summary message
            summary_message = f"Fetched data from {len(table_dataframes)} table(s), {total_rows:,} total rows"
            summary_details = f"{len(table_dataframes)} table(s), {total_rows:,} rows, {len(all_columns)} unique columns"
            
            # Prepare structured summary data for WebSocket
            summary_data = {
                "fetch_summary": {
                    "total_tables": len(table_dataframes),
                    "total_rows": total_rows,
                    "total_unique_columns": len(all_columns),
                    "duration_seconds": round(duration, 2),
                    "table_details": table_details
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
    
    return {
        "raw_dataframes": table_dataframes,
        "table_data": table_data,
        "fetched_data": fetched_data,
        "fetched_data_columns": fetched_data_columns,
        "status": "data_fetched",
    }


def _log_sample_data(df: pl.DataFrame, table_name: str, node_name: str) -> None:
    """Log sample data from a DataFrame."""
    sample_rows = df.head(3).to_dicts()
    col_count = len(df.columns)
    
    logger.info(f"[{node_name}]   📝 Sample data (first {min(3, len(sample_rows))} rows):")
    for idx, row in enumerate(sample_rows, 1):
        row_items = list(row.items())[:10]
        sample_data = {}
        for k, v in row_items:
            val_str = str(v) if v is not None else "null"
            sample_data[k] = val_str[:100] + ('...' if len(val_str) > 100 else '')
        
        logger.info(f"[{node_name}]      Row {idx}: {sample_data}")
        if col_count > 10:
            logger.info(f"[{node_name}]      ... ({col_count - 10} more columns)")


# =============================================================================
# Query Execution (Data Source Abstraction Layer)
# =============================================================================

async def _execute_via_connector(
    query_plan: Dict[str, Any],
    data_source_config: Dict[str, Any],
    intent: FetchIntent = FetchIntent.ANALYSIS,
) -> Dict[str, DataResult]:
    """Execute queries via ConnectorFactory; returns Dict[table_name, DataResult]."""
    from ...database.clickhouse import get_clickhouse_executor

    data_source_type = (data_source_config.get("type") or "").lower()
    if data_source_type == "sap_datasphere":
        data_source_type = "sap"
    logger.info(f"[fetch_data] 🔌 Using connector for {data_source_type}")

    try:
        connector = ConnectorFactory.get_connector(data_source_config)
    except Exception as e:
        logger.error(f"[fetch_data] Failed to get connector: {e}", exc_info=True)
        raise

    loop = asyncio.get_event_loop()
    executor = get_clickhouse_executor()
    try:
        # Connectors expose sync fetch_data; run in thread to avoid blocking
        table_dataframes = await loop.run_in_executor(
            executor,
            lambda: connector.fetch_data(query_plan),
        )
    finally:
        connector.close()

    if not table_dataframes:
        return {}

    # Convert Dict[str, pd.DataFrame] -> Dict[str, DataResult]
    results: Dict[str, DataResult] = {}
    for table_name, pdf in table_dataframes.items():
        if pdf is None or pdf.empty:
            results[table_name] = DataResult(
                lf=pl.DataFrame(schema={}).lazy(),
                row_count=0,
                schema={},
                table_name=table_name,
                intent=intent,
            )
            continue
        try:
            pl_df = pandas_to_polars(pdf)
        except Exception as e:
            logger.warning(f"[fetch_data] pandas_to_polars failed for {table_name}: {e}, using from_dicts")
            pl_df = pl.from_pandas(pdf)
        data_result = DataResult.from_polars_dataframe(
            df=pl_df, table_name=table_name, intent=intent
        )
        results[table_name] = data_result
        logger.info(f"[fetch_data] ✅ {table_name}: {len(pdf):,} rows")
    return results


