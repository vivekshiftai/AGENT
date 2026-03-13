"""Schema retrieval node - fetches table schemas via Data Source Abstraction Layer or gateway."""
from typing import Dict, Any, Optional, List
import logging
import json
from datetime import datetime, date, timedelta
import re
import pandas as pd
from ...datasources import ConnectorFactory, SUPPORTED_SOURCE_TYPES
from ...database.data_source_gateway import DataSourceGateway
from ..state import AnalyticsState
from shared.exceptions import DatabaseException

logger = logging.getLogger(__name__)

def _parse_possible_date(value: Any) -> Optional[date]:
    """Parse a value into a date for sample-data hinting (CSV/Excel-friendly)."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    s = str(value).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None

    # ISO: 2025-03-31
    try:
        if re.match(r"^\d{4}-\d{2}-\d{2}", s):
            return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except Exception:
        pass

    # MM/DD/YYYY or YYYY/MM/DD
    try:
        if re.match(r"^\d{2}/\d{2}/\d{4}$", s):
            return datetime.strptime(s, "%m/%d/%Y").date()
        if re.match(r"^\d{4}/\d{2}/\d{2}$", s):
            return datetime.strptime(s, "%Y/%m/%d").date()
    except Exception:
        pass

    # Numeric-like (floats often render like 3312025.0)
    try:
        if re.match(r"^\d+(\.0)?$", s):
            n = int(float(s))
            # Excel serial date
            if 0 < n < 100000:
                return date(1899, 12, 30) + timedelta(days=n)
            # YYYYMMDD
            if 19000000 < n < 30000000:
                y = n // 10000
                m = (n % 10000) // 100
                d = n % 100
                if 1900 <= y <= 2100 and 1 <= m <= 12 and 1 <= d <= 31:
                    return date(y, m, d)
            # MDDYYYY / MMDDYYYY
            n_str = str(n)
            if len(n_str) in (6, 7, 8):
                year = int(n_str[-4:])
                prefix = n_str[:-4]
                if 1900 <= year <= 2100 and 1 <= len(prefix) <= 4:
                    padded = prefix.zfill(4)  # MMDD
                    mm = int(padded[:2])
                    dd = int(padded[2:])
                    if 1 <= mm <= 12 and 1 <= dd <= 31:
                        return date(year, mm, dd)
    except Exception:
        return None

    return None


def _compute_date_hints(sample_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute date coverage hints from up to 10 sample rows to anchor relative periods like 'last month'."""
    if not sample_rows:
        return {}

    col_dates: Dict[str, List[date]] = {}
    for row in sample_rows[:10]:
        if not isinstance(row, dict):
            continue
        for col, val in row.items():
            if not isinstance(col, str):
                continue
            if not any(k in col.lower() for k in ("date", "created", "time", "dt", " on")):
                continue
            parsed = _parse_possible_date(val)
            if parsed:
                col_dates.setdefault(col, []).append(parsed)

    if not col_dates:
        return {}

    hints: Dict[str, Any] = {"date_columns": {}}
    for col, dates_list in col_dates.items():
        min_d = min(dates_list)
        max_d = max(dates_list)

        first_of_max_month = date(max_d.year, max_d.month, 1)
        last_month_end = first_of_max_month - timedelta(days=1)
        last_month_start = date(last_month_end.year, last_month_end.month, 1)

        hints["date_columns"][col] = {
            "sample_min_date": min_d.isoformat(),
            "sample_max_date": max_d.isoformat(),
            "relative_last_month_start": last_month_start.isoformat(),
            "relative_last_month_end_exclusive": (last_month_end + timedelta(days=1)).isoformat(),
        }

    return hints


async def get_schema_node(state: AnalyticsState) -> Dict[str, Any]:
    """
    Retrieve comprehensive schema information and sample data for selected tables.
    
    This node fetches the complete schema (columns, types, descriptions) and sample
    data for each selected table to provide context for SQL plan generation.
    
    Args:
        state: Current analytics state containing:
            - selected_tables: List of table names to fetch schemas for
        
    Returns:
        Updated state dictionary with:
            - schema_context: Combined schema and sample data for all tables
            - status: "schema_retrieved" on success, "error" on failure
    """
    start_time = datetime.now()
    node_name = "get_schema"
    
    # Record actual start time in registry for accurate timing
    from ..node_timing_registry import get_node_timing_registry
    registry = get_node_timing_registry()
    if registry:
        registry.record_node_start(node_name, start_time)
    
    logger.info(f"[{node_name}] Starting Phase 1 Step 4: Schema Retrieval")
    
    selected_tables = state.get("selected_tables", [])

    if not selected_tables:
        logger.error(f"[{node_name}] No tables selected - cannot retrieve schema")
        return {
            "schema_context": "",
            "errors": state.get("errors", []) + ["No tables selected for schema retrieval"],
            "status": "error",
        }
    
    logger.info(f"[{node_name}] Retrieving schema and sample data for {len(selected_tables)} table(s): {', '.join(selected_tables)}")
    
    try:
        # Check if we have cached DataFrames (from load_data_node)
        dataframes = state.get("dataframes", {})
        data_source_config = state.get("data_source_config")
        data_source_type = data_source_config.get("type", "").lower() if data_source_config else ""
        
        # Use Data Source Abstraction Layer for clickhouse, excel, csv, sap
        normalized_type = "sap" if data_source_type in ("sap", "sap_datasphere") else data_source_type
        if normalized_type in SUPPORTED_SOURCE_TYPES:
            try:
                connector_kwargs = {}
                if normalized_type == "sap":
                    connector_kwargs["sap_view_schemas"] = state.get("sap_view_schemas", {})
                    connector_kwargs["sap_access_token"] = state.get("sap_access_token")
                connector = ConnectorFactory.get_connector(data_source_config, **connector_kwargs)
                schema_parts = []
                unified_schema = {"tables": {}}
                for table_name in selected_tables:
                    try:
                        logger.debug(f"[{node_name}] Fetching schema via connector for table: {table_name}")
                        schema_str = connector.get_schema(table_name)
                        schema_parts.append(schema_str)
                        columns_info = {}
                        for line in schema_str.split("\n"):
                            if ":" in line and not line.strip().startswith("Table:"):
                                parts = line.strip().lstrip("- ").split(":", 1)
                                if len(parts) >= 2:
                                    col_name = parts[0].strip()
                                    col_type = parts[1].strip()
                                    columns_info[col_name] = {"type": col_type, "nullable": True}
                        unified_schema["tables"][table_name] = {"columns": columns_info}
                    except Exception as e:
                        logger.warning(f"[{node_name}] Failed to get schema for table {table_name}: {str(e)}")
                        continue
                connector.close()
                schema_context = "\n\n".join(schema_parts)
                duration = (datetime.now() - start_time).total_seconds()
                logger.info(
                    f"[{node_name}] Schema retrieval completed via connector | Type: {normalized_type} | "
                    f"Tables: {len(selected_tables)} | Duration: {duration:.2f}s"
                )
                return {
                    "schema_context": schema_context,
                    "unified_schema": unified_schema,
                    "status": "schema_retrieved",
                }
            except Exception as e:
                logger.warning(f"[{node_name}] Connector path failed: {e}", exc_info=True)

        # For postgres, mysql, or other types: use gateway
        if data_source_config:
            db_client = DataSourceGateway(data_source_config)
        else:
            # No data source configured
            logger.error(f"[{node_name}] No data source config found in state")
            raise DatabaseException(
                "No active data source configured. Please configure and activate a data source through the Data Source Manager."
            )
        
        schema_parts = []
        unified_schema = {"tables": {}}  # Build structured schema format
        
        # Fetch schema and sample data for each selected table
        for table_name in selected_tables:
            try:
                logger.debug(f"[{node_name}] Fetching schema for table: {table_name}")
                schema_str = await db_client.get_table_schema(table_name)
                schema_parts.append(schema_str)
                
                # Parse schema string to build structured format
                columns_info = {}
                for line in schema_str.split('\n'):
                    if ':' in line and not line.strip().startswith('Table:'):
                        # Format: "  - column_name: data_type"
                        parts = line.strip().lstrip('- ').split(':', 1)
                        if len(parts) >= 2:
                            col_name = parts[0].strip()
                            col_type = parts[1].strip()
                            columns_info[col_name] = {
                                "type": col_type,
                                "nullable": True  # Default to nullable
                            }
                
                # No longer collecting sample data - we have column names
                unified_schema["tables"][table_name] = {
                    "columns": columns_info,
                }
                
            except Exception as e:
                logger.warning(f"[{node_name}] Failed to retrieve schema/data for table {table_name}: {str(e)}")
                continue
        
        # Combine schema only (no sample data needed - we have column names)
        schema_context = "\n\n".join(schema_parts)
        
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"[{node_name}] Schema retrieval completed | Tables: {len(selected_tables)} | Duration: {duration:.2f}s")
        logger.info(f"[{node_name}] Built unified_schema for {len(unified_schema['tables'])} tables")
        logger.info(f"[{node_name}] Phase 1 Step 4 completed - proceeding to SQL plan generation")
        
        # Close gateway if it was created
        if data_source_config and hasattr(db_client, 'close'):
            try:
                db_client.close()
            except Exception:
                pass

        # Prepare full output
        output = {
            "schema_context": schema_context,
            "unified_schema": unified_schema,  # Add structured schema format
            "status": "schema_retrieved",
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
        logger.error(f"[{node_name}] Schema retrieval failed after {duration:.2f}s: {str(e)}", exc_info=True)

        # Prepare error output
        error_output = {
            "schema_context": "",
            "unified_schema": {"tables": {}},  # Empty unified_schema on error
            "errors": state.get("errors", []) + [f"Failed to retrieve schema: {str(e)}"],
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

