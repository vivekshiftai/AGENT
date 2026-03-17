"""Fetch data using connector factory; store results in state.rawData.
Includes CHG-specific ClickHouse queries when CHG tables are detected."""
import logging
import re
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

from src.domain.repositories.data_repository import DataRepository
from src.application.use_cases.fetch_data_usecase import FetchDataUseCase

logger = logging.getLogger(__name__)

DATE_COLUMN_NAMES = frozenset({
    "date", "event_date", "created_at", "updated_at", "timestamp", "dt", "start", "end",
    "order_date", "plan_date", "due_date", "cleaning_date",
})
DATE_TYPE_HINTS = ("date", "datetime", "timestamp")

CHG_TABLE_PREFIX = "chg_"

# ── CHG-specific queries ────────────────────────────────────────────────

FETCH_OPEN_SALES_ORDERS = """
SELECT
    wo.work_order_id AS order_id,
    wo.linked_so AS customer_name,
    wo.planned_start AS required_ship_date,
    wo.priority,
    wo.status AS order_status,
    wo.line_id,
    wo.product_id,
    wo.product_name,
    0 AS qty_ordered_cases,
    wo.planned_qty_lbs AS qty_ordered_lbs,
    wo.planned_end AS line_ship_date,
    wo.status AS line_status,
    wo.recipe_id,
    wo.plant_id,
    wo.batch_number
FROM chg_work_orders wo
WHERE wo.status IN ('PLANNED','IN_PROGRESS')
ORDER BY
    multiIf(wo.priority='CRITICAL',1, wo.priority='HIGH',2,
            wo.priority='MEDIUM',3, 4) ASC,
    wo.planned_start ASC
"""

FETCH_MACHINE_AVAILABILITY = """
SELECT machine_id, machine_name, plant_id, line_id,
       avail_date, shift, shift_start, shift_end,
       available_hrs, status, sub_status, reason,
       downtime_min, planned_wo, planned_sku
FROM chg_machine_availability
ORDER BY machine_id, avail_date, shift
"""

FETCH_ALLERGEN_MATRIX = """
SELECT matrix_id, machine_id, machine_name, plant_id,
       from_sku, from_allergens, to_sku, to_allergens,
       clean_type, clean_duration_min, atp_swab_required,
       atp_threshold_rlu, production_hold, hold_min,
       risk_level, regulatory_basis, notes
FROM chg_allergen_clean_matrix
"""

FETCH_MRP_ALERTS = """
SELECT mrp_id, work_order_id, product_id, ingredient_id,
       ingredient_name, required_qty_lbs, stock_on_hand_lbs,
       in_transit_lbs, shortage_lbs, days_coverage,
       required_by_date, action_needed, risk_level, notes
FROM chg_mrp
WHERE risk_level IN ('RED','YELLOW')
ORDER BY risk_level DESC, required_by_date ASC
"""

FETCH_MACHINES = """
SELECT machine_id, plant_id, line_id, machine_code,
       machine_name, machine_type,
       capacity_lbs_hr, min_batch_lbs, max_batch_lbs,
       setup_time_min, clean_basic_min,
       clean_allergen_min, clean_deep_min,
       oee_percent, mtbf_hours, mttr_hours,
       current_status, last_pm_date, next_pm_date,
       allergen_clearance_req, notes
FROM chg_machines
WHERE current_status != 'OFFLINE'
"""

FETCH_EXISTING_WORK_ORDERS = """
SELECT work_order_id, product_id, product_name, recipe_id,
       plant_id, line_id, planned_qty_lbs, planned_start,
       planned_end, shift, status, priority, linked_so,
       batch_number
FROM chg_work_orders
WHERE status IN ('PLANNED','IN_PROGRESS')
ORDER BY planned_start ASC
"""

FETCH_OPEN_POS = """
SELECT po_id, supplier_id, supplier_name, ingredient_id,
       ingredient_name, ordered_qty_lbs, expected_date,
       po_status, plant_destination
FROM chg_purchase_orders
WHERE po_status IN ('CONFIRMED','IN_TRANSIT','PLACED')
ORDER BY expected_date ASC
"""

FETCH_PLANTS = """
SELECT plant_id, plant_name, plant_code, city, state_province,
       num_production_lines, capacity_lbs_per_day,
       primary_products, primary_brands, status
FROM chg_plants
WHERE status != 'CLOSED'
"""

FETCH_PRODUCTION_LINES = """
SELECT line_id, plant_id, line_name, line_code, line_type,
       allergen_status, allergen_type,
       actual_capacity_lbs_hr, status
FROM chg_production_lines
WHERE status NOT IN ('DECOMMISSIONED','OFFLINE')
"""


def _find_date_column_from_schema(repo: DataRepository, table: str) -> Optional[str]:
    try:
        schema_str = repo.get_schema(table)
    except Exception as e:
        logger.warning("DataFetchNode: could not fetch schema for table %s: %s", table, e)
        return None
    if not schema_str or not schema_str.strip():
        return None
    for line in schema_str.splitlines():
        line = line.strip()
        if not line or line.startswith("Table:"):
            continue
        match = re.match(r"^\s*(?:-\s*)?(\w+)\s*:\s*(.+)$", line)
        if not match:
            continue
        col_name_raw = (match.group(1) or "").strip()
        col_name_lower = col_name_raw.lower()
        col_type = (match.group(2) or "").strip().lower()
        if col_name_lower in DATE_COLUMN_NAMES:
            return col_name_raw
        if any(hint in col_type for hint in DATE_TYPE_HINTS):
            return col_name_raw
    return None


def _is_chg_datasource(tables: List[str]) -> bool:
    """Detect CHG datasource by presence of chg_ prefixed tables."""
    return any(str(t).lower().startswith(CHG_TABLE_PREFIX) for t in tables)


def _run_chg_query(repo: DataRepository, query: str, label: str) -> List[Dict[str, Any]]:
    """Execute a CHG-specific query and return rows as dicts."""
    try:
        result = repo.fetch_data(queries=[query])
        for table_key, df in result.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                rows = df.to_dict(orient="records")
                logger.info("DataFetchNode CHG: %s returned %d rows", label, len(rows))
                return rows
        logger.info("DataFetchNode CHG: %s returned 0 rows", label)
        return []
    except Exception as e:
        logger.warning("DataFetchNode CHG: %s query failed: %s", label, e)
        return []


def run(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    For each config in state.requiredSources, connect and fetch data.
    When CHG tables are detected, runs CHG-specific queries and populates
    chg_* state fields in addition to rawData.
    """
    if state.get("needsClarification") or state.get("rejected"):
        return {**state, "rawData": state.get("rawData") or {}}

    configs = state.get("requiredSources") or []
    date_range = state.get("dateRange") or (state.get("analysisResult") or {}).get("date_range")
    raw_data = {}
    errors = []

    chg_state_updates: Dict[str, Any] = {}

    for config in configs:
        source_name = config.get("name") or config.get("type") or "unknown"
        try:
            repo = DataRepository(config)
            use_case = FetchDataUseCase(repo)
            try:
                tables = repo.list_tables() or []
            except Exception as e:
                logger.warning("DataFetchNode: could not list tables for %s: %s", source_name, e)
                tables = []

            if tables:
                tables = sorted(
                    tables,
                    key=lambda t: (0 if str(t).lower() in ("sales_orders", "salesorders", "chg_sales_orders") else 1, str(t).lower()),
                )

            # ── CHG-specific fetch ──────────────────────────────────
            if _is_chg_datasource(tables):
                logger.info("DataFetchNode: detected CHG datasource (%s), running CHG queries", source_name)

                today = date.today()
                chg_state_updates["planning_week_start"] = today.isoformat()
                chg_state_updates["planning_week_end"] = (today + timedelta(days=7)).isoformat()

                chg_state_updates["salesOrders"] = _run_chg_query(repo, FETCH_OPEN_SALES_ORDERS, "sales_orders")
                chg_state_updates["chg_machines"] = _run_chg_query(repo, FETCH_MACHINES, "machines")
                chg_state_updates["chg_machine_availability"] = _run_chg_query(repo, FETCH_MACHINE_AVAILABILITY, "availability")
                chg_state_updates["chg_allergen_matrix"] = _run_chg_query(repo, FETCH_ALLERGEN_MATRIX, "allergen_matrix")
                chg_state_updates["chg_mrp_alerts"] = _run_chg_query(repo, FETCH_MRP_ALERTS, "mrp_alerts")
                chg_state_updates["chg_work_orders_existing"] = _run_chg_query(repo, FETCH_EXISTING_WORK_ORDERS, "work_orders")
                chg_state_updates["chg_open_pos"] = _run_chg_query(repo, FETCH_OPEN_POS, "open_pos")
                chg_state_updates["chg_plants"] = _run_chg_query(repo, FETCH_PLANTS, "plants")
                chg_state_updates["chg_production_lines"] = _run_chg_query(repo, FETCH_PRODUCTION_LINES, "production_lines")
                chg_state_updates["_is_chg"] = True

            # ── Generic table fetch ─────────────────────────────────
            start_s = (date_range or {}).get("start")
            end_s = (date_range or {}).get("end")
            have_date_range = bool(start_s and end_s)

            if not tables:
                queries = ["SELECT 1"]
                logger.info("DataFetchNode: no tables for %s", source_name)
            else:
                queries = []
                max_tables = 100
                for table in tables[:max_tables]:
                    date_col = _find_date_column_from_schema(repo, table) if have_date_range else None
                    if date_col and start_s and end_s:
                        queries.append(
                            f"SELECT * FROM {table} WHERE {date_col} >= '{start_s}' AND {date_col} <= '{end_s}' LIMIT 1000"
                        )
                    else:
                        queries.append(f"SELECT * FROM {table} LIMIT 1000")
                if len(tables) > max_tables:
                    logger.info("DataFetchNode: capped to first %s tables (of %s) for %s", max_tables, len(tables), source_name)
                logger.info("DataFetchNode: fetching %s table(s) for %s", len(queries), source_name)

            result = use_case.execute(queries=queries, date_range=date_range)
            if result.get("error"):
                errors.append(f"{source_name}: {result['error']}")
                continue
            raw_data[source_name] = result.get("data", {})
        except Exception as e:
            logger.exception("Data fetch failed for %s: %s", source_name, e)
            errors.append(f"{source_name}: {e}")

    logger.info(
        "DataFetchNode: fetched from %s source(s), rawData keys=%s, errors=%s, chg=%s",
        len(configs), list(raw_data.keys()), errors or None,
        bool(chg_state_updates.get("_is_chg")),
    )
    return {
        **state,
        "rawData": raw_data,
        "dataFetchError": "; ".join(errors) if errors else None,
        **chg_state_updates,
    }
