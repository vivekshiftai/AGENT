"""Convert raw data from connectors into unified records preserving all columns."""
import logging
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

PLANT_KEYS = ("plant", "plant_id", "plant_name", "factory", "site")
LINE_KEYS = ("line", "line_id", "production_line", "line_name")
MACHINE_KEYS = ("machine", "machine_id", "machine_name", "equipment", "equipment_id")
PRODUCT_KEYS = ("product", "product_id", "product_name", "item", "item_id", "sku", "material")
QTY_KEYS = ("quantity", "qty", "amount", "demand", "order_qty", "planned_qty")
DATE_KEYS = ("date", "order_date", "plan_date", "due_date", "start_date", "created_at", "scheduled_date")
CLEANING_KEYS = ("cleaning_time", "clean_time", "changeover_time", "setup_time", "cleaning_duration")

# Sales orders (for user-visible order-material grouping)
SO_ORDER_ID_KEYS = ("order_id", "sales_order", "sales_order_id", "so_id", "order", "document", "doc_id")
SO_MATERIAL_KEYS = ("material", "material_id", "material_code", "sku", "item", "product", "fg_material", "finished_product")
SO_PARENT_MATERIAL_KEYS = ("parent_material", "parent_sku", "parent_item", "parent_product", "parent_material_id")
SO_FG_KEYS = ("finished_product", "fg_product", "fg_material", "fg_sku")
SO_QTY_KEYS = ("order_qty", "quantity", "qty", "amount", "demand")
SO_DUE_DATE_KEYS = ("due_date", "delivery_date", "required_date", "promise_date", "ship_date", "order_date")


def _first_match(row_dict: dict, candidates: tuple, cols_lower: dict) -> Any:
    """Return the value for the first matching column name (case-insensitive)."""
    for key in candidates:
        real_col = cols_lower.get(key)
        if real_col is not None:
            val = row_dict.get(real_col)
            if val is not None and str(val).strip():
                return val
    return None


def run(state: Dict[str, Any]) -> Dict[str, Any]:
    if state.get("needsClarification") or state.get("rejected"):
        return {**state, "normalizedData": state.get("normalizedData") or []}

    raw_data = state.get("rawData") or {}
    normalized: List[Dict[str, Any]] = []
    table_schemas: Dict[str, List[str]] = {}
    sales_orders_by_material: Dict[str, Dict[str, Any]] = {}
    sales_orders_rows: List[Dict[str, Any]] = []

    for source_name, tables in raw_data.items():
        if not isinstance(tables, dict):
            continue
        for table_name, df in tables.items():
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue
            cols_lower = {c.lower(): c for c in df.columns}
            table_schemas[table_name] = list(df.columns)
            for _, row in df.iterrows():
                record = _row_to_record(row, source_name, table_name, cols_lower)
                if record:
                    normalized.append(record)

                so = _row_to_sales_order(row, source_name, table_name, cols_lower)
                if so:
                    sales_orders_rows.append(so)
                    mat = so.get("material") or "Unknown"
                    bucket = sales_orders_by_material.setdefault(
                        mat,
                        {
                            "material": mat,
                            "parent_material": None,
                            "finished_product": None,
                            "total_qty": 0.0,
                            "orders": 0,
                            "earliest_due": None,
                            "latest_due": None,
                        },
                    )
                    bucket["orders"] += 1
                    bucket["total_qty"] += float(so.get("quantity") or 0)
                    if so.get("parent_material"):
                        bucket["parent_material"] = bucket["parent_material"] or so.get("parent_material")
                    if so.get("finished_product"):
                        bucket["finished_product"] = bucket["finished_product"] or so.get("finished_product")
                    due = so.get("due_date")
                    if due:
                        bucket["earliest_due"] = min(bucket["earliest_due"], due) if bucket["earliest_due"] else due
                        bucket["latest_due"] = max(bucket["latest_due"], due) if bucket["latest_due"] else due

    logger.info(
        "DataNormalizationNode: normalized %s records from %s raw source(s), tables=%s",
        len(normalized), len(raw_data), list(table_schemas.keys()),
    )
    if sales_orders_by_material:
        logger.info(
            "DataNormalizationNode: extracted %s sales order row(s), %s material group(s)",
            len(sales_orders_rows),
            len(sales_orders_by_material),
        )
    updates: Dict[str, Any] = {
        "normalizedData": normalized,
        "tableSchemas": table_schemas,
        "salesOrdersByMaterial": list(sales_orders_by_material.values()),
    }
    if not state.get("_is_chg"):
        updates["salesOrders"] = sales_orders_rows

    return {**state, **updates}


def _row_to_record(row: Any, source: str, table_name: str, cols_lower: dict) -> Dict[str, Any]:
    try:
        row_dict = row.to_dict() if hasattr(row, "to_dict") else dict(row)
    except Exception:
        return {}

    plant = _first_match(row_dict, PLANT_KEYS, cols_lower)
    line = _first_match(row_dict, LINE_KEYS, cols_lower)
    machine = _first_match(row_dict, MACHINE_KEYS, cols_lower)
    product = _first_match(row_dict, PRODUCT_KEYS, cols_lower)
    qty = _first_match(row_dict, QTY_KEYS, cols_lower)
    d = _first_match(row_dict, DATE_KEYS, cols_lower)
    cleaning_time = _first_match(row_dict, CLEANING_KEYS, cols_lower)

    try:
        qty = float(qty) if qty is not None else 0
    except (TypeError, ValueError):
        qty = 0

    if hasattr(d, "date"):
        d = d.date().isoformat()
    elif isinstance(d, str) and len(d) >= 10:
        d = d[:10]
    elif isinstance(d, (tuple, list)) and len(d) >= 3:
        d = "%s-%s-%s" % (d[0], d[1], d[2])
        d = d[:10]
    else:
        d = date.today().isoformat()

    try:
        cleaning_time = float(cleaning_time) if cleaning_time is not None else None
    except (TypeError, ValueError):
        cleaning_time = None

    if not product:
        cols = list(row_dict.keys())
        product = row_dict.get(cols[0]) if cols else ""

    return {
        "source": source,
        "table": table_name,
        "plant": str(plant) if plant else None,
        "line": str(line) if line else None,
        "machine": str(machine) if machine else None,
        "product": str(product),
        "quantity": qty,
        "date": d,
        "cleaning_time": cleaning_time,
        "raw": {k: (str(v)[:100] if v is not None else None) for k, v in list(row_dict.items())[:20]},
    }


def _row_to_sales_order(row: Any, source: str, table_name: str, cols_lower: dict) -> Optional[Dict[str, Any]]:
    """
    Best-effort extract a SalesOrder row. Returns None if row doesn't look like a sales order.
    """
    tn = (table_name or "").lower()
    if "sales" not in tn and "order" not in tn:
        return None
    try:
        row_dict = row.to_dict() if hasattr(row, "to_dict") else dict(row)
    except Exception:
        return None

    order_id = _first_match(row_dict, SO_ORDER_ID_KEYS, cols_lower)
    material = _first_match(row_dict, SO_MATERIAL_KEYS, cols_lower)
    parent_material = _first_match(row_dict, SO_PARENT_MATERIAL_KEYS, cols_lower)
    finished_product = _first_match(row_dict, SO_FG_KEYS, cols_lower)
    qty = _first_match(row_dict, SO_QTY_KEYS, cols_lower)
    due = _first_match(row_dict, SO_DUE_DATE_KEYS, cols_lower)

    # If there's no material and no order id, it's likely not a sales order
    if not material and not order_id:
        return None

    try:
        qty_f = float(qty) if qty is not None else 0.0
    except (TypeError, ValueError):
        qty_f = 0.0

    if hasattr(due, "date"):
        due_s = due.date().isoformat()
    elif isinstance(due, str) and len(due) >= 10:
        due_s = due[:10]
    elif isinstance(due, (tuple, list)) and len(due) >= 3:
        due_s = ("%s-%s-%s" % (due[0], due[1], due[2]))[:10]
    else:
        due_s = None

    return {
        "source": source,
        "order_id": str(order_id) if order_id is not None else None,
        "material": str(material) if material is not None else None,
        "parent_material": str(parent_material) if parent_material is not None else None,
        "finished_product": str(finished_product) if finished_product is not None else None,
        "quantity": qty_f,
        "due_date": due_s,
    }
