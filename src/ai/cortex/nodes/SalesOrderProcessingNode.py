"""Extract and process sales orders from normalized data."""
import logging
from collections import defaultdict
from datetime import date
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

SO_ORDER_ID_KEYS = ("order_id", "sales_order", "sales_order_id", "so_id", "order", "document", "doc_id")
SO_CUSTOMER_KEYS = ("customer_id", "customer", "customer_name", "sold_to", "ship_to")
SO_MATERIAL_KEYS = ("material", "material_id", "material_code", "sku", "item", "product", "product_id")
SO_QTY_KEYS = ("order_qty", "quantity", "qty", "amount", "demand")
SO_DUE_DATE_KEYS = ("due_date", "delivery_date", "required_date", "promise_date", "ship_date")
SO_PRIORITY_KEYS = ("priority", "urgency", "importance")
SO_ALLERGEN_KEYS = ("allergens", "allergen_list", "contains_allergens")


def _first_match(row_dict: dict, candidates: tuple, cols_lower: dict) -> Any:
    """Return the value for the first matching column name (case-insensitive)."""
    for key in candidates:
        real_col = cols_lower.get(key)
        if real_col is not None:
            val = row_dict.get(real_col)
            if val is not None and str(val).strip():
                return val
    return None


def _parse_date(value: Any) -> Optional[str]:
    """Parse date value to ISO string."""
    if value is None:
        return None
    if hasattr(value, "date"):
        return value.date().isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()[:10]
    if isinstance(value, str) and len(value) >= 10:
        return value[:10]
    return None


def _parse_allergens(value: Any) -> List[str]:
    """Parse allergens from various formats."""
    if not value:
        return []
    if isinstance(value, list):
        return [str(a).strip() for a in value if a]
    if isinstance(value, str):
        if "," in value:
            return [a.strip() for a in value.split(",") if a.strip()]
        if ";" in value:
            return [a.strip() for a in value.split(";") if a.strip()]
        return [value.strip()] if value.strip() else []
    return []


def run(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract sales orders from normalized data and aggregate into product targets.
    
    Sets:
    - salesOrders: list of extracted sales order dicts
    - productTargets: list of aggregated product targets
    - salesOrdersByMaterial: (updated) aggregated by material for UI
    """
    if state.get("needsClarification") or state.get("rejected"):
        return {**state}

    # CHG path: salesOrders already populated by DataFetchNode from work orders
    existing_so = state.get("salesOrders") or []
    if existing_so and state.get("_is_chg"):
        return _process_chg_sales_orders(state, existing_so)

    normalized_data = state.get("normalizedData") or []
    raw_data = state.get("rawData") or {}
    
    sales_orders: List[Dict[str, Any]] = []
    product_targets: Dict[str, Dict[str, Any]] = {}
    
    for source_name, tables in raw_data.items():
        if not isinstance(tables, dict):
            continue
        for table_name, df in tables.items():
            tn_lower = (table_name or "").lower()
            if "sales" not in tn_lower and "order" not in tn_lower and "demand" not in tn_lower:
                continue
            
            try:
                import pandas as pd
                if not isinstance(df, pd.DataFrame) or df.empty:
                    continue
                
                cols_lower = {c.lower(): c for c in df.columns}
                
                for _, row in df.iterrows():
                    row_dict = row.to_dict()
                    
                    order_id = _first_match(row_dict, SO_ORDER_ID_KEYS, cols_lower)
                    material = _first_match(row_dict, SO_MATERIAL_KEYS, cols_lower)
                    customer = _first_match(row_dict, SO_CUSTOMER_KEYS, cols_lower)
                    qty = _first_match(row_dict, SO_QTY_KEYS, cols_lower)
                    due_date = _first_match(row_dict, SO_DUE_DATE_KEYS, cols_lower)
                    priority = _first_match(row_dict, SO_PRIORITY_KEYS, cols_lower)
                    allergens = _first_match(row_dict, SO_ALLERGEN_KEYS, cols_lower)
                    
                    if not material and not order_id:
                        continue
                    
                    try:
                        qty_f = float(qty) if qty is not None else 0.0
                    except (TypeError, ValueError):
                        qty_f = 0.0
                    
                    try:
                        priority_i = int(priority) if priority is not None else 0
                    except (TypeError, ValueError):
                        priority_i = 0
                    
                    due_date_s = _parse_date(due_date)
                    allergen_list = _parse_allergens(allergens)
                    
                    so = {
                        "source": source_name,
                        "order_id": str(order_id) if order_id else None,
                        "customer_id": str(customer) if customer else None,
                        "material": str(material) if material else None,
                        "product_id": str(material) if material else None,
                        "product_name": str(material) if material else None,
                        "quantity": qty_f,
                        "due_date": due_date_s,
                        "priority": priority_i,
                        "allergens": allergen_list,
                    }
                    sales_orders.append(so)
                    
                    if material:
                        mat_key = str(material)
                        if mat_key not in product_targets:
                            product_targets[mat_key] = {
                                "product_id": mat_key,
                                "product_name": mat_key,
                                "total_quantity": 0.0,
                                "earliest_due_date": None,
                                "latest_due_date": None,
                                "priority": 0,
                                "order_count": 0,
                                "allergens": set(),
                                "source_orders": [],
                            }
                        
                        pt = product_targets[mat_key]
                        pt["total_quantity"] += qty_f
                        pt["order_count"] += 1
                        pt["priority"] = max(pt["priority"], priority_i)
                        pt["allergens"].update(allergen_list)
                        if order_id:
                            pt["source_orders"].append(str(order_id))
                        
                        if due_date_s:
                            if pt["earliest_due_date"] is None or due_date_s < pt["earliest_due_date"]:
                                pt["earliest_due_date"] = due_date_s
                            if pt["latest_due_date"] is None or due_date_s > pt["latest_due_date"]:
                                pt["latest_due_date"] = due_date_s
            
            except Exception as e:
                logger.warning("SalesOrderProcessingNode: error processing %s.%s: %s", source_name, table_name, e)
                continue

    for pt in product_targets.values():
        pt["allergens"] = list(pt["allergens"])

    product_targets_list = sorted(
        product_targets.values(),
        key=lambda x: (x.get("earliest_due_date") or "9999-12-31", -x.get("priority", 0))
    )

    logger.info(
        "SalesOrderProcessingNode: extracted %d sales orders, %d product targets",
        len(sales_orders), len(product_targets_list)
    )

    sales_orders_by_material = [
        {
            "material": pt["product_id"],
            "total_qty": pt["total_quantity"],
            "orders": pt["order_count"],
            "earliest_due": pt["earliest_due_date"],
            "latest_due": pt["latest_due_date"],
            "allergens": pt["allergens"],
        }
        for pt in product_targets_list
    ]

    return {
        **state,
        "salesOrders": sales_orders,
        "productTargets": product_targets_list,
        "salesOrdersByMaterial": sales_orders_by_material,
    }


def _process_chg_sales_orders(state: Dict[str, Any], sales_orders: List[Dict]) -> Dict[str, Any]:
    """Build productTargets from CHG sales orders (work orders)."""
    product_targets: Dict[str, Dict[str, Any]] = {}

    for so in sales_orders:
        pid = so.get("product_id")
        if not pid:
            continue

        qty = 0.0
        try:
            qty = float(so.get("qty_ordered_lbs") or so.get("planned_qty_lbs") or 0)
        except (TypeError, ValueError):
            pass

        due = _parse_date(so.get("line_ship_date") or so.get("required_ship_date") or so.get("planned_end"))
        priority_raw = so.get("priority", "MEDIUM")
        priority_i = {"CRITICAL": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1}.get(str(priority_raw).upper(), 0)

        if pid not in product_targets:
            product_targets[pid] = {
                "product_id": pid,
                "product_name": so.get("product_name", pid),
                "total_quantity": 0.0,
                "earliest_due_date": None,
                "latest_due_date": None,
                "priority": 0,
                "order_count": 0,
                "allergens": set(),
                "source_orders": [],
            }

        pt = product_targets[pid]
        pt["total_quantity"] += qty
        pt["order_count"] += 1
        pt["priority"] = max(pt["priority"], priority_i)
        oid = so.get("order_id") or so.get("work_order_id")
        if oid:
            pt["source_orders"].append(str(oid))
        if due:
            if pt["earliest_due_date"] is None or due < pt["earliest_due_date"]:
                pt["earliest_due_date"] = due
            if pt["latest_due_date"] is None or due > pt["latest_due_date"]:
                pt["latest_due_date"] = due

    for pt in product_targets.values():
        pt["allergens"] = list(pt.get("allergens") or [])

    product_targets_list = sorted(
        product_targets.values(),
        key=lambda x: (x.get("earliest_due_date") or "9999-12-31", -x.get("priority", 0)),
    )

    sales_orders_by_material = [
        {
            "material": pt["product_id"],
            "product_name": pt.get("product_name", ""),
            "total_qty": pt["total_quantity"],
            "orders": pt["order_count"],
            "earliest_due": pt["earliest_due_date"],
            "latest_due": pt["latest_due_date"],
            "allergens": pt["allergens"],
        }
        for pt in product_targets_list
    ]

    logger.info(
        "SalesOrderProcessingNode CHG: %d sales orders, %d product targets",
        len(sales_orders), len(product_targets_list),
    )

    return {
        **state,
        "salesOrders": sales_orders,
        "productTargets": product_targets_list,
        "salesOrdersByMaterial": sales_orders_by_material,
    }
