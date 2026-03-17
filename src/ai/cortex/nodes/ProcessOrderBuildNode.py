"""Convert Sales Order line items + recipes into concrete Process Orders.
Each PO = one batch of a recipe, assigned to a plant.
Groups PO steps by machine_id to build per-machine queues.
Pure Python — no LLM needed here."""
import logging
import math
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

PRIORITY_ORDER = {"CRITICAL": 1, "HIGH": 2, "MEDIUM": 3, "LOW": 4}


def _to_date_str(val) -> str:
    """Convert a date/datetime/string to ISO date string for sorting."""
    if val is None:
        return ""
    if hasattr(val, "isoformat"):
        return val.isoformat()[:10]
    return str(val)[:10]


def run(state: Dict[str, Any]) -> Dict[str, Any]:
    if state.get("needsClarification") or state.get("rejected"):
        return {**state}

    is_chg = state.get("_is_chg", False)
    if not is_chg:
        return {**state}

    sales_orders = state.get("salesOrders") or []
    recipes_by_product = state.get("recipes") or {}
    process_orders: List[Dict[str, Any]] = []
    exceptions: List[Dict[str, Any]] = list(state.get("scheduling_exceptions") or [])

    for so_item in sales_orders:
        product_id = so_item.get("product_id")
        if not product_id:
            continue
        recipe = recipes_by_product.get(product_id)
        if not recipe:
            exceptions.append({
                "type": "NO_RECIPE",
                "severity": "HIGH",
                "message": f"No active recipe found for {product_id} ({so_item.get('product_name', '')})",
                "so_id": so_item.get("order_id"),
            })
            continue

        qty_lbs = float(
            so_item.get("qty_ordered_lbs")
            or so_item.get("planned_qty_lbs")
            or so_item.get("quantity")
            or so_item.get("qty_ordered_cases", 0)
            or 0
        )
        batch_size = float(recipe.get("batch_size_lbs") or 1000)
        if qty_lbs <= 0:
            continue
        num_batches = math.ceil(qty_lbs / batch_size)

        for batch_num in range(1, num_batches + 1):
            qty_this_batch = min(batch_size, qty_lbs - (batch_num - 1) * batch_size)
            po = {
                "process_order_id": f"PO-{so_item.get('order_id', 'X')}-{batch_num}",
                "sales_order_id": so_item.get("order_id"),
                "sales_order_line": so_item.get("line_id"),
                "customer_name": so_item.get("customer_name"),
                "product_id": product_id,
                "product_name": so_item.get("product_name", product_id),
                "recipe_id": recipe.get("recipe_id"),
                "plant_id": recipe.get("plant_id"),
                "batch_number": batch_num,
                "total_batches": num_batches,
                "qty_lbs": round(qty_this_batch, 2),
                "priority": so_item.get("priority", "MEDIUM"),
                "required_by": _to_date_str(so_item.get("line_ship_date") or so_item.get("required_ship_date") or so_item.get("planned_end")),
                "allergens": [a.strip() for a in (recipe.get("allergens_present") or "").split(",") if a.strip()],
                "steps": recipe.get("steps") or [],
            }
            process_orders.append(po)

    # Build per-machine queues from process order steps
    machine_queues: Dict[str, List[Dict[str, Any]]] = {}
    for po in process_orders:
        for step in po["steps"]:
            mid = step.get("machine_id", "MANUAL")
            if mid == "MANUAL":
                continue
            machine_queues.setdefault(mid, []).append({
                "process_order_id": po["process_order_id"],
                "product_id": po["product_id"],
                "product_name": po["product_name"],
                "allergens": po["allergens"],
                "priority": po["priority"],
                "required_by": po["required_by"],
                "customer_name": po["customer_name"],
                "batch_number": po["batch_number"],
                "total_batches": po["total_batches"],
                "qty_lbs": po["qty_lbs"],
                "step_number": step.get("step_number", 1),
                "step_name": step.get("step_name", ""),
                "step_type": step.get("step_type", "PROCESSING"),
                "duration_min": step.get("duration_min", 0),
                "wait_after_min": step.get("wait_after_min", 0),
                "qa_check_required": step.get("qa_check_required", 0),
                "operator_notes": step.get("operator_notes", ""),
            })

    for mid in machine_queues:
        machine_queues[mid].sort(
            key=lambda x: (
                PRIORITY_ORDER.get(x.get("priority", "MEDIUM"), 5),
                x.get("required_by") or "9999",
            )
        )

    logger.info(
        "ProcessOrderBuildNode: %d process orders, %d machine queues, %d exceptions",
        len(process_orders), len(machine_queues), len(exceptions),
    )

    return {
        **state,
        "process_orders": process_orders,
        "machine_queues": machine_queues,
        "scheduling_exceptions": exceptions,
    }
