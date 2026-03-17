"""Check inventory levels for raw materials and finished goods."""
import logging
from typing import Any, Dict, List, Optional

from src.domain.services.inventory_service import InventoryService
from src.domain.services.recipe_service import RecipeService
from src.domain.entities.recipe import Recipe

logger = logging.getLogger(__name__)

INVENTORY_TABLE_HINTS = ("inventory", "stock", "on_hand", "warehouse", "storage")
ITEM_KEYS = ("item_id", "material_id", "product_id", "sku", "material", "product")
QTY_KEYS = ("quantity", "qty", "on_hand", "available", "stock_qty")
LOCATION_KEYS = ("location", "location_id", "warehouse", "plant", "site")


def run(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Check inventory levels for materials needed by recipes.
    
    Sets:
    - inventory: dict of item_id -> available quantity
    - inventoryCheck: dict with availability status per material
    """
    if state.get("needsClarification") or state.get("rejected"):
        return {**state}

    product_targets = state.get("productTargets") or []
    recipes = state.get("recipes") or {}
    raw_data = state.get("rawData") or {}
    
    inventory_records = []
    for source_name, tables in raw_data.items():
        if not isinstance(tables, dict):
            continue
        for table_name, df in tables.items():
            tn_lower = (table_name or "").lower()
            if not any(hint in tn_lower for hint in INVENTORY_TABLE_HINTS):
                continue
            
            try:
                import pandas as pd
                if not isinstance(df, pd.DataFrame) or df.empty:
                    continue
                
                cols_lower = {c.lower(): c for c in df.columns}
                
                for _, row in df.iterrows():
                    row_dict = row.to_dict()
                    
                    item_id = None
                    for key in ITEM_KEYS:
                        real_col = cols_lower.get(key)
                        if real_col and row_dict.get(real_col):
                            item_id = str(row_dict[real_col])
                            break
                    
                    qty = 0.0
                    for key in QTY_KEYS:
                        real_col = cols_lower.get(key)
                        if real_col and row_dict.get(real_col) is not None:
                            try:
                                qty = float(row_dict[real_col])
                            except (TypeError, ValueError):
                                pass
                            break
                    
                    location = None
                    for key in LOCATION_KEYS:
                        real_col = cols_lower.get(key)
                        if real_col and row_dict.get(real_col):
                            location = str(row_dict[real_col])
                            break
                    
                    if item_id:
                        inventory_records.append({
                            "item_id": item_id,
                            "quantity": qty,
                            "location": location or "default",
                        })
            except Exception as e:
                logger.warning("InventoryCheckNode: error reading %s.%s: %s", source_name, table_name, e)

    inventory_service = InventoryService(inventory_records)
    logger.info("InventoryCheckNode: loaded %d inventory records", len(inventory_records))

    inventory_dict = {}
    for item_id in set(r["item_id"] for r in inventory_records):
        inventory_dict[item_id] = inventory_service.get_available_quantity(item_id)

    inventory_check = {
        "materials": {},
        "shortages": [],
        "sufficient_count": 0,
        "shortage_count": 0,
    }

    recipe_service = state.get("_recipe_service")
    if recipe_service is None:
        recipe_service = RecipeService()
        for product_id, recipe_dict in recipes.items():
            recipe = Recipe(
                recipe_id=recipe_dict.get("recipe_id", f"recipe-{product_id}"),
                product_id=product_id,
                product_name=recipe_dict.get("product_name", product_id),
                components=[],
                allergens=recipe_dict.get("allergens", []),
            )
            recipe_service.add_recipe(recipe)

    for pt in product_targets:
        product_id = pt.get("product_id")
        quantity = pt.get("total_quantity", 0)
        
        if not product_id:
            continue
        
        recipe = recipe_service.get_recipe_for_product(product_id)
        if not recipe:
            continue
        
        availability = inventory_service.check_recipe_availability(recipe, quantity)
        
        for material_id, status in availability.items():
            inventory_check["materials"][material_id] = status
            if status["sufficient"]:
                inventory_check["sufficient_count"] += 1
            else:
                inventory_check["shortage_count"] += 1
                inventory_check["shortages"].append({
                    "product_id": product_id,
                    "material_id": material_id,
                    "required": status["required"],
                    "available": status["available"],
                    "shortage": status["shortage"],
                })

    logger.info(
        "InventoryCheckNode: %d materials checked, %d sufficient, %d shortages",
        len(inventory_check["materials"]),
        inventory_check["sufficient_count"],
        inventory_check["shortage_count"],
    )

    return {
        **state,
        "inventory": inventory_dict,
        "inventoryCheck": inventory_check,
        "_inventory_service": inventory_service,
    }
