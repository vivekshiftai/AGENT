"""Fetch recipes/BOMs for products that need to be planned.
For CHG datasource, fetches step-level recipe detail from chg_recipe_steps/header."""
import logging
from typing import Any, Dict, List, Optional

import pandas as pd

from src.domain.repositories.data_repository import DataRepository
from src.domain.services.recipe_service import RecipeService

logger = logging.getLogger(__name__)

RECIPE_TABLE_HINTS = ("recipe", "bom", "bill_of_material", "formula", "routing")
COMPONENT_KEYS = ("component", "material", "ingredient", "raw_material", "item")
QTY_KEYS = ("quantity", "qty", "amount", "usage")
ALLERGEN_KEYS = ("allergens", "allergen", "contains")
MACHINE_KEYS = ("machine", "equipment", "workcenter", "resource")
CLEANING_KEYS = ("cleaning_time", "clean_time", "changeover", "setup_time")

FETCH_RECIPE_STEPS = """
SELECT
    rs.step_id, rs.recipe_id, rs.step_number, rs.step_name,
    rs.step_type, rs.machine_id, rs.machine_name,
    rs.duration_min, rs.wait_after_min,
    rs.temp_f_min, rs.temp_f_max,
    rs.ingredient_id, rs.qty_per_batch_lbs,
    rs.critical_param, rs.spec_min, rs.spec_max, rs.spec_unit,
    rs.qa_check_required, rs.operator_notes,
    rh.product_id, rh.batch_size_lbs, rh.yield_pct,
    rh.allergens_present, rh.plant_id
FROM chg_recipe_steps rs
JOIN chg_recipe_header rh ON rs.recipe_id = rh.recipe_id
WHERE rh.product_id IN ({product_ids})
  AND rh.status = 'ACTIVE'
ORDER BY rh.product_id, rs.step_number ASC
"""


def _build_chg_recipes(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Group step rows into recipe dicts keyed by product_id."""
    recipes: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        pid = row.get("product_id")
        if not pid:
            continue
        if pid not in recipes:
            recipes[pid] = {
                "recipe_id": row.get("recipe_id", ""),
                "product_id": pid,
                "batch_size_lbs": row.get("batch_size_lbs", 1000),
                "yield_pct": row.get("yield_pct", 100),
                "allergens_present": row.get("allergens_present", ""),
                "plant_id": row.get("plant_id"),
                "steps": [],
            }
        recipes[pid]["steps"].append({
            "step_id": row.get("step_id"),
            "step_number": row.get("step_number"),
            "step_name": row.get("step_name", ""),
            "step_type": row.get("step_type", "PROCESSING"),
            "machine_id": row.get("machine_id", "MANUAL"),
            "machine_name": row.get("machine_name", ""),
            "duration_min": row.get("duration_min", 0),
            "wait_after_min": row.get("wait_after_min", 0),
            "temp_f_min": row.get("temp_f_min"),
            "temp_f_max": row.get("temp_f_max"),
            "ingredient_id": row.get("ingredient_id"),
            "qty_per_batch_lbs": row.get("qty_per_batch_lbs"),
            "critical_param": row.get("critical_param"),
            "spec_min": row.get("spec_min"),
            "spec_max": row.get("spec_max"),
            "spec_unit": row.get("spec_unit"),
            "qa_check_required": int(row.get("qa_check_required", 0) or 0),
            "operator_notes": row.get("operator_notes", ""),
        })
    return recipes


def run(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fetch recipes/BOMs for products in productTargets.
    For CHG: runs step-level query and builds enriched recipe objects.
    For generic: scans rawData tables for recipe hints.
    """
    if state.get("needsClarification") or state.get("rejected"):
        return {**state}

    product_targets = state.get("productTargets") or []
    raw_data = state.get("rawData") or {}
    is_chg = state.get("_is_chg", False)
    sales_orders = state.get("salesOrders") or []

    # ── CHG path: fetch from recipe_steps + recipe_header ───────────
    if is_chg:
        product_ids_from_targets = [pt.get("product_id") for pt in product_targets if pt.get("product_id")]
        product_ids_from_so = [so.get("product_id") for so in sales_orders if so.get("product_id")]
        all_product_ids = list(set(product_ids_from_targets + product_ids_from_so))

        if all_product_ids:
            quoted = ", ".join(f"'{pid}'" for pid in all_product_ids)
            query = FETCH_RECIPE_STEPS.format(product_ids=quoted)

            rows = []
            configs = state.get("requiredSources") or []
            for config in configs:
                try:
                    repo = DataRepository(config)
                    result = repo.fetch_data(queries=[query])
                    for _, df in result.items():
                        if isinstance(df, pd.DataFrame) and not df.empty:
                            rows = df.to_dict(orient="records")
                            break
                    if rows:
                        break
                except Exception as e:
                    logger.warning("RecipeFetchNode CHG: query failed on %s: %s", config.get("name"), e)

            if rows:
                recipes_dict = _build_chg_recipes(rows)
                logger.info("RecipeFetchNode CHG: built %d recipes with steps from %d rows", len(recipes_dict), len(rows))
                return {**state, "recipes": recipes_dict}

        logger.info("RecipeFetchNode CHG: no recipe rows found, falling through to generic path")

    # ── Generic path ────────────────────────────────────────────────
    recipe_service = RecipeService()
    recipe_records = []
    for source_name, tables in raw_data.items():
        if not isinstance(tables, dict):
            continue
        for table_name, df in tables.items():
            tn_lower = (table_name or "").lower()
            if not any(hint in tn_lower for hint in RECIPE_TABLE_HINTS):
                continue
            try:
                if not isinstance(df, pd.DataFrame) or df.empty:
                    continue
                for _, row in df.iterrows():
                    recipe_records.append(row.to_dict())
            except Exception as e:
                logger.warning("RecipeFetchNode: error reading %s.%s: %s", source_name, table_name, e)

    if recipe_records:
        recipe_service.load_recipes(recipe_records)
        logger.info("RecipeFetchNode: loaded %d recipes from data", len(recipe_service.get_all_recipes()))

    recipes_dict = {}
    for pt in product_targets:
        product_id = pt.get("product_id") or pt.get("material")
        if not product_id:
            continue

        recipe = recipe_service.get_recipe_for_product(product_id)
        if not recipe:
            recipe = recipe_service.create_default_recipe(
                product_id=product_id,
                product_name=pt.get("product_name") or product_id,
            )
            if pt.get("allergens"):
                recipe.allergens = list(pt["allergens"])

        recipes_dict[product_id] = recipe.to_dict()

    logger.info("RecipeFetchNode: %d recipes for %d product targets", len(recipes_dict), len(product_targets))

    return {
        **state,
        "recipes": recipes_dict,
        "_recipe_service": recipe_service,
    }
