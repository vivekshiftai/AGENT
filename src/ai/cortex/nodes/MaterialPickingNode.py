"""Create material pick lists based on recipes and inventory."""
import logging
from datetime import date
from typing import Any, Dict, List, Optional

from src.domain.entities.production_task import ProductionTask, TaskType, TaskStatus
from src.domain.services.inventory_service import InventoryService
from src.domain.services.recipe_service import RecipeService
from src.domain.services.material_picking_service import MaterialPickingService

logger = logging.getLogger(__name__)


def run(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create material pick lists for production tasks.
    
    Based on recipes and inventory, picks required materials and flags shortages.
    Sets:
    - materialPicks: list of pick list dicts
    - productionTasks: updated with material requirements
    """
    if state.get("needsClarification") or state.get("rejected"):
        return {**state}

    product_targets = state.get("productTargets") or []
    recipes = state.get("recipes") or {}
    
    inventory_service = state.get("_inventory_service")
    if inventory_service is None:
        inventory_service = InventoryService(state.get("inventory") or {})
    
    recipe_service = state.get("_recipe_service")
    if recipe_service is None:
        recipe_service = RecipeService()
        for product_id, recipe_dict in recipes.items():
            from src.domain.entities.recipe import Recipe, RecipeComponent
            components = []
            for comp in recipe_dict.get("components", []):
                components.append(RecipeComponent(
                    material_id=comp.get("material_id", ""),
                    material_name=comp.get("material_name", ""),
                    quantity_per_unit=comp.get("quantity_per_unit", 1),
                    unit=comp.get("unit", "KG"),
                    allergens=comp.get("allergens", []),
                ))
            recipe = Recipe(
                recipe_id=recipe_dict.get("recipe_id", f"recipe-{product_id}"),
                product_id=product_id,
                product_name=recipe_dict.get("product_name", product_id),
                components=components,
                allergens=recipe_dict.get("allergens", []),
                cleaning_time_minutes=recipe_dict.get("cleaning_time_minutes", 60),
                changeover_time_minutes=recipe_dict.get("changeover_time_minutes", 30),
            )
            recipe_service.add_recipe(recipe)

    picking_service = MaterialPickingService(inventory_service, recipe_service)

    production_tasks = []
    for i, pt in enumerate(product_targets):
        product_id = pt.get("product_id")
        if not product_id:
            continue
        
        recipe = recipe_service.get_recipe_for_product(product_id)
        allergens = recipe.get_all_allergens() if recipe else pt.get("allergens", [])
        
        due_date = None
        if pt.get("earliest_due_date"):
            try:
                due_date = date.fromisoformat(pt["earliest_due_date"])
            except (ValueError, TypeError):
                pass
        
        task = ProductionTask(
            task_id=f"task-{i+1}-{product_id}",
            task_type=TaskType.PRODUCTION,
            product_id=product_id,
            product_name=pt.get("product_name") or product_id,
            quantity=pt.get("total_quantity", 0),
            delivery_target_date=due_date,
            priority=pt.get("priority", 0),
            allergens=allergens,
            recipe_id=recipe.recipe_id if recipe else None,
            source_order_ids=pt.get("source_orders", []),
            status=TaskStatus.PENDING,
        )
        
        if recipe:
            duration = recipe.total_duration_minutes
            if duration == 0:
                duration = task.quantity * 0.1
            task.estimated_duration_minutes = duration
        
        production_tasks.append(task)

    pick_lists = picking_service.create_pick_lists_for_tasks(production_tasks)

    material_picks_dicts = [pl.to_dict() for pl in pick_lists]
    production_tasks_dicts = [t.to_dict() for t in production_tasks]

    shortages = picking_service.get_all_shortages()
    procurement_list = picking_service.get_procurement_list()

    logger.info(
        "MaterialPickingNode: created %d pick lists for %d tasks, %d shortages",
        len(pick_lists), len(production_tasks), len(shortages)
    )

    return {
        **state,
        "materialPicks": material_picks_dicts,
        "productionTasks": production_tasks_dicts,
        "materialShortages": shortages,
        "procurementList": procurement_list,
        "_production_tasks": production_tasks,
        "_picking_service": picking_service,
    }
