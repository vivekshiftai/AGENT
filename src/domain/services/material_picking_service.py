"""Material picking service for production planning."""
import logging
import uuid
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from src.domain.entities.material_pick import MaterialPick, PickList, PickStatus
from src.domain.entities.production_task import ProductionTask, MaterialRequirement
from src.domain.entities.recipe import Recipe
from src.domain.services.inventory_service import InventoryService
from src.domain.services.recipe_service import RecipeService

logger = logging.getLogger(__name__)


class MaterialPickingService:
    """Service for material picking and reservation."""

    def __init__(
        self,
        inventory_service: InventoryService,
        recipe_service: RecipeService,
    ):
        self._inventory = inventory_service
        self._recipes = recipe_service
        self._pick_lists: Dict[str, PickList] = {}

    def create_pick_list(
        self,
        task: ProductionTask,
        recipe: Optional[Recipe] = None,
    ) -> PickList:
        """Create a pick list for a production task."""
        if not recipe and task.product_id:
            recipe = self._recipes.get_recipe_for_product(task.product_id)
        
        if not recipe:
            logger.warning(
                "MaterialPickingService: no recipe for task %s product %s",
                task.task_id, task.product_id
            )
            return PickList(
                pick_list_id=f"pick-{task.task_id}",
                task_id=task.task_id,
                product_id=task.product_id or "",
                product_name=task.product_name or "",
                production_quantity=task.quantity,
                picks=[],
                created_date=date.today(),
                status="no_recipe",
            )
        
        requirements = recipe.get_material_requirement(task.quantity)
        picks = []
        
        for material_id, required_qty in requirements.items():
            component = next(
                (c for c in recipe.components if c.material_id == material_id),
                None
            )
            material_name = component.material_name if component else material_id
            
            available = self._inventory.get_available_quantity(material_id)
            reserved, shortage = self._inventory.reserve_quantity(
                material_id, required_qty, task_id=task.task_id
            )
            
            status = PickStatus.RESERVED if shortage == 0 else PickStatus.SHORTAGE
            
            picks.append(MaterialPick(
                pick_id=f"pick-{task.task_id}-{material_id}",
                task_id=task.task_id,
                material_id=material_id,
                material_name=material_name,
                required_quantity=required_qty,
                reserved_quantity=reserved,
                shortage_quantity=shortage,
                status=status,
                needs_procurement=shortage > 0,
            ))
        
        pick_list = PickList(
            pick_list_id=f"picklist-{task.task_id}",
            task_id=task.task_id,
            product_id=task.product_id or "",
            product_name=task.product_name or "",
            production_quantity=task.quantity,
            picks=picks,
            created_date=date.today(),
            status="complete" if not any(p.needs_procurement for p in picks) else "shortage",
        )
        
        self._pick_lists[pick_list.pick_list_id] = pick_list
        return pick_list

    def create_pick_lists_for_tasks(
        self,
        tasks: List[ProductionTask],
    ) -> List[PickList]:
        """Create pick lists for multiple tasks."""
        pick_lists = []
        for task in tasks:
            if task.task_type.value == "production":
                pick_list = self.create_pick_list(task)
                pick_lists.append(pick_list)
                task.material_requirements = [
                    MaterialRequirement(
                        material_id=p.material_id,
                        material_name=p.material_name,
                        required_quantity=p.required_quantity,
                        available_quantity=self._inventory.get_available_quantity(p.material_id),
                        reserved_quantity=p.reserved_quantity,
                    )
                    for p in pick_list.picks
                ]
        return pick_lists

    def get_pick_list(self, pick_list_id: str) -> Optional[PickList]:
        """Get a pick list by ID."""
        return self._pick_lists.get(pick_list_id)

    def get_pick_list_for_task(self, task_id: str) -> Optional[PickList]:
        """Get pick list for a task."""
        for pl in self._pick_lists.values():
            if pl.task_id == task_id:
                return pl
        return None

    def get_all_shortages(self) -> List[Dict[str, Any]]:
        """Get all material shortages across pick lists."""
        shortages = []
        for pl in self._pick_lists.values():
            for pick in pl.picks:
                if pick.needs_procurement:
                    shortages.append({
                        "pick_list_id": pl.pick_list_id,
                        "task_id": pl.task_id,
                        "product_id": pl.product_id,
                        "material_id": pick.material_id,
                        "material_name": pick.material_name,
                        "required": pick.required_quantity,
                        "shortage": pick.shortage_quantity,
                    })
        return shortages

    def release_pick_list(self, pick_list_id: str) -> bool:
        """Release all reservations for a pick list."""
        pick_list = self._pick_lists.get(pick_list_id)
        if not pick_list:
            return False
        
        for pick in pick_list.picks:
            if pick.reserved_quantity > 0:
                self._inventory.release_reservation(
                    pick.material_id,
                    pick.reserved_quantity,
                )
                pick.reserved_quantity = 0
                pick.status = PickStatus.CANCELLED
        
        pick_list.status = "cancelled"
        return True

    def get_procurement_list(self) -> List[Dict[str, Any]]:
        """Get aggregated list of materials needing procurement."""
        aggregated: Dict[str, Dict[str, Any]] = {}
        
        for pl in self._pick_lists.values():
            for pick in pl.picks:
                if pick.needs_procurement:
                    if pick.material_id not in aggregated:
                        aggregated[pick.material_id] = {
                            "material_id": pick.material_id,
                            "material_name": pick.material_name,
                            "total_shortage": 0,
                            "affected_tasks": [],
                        }
                    aggregated[pick.material_id]["total_shortage"] += pick.shortage_quantity
                    aggregated[pick.material_id]["affected_tasks"].append(pl.task_id)
        
        return list(aggregated.values())

    def to_dict(self) -> Dict[str, Any]:
        """Export pick lists."""
        return {
            "pick_lists": {k: v.to_dict() for k, v in self._pick_lists.items()},
            "total_shortages": len(self.get_all_shortages()),
        }
