"""Inventory service for checking stock levels and availability."""
import logging
from collections import defaultdict
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from src.domain.entities.inventory import Inventory
from src.domain.entities.recipe import Recipe

logger = logging.getLogger(__name__)


class InventoryService:
    """Service for inventory management and availability checking."""

    def __init__(self, inventory_data: Optional[List[Dict[str, Any]]] = None):
        self._inventory: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
        self._reserved: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
        if inventory_data:
            self.load_inventory(inventory_data)

    def load_inventory(self, data: List[Dict[str, Any]]) -> None:
        """Load inventory from normalized data records."""
        for record in data:
            item_id = str(record.get("item_id") or record.get("material_id") or record.get("product") or "")
            location = str(record.get("location_id") or record.get("location") or "default")
            qty = float(record.get("quantity") or record.get("qty") or record.get("on_hand") or 0)
            if item_id:
                self._inventory[item_id][location] += qty
        logger.info("InventoryService: loaded %d items", len(self._inventory))

    def get_available_quantity(self, item_id: str, location: Optional[str] = None) -> float:
        """Get available quantity for an item (total - reserved)."""
        if location:
            total = self._inventory.get(item_id, {}).get(location, 0)
            reserved = self._reserved.get(item_id, {}).get(location, 0)
            return max(0, total - reserved)
        
        total = sum(self._inventory.get(item_id, {}).values())
        reserved = sum(self._reserved.get(item_id, {}).values())
        return max(0, total - reserved)

    def get_total_quantity(self, item_id: str) -> float:
        """Get total quantity across all locations."""
        return sum(self._inventory.get(item_id, {}).values())

    def reserve_quantity(
        self,
        item_id: str,
        quantity: float,
        location: Optional[str] = None,
        task_id: Optional[str] = None,
    ) -> Tuple[float, float]:
        """
        Reserve quantity for a task. Returns (reserved_qty, shortage_qty).
        """
        loc = location or "default"
        available = self.get_available_quantity(item_id, loc)
        
        if available >= quantity:
            self._reserved[item_id][loc] += quantity
            return quantity, 0.0
        else:
            self._reserved[item_id][loc] += available
            shortage = quantity - available
            return available, shortage

    def release_reservation(
        self,
        item_id: str,
        quantity: float,
        location: Optional[str] = None,
    ) -> None:
        """Release a reservation."""
        loc = location or "default"
        current = self._reserved.get(item_id, {}).get(loc, 0)
        self._reserved[item_id][loc] = max(0, current - quantity)

    def check_recipe_availability(
        self,
        recipe: Recipe,
        production_quantity: float,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Check material availability for a recipe.
        Returns dict of material_id -> { required, available, shortage }.
        """
        requirements = recipe.get_material_requirement(production_quantity)
        result = {}
        
        for material_id, required_qty in requirements.items():
            available = self.get_available_quantity(material_id)
            shortage = max(0, required_qty - available)
            result[material_id] = {
                "material_id": material_id,
                "required": required_qty,
                "available": available,
                "shortage": shortage,
                "sufficient": shortage == 0,
            }
        
        return result

    def check_multiple_recipes(
        self,
        recipes_with_quantities: List[Tuple[Recipe, float]],
    ) -> Dict[str, Dict[str, Any]]:
        """
        Check availability for multiple recipes (aggregated requirements).
        """
        aggregated: Dict[str, float] = defaultdict(float)
        
        for recipe, qty in recipes_with_quantities:
            requirements = recipe.get_material_requirement(qty)
            for material_id, required_qty in requirements.items():
                aggregated[material_id] += required_qty
        
        result = {}
        for material_id, total_required in aggregated.items():
            available = self.get_available_quantity(material_id)
            shortage = max(0, total_required - available)
            result[material_id] = {
                "material_id": material_id,
                "required": total_required,
                "available": available,
                "shortage": shortage,
                "sufficient": shortage == 0,
            }
        
        return result

    def get_all_shortages(self) -> List[Dict[str, Any]]:
        """Get list of all items with shortages (reserved > available)."""
        shortages = []
        for item_id in set(list(self._inventory.keys()) + list(self._reserved.keys())):
            total = sum(self._inventory.get(item_id, {}).values())
            reserved = sum(self._reserved.get(item_id, {}).values())
            if reserved > total:
                shortages.append({
                    "item_id": item_id,
                    "total": total,
                    "reserved": reserved,
                    "shortage": reserved - total,
                })
        return shortages

    def get_inventory_summary(self) -> Dict[str, Any]:
        """Get summary of inventory status."""
        total_items = len(self._inventory)
        total_reserved_items = len(self._reserved)
        shortages = self.get_all_shortages()
        
        return {
            "total_items": total_items,
            "total_reserved_items": total_reserved_items,
            "shortage_count": len(shortages),
            "shortages": shortages,
        }

    def to_dict(self) -> Dict[str, Any]:
        """Export inventory state."""
        return {
            "inventory": {k: dict(v) for k, v in self._inventory.items()},
            "reserved": {k: dict(v) for k, v in self._reserved.items()},
        }
