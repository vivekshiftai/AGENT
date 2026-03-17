"""Recipe service for managing BOMs and production recipes."""
import logging
from typing import Any, Dict, List, Optional

from src.domain.entities.recipe import Recipe, RecipeComponent, ProductionStep

logger = logging.getLogger(__name__)


class RecipeService:
    """Service for recipe/BOM management."""

    def __init__(self, recipe_data: Optional[List[Dict[str, Any]]] = None):
        self._recipes: Dict[str, Recipe] = {}
        self._product_to_recipe: Dict[str, str] = {}
        if recipe_data:
            self.load_recipes(recipe_data)

    def load_recipes(self, data: List[Dict[str, Any]]) -> None:
        """Load recipes from normalized data records."""
        for record in data:
            recipe = self._parse_recipe(record)
            if recipe:
                self._recipes[recipe.recipe_id] = recipe
                self._product_to_recipe[recipe.product_id] = recipe.recipe_id
        logger.info("RecipeService: loaded %d recipes", len(self._recipes))

    def _parse_recipe(self, record: Dict[str, Any]) -> Optional[Recipe]:
        """Parse a recipe from a data record."""
        recipe_id = str(record.get("recipe_id") or record.get("bom_id") or "")
        product_id = str(record.get("product_id") or record.get("material") or record.get("finished_product") or "")
        
        if not recipe_id and not product_id:
            return None
        
        if not recipe_id:
            recipe_id = f"recipe-{product_id}"
        
        components = []
        raw_components = record.get("components") or record.get("materials") or record.get("bom_items") or []
        if isinstance(raw_components, list):
            for comp in raw_components:
                if isinstance(comp, dict):
                    components.append(RecipeComponent(
                        material_id=str(comp.get("material_id") or comp.get("component_id") or ""),
                        material_name=str(comp.get("material_name") or comp.get("component_name") or ""),
                        quantity_per_unit=float(comp.get("quantity") or comp.get("qty") or 1),
                        unit=str(comp.get("unit") or "KG"),
                        is_raw_material=comp.get("is_raw", True),
                        allergens=comp.get("allergens") or [],
                    ))
        
        steps = []
        raw_steps = record.get("steps") or record.get("production_steps") or record.get("operations") or []
        if isinstance(raw_steps, list):
            for i, step in enumerate(raw_steps):
                if isinstance(step, dict):
                    steps.append(ProductionStep(
                        step_id=str(step.get("step_id") or f"step-{i+1}"),
                        step_name=str(step.get("step_name") or step.get("operation") or f"Step {i+1}"),
                        sequence=int(step.get("sequence") or i + 1),
                        duration_minutes=float(step.get("duration") or step.get("duration_minutes") or 60),
                        required_machine_type=step.get("machine_type"),
                        required_machine_id=step.get("machine_id"),
                    ))
        
        allergens = record.get("allergens") or []
        if not allergens and components:
            all_allergens = set()
            for comp in components:
                all_allergens.update(comp.allergens)
            allergens = list(all_allergens)
        
        return Recipe(
            recipe_id=recipe_id,
            product_id=product_id,
            product_name=str(record.get("product_name") or product_id),
            version=str(record.get("version") or "1.0"),
            yield_quantity=float(record.get("yield") or record.get("yield_quantity") or 1),
            yield_unit=str(record.get("yield_unit") or "EA"),
            components=components,
            production_steps=steps,
            allergens=allergens,
            required_machine_types=record.get("machine_types") or [],
            cleaning_time_minutes=float(record.get("cleaning_time") or record.get("cleaning_minutes") or 60),
            changeover_time_minutes=float(record.get("changeover_time") or record.get("changeover_minutes") or 30),
        )

    def get_recipe(self, recipe_id: str) -> Optional[Recipe]:
        """Get recipe by ID."""
        return self._recipes.get(recipe_id)

    def get_recipe_for_product(self, product_id: str) -> Optional[Recipe]:
        """Get recipe for a product."""
        recipe_id = self._product_to_recipe.get(product_id)
        if recipe_id:
            return self._recipes.get(recipe_id)
        return None

    def get_all_recipes(self) -> List[Recipe]:
        """Get all loaded recipes."""
        return list(self._recipes.values())

    def get_material_requirements(
        self,
        product_id: str,
        quantity: float,
    ) -> Dict[str, float]:
        """Get material requirements for producing a quantity of a product."""
        recipe = self.get_recipe_for_product(product_id)
        if not recipe:
            return {}
        return recipe.get_material_requirement(quantity)

    def get_allergens_for_product(self, product_id: str) -> List[str]:
        """Get all allergens for a product."""
        recipe = self.get_recipe_for_product(product_id)
        if not recipe:
            return []
        return recipe.get_all_allergens()

    def get_production_duration(self, product_id: str) -> float:
        """Get total production duration in minutes for a product."""
        recipe = self.get_recipe_for_product(product_id)
        if not recipe:
            return 0.0
        return recipe.total_duration_minutes

    def get_required_machines(self, product_id: str) -> List[str]:
        """Get required machine types for a product."""
        recipe = self.get_recipe_for_product(product_id)
        if not recipe:
            return []
        return recipe.required_machine_types

    def add_recipe(self, recipe: Recipe) -> None:
        """Add or update a recipe."""
        self._recipes[recipe.recipe_id] = recipe
        self._product_to_recipe[recipe.product_id] = recipe.recipe_id

    def create_default_recipe(self, product_id: str, product_name: str) -> Recipe:
        """Create a default recipe for a product without BOM data."""
        recipe = Recipe(
            recipe_id=f"default-recipe-{product_id}",
            product_id=product_id,
            product_name=product_name,
            version="default",
            yield_quantity=1.0,
            components=[],
            production_steps=[
                ProductionStep(
                    step_id="default-step-1",
                    step_name="Production",
                    sequence=1,
                    duration_minutes=60,
                )
            ],
            cleaning_time_minutes=60,
            changeover_time_minutes=30,
        )
        self.add_recipe(recipe)
        return recipe

    def to_dict(self) -> Dict[str, Any]:
        """Export recipes."""
        return {
            "recipes": {k: v.to_dict() for k, v in self._recipes.items()},
            "product_mapping": self._product_to_recipe,
        }
