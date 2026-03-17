"""Recipe/BOM (Bill of Materials) entity for production planning."""
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class RecipeComponent:
    """Single component/material in a recipe."""
    
    material_id: str
    material_name: str
    quantity_per_unit: float
    unit: str = "KG"
    is_raw_material: bool = True
    allergens: List[str] = field(default_factory=list)
    substitutes: List[str] = field(default_factory=list)
    extra: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "material_id": self.material_id,
            "material_name": self.material_name,
            "quantity_per_unit": self.quantity_per_unit,
            "unit": self.unit,
            "is_raw_material": self.is_raw_material,
            "allergens": self.allergens,
            "substitutes": self.substitutes,
        }


@dataclass
class ProductionStep:
    """Single step in the production process."""
    
    step_id: str
    step_name: str
    sequence: int
    duration_minutes: float
    required_machine_type: Optional[str] = None
    required_machine_id: Optional[str] = None
    description: Optional[str] = None
    extra: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "step_id": self.step_id,
            "step_name": self.step_name,
            "sequence": self.sequence,
            "duration_minutes": self.duration_minutes,
            "required_machine_type": self.required_machine_type,
            "required_machine_id": self.required_machine_id,
            "description": self.description,
        }


@dataclass
class Recipe:
    """Recipe/BOM for producing a finished product."""
    
    recipe_id: str
    product_id: str
    product_name: str
    version: str = "1.0"
    yield_quantity: float = 1.0
    yield_unit: str = "EA"
    components: List[RecipeComponent] = field(default_factory=list)
    production_steps: List[ProductionStep] = field(default_factory=list)
    allergens: List[str] = field(default_factory=list)
    required_machine_types: List[str] = field(default_factory=list)
    cleaning_time_minutes: float = 0.0
    changeover_time_minutes: float = 0.0
    extra: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "recipe_id": self.recipe_id,
            "product_id": self.product_id,
            "product_name": self.product_name,
            "version": self.version,
            "yield_quantity": self.yield_quantity,
            "yield_unit": self.yield_unit,
            "components": [c.to_dict() for c in self.components],
            "production_steps": [s.to_dict() for s in self.production_steps],
            "allergens": self.allergens,
            "required_machine_types": self.required_machine_types,
            "cleaning_time_minutes": self.cleaning_time_minutes,
            "changeover_time_minutes": self.changeover_time_minutes,
        }

    @property
    def total_duration_minutes(self) -> float:
        return sum(step.duration_minutes for step in self.production_steps)

    def get_material_requirement(self, production_quantity: float) -> Dict[str, float]:
        """Calculate material requirements for a given production quantity."""
        multiplier = production_quantity / self.yield_quantity if self.yield_quantity > 0 else production_quantity
        return {
            comp.material_id: comp.quantity_per_unit * multiplier
            for comp in self.components
        }

    def get_all_allergens(self) -> List[str]:
        """Get all allergens from recipe and components."""
        all_allergens = set(self.allergens)
        for comp in self.components:
            all_allergens.update(comp.allergens)
        return sorted(all_allergens)
