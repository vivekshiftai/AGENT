"""Machine/equipment entity for production planning."""
from dataclasses import dataclass, field
from datetime import date, datetime, time
from typing import Any, Dict, List, Optional


@dataclass
class MaintenanceWindow:
    """Scheduled maintenance period for a machine."""
    
    start: datetime
    end: datetime
    reason: str = "scheduled maintenance"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "reason": self.reason,
        }


@dataclass
class AllergenCleaningTime:
    """Cleaning time required when switching between allergen types."""
    
    from_allergen: str
    to_allergen: str
    cleaning_minutes: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "from_allergen": self.from_allergen,
            "to_allergen": self.to_allergen,
            "cleaning_minutes": self.cleaning_minutes,
        }


@dataclass
class Machine:
    """Production machine/equipment."""
    
    machine_id: str
    machine_name: str
    machine_type: str
    plant_id: Optional[str] = None
    line_id: Optional[str] = None
    capacity_per_hour: float = 100.0
    capacity_unit: str = "EA"
    available_hours_per_day: float = 8.0
    shift_start: time = field(default_factory=lambda: time(6, 0))
    shift_end: time = field(default_factory=lambda: time(22, 0))
    compatible_products: List[str] = field(default_factory=list)
    changeover_time_minutes: float = 30.0
    default_cleaning_time_minutes: float = 60.0
    allergen_cleaning_times: List[AllergenCleaningTime] = field(default_factory=list)
    maintenance_windows: List[MaintenanceWindow] = field(default_factory=list)
    current_product: Optional[str] = None
    current_allergens: List[str] = field(default_factory=list)
    status: str = "available"
    extra: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "machine_id": self.machine_id,
            "machine_name": self.machine_name,
            "machine_type": self.machine_type,
            "plant_id": self.plant_id,
            "line_id": self.line_id,
            "capacity_per_hour": self.capacity_per_hour,
            "capacity_unit": self.capacity_unit,
            "available_hours_per_day": self.available_hours_per_day,
            "shift_start": self.shift_start.isoformat() if self.shift_start else None,
            "shift_end": self.shift_end.isoformat() if self.shift_end else None,
            "compatible_products": self.compatible_products,
            "changeover_time_minutes": self.changeover_time_minutes,
            "default_cleaning_time_minutes": self.default_cleaning_time_minutes,
            "allergen_cleaning_times": [a.to_dict() for a in self.allergen_cleaning_times],
            "current_product": self.current_product,
            "current_allergens": self.current_allergens,
            "status": self.status,
        }

    def get_cleaning_time(self, from_allergens: List[str], to_allergens: List[str]) -> float:
        """Calculate cleaning time when switching between allergen sets."""
        if not from_allergens or not to_allergens:
            return 0.0
        
        from_set = set(from_allergens)
        to_set = set(to_allergens)
        
        if from_set == to_set:
            return 0.0
        
        max_cleaning = 0.0
        for act in self.allergen_cleaning_times:
            if act.from_allergen in from_set and act.to_allergen in to_set:
                max_cleaning = max(max_cleaning, act.cleaning_minutes)
        
        return max_cleaning if max_cleaning > 0 else self.default_cleaning_time_minutes

    def can_produce(self, product_id: str) -> bool:
        """Check if machine can produce a specific product."""
        if not self.compatible_products:
            return True
        return product_id in self.compatible_products

    def is_available_at(self, dt: datetime) -> bool:
        """Check if machine is available at a specific datetime."""
        if self.status != "available":
            return False
        for mw in self.maintenance_windows:
            if mw.start <= dt <= mw.end:
                return False
        return True

    def estimate_production_time(self, quantity: float) -> float:
        """Estimate production time in minutes for a given quantity."""
        if self.capacity_per_hour <= 0:
            return float('inf')
        hours = quantity / self.capacity_per_hour
        return hours * 60
