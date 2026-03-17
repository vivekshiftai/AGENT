"""Material picking entity for production planning."""
from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Any, Dict, List, Optional


class PickStatus(str, Enum):
    """Status of a material pick."""
    PENDING = "pending"
    RESERVED = "reserved"
    PICKED = "picked"
    SHORTAGE = "shortage"
    CANCELLED = "cancelled"


@dataclass
class MaterialPick:
    """Material pick for a production task."""
    
    pick_id: str
    task_id: str
    material_id: str
    material_name: str
    required_quantity: float
    picked_quantity: float = 0.0
    reserved_quantity: float = 0.0
    unit: str = "KG"
    location_id: Optional[str] = None
    batch_id: Optional[str] = None
    status: PickStatus = PickStatus.PENDING
    shortage_quantity: float = 0.0
    needs_procurement: bool = False
    expected_arrival_date: Optional[date] = None
    extra: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        self.shortage_quantity = max(0, self.required_quantity - self.reserved_quantity - self.picked_quantity)
        self.needs_procurement = self.shortage_quantity > 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pick_id": self.pick_id,
            "task_id": self.task_id,
            "material_id": self.material_id,
            "material_name": self.material_name,
            "required_quantity": self.required_quantity,
            "picked_quantity": self.picked_quantity,
            "reserved_quantity": self.reserved_quantity,
            "unit": self.unit,
            "location_id": self.location_id,
            "batch_id": self.batch_id,
            "status": self.status.value,
            "shortage_quantity": self.shortage_quantity,
            "needs_procurement": self.needs_procurement,
            "expected_arrival_date": self.expected_arrival_date.isoformat() if self.expected_arrival_date else None,
        }


@dataclass
class PickList:
    """Collection of material picks for a production order."""
    
    pick_list_id: str
    task_id: str
    product_id: str
    product_name: str
    production_quantity: float
    picks: List[MaterialPick] = field(default_factory=list)
    created_date: Optional[date] = None
    status: str = "pending"
    extra: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pick_list_id": self.pick_list_id,
            "task_id": self.task_id,
            "product_id": self.product_id,
            "product_name": self.product_name,
            "production_quantity": self.production_quantity,
            "picks": [p.to_dict() for p in self.picks],
            "created_date": self.created_date.isoformat() if self.created_date else None,
            "status": self.status,
            "total_picks": len(self.picks),
            "picks_with_shortage": sum(1 for p in self.picks if p.needs_procurement),
        }

    @property
    def has_shortages(self) -> bool:
        return any(p.needs_procurement for p in self.picks)

    @property
    def total_shortage_items(self) -> int:
        return sum(1 for p in self.picks if p.needs_procurement)

    @property
    def all_materials_available(self) -> bool:
        return all(p.status in (PickStatus.RESERVED, PickStatus.PICKED) for p in self.picks)
