"""Production task entity with allergen support for scheduling."""
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class TaskType(str, Enum):
    """Type of production task."""
    PRODUCTION = "production"
    CLEANING = "cleaning"
    CHANGEOVER = "changeover"
    MAINTENANCE = "maintenance"


class TaskStatus(str, Enum):
    """Status of a production task."""
    PENDING = "pending"
    SCHEDULED = "scheduled"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    BLOCKED = "blocked"
    CANCELLED = "cancelled"


@dataclass
class MaterialRequirement:
    """Material requirement for a production task."""
    
    material_id: str
    material_name: str
    required_quantity: float
    available_quantity: float
    reserved_quantity: float = 0.0
    unit: str = "KG"
    shortage: float = 0.0
    
    def __post_init__(self):
        self.shortage = max(0, self.required_quantity - self.available_quantity)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "material_id": self.material_id,
            "material_name": self.material_name,
            "required_quantity": self.required_quantity,
            "available_quantity": self.available_quantity,
            "reserved_quantity": self.reserved_quantity,
            "unit": self.unit,
            "shortage": self.shortage,
        }


@dataclass
class ProductionTask:
    """Production task for scheduling with allergen awareness."""
    
    task_id: str
    task_type: TaskType = TaskType.PRODUCTION
    product_id: Optional[str] = None
    product_name: Optional[str] = None
    quantity: float = 0.0
    unit: str = "EA"
    
    machine_id: Optional[str] = None
    machine_name: Optional[str] = None
    plant_id: Optional[str] = None
    line_id: Optional[str] = None
    
    scheduled_start: Optional[datetime] = None
    scheduled_end: Optional[datetime] = None
    actual_start: Optional[datetime] = None
    actual_end: Optional[datetime] = None
    
    estimated_duration_minutes: float = 0.0
    delivery_target_date: Optional[date] = None
    priority: int = 0
    
    allergens: List[str] = field(default_factory=list)
    requires_cleaning: bool = False
    cleaning_duration_minutes: float = 0.0
    
    recipe_id: Optional[str] = None
    material_requirements: List[MaterialRequirement] = field(default_factory=list)
    
    source_order_ids: List[str] = field(default_factory=list)
    dependencies: List[str] = field(default_factory=list)
    
    status: TaskStatus = TaskStatus.PENDING
    progress: int = 0
    risk_level: str = "low"
    risk_notes: Optional[str] = None
    
    extra: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "task_type": self.task_type.value,
            "product_id": self.product_id,
            "product_name": self.product_name,
            "quantity": self.quantity,
            "unit": self.unit,
            "machine_id": self.machine_id,
            "machine_name": self.machine_name,
            "plant_id": self.plant_id,
            "line_id": self.line_id,
            "scheduled_start": self.scheduled_start.isoformat() if self.scheduled_start else None,
            "scheduled_end": self.scheduled_end.isoformat() if self.scheduled_end else None,
            "estimated_duration_minutes": self.estimated_duration_minutes,
            "delivery_target_date": self.delivery_target_date.isoformat() if self.delivery_target_date else None,
            "priority": self.priority,
            "allergens": self.allergens,
            "requires_cleaning": self.requires_cleaning,
            "cleaning_duration_minutes": self.cleaning_duration_minutes,
            "recipe_id": self.recipe_id,
            "material_requirements": [m.to_dict() for m in self.material_requirements],
            "source_order_ids": self.source_order_ids,
            "dependencies": self.dependencies,
            "status": self.status.value,
            "progress": self.progress,
            "risk_level": self.risk_level,
            "risk_notes": self.risk_notes,
        }

    def to_gantt_dict(self) -> Dict[str, Any]:
        """Convert to Gantt chart format."""
        start_str = self.scheduled_start.strftime("%Y-%m-%d") if self.scheduled_start else date.today().isoformat()
        end_str = self.scheduled_end.strftime("%Y-%m-%d") if self.scheduled_end else start_str
        
        custom_class = "bar-production"
        if self.task_type == TaskType.CLEANING:
            custom_class = "bar-cleaning"
        elif self.task_type == TaskType.CHANGEOVER:
            custom_class = "bar-changeover"
        elif self.task_type == TaskType.MAINTENANCE:
            custom_class = "bar-maintenance"
        
        return {
            "id": self.task_id,
            "name": self.product_name or self.task_type.value,
            "start": start_str,
            "end": end_str,
            "progress": self.progress,
            "custom_class": custom_class,
            "type": self.task_type.value,
            "plant": self.plant_id or "",
            "line": self.line_id or "",
            "machine": self.machine_id or "",
            "product": self.product_name or "",
            "quantity": self.quantity,
            "allergens": self.allergens,
            "risk_level": self.risk_level,
            "delivery_target": self.delivery_target_date.isoformat() if self.delivery_target_date else None,
        }

    @property
    def has_material_shortage(self) -> bool:
        return any(m.shortage > 0 for m in self.material_requirements)

    @property
    def total_shortage_value(self) -> float:
        return sum(m.shortage for m in self.material_requirements)

    def is_at_risk(self, current_date: date) -> bool:
        """Check if task is at risk of missing delivery date."""
        if not self.delivery_target_date:
            return False
        if not self.scheduled_end:
            return True
        return self.scheduled_end.date() > self.delivery_target_date
