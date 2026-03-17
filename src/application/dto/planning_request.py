"""DTO for production planning requests and responses."""
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


class PlanningRequest:
    """Request for a production plan."""

    def __init__(
        self,
        plan_id: str,
        input_data: Optional[Dict[str, Any]] = None,
        use_frepple: bool = False,
    ):
        self.plan_id = plan_id
        self.input_data = input_data or {}
        self.use_frepple = use_frepple


@dataclass
class ProductTargetDTO:
    """Product target for planning."""
    product_id: str
    product_name: str
    total_quantity: float
    earliest_due_date: Optional[str] = None
    latest_due_date: Optional[str] = None
    priority: int = 0
    order_count: int = 0
    allergens: List[str] = field(default_factory=list)


@dataclass
class MaterialShortageDTO:
    """Material shortage information."""
    material_id: str
    material_name: str
    required: float
    available: float
    shortage: float
    affected_products: List[str] = field(default_factory=list)


@dataclass
class MachineScheduleDTO:
    """Machine schedule information."""
    machine_id: str
    machine_name: str
    task_sequence: List[str] = field(default_factory=list)
    utilization_percent: float = 0.0
    cleaning_events: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class ValidationIssueDTO:
    """Validation issue."""
    issue_type: str
    severity: str
    message: str
    task_id: Optional[str] = None
    details: Optional[Dict[str, Any]] = None


@dataclass
class PlanningResponseDTO:
    """Comprehensive production planning response."""
    plan_id: str
    response: str
    gantt_tasks: List[Dict[str, Any]] = field(default_factory=list)
    sales_orders_by_material: List[Dict[str, Any]] = field(default_factory=list)
    product_targets: List[ProductTargetDTO] = field(default_factory=list)
    material_shortages: List[MaterialShortageDTO] = field(default_factory=list)
    machine_schedules: List[MachineScheduleDTO] = field(default_factory=list)
    validation_issues: List[ValidationIssueDTO] = field(default_factory=list)
    risk_level: str = "low"
    summary: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "response": self.response,
            "gantt_tasks": self.gantt_tasks,
            "sales_orders_by_material": self.sales_orders_by_material,
            "product_targets": [
                {
                    "product_id": pt.product_id,
                    "product_name": pt.product_name,
                    "total_quantity": pt.total_quantity,
                    "earliest_due_date": pt.earliest_due_date,
                    "latest_due_date": pt.latest_due_date,
                    "priority": pt.priority,
                    "order_count": pt.order_count,
                    "allergens": pt.allergens,
                }
                for pt in self.product_targets
            ],
            "material_shortages": [
                {
                    "material_id": ms.material_id,
                    "material_name": ms.material_name,
                    "required": ms.required,
                    "available": ms.available,
                    "shortage": ms.shortage,
                    "affected_products": ms.affected_products,
                }
                for ms in self.material_shortages
            ],
            "machine_schedules": [
                {
                    "machine_id": ms.machine_id,
                    "machine_name": ms.machine_name,
                    "task_sequence": ms.task_sequence,
                    "utilization_percent": ms.utilization_percent,
                    "cleaning_events": ms.cleaning_events,
                }
                for ms in self.machine_schedules
            ],
            "validation_issues": [
                {
                    "issue_type": vi.issue_type,
                    "severity": vi.severity,
                    "message": vi.message,
                    "task_id": vi.task_id,
                    "details": vi.details,
                }
                for vi in self.validation_issues
            ],
            "risk_level": self.risk_level,
            "summary": self.summary,
        }
