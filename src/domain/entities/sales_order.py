"""Sales order entity for production planning."""
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Optional


@dataclass
class SalesOrderLine:
    """Individual line item in a sales order."""
    
    line_id: str
    product_id: str
    product_name: str
    quantity: float
    unit: str = "EA"
    delivery_date: Optional[date] = None
    priority: int = 0
    allergens: List[str] = field(default_factory=list)
    extra: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "line_id": self.line_id,
            "product_id": self.product_id,
            "product_name": self.product_name,
            "quantity": self.quantity,
            "unit": self.unit,
            "delivery_date": self.delivery_date.isoformat() if self.delivery_date else None,
            "priority": self.priority,
            "allergens": self.allergens,
            **(self.extra or {}),
        }


@dataclass
class SalesOrder:
    """Sales order (customer demand) for production planning."""
    
    order_id: str
    customer_id: Optional[str] = None
    customer_name: Optional[str] = None
    order_date: Optional[date] = None
    delivery_target_date: Optional[date] = None
    priority: int = 0
    status: str = "open"
    lines: List[SalesOrderLine] = field(default_factory=list)
    extra: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "order_id": self.order_id,
            "customer_id": self.customer_id,
            "customer_name": self.customer_name,
            "order_date": self.order_date.isoformat() if self.order_date else None,
            "delivery_target_date": self.delivery_target_date.isoformat() if self.delivery_target_date else None,
            "priority": self.priority,
            "status": self.status,
            "lines": [line.to_dict() for line in self.lines],
        }

    @property
    def total_quantity(self) -> float:
        return sum(line.quantity for line in self.lines)

    @property
    def earliest_delivery(self) -> Optional[date]:
        dates = [line.delivery_date for line in self.lines if line.delivery_date]
        return min(dates) if dates else self.delivery_target_date


@dataclass
class ProductTarget:
    """Aggregated production target for a product across all orders."""
    
    product_id: str
    product_name: str
    total_quantity: float
    earliest_due_date: Optional[date] = None
    latest_due_date: Optional[date] = None
    priority: int = 0
    order_count: int = 0
    allergens: List[str] = field(default_factory=list)
    source_orders: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "product_id": self.product_id,
            "product_name": self.product_name,
            "total_quantity": self.total_quantity,
            "earliest_due_date": self.earliest_due_date.isoformat() if self.earliest_due_date else None,
            "latest_due_date": self.latest_due_date.isoformat() if self.latest_due_date else None,
            "priority": self.priority,
            "order_count": self.order_count,
            "allergens": self.allergens,
            "source_orders": self.source_orders,
        }
