"""Demand entity for production planning."""
from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass
class Demand:
    """Customer or internal demand for a product/quantity by date."""

    id: str
    item_id: str
    quantity: float
    due_date: date
    customer_id: Optional[str] = None
    priority: int = 0
