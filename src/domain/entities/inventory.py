"""Inventory entity for production planning."""
from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass
class Inventory:
    """Inventory level for an item at a location and point in time."""

    item_id: str
    location_id: str
    quantity: float
    on_hand_date: date
    batch_id: Optional[str] = None
