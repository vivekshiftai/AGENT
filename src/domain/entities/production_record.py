"""Unified production record for aggregated multi-source data."""
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Optional


@dataclass
class ProductionRecord:
    """
    Normalized record across sources for aggregation.

    Example:
        source: "sap"
        product: "PROD-A"
        quantity: 100.0
        date: 2026-01-10
    """

    source: str
    product: str
    quantity: float
    date: date
    extra: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "product": self.product,
            "quantity": self.quantity,
            "date": self.date.isoformat() if hasattr(self.date, "isoformat") else str(self.date),
            **(self.extra or {}),
        }
