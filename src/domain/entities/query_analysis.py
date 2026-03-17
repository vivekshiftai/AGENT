"""Structured result of query analysis for the reasoning pipeline."""
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class QueryAction(str, Enum):
    """Action determined by query analysis."""

    PROCEED = "proceed"
    ASK_CLARIFICATION = "ask_clarification"
    REJECT = "reject"


@dataclass
class DateRange:
    """Date range for filtering data."""

    start: str  # YYYY-MM-DD
    end: str  # YYYY-MM-DD

    def to_dict(self) -> Dict[str, str]:
        return {"start": self.start, "end": self.end}


@dataclass
class QueryAnalysisResult:
    """
    Structured decision from query analysis stage.

    Example:
        intent: fetch_production_data
        needs_clarification: False
        date_range: { start: "2026-01-01", end: "2026-01-15" }
        required_sources: ["sap", "clickhouse"]
        action: proceed
    """

    intent: str
    needs_clarification: bool
    date_range: Optional[DateRange] = None
    required_sources: List[str] = field(default_factory=list)
    action: QueryAction = QueryAction.PROCEED
    clarification_question: Optional[str] = None
    rejection_reason: Optional[str] = None
    raw: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        out = {
            "intent": self.intent,
            "needs_clarification": self.needs_clarification,
            "required_sources": self.required_sources,
            "action": self.action.value,
        }
        if self.date_range:
            out["date_range"] = self.date_range.to_dict()
        if self.clarification_question:
            out["clarification_question"] = self.clarification_question
        if self.rejection_reason:
            out["rejection_reason"] = self.rejection_reason
        return out
