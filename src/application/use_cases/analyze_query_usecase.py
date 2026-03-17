"""Use case: analyze user query and return structured decision."""
import logging
from typing import Any, Dict

from src.domain.entities.query_analysis import (
    DateRange,
    QueryAction,
    QueryAnalysisResult,
)

logger = logging.getLogger(__name__)


class AnalyzeQueryUseCase:
    """Analyzes a natural language query and returns intent, date range, sources, action."""

    def __init__(self, query_analyzer=None):
        """query_analyzer: callable(state) -> QueryAnalysisResult or dict. Optional LLM."""
        self._analyzer = query_analyzer

    def execute(self, user_message: str, context: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Return structured analysis: intent, needs_clarification, date_range,
        required_sources, action (proceed | ask_clarification | reject).
        """
        context = context or {}
        state = {"messages": [{"role": "user", "content": user_message}], **context}
        try:
            if self._analyzer:
                result = self._analyzer(state)
                if isinstance(result, QueryAnalysisResult):
                    return result.to_dict()
                if isinstance(result, dict):
                    return result
            # Default: heuristic analysis without LLM
            return self._heuristic_analysis(user_message)
        except Exception as e:
            logger.exception("AnalyzeQueryUseCase failed: %s", e)
            return {
                "intent": "unknown",
                "needs_clarification": True,
                "required_sources": [],
                "action": QueryAction.ASK_CLARIFICATION.value,
                "clarification_question": "I couldn't parse your request. Please specify what data you need and the date range.",
                "error": str(e),
            }

    def _heuristic_analysis(self, message: str) -> Dict[str, Any]:
        """Simple rule-based analysis when no LLM is provided."""
        msg = (message or "").strip().lower()
        # Default action proceed; require date range for production/demand/inventory
        needs_clarification = False
        clarification_question = None
        date_range = None
        # Try to detect date range in message (simple patterns)
        import re
        date_match = re.search(
            r"(?:between|from)\s+(\d{4}-\d{2}-\d{2}|\w+\s+\d{1,2},?\s+\d{4})\s+(?:and|to)\s+(\d{4}-\d{2}-\d{2}|\w+\s+\d{1,2},?\s+\d{4})",
            message or "",
            re.IGNORECASE,
        )
        if date_match:
            start_s, end_s = date_match.group(1), date_match.group(2)
            # Normalize to YYYY-MM-DD if needed (simplified)
            if re.match(r"\d{4}-\d{2}-\d{2}", start_s) and re.match(r"\d{4}-\d{2}-\d{2}", end_s):
                date_range = {"start": start_s, "end": end_s}
        if any(k in msg for k in ("demand", "production", "inventory", "schedule", "plan")):
            if not date_range and "date" not in msg and "when" not in msg:
                needs_clarification = True
                clarification_question = "Which date range should I use for this data?"
        required_sources = []
        if "sap" in msg or "datasphere" in msg:
            required_sources.append("sap")
        if "clickhouse" in msg:
            required_sources.append("clickhouse")
        if "postgres" in msg or "postgresql" in msg:
            required_sources.append("postgres")
        if "mysql" in msg:
            required_sources.append("mysql")
        if "excel" in msg or "csv" in msg or "file" in msg:
            required_sources.append("excel")
        if not required_sources:
            required_sources = ["sap", "clickhouse", "postgres"]
        action = QueryAction.ASK_CLARIFICATION.value if needs_clarification else QueryAction.PROCEED.value
        return {
            "intent": "fetch_production_data" if not needs_clarification else "clarify",
            "needs_clarification": needs_clarification,
            "date_range": date_range,
            "required_sources": required_sources,
            "action": action,
            "clarification_question": clarification_question,
        }
