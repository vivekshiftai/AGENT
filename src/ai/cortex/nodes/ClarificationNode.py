"""Check if clarification is required; if so return clarification message and pause graph."""
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


def run(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    If analysisResult indicates ask_clarification or reject, set response and
    needsClarification/rejected so the graph can pause or end.
    """
    analysis = state.get("analysisResult") or {}
    action = (analysis.get("action") or "").lower()

    if action == "ask_clarification":
        question = analysis.get("clarification_question") or "Could you provide more details?"
        logger.info("Clarification needed: %s", question[:80])
        return {
            **state,
            "response": question,
            "needsClarification": True,
        }
    if action == "reject":
        reason = analysis.get("rejection_reason") or "Request could not be processed."
        return {
            **state,
            "response": reason,
            "rejected": True,
        }
    return {
        **state,
        "needsClarification": False,
        "rejected": False,
    }
