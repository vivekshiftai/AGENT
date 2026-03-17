"""Single LLM step: read user query and decide action, intent, date range, sources, and whether to ask for more info."""
import json
import logging
import re
from typing import Any, Dict, Optional

from src.domain.entities.query_analysis import QueryAction

logger = logging.getLogger(__name__)


def _parse_llm_analysis(text: str) -> Optional[Dict[str, Any]]:
    """Parse JSON from LLM response (strip markdown if present)."""
    if not text or not text.strip():
        return None
    raw = text.strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```\s*$", "", raw)
    raw = raw.strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def run(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    One LLM call: read user query and decide whether we can proceed, need clarification, or reject.
    Sets analysisResult (intent, action, date_range, clarification_question). Datasource is chosen by the user in the UI.
    Fallback: AnalyzeQueryUseCase (heuristic) when LLM unavailable or parse fails.
    """
    user_query = state.get("userQuery") or ""
    if not user_query and state.get("messages"):
        last = state["messages"][-1]
        user_query = last.get("content", "") if isinstance(last, dict) else str(last)
    if not user_query:
        return {
            **state,
            "analysisResult": {
                "intent": "unknown",
                "needs_clarification": True,
                "action": QueryAction.ASK_CLARIFICATION.value,
                "clarification_question": "Please provide your question or request.",
            },
            "dateRange": None,
            "needsClarification": True,
        }

    result = None
    try:
        from src.ai.llm.client_factory import get_llm_client
        from src.ai.llm.prompts.query_analysis import (
            QUERY_UNDERSTANDING_SYSTEM,
            build_query_understanding_prompt,
        )
        from src.core.config import settings
        llm = get_llm_client()
        model = settings.planning_query_understanding_model or getattr(
            llm, "_default_model", lambda: "claude-sonnet-4-6"
        )()
        user_prompt = build_query_understanding_prompt(user_query)
        if hasattr(llm, "call_llm_unified"):
            response = llm.call_llm_unified(
                model=model,
                system_prompt=QUERY_UNDERSTANDING_SYSTEM,
                user_prompt=user_prompt,
                use_json_mode=True,
                default_max_tokens=1024,
            )
        else:
            response = llm.invoke(
                [
                    {"role": "system", "content": QUERY_UNDERSTANDING_SYSTEM + "\n\nRespond with valid JSON only."},
                    {"role": "user", "content": user_prompt},
                ],
                model=model,
                max_tokens=1024,
            )
        result = _parse_llm_analysis(response)
    except Exception as e:
        logger.warning("QueryUnderstanding LLM failed, using heuristic: %s", e)

    if not result or not isinstance(result, dict):
        analyze_uc = state.get("_analyze_query_use_case")
        if not analyze_uc:
            from src.application.use_cases.analyze_query_usecase import AnalyzeQueryUseCase
            analyze_uc = AnalyzeQueryUseCase()
        result = analyze_uc.execute(user_query, context=state)

    action = (result.get("action") or "proceed").strip().lower()
    if action not in ("proceed", "ask_clarification", "reject"):
        action = "proceed"
    date_range = result.get("date_range") or result.get("dateRange")
    needs_clarification = result.get("needs_clarification", action == "ask_clarification") or (action == "ask_clarification")
    products = result.get("products")
    if products is not None and not isinstance(products, list):
        products = [products] if products else []
    elif products is None:
        products = []

    logger.info(
        "Query understanding: action=%s intent=%s need_clarification=%s products=%s",
        action, result.get("intent"), needs_clarification, products or None,
    )
    return {
        **state,
        "analysisResult": {
            **result,
            "action": action,
            "date_range": date_range,
            "needs_clarification": needs_clarification,
            "products": products,
        },
        "dateRange": date_range,
        "products": products,
        "needsClarification": needs_clarification,
    }
