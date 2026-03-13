"""Query analysis node - analyzes user intent to understand what they are asking about."""
from typing import Dict, Any
import logging
from datetime import datetime
from ...llm.azure_openai import AzureOpenAIClient
from ..state import AnalyticsState
from ..prompts import QUERY_ANALYSIS_SYSTEM_PROMPT, get_query_analysis_user_prompt
from ..utils import parse_json_response, save_llm_call_input, save_llm_call_output
from config.settings import settings

logger = logging.getLogger(__name__)

_MAX_INTENT_RETRIES = 1


def _parse_intent_response(response: str, user_query: str) -> Dict[str, Any]:
    """Parse LLM response as a single JSON object. Raises if not a dict."""
    if not response or not response.strip():
        raise ValueError("Empty response from LLM")
    parsed = parse_json_response(response, expected_type=dict)
    if not isinstance(parsed, dict):
        raise ValueError(f"Expected dict, got {type(parsed)}")
    return parsed


async def parse_query_node(state: AnalyticsState, model: str = None) -> Dict[str, Any]:
    """
    Parse user query - analyzes user intent. Returns parsed_intent as a valid dict.
    Enforces strict JSON object from LLM with retry; single fallback only on failure.
    """
    start_time = datetime.now()
    node_name = "query_analysis"

    from ..node_timing_registry import get_node_timing_registry
    registry = get_node_timing_registry()
    if registry:
        registry.record_node_start(node_name, start_time)

    logger.info(f"[{node_name}] Starting Phase 1: Query Intent Analysis")

    user_query = state.get("user_query", "")
    analysis_mode = state.get("analysis_mode", "normal")
    user_context = state.get("user_context")
    feedback_summary = state.get("feedback_summary")
    org_context = state.get("org_context")

    if not user_query:
        logger.warning(f"[{node_name}] Empty user query - creating fallback response")
        return {
            "parsed_intent": {"user_query": "", "intent_explanation": ""},
            "status": "parsed",
        }

    logger.info(f"[{node_name}] Analyzing user query: '{user_query[:100]}{'...' if len(user_query) > 100 else ''}'")

    llm_client = state.get("llm_client") or AzureOpenAIClient()
    model_name = model or settings.analytics_parse_query_model
    from datetime import date as _date
    user_prompt = get_query_analysis_user_prompt(
        user_query, analysis_mode,
        user_context=user_context,
        feedback_summary=feedback_summary,
        org_context=org_context,
        current_date_iso=_date.today().isoformat(),
    )
    query_id = state.get("query_id")
    save_llm_call_input(
        node_name=node_name,
        query_id=query_id,
        system_prompt=QUERY_ANALYSIS_SYSTEM_PROMPT,
        user_prompt=user_prompt,
        extra={"model": model_name},
    )
    parsed_response = None
    last_error = None

    for attempt in range(_MAX_INTENT_RETRIES + 1):
        try:
            response = await llm_client._call_llm_unified(
                model=model_name,
                system_prompt=QUERY_ANALYSIS_SYSTEM_PROMPT,
                user_prompt=user_prompt,
                node_name=node_name,
                query_id=query_id,
                temperature=0.3,
                use_json_mode=True,
            )
            parsed_response = _parse_intent_response(response, user_query)
            save_llm_call_output(
                node_name=node_name,
                query_id=query_id,
                raw_response=response,
                parsed=parsed_response,
            )
            break
        except Exception as e:
            last_error = e
            logger.warning(f"[{node_name}] Intent parse attempt {attempt + 1} failed: {e}")
            if attempt < _MAX_INTENT_RETRIES:
                logger.info(f"[{node_name}] Retrying LLM call for valid JSON object")

    if parsed_response is None:
        logger.warning(f"[{node_name}] All attempts failed ({last_error}), using minimal fallback")
        parsed_response = {
            "user_query": user_query,
            "intent_explanation": "",
        }

    analyzed_user_query = parsed_response.get("user_query", user_query)
    intent_explanation = parsed_response.get("intent_explanation", "")
    if not isinstance(intent_explanation, str):
        intent_explanation = str(intent_explanation)
    if not intent_explanation:
        intent_explanation = f"User is asking about: {user_query}"

    parsed_intent = dict(parsed_response)
    parsed_intent["user_query"] = analyzed_user_query
    parsed_intent["intent_explanation"] = intent_explanation

    duration = (datetime.now() - start_time).total_seconds()
    logger.info(
        f"[{node_name}] Query intent analysis completed | Duration: {duration:.2f}s | "
        f"Query: {analyzed_user_query[:80]}{'...' if len(analyzed_user_query) > 80 else ''} | "
        f"Intent: {len(intent_explanation)} chars"
    )
    # Log parsed intent for debugging: keys, query_type, analytical_scope, intent_explanation preview
    if isinstance(parsed_intent, dict):
        _keys = list(parsed_intent.keys())
        _qt = parsed_intent.get("query_type", "")
        _scope = parsed_intent.get("analytical_scope", "")
        _preview = (intent_explanation or "")[:400].replace("\n", " ")
        if len(intent_explanation or "") > 400:
            _preview += "..."
        logger.info(
            f"[{node_name}] parsed_intent | keys={_keys} | query_type={_qt!r} | analytical_scope={_scope!r}"
        )
        logger.info(f"[{node_name}] intent_explanation (preview): {_preview}")

    ws_manager = state.get("ws_manager")
    if ws_manager:
        try:
            query_type = parsed_intent.get("query_type", "unknown") if isinstance(parsed_intent, dict) else "unknown"
            await ws_manager.send_progress(
                node_name=node_name,
                message="Query analysis complete",
                status="complete",
                details=f"Analyzed query type: {query_type}",
                data={
                    "query_analysis_summary": {
                        "query_type": query_type,
                        "analysis_mode": analysis_mode,
                        "has_user_context": bool(user_context),
                        "has_feedback": bool(feedback_summary),
                        "has_org_context": bool(org_context),
                        "duration_seconds": round(duration, 2),
                        "query_length": len(user_query),
                    }
                },
            )
        except Exception as e:
            logger.warning(f"[{node_name}] Failed to send summary to frontend: {e}")

    return {
        "parsed_intent": parsed_intent,
        "status": "parsed",
    }
