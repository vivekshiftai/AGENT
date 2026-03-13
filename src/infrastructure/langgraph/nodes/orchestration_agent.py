"""Orchestration agent node — entry point; decides flow (clarification / simple / moderate) from user query only."""
from typing import Dict, Any, List
import logging
from datetime import datetime

from ...llm.azure_openai import AzureOpenAIClient
from ..state import AnalyticsState
from ..prompts import ORCHESTRATION_AGENT_SYSTEM_PROMPT, get_orchestration_agent_user_prompt
from ..utils import parse_json_response, save_llm_call_input, save_llm_call_output
from config.settings import settings

logger = logging.getLogger(__name__)


async def orchestration_agent_node(state: AnalyticsState, model: str = None) -> Dict[str, Any]:
    """
    Entry point: decide flow from user query only. Next node in graph is query_analysis (when simple/moderate) or END (clarification).

    - If the query is NOT clear enough → decision=clarification; graph ends, return message to user.
    - If simple or moderate → graph routes to query_analysis node, then to simple_workflow or moderate_workflow.
    """
    start_time = datetime.now()
    node_name = "orchestration_agent"

    logger.info(f"[{node_name}] Starting (entry point)")

    state_updates: Dict[str, Any] = {}
    user_query = (state.get("user_query") or "").strip()

    # Entry point: orchestration LLM sees only the user query (no parsed_intent yet)
    llm_client = state.get("llm_client") or AzureOpenAIClient()
    model_name = model or getattr(settings, "analytics_orchestration_agent_model", None) or settings.analytics_parse_query_model
    from datetime import date as _date
    user_prompt = get_orchestration_agent_user_prompt(user_query, current_date_iso=_date.today().isoformat())
    query_id = state.get("query_id")

    save_llm_call_input(
        node_name=node_name,
        query_id=query_id,
        system_prompt=ORCHESTRATION_AGENT_SYSTEM_PROMPT,
        user_prompt=user_prompt,
        extra={"model": model_name},
    )

    try:
        response = await llm_client._call_llm_unified(
            model=model_name,
            system_prompt=ORCHESTRATION_AGENT_SYSTEM_PROMPT,
            user_prompt=user_prompt,
            node_name=node_name,
            query_id=query_id,
            temperature=0.2,
            use_json_mode=True,
        )
        parsed = parse_json_response(response, expected_type=dict)
        if not isinstance(parsed, dict):
            raise ValueError("Expected dict")
        save_llm_call_output(node_name=node_name, query_id=query_id, raw_response=response, parsed=parsed)

        decision = (parsed.get("decision") or "").strip().lower()
        if decision not in ("clarification", "simple", "moderate"):
            decision = "moderate"
        # Suggestions must be a separate list of strings (UI shows as clickable options)
        raw_suggestions = parsed.get("suggestions")
        suggestions: List[str] = []
        if isinstance(raw_suggestions, list):
            for s in raw_suggestions:
                if not s:
                    continue
                if isinstance(s, str):
                    suggestions.append(s.strip())
                elif isinstance(s, dict):
                    # LLM sometimes returns {"text": "..."} or {"query": "..."}
                    text = (s.get("text") or s.get("query") or s.get("label") or "").strip()
                    if text:
                        suggestions.append(text)
                else:
                    suggestions.append(str(s).strip())
            suggestions = [q for q in suggestions if q]
        if decision == "clarification":
            # Strict JSON form: clarification_message (string) + suggestions (array of strings)
            llm_message = (parsed.get("clarification_message") or "").strip()
            clarification_message = llm_message or "What would you like me to focus on? You can ask about revenue, costs, trends, or specific metrics."
            # Always return a list for suggestions (UI expects array; empty list if LLM omitted or wrong type)
            clarification_suggestions = suggestions if suggestions else []
        else:
            clarification_message = None
            clarification_suggestions = []

        logger.info(f"[{node_name}] Decision: {decision}" + (f" | clarification length: {len(clarification_message)}" if clarification_message else "") + (f" | suggestions: {len(suggestions)}" if suggestions else ""))

        return {
            **state_updates,
            "orchestrator_decision": decision,
            "clarification_message": clarification_message if decision == "clarification" else None,
            "clarification_suggestions": clarification_suggestions if decision == "clarification" else [],
        }
    except Exception as e:
        logger.warning(f"[{node_name}] LLM orchestration failed: {e} — defaulting to moderate")
        return {**state_updates, "orchestrator_decision": "moderate", "clarification_message": None, "clarification_suggestions": []}
