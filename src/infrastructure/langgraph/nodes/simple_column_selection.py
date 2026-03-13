"""Simple flow column selection: one dimension (user-asked) + query-related measures only.

Used only when orchestrator_decision == "simple". Outputs filtered_analytical_dimensions
and filtered_analytical_measures for consumption by simple_analytical_fetch_plan.
"""
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

from ...llm.azure_openai import AzureOpenAIClient
from ..state import AnalyticsState
from ..prompts import (
    SIMPLE_COLUMN_SELECTION_SYSTEM_PROMPT,
    get_simple_column_selection_user_prompt,
)
from .analytical_column_selection import run_analytical_date_fiscal_filter
from ..utils import (
    parse_json_response_required_dict,
    save_llm_call_input,
    save_llm_call_output,
)

logger = logging.getLogger(__name__)

NODE_NAME = "simple_column_selection"


def _column_line(col: Dict[str, Any]) -> str:
    name = (col.get("name") or "").strip()
    label = (col.get("label") or name).strip()
    data_type = (col.get("data_type") or "Edm.String").strip()
    if not name:
        return ""
    return f"  {name} | {label} | {data_type}"


def _find_by_name(columns: List[Dict[str, Any]], name: str) -> Optional[Dict[str, Any]]:
    name = (name or "").strip()
    for c in columns or []:
        if not isinstance(c, dict):
            continue
        if (c.get("name") or "").strip() == name:
            return dict(c)
    return None


async def simple_column_selection_node(
    state: AnalyticsState,
    model: str = None,
) -> Dict[str, Any]:
    """
    Select exactly one dimension (the one the user asked about) and only
    query-related measures. Used only in the simple flow.

    Reads: user_query, parsed_intent, analytical_dimensions, analytical_measures.
    Writes: filtered_analytical_dimensions (list of one dict), filtered_analytical_measures.
    """
    start_time = datetime.now()
    user_query = state.get("user_query", "")
    parsed_intent = state.get("parsed_intent", {})
    dimensions = state.get("analytical_dimensions", [])
    measures = state.get("analytical_measures", [])

    from ..node_timing_registry import get_node_timing_registry
    registry = get_node_timing_registry()
    if registry:
        registry.record_node_start(NODE_NAME, start_time)

    if not dimensions and not measures:
        logger.info("%s No analytical schema - skipping", NODE_NAME)
        return {}

    if not user_query:
        logger.warning("%s No user query - skipping", NODE_NAME)
        return {}

    dimensions_text = "\n".join(
        _column_line(d) for d in dimensions if _column_line(d)
    )
    measures_text = "\n".join(
        _column_line(m) for m in measures if _column_line(m)
    )

    view_name: Optional[str] = None
    for d in dimensions:
        v = (d.get("view_name") or "").strip()
        if v and v != "_default":
            view_name = v
            break
    if not view_name:
        for m in measures:
            v = (m.get("view_name") or "").strip()
            if v and v != "_default":
                view_name = v
                break

    user_prompt = get_simple_column_selection_user_prompt(
        user_query=user_query,
        parsed_intent=parsed_intent,
        dimensions_text=dimensions_text,
        measures_text=measures_text,
        view_name=view_name,
    )

    llm_client = state.get("llm_client") or AzureOpenAIClient()
    from config.settings import settings
    model_name = model or settings.analytics_analytical_column_selection_model
    query_id = state.get("query_id")

    save_llm_call_input(
        query_id=query_id,
        node_name=NODE_NAME,
        call_suffix="simple_col",
        system_prompt=SIMPLE_COLUMN_SELECTION_SYSTEM_PROMPT,
        user_prompt=user_prompt,
    )

    try:
        response = await llm_client._call_llm_unified(
            model=model_name,
            system_prompt=SIMPLE_COLUMN_SELECTION_SYSTEM_PROMPT,
            user_prompt=user_prompt,
            node_name=NODE_NAME,
            query_id=query_id,
            temperature=0.2,
            use_json_mode=True,
        )
    except Exception as e:
        logger.warning("%s LLM call failed: %s — using first dimension only, no measures", NODE_NAME, e)
        filtered_dims: List[Dict[str, Any]] = []
        if dimensions:
            d = dict(dimensions[0])
            d.setdefault("selection_reasoning", "Fallback: first dimension (LLM failed).")
            filtered_dims.append(d)
        return {
            "filtered_analytical_dimensions": filtered_dims,
            "filtered_analytical_measures": [],
        }

    raw = (response or "").strip()
    save_llm_call_output(node_name=NODE_NAME, query_id=query_id, raw_response=raw, call_suffix="simple_col")

    parsed = parse_json_response_required_dict(raw, NODE_NAME)
    if not parsed:
        logger.warning("%s No valid JSON — using first dimension only, no measures", NODE_NAME)
        filtered_dims_fallback: List[Dict[str, Any]] = []
        if dimensions:
            d = dict(dimensions[0])
            d.setdefault("selection_reasoning", "Fallback: first dimension (no valid JSON).")
            filtered_dims_fallback.append(d)
        return {
            "filtered_analytical_dimensions": filtered_dims_fallback,
            "filtered_analytical_measures": [],
        }

    # No related data for user query → end flow; UI shows message + suggested_queries (like clarification)
    no_related = parsed.get("no_related_data") in (True, "true", "yes")
    if no_related:
        user_message = (parsed.get("user_message") or "There is no related data for your query in this data source. Try one of the suggestions below.").strip()
        raw_sug = parsed.get("suggested_queries")
        suggested_queries: List[str] = []
        if isinstance(raw_sug, list):
            suggested_queries = [str(s).strip() for s in raw_sug if s][:5]
        if not suggested_queries:
            suggested_queries = ["Revenue by plant for last quarter", "Spend by category YTD", "Top 5 products by sales this year"]
        logger.info("%s No related data for query — ending flow; user_message=%s, suggestions=%s", NODE_NAME, user_message[:60], len(suggested_queries))
        return {
            "data_sufficiency_result": {
                "can_answer": False,
                "reason_if_not": "Column selection: no dimension/measure match for user query.",
                "summary_of_what_we_have": "Available dimensions and measures in schema.",
                "user_message": user_message,
                "suggested_queries": suggested_queries,
            },
            "filtered_analytical_dimensions": [],
            "filtered_analytical_measures": [],
        }

    primary_dim_name = (parsed.get("primary_dimension") or "").strip()
    measure_names = parsed.get("measures")
    if not isinstance(measure_names, list):
        measure_names = []

    filtered_dims: List[Dict[str, Any]] = []
    if primary_dim_name:
        dim_dict = _find_by_name(dimensions, primary_dim_name)
        if dim_dict:
            dim_dict.setdefault("selection_reasoning", "User-asked dimension for simple flow.")
            filtered_dims.append(dim_dict)
            logger.info("%s Primary dimension: %s", NODE_NAME, primary_dim_name)
        else:
            logger.warning("%s LLM dimension %r not in schema — using first dimension", NODE_NAME, primary_dim_name)
            if dimensions:
                d = dict(dimensions[0])
                d.setdefault("selection_reasoning", "Fallback: first dimension (LLM name not in schema).")
                filtered_dims.append(d)

    if not filtered_dims and dimensions:
        d = dict(dimensions[0])
        d.setdefault("selection_reasoning", "Fallback: first dimension (no primary_dimension).")
        filtered_dims.append(d)
        logger.info("%s No primary dimension — using first: %s", NODE_NAME, d.get("name"))

    filtered_meas: List[Dict[str, Any]] = []
    for name in measure_names:
        m = _find_by_name(measures, name)
        if m:
            m.setdefault("selection_reasoning", "Query-related measure for simple flow.")
            filtered_meas.append(m)
    if not filtered_meas:
        logger.info("%s No measures selected by LLM — keeping measures empty", NODE_NAME)

    logger.info(
        "%s Selected 1 dimension, %s measures",
        NODE_NAME,
        len(filtered_meas),
    )

    result: Dict[str, Any] = {
        "filtered_analytical_dimensions": filtered_dims,
        "filtered_analytical_measures": filtered_meas,
    }
    try:
        filter_updates = await run_analytical_date_fiscal_filter(
            state, filtered_dims, filtered_meas, NODE_NAME
        )
        result.update(filter_updates)
    except Exception as e:
        logger.warning("%s Date/fiscal filter failed: %s", NODE_NAME, e)
    return result
