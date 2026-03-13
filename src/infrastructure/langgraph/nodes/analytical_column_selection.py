"""Analytical Column Selection Node - LLM-driven selection and date/fiscal filter.

This node makes up to two kinds of LLM calls in the analytical flow:

1. COLUMN SELECTION (one or more LLM calls): After the analytical schema has been
   extracted (dimensions and measures with labels), an LLM selects columns relevant
   to the user's query. Output is a filtered schema used by chart pre-plan and
   analytical_fetch_plan.

2. DATE OR FISCAL FILTER (exactly one path):
   - If the view has date columns (Edm.Date): use the DATE FILTER LLM to get
     date_column, start_date, end_date (YYYY-MM-DD) for filtering.
   - If the view has no date columns but has fiscal columns (analytical view with
     Edm.Int64 fiscal dimensions): use the FISCAL FILTER LLM to get fiscal_column
     and start_value/end_value (integer period codes) for input_parameters.

This module does NOT execute SAP fetches or construct API calls.
"""
import asyncio
import logging
import json
from typing import Dict, Any, List, Optional, Set, Tuple, Union
from datetime import date, datetime

from ...llm.azure_openai import AzureOpenAIClient
from ..state import AnalyticsState
from ..prompts import (
    ANALYTICAL_COLUMN_SELECTION_SYSTEM_PROMPT,
    get_analytical_column_selection_user_prompt,
    ANALYTICAL_DATE_FILTER_SYSTEM_PROMPT,
    get_analytical_date_filter_user_prompt,
    ANALYTICAL_FISCAL_FILTER_SYSTEM_PROMPT,
    get_analytical_fiscal_filter_user_prompt,
)
from ..utils import (
    LLM_COL_VALIDATE_TAG,
    parse_json_response,
    parse_json_response_required_dict,
    save_llm_call_input,
    save_llm_call_output,
    try_repair_truncated_analytical_selection,
    validate_llm_columns,
)
from ..utils.sap_fetch_helpers import (
    is_fiscal_column,
    build_fiscal_input_parameters,
    choose_fiscal_granularity,
    compute_fiscal_range_for_dates,
    compute_ytd_fiscal_range,
    pick_fiscal_input_parameter_column,
)
from ..process_order_columns import (
    PROCESS_ORDER_DATE_COLUMN,
    PROCESS_ORDER_DIMENSIONS,
    PROCESS_ORDER_MEASURES,
)

logger = logging.getLogger(__name__)

# Tag for easy grepping when debugging column selection failures
COLUMN_SELECT_TAG = "[COLUMN_SELECT]"

# Single combined list of columns (dimensions + measures); each LLM call receives 300 columns
COLUMNS_PER_CHUNK = 1000
# When total columns <= this, use a single LLM call
SINGLE_CALL_MAX_COLUMNS = 1000
# Max concurrent LLM calls when processing column chunks (avoid overload)
MAX_CONCURRENT_LLM_CALLS = 5

# EDM types for date dimensions (to ask LLM for date filter)
_DATE_EDM_TYPES = {"Edm.Date", "Edm.DateTimeOffset", "Edm.TimeOfDay"}
# Date filter: only use type "date" (Edm.Date). No hardcoded fiscal date; DateTimeOffset/TimeOfDay commented out for now.
_DATE_FILTER_TYPES_ONLY = {"Edm.Date"}
# Fiscal column types — used when no Edm.Date columns; detect via name + Int64 type
_FISCAL_EDM_TYPES = {"Edm.Int64", "Edm.Int32"}

def _extract_dimensions_from_parsed(parsed: Dict[str, Any]) -> Tuple[List[str], Dict[str, str], Dict[str, int]]:
    """
    Extract dimension names, reasoning, and priority from LLM response.
    Handles dimensions as array of strings or array of {name, reasoning, priority}.
    Returns (list of names, dict of name -> reasoning, dict of name -> priority).
    """
    if not isinstance(parsed, dict):
        return [], {}, {}
    inner = parsed
    for key in ("output", "selection", "result", "data"):
        if isinstance(parsed.get(key), dict):
            inner = parsed[key]
            break
    arr = inner.get("dimensions") or []
    if not isinstance(arr, list):
        return [], {}, {}
    names: List[str] = []
    reasons: Dict[str, str] = {}
    priorities: Dict[str, int] = {}
    for x in arr:
        if not x:
            continue
        if isinstance(x, dict):
            name = (x.get("name") or x.get("column") or "").strip()
            if name:
                names.append(name)
                r = x.get("reasoning") or x.get("reason")
                if r and isinstance(r, str):
                    reasons[name] = r.strip()
                # Extract priority (0-9, where 0 = highest)
                p = x.get("priority")
                if p is not None:
                    try:
                        priorities[name] = int(p)
                    except (ValueError, TypeError):
                        priorities[name] = 5  # default priority
        else:
            n = str(x).strip()
            if n:
                names.append(n)
    return names, reasons, priorities


def _flatten_measure_names_from_by_category(mbc: Dict[str, List[str]]) -> List[str]:
    """Return a flat list of all measure names from measures_by_category (normalized form)."""
    if not mbc or not isinstance(mbc, dict):
        return []
    return [n for names in mbc.values() for n in (names or []) if isinstance(n, str) and n.strip()]


def _normalize_measures_by_category_from_llm(raw: Any) -> Tuple[Dict[str, List[str]], Dict[str, int]]:
    """
    Normalize LLM's measures_by_category to Dict[category_name, List[measure_name]].
    Also extracts category priorities.
    Handles object with array of names or array of {name: "...", priority: N}; keys are category labels (kept as-is).
    Returns (dict of category->names, dict of category->priority).
    """
    if not raw or not isinstance(raw, dict):
        return {}, {}
    out: Dict[str, List[str]] = {}
    priorities: Dict[str, int] = {}
    for cat_key, val in raw.items():
        cat = (cat_key or "").strip()
        if not cat:
            continue
        # Extract category priority if present
        cat_priority = None
        if isinstance(val, dict):
            # New format: {"priority": 0, "measures": [...]}
            cat_priority = val.get("priority")
            val = val.get("measures", [])
        if isinstance(val, list):
            names = []
            for x in val:
                if isinstance(x, dict):
                    n = (x.get("name") or x.get("column") or "").strip()
                    if n:
                        names.append(n)
                elif isinstance(x, str) and x.strip():
                    names.append(x.strip())
            if names:
                out[cat] = names
                if cat_priority is not None:
                    try:
                        priorities[cat] = int(cat_priority)
                    except (ValueError, TypeError):
                        priorities[cat] = 5
        elif isinstance(val, str) and val.strip():
            out[cat] = [val.strip()]
            if cat_priority is not None:
                try:
                    priorities[cat] = int(cat_priority)
                except (ValueError, TypeError):
                    priorities[cat] = 5
    return out, priorities


def _build_measures_by_category_from_llm(
    filtered_meas: List[Dict[str, Any]],
    measures_by_category: Dict[str, List[str]],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Build filtered_analytical_measures_by_group from LLM's measures_by_category.
    measures_by_category: category_name -> list of measure names (from LLM).
    Each measure dict gets "category" set; returns dict category -> list of full measure dicts.
    Measures in filtered_meas that are not in any category are placed in "other".
    """
    name_to_measure: Dict[str, Dict[str, Any]] = {}
    for m in filtered_meas or []:
        if not isinstance(m, dict):
            continue
        name = (m.get("name") or "").strip()
        if name:
            name_to_measure[name] = m
    by_group: Dict[str, List[Dict[str, Any]]] = {}
    assigned: Set[str] = set()
    for category, names in (measures_by_category or {}).items():
        if not category:
            continue
        lst: List[Dict[str, Any]] = []
        for n in names or []:
            name = (n if isinstance(n, str) else (n.get("name") or n.get("column") or "")).strip()
            if not name:
                continue
            if name in name_to_measure:
                m = dict(name_to_measure[name])
                m["category"] = category
                lst.append(m)
                assigned.add(name)
        if lst:
            by_group[category] = lst
    # Any measure not assigned by the LLM goes to "other"
    other: List[Dict[str, Any]] = []
    for name, m in name_to_measure.items():
        if name not in assigned:
            mc = dict(m)
            mc["category"] = "other"
            other.append(mc)
    if other:
        by_group["other"] = other
    return by_group


def _build_selected_columns_text(
    filtered_dims: List[Dict[str, Any]],
    filtered_meas: List[Dict[str, Any]],
) -> str:
    """Build text for LLM: one line per column as '  name | label | data_type' (dimensions then measures)."""
    lines: List[str] = []
    for col in (filtered_dims or []) + (filtered_meas or []):
        if not isinstance(col, dict):
            continue
        name = (col.get("name") or "").strip()
        label = (col.get("label") or name).strip()
        data_type = (col.get("data_type") or "Edm.String").strip()
        if name:
            lines.append(f"  {name} | {label} | {data_type}")
    return "\n".join(lines) if lines else ""


def _schema_column_to_data_type(
    filtered_dims: List[Dict[str, Any]],
    filtered_meas: List[Dict[str, Any]],
) -> Dict[str, str]:
    """Build map column name -> data_type from schema (dimensions + measures). First occurrence wins."""
    out: Dict[str, str] = {}
    for col in (filtered_dims or []) + (filtered_meas or []):
        if not isinstance(col, dict):
            continue
        name = (col.get("name") or "").strip()
        if not name or name in out:
            continue
        out[name] = (col.get("data_type") or "Edm.String").strip()
    return out


def _parse_and_validate_value_filters(
    value_filters_raw: Any,
    allowed_column_names: Set[str],
    node_name: str,
    schema_column_to_data_type: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """Parse value_filters from LLM response; keep only entries whose column is in allowed_column_names.
    If schema_column_to_data_type is provided, use schema data_type for each column (override LLM type when different)."""
    result: List[Dict[str, Any]] = []
    if not isinstance(value_filters_raw, list):
        return result
    schema_types = schema_column_to_data_type or {}
    for vf in value_filters_raw:
        if not isinstance(vf, dict):
            continue
        col = (vf.get("column") or vf.get("column_name") or "").strip()
        if not col or col not in allowed_column_names:
            if col:
                logger.debug(
                    f"[{node_name}] value_filters: skipping column {col!r} (not in selected columns)"
                )
            continue
        op = (vf.get("operator") or "eq").strip() or "eq"
        value = vf.get("value")
        if value is None:
            continue
        llm_data_type = (vf.get("data_type") or "Edm.String").strip()
        data_type = schema_types.get(col, llm_data_type)
        if schema_types and col in schema_types and schema_types[col] != llm_data_type:
            logger.info(
                f"[{node_name}] value_filters: column {col!r} LLM type {llm_data_type!r} != schema type {schema_types[col]!r} — using schema type"
            )
        result.append({
            "column": col,
            "operator": op,
            "value": str(value).strip(),
            "data_type": data_type,
        })
    return result


async def run_analytical_date_fiscal_filter(
    state: AnalyticsState,
    filtered_dims: List[Dict[str, Any]],
    filtered_meas: List[Dict[str, Any]],
    node_name: str = "analytical_column_selection",
) -> Dict[str, Any]:
    """
    Run date or fiscal filter LLM using the same prompts as full analysis.
    Returns dict with optional keys: analytical_date_filter, sap_fiscal_filter, applied_date_filters.
    Used by both analytical_column_selection and simple_column_selection (direct responses).
    """
    from config.settings import settings

    result: Dict[str, Any] = {}
    user_query = (state.get("user_query") or "").strip()
    parsed_intent = state.get("parsed_intent")
    query_id = state.get("query_id")
    llm_client = state.get("llm_client") or AzureOpenAIClient()

    all_dimensions_for_date_filter = state.get("analytical_dimensions", [])
    date_dims = [
        d for d in all_dimensions_for_date_filter
        if isinstance(d, dict) and (d.get("data_type") or "").strip() in _DATE_FILTER_TYPES_ONLY
    ]
    fiscal_dims = [
        d for d in all_dimensions_for_date_filter
        if isinstance(d, dict) and is_fiscal_column(d.get("name", ""), d.get("data_type", ""))
    ]

    if not user_query:
        return result

    current_date_iso = date.today().isoformat()
    selected_columns_text = _build_selected_columns_text(filtered_dims, filtered_meas)
    allowed_column_names = {
        (c.get("name") or "").strip()
        for c in (filtered_dims or []) + (filtered_meas or [])
        if isinstance(c, dict) and (c.get("name") or "").strip()
    }
    schema_column_to_data_type = _schema_column_to_data_type(filtered_dims, filtered_meas)

    try:
        # --- PATH A: Edm.Date columns exist → standard date filter ---
        if date_dims:
            logger.info(
                f"[{node_name}] [DATE FILTER FLOW] Triggering date filter LLM: {len(date_dims)} date dimension(s)"
            )
            date_dimensions_text = "\n".join(
                f"  {d.get('name', '')} | {d.get('label', d.get('name', ''))}" for d in date_dims
            )
            date_filter_user = get_analytical_date_filter_user_prompt(
                user_query=user_query,
                parsed_intent=parsed_intent,
                date_dimensions_text=date_dimensions_text,
                current_date_iso=current_date_iso,
                selected_columns_text=selected_columns_text,
            )
            save_llm_call_input(
                node_name=node_name,
                query_id=query_id,
                system_prompt=ANALYTICAL_DATE_FILTER_SYSTEM_PROMPT,
                user_prompt=date_filter_user,
                call_suffix="date_filter",
            )
            try:
                date_filter_model = settings.analytics_analytical_date_filter_model
                date_resp = await llm_client._call_llm_unified(
                    model=date_filter_model,
                    system_prompt=ANALYTICAL_DATE_FILTER_SYSTEM_PROMPT,
                    user_prompt=date_filter_user,
                    node_name=node_name,
                    query_id=query_id,
                    temperature=0.0,
                    use_json_mode=True,
                )
                parsed = parse_json_response_required_dict(
                    date_resp or "", node_name=node_name, extract_from_list=True
                )
                save_llm_call_output(
                    node_name=node_name,
                    query_id=query_id,
                    raw_response=date_resp,
                    parsed=parsed,
                    call_suffix="date_filter",
                )
                if parsed:
                    dc = parsed.get("date_column")
                    start = parsed.get("start_date")
                    end = parsed.get("end_date")
                    valid_names = {d.get("name", "") for d in date_dims}
                    if dc and str(dc).strip() and (str(dc).strip() in valid_names):
                        start = str(start).strip() if start else None
                        end = str(end).strip() if end else None
                        if start and end:
                            date_filter_obj: Dict[str, Any] = {
                                "date_column": str(dc).strip(),
                                "start_date": start,
                                "end_date": end,
                            }
                            value_filters = _parse_and_validate_value_filters(
                                parsed.get("value_filters"),
                                allowed_column_names,
                                node_name,
                                schema_column_to_data_type=schema_column_to_data_type,
                            )
                            if value_filters:
                                date_filter_obj["value_filters"] = value_filters
                            result["analytical_date_filter"] = date_filter_obj
                            time_period_description = (
                                f"YTD ({start} to {end})" if start == f"{date.today().year}-01-01" and end == current_date_iso else f"{start} to {end}"
                            )
                            result["applied_date_filters"] = {
                                "filter_applied": True,
                                "date_range": {"start_date": start, "end_date": end, "date_column": str(dc).strip()},
                                "filter_source": node_name,
                                "time_period_description": time_period_description,
                            }
            except Exception as e:
                logger.warning(f"[{node_name}] Date filter LLM call failed: {e}")

        # --- PATH B: No Edm.Date but fiscal columns exist → fiscal filter ---
        elif fiscal_dims:
            logger.info(
                f"[{node_name}] [FISCAL FILTER FLOW] Triggering fiscal filter: {len(fiscal_dims)} fiscal dimension(s)"
            )
            fiscal_dimensions_text = "\n".join(
                f"  {d.get('name', '')} | {d.get('label', d.get('name', ''))}" for d in fiscal_dims
            )
            fiscal_filter_user = get_analytical_fiscal_filter_user_prompt(
                user_query=user_query,
                parsed_intent=parsed_intent,
                fiscal_dimensions_text=fiscal_dimensions_text,
                current_date_iso=current_date_iso,
                selected_columns_text=selected_columns_text,
            )
            save_llm_call_input(
                node_name=node_name,
                query_id=query_id,
                system_prompt=ANALYTICAL_FISCAL_FILTER_SYSTEM_PROMPT,
                user_prompt=fiscal_filter_user,
                call_suffix="fiscal_filter",
            )
            try:
                fiscal_filter_model = settings.analytics_analytical_fiscal_filter_model
                fiscal_resp = await llm_client._call_llm_unified(
                    model=fiscal_filter_model,
                    system_prompt=ANALYTICAL_FISCAL_FILTER_SYSTEM_PROMPT,
                    user_prompt=fiscal_filter_user,
                    node_name=node_name,
                    query_id=query_id,
                    temperature=0.0,
                    use_json_mode=True,
                )
                parsed = parse_json_response_required_dict(
                    fiscal_resp or "", node_name=node_name, extract_from_list=True
                )
                save_llm_call_output(
                    node_name=node_name,
                    query_id=query_id,
                    raw_response=fiscal_resp,
                    parsed=parsed,
                    call_suffix="fiscal_filter",
                )
                if parsed:
                    fc = parsed.get("fiscal_column")
                    start_val = parsed.get("start_value")
                    end_val = parsed.get("end_value")
                    granularity = parsed.get("granularity", "week")
                    valid_fiscal_names = {d.get("name", "") for d in fiscal_dims}
                    fiscal_value_filters = _parse_and_validate_value_filters(
                        parsed.get("value_filters"),
                        allowed_column_names,
                        node_name,
                        schema_column_to_data_type=schema_column_to_data_type,
                    )

                    def _fiscal_filter_obj(
                        fiscal_col: str,
                        sv: int,
                        ev: int,
                        gran: str,
                        inp_params: Dict[str, Any],
                    ) -> Dict[str, Any]:
                        obj: Dict[str, Any] = {
                            "fiscal_column": fiscal_col,
                            "start_value": sv,
                            "end_value": ev,
                            "granularity": gran,
                            "input_parameters": inp_params,
                        }
                        if fiscal_value_filters:
                            obj["value_filters"] = fiscal_value_filters
                        return obj

                    if fc and str(fc).strip() in valid_fiscal_names:
                        fc = str(fc).strip()
                        try:
                            start_val = int(start_val) if start_val is not None else None
                            end_val = int(end_val) if end_val is not None else None
                        except (ValueError, TypeError):
                            start_val = None
                            end_val = None
                        if start_val is not None and end_val is not None:
                            input_params = build_fiscal_input_parameters(fc, start_val, end_val)
                            result["sap_fiscal_filter"] = _fiscal_filter_obj(
                                fc, start_val, end_val, granularity, input_params
                            )
                            result["applied_date_filters"] = {
                                "filter_applied": True,
                                "date_range": {"start_date": str(start_val), "end_date": str(end_val), "date_column": fc},
                                "filter_source": f"{node_name}_fiscal",
                                "time_period_description": f"Fiscal {granularity} {start_val} to {end_val}",
                            }
                        else:
                            granularity = choose_fiscal_granularity(user_query, parsed_intent)
                            fiscal_cols_dicts = [{"name": d.get("name", ""), "type": d.get("data_type", "")} for d in fiscal_dims]
                            fc_fallback = pick_fiscal_input_parameter_column(fiscal_cols_dicts, granularity)
                            if fc_fallback:
                                sv, ev = compute_ytd_fiscal_range(granularity)
                                input_params = build_fiscal_input_parameters(fc_fallback, sv, ev)
                                result["sap_fiscal_filter"] = _fiscal_filter_obj(
                                    fc_fallback, sv, ev, granularity, input_params
                                )
                    else:
                        granularity = choose_fiscal_granularity(user_query, parsed_intent)
                        fiscal_cols_dicts = [{"name": d.get("name", ""), "type": d.get("data_type", "")} for d in fiscal_dims]
                        fc_fallback = pick_fiscal_input_parameter_column(fiscal_cols_dicts, granularity)
                        if fc_fallback:
                            sv, ev = compute_ytd_fiscal_range(granularity)
                            input_params = build_fiscal_input_parameters(fc_fallback, sv, ev)
                            result["sap_fiscal_filter"] = _fiscal_filter_obj(
                                fc_fallback, sv, ev, granularity, input_params
                            )
                else:
                    granularity = choose_fiscal_granularity(user_query, parsed_intent)
                    fiscal_cols_dicts = [{"name": d.get("name", ""), "type": d.get("data_type", "")} for d in fiscal_dims]
                    fc_fallback = pick_fiscal_input_parameter_column(fiscal_cols_dicts, granularity)
                    if fc_fallback:
                        sv, ev = compute_ytd_fiscal_range(granularity)
                        input_params = build_fiscal_input_parameters(fc_fallback, sv, ev)
                        result["sap_fiscal_filter"] = {
                            "fiscal_column": fc_fallback,
                            "start_value": sv,
                            "end_value": ev,
                            "granularity": granularity,
                            "input_parameters": input_params,
                        }
            except Exception as e:
                logger.warning(f"[{node_name}] Fiscal filter LLM call failed: {e}")
                granularity = choose_fiscal_granularity(user_query, parsed_intent)
                fiscal_cols_dicts = [{"name": d.get("name", ""), "type": d.get("data_type", "")} for d in fiscal_dims]
                fc_fallback = pick_fiscal_input_parameter_column(fiscal_cols_dicts, granularity)
                if fc_fallback:
                    sv, ev = compute_ytd_fiscal_range(granularity)
                    input_params = build_fiscal_input_parameters(fc_fallback, sv, ev)
                    result["sap_fiscal_filter"] = {
                        "fiscal_column": fc_fallback,
                        "start_value": sv,
                        "end_value": ev,
                        "granularity": granularity,
                        "input_parameters": input_params,
                    }
    except Exception as e:
        logger.warning(f"[{node_name}] Date/fiscal filter section failed: {e}")

    return result


def _prefilter_schema_by_relevance(
    dimensions: List[Dict[str, Any]],
    measures: List[Dict[str, Any]],
    user_query: str,
    parsed_intent: Optional[Dict[str, Any]],
    max_total: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Reduce schema size by keeping columns relevant to query (keyword/heuristic). Returns (dims, meas) with combined length <= max_total."""
    if (len(dimensions) + len(measures)) <= max_total:
        return dimensions, measures
    q = (user_query or "").lower()
    intent_text = ""
    if isinstance(parsed_intent, dict):
        intent_text = (parsed_intent.get("intent_explanation") or "")[:500]
    combined = (q + " " + intent_text).lower()
    # Relevance keywords: prefer columns whose name/label match query or common analysis terms
    keywords = set()
    for w in combined.replace(",", " ").replace(".", " ").split():
        if len(w) > 2:
            keywords.add(w)
    # Always keep date-like and key dimensions
    priority_names = {"date", "time", "year", "month", "day", "id", "key", "code", "name"}

    def score(col: Dict[str, Any]) -> int:
        name = (col.get("name") or "").lower()
        label = (col.get("label") or name).lower()
        s = 0
        if any(p in name or p in label for p in priority_names):
            s += 100
        for k in keywords:
            if k in name or k in label:
                s += 10
        return s

    dim_sorted = sorted(dimensions, key=score, reverse=True)
    meas_sorted = sorted(measures, key=score, reverse=True)
    # Reserve ~40% for measures, 60% for dimensions
    n_meas = min(len(meas_sorted), max(1, max_total * 4 // 10))
    n_dim = min(len(dim_sorted), max(1, max_total - n_meas))
    return dim_sorted[:n_dim], meas_sorted[:n_meas]


def _query_keywords(user_query: str, parsed_intent: Optional[Dict[str, Any]]) -> Set[str]:
    """Extract meaningful keywords from user query and intent for column relevance scoring."""
    q = (user_query or "").lower()
    intent_text = ""
    if isinstance(parsed_intent, dict):
        intent_text = (parsed_intent.get("intent_explanation") or "")[:500]
    combined = (q + " " + intent_text).lower()
    # Skip very short tokens and common stopwords
    stop = {"the", "and", "for", "with", "from", "this", "that", "what", "which", "when", "how", "all", "get", "show", "give", "need", "want", "can", "has", "have", "are", "was", "were", "been", "being", "will", "would", "could", "should", "about", "into", "through", "during"}
    keywords = set()
    for w in combined.replace(",", " ").replace(".", " ").replace("'", " ").split():
        w = w.strip()
        if len(w) > 2 and w not in stop:
            keywords.add(w)
    return keywords


def _score_column_by_keywords(col: Dict[str, Any], keywords: Set[str]) -> int:
    """Score a column (dimension or measure) by relevance to query keywords. Higher = more relevant."""
    name = (col.get("name") or "").lower()
    label = (col.get("label") or name).lower()
    text = name + " " + label
    score = 0
    for k in keywords:
        if k in name or k in label:
            score += 10
        elif k in text:
            score += 5
    # Priority terms that often appear in analytics (boost if present)
    priority = {"date", "time", "year", "month", "day", "week", "fiscal", "plant", "region", "revenue", "amount", "quantity", "order", "sales", "cost", "price", "customer", "product", "category", "quantity", "qty", "value", "total", "net", "gross"}
    for p in priority:
        if p in name or p in label:
            score += 5
    return score


def _fallback_columns_by_query_keywords(
    dimensions: List[Dict[str, Any]],
    measures: List[Dict[str, Any]],
    user_query: str,
    parsed_intent: Optional[Dict[str, Any]],
    node_name: str,
    min_dims: int = 10,
    max_dims: int = 40,
    min_meas: int = 10,
    max_meas: int = 30,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Fallback when LLM returns no columns: select dimensions and measures that match
    user query keywords (e.g. revenue, plant, region) instead of blindly taking first N.
    """
    keywords = _query_keywords(user_query, parsed_intent)
    if keywords:
        logger.info(f"[{node_name}] Keyword fallback using query terms: {sorted(keywords)[:15]}")
    # Score and sort by relevance (then by original order for ties)
    dim_sorted = sorted(
        dimensions,
        key=lambda c: (_score_column_by_keywords(c, keywords), -dimensions.index(c)),
        reverse=True,
    )
    meas_sorted = sorted(
        measures,
        key=lambda c: (_score_column_by_keywords(c, keywords), -measures.index(c)),
        reverse=True,
    )
    n_dims = min(max(min_dims, len(dimensions) // 20), max_dims, len(dimensions)) if dimensions else 0
    n_meas = min(max(min_meas, len(measures) // 10), max_meas, len(measures)) if measures else 0
    return dim_sorted[:n_dims], meas_sorted[:n_meas]


def _format_columns_for_prompt(
    columns: List[Dict[str, Any]],
    role: str,
) -> str:
    """
    Format dimension or measure columns for the LLM: simple "name | label" per line.
    If a column has a "description" (e.g. SAP calculation note for Total_Revenue), append it.
    Keeps the prompt clean; LLM returns only column names in two groups.
    """
    if not columns:
        return f"No {role}s available."

    lines = [f"**{role.upper()}S ({len(columns)}):**", ""]
    for col in columns:
        name = col.get("name", "")
        label = col.get("label", name) or name
        desc = col.get("description", "").strip()
        line = f"  {name} | {label}"
        if desc:
            line += f" — {desc}"
        lines.append(line)
    return "\n".join(lines).strip()


def _format_combined_columns_for_prompt(columns_with_role: List[Tuple[Dict[str, Any], str]]) -> str:
    """
    Format a combined list of columns (dimensions + measures) as: name | label | dimension|measure.
    If a column has a "description" (e.g. SAP calculation note for Total_Revenue), append it.
    Gives the LLM both name and label for better understanding; type helps context.
    """
    if not columns_with_role:
        return "No columns in this batch."
    lines = []
    for col, role in columns_with_role:
        name = col.get("name", "")
        label = col.get("label", name) or name
        desc = col.get("description", "").strip()
        line = f"  {name} | {label} | {role}"
        if desc:
            line += f" — {desc}"
        lines.append(line)
    return "\n".join(lines).strip()


def _extract_selected_columns_from_parsed(parsed: Dict[str, Any]) -> tuple[List[str], Dict[str, str]]:
    """
    Extract column names and per-column reasoning from LLM response (combined-column format).
    Handles selected_columns as array of strings or array of {name, reasoning}.
    Returns (list of names, dict of name -> reasoning).
    """
    if not isinstance(parsed, dict):
        return [], {}
    inner = parsed
    for key in ("output", "selection", "result", "data"):
        if isinstance(parsed.get(key), dict):
            inner = parsed[key]
            break
    cols = inner.get("selected_columns") or inner.get("columns") or []
    if not isinstance(cols, list):
        return [], {}
    names: List[str] = []
    reasons: Dict[str, str] = {}
    for x in cols:
        if not x:
            continue
        if isinstance(x, dict):
            name = x.get("name") or x.get("column")
            if name:
                name = str(name)
                names.append(name)
                r = x.get("reasoning") or x.get("reason")
                if r and isinstance(r, str):
                    reasons[name] = r.strip()
        else:
            names.append(str(x))
    return names, reasons


def _extract_dimensions_measures_from_parsed(parsed: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    """
    Extract dimension and measure name lists from LLM response. Handles multiple key names
    and one level of nesting (e.g. output.dimensions, selection.measures).
    Also supports combined format: selected_columns split by name_to_role.
    """
    if not isinstance(parsed, dict):
        return [], []
    # Unwrap one level if wrapped
    inner = parsed
    for key in ("output", "selection", "result", "data", "columns"):
        if isinstance(parsed.get(key), dict):
            inner = parsed[key]
            break
    dims = (
        inner.get("dimensions")
        or inner.get("selected_dimensions")
        or inner.get("dimension_names")
        or []
    )
    meas = (
        inner.get("measures")
        or inner.get("selected_measures")
        or inner.get("measure_names")
        or []
    )
    if not isinstance(dims, list):
        dims = []
    if not isinstance(meas, list):
        meas = []
    dims = [x.get("name", x) if isinstance(x, dict) else x for x in dims]
    meas = [x.get("name", x) if isinstance(x, dict) else x for x in meas]
    return [str(n) for n in dims if n], [str(n) for n in meas if n]


def _validate_selection(
    selected: List[Union[str, Dict[str, Any]]],
    available: List[Dict[str, Any]],
    role: str,
    node_name: str,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Validate LLM-selected columns against the available column list using common
    validation and logging (see utils.validate_llm_columns). This is the main
    gate for LLM column selection — invalid names are logged with [LLM_COL_VALIDATE]
    and skipped.

    Accepts either a list of column name strings or a list of dicts with "name".
    Returns (validated list of full column dicts, list of invalid names that were skipped).
    """
    available_names = [col.get("name", "") for col in available if col.get("name")]
    source = f"analytical_column_selection.{role}s"
    validated_list, invalid_list, corrections = validate_llm_columns(
        suggested_col_names=selected,
        available_col_names=available_names,
        source=source,
        node_name=node_name,
        case_insensitive=True,
        available_objects=available,
    )
    result = [v for v in validated_list if isinstance(v, dict)]
    if corrections:
        logger.info(
            f"[{node_name}] [{LLM_COL_VALIDATE_TAG}] {role}s: case corrections applied: {corrections}"
        )
    return result, invalid_list


async def _call_llm_for_columns_chunk(
    columns_with_role: List[Tuple[Dict[str, Any], str]],
    user_query: str,
    parsed_intent: Optional[Dict[str, Any]],
    llm_client: Any,
    model_name: str,
    node_name: str,
    query_id: Optional[str],
    chunk_hint: Optional[str],
    view_name: Optional[str] = None,
    call_suffix: Optional[str] = None,
    analysis_mode: str = "normal",
) -> Dict[str, Any]:
    """
    Call LLM for one chunk of columns (combined list, 300 max). Each column is (col_dict, "dimension"|"measure").
    Sends name | label | type for better understanding. Returns {'dimensions': [...], 'measures': [...], 'column_reasons': {name: reasoning}}.
    When view_name is set, the prompt states that columns are from this view only (used for multi-view separate calls).
    analysis_mode: "normal" = minimal columns; "deep_research" = maximum columns for full depth analysis.
    """
    columns_text = _format_combined_columns_for_prompt(columns_with_role)
    user_prompt = get_analytical_column_selection_user_prompt(
        user_query=user_query,
        parsed_intent=parsed_intent,
        columns_text=columns_text,
        chunk_hint=chunk_hint,
        view_name=view_name,
        analysis_mode=analysis_mode,
    )
    system_prompt = ANALYTICAL_COLUMN_SELECTION_SYSTEM_PROMPT
    save_llm_call_input(
        node_name=node_name,
        query_id=query_id,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        extra={"view_name": view_name, "chunk_hint": chunk_hint},
        call_suffix=call_suffix,
    )
    response = await llm_client._call_llm_unified(
        model=model_name,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        node_name=node_name,
        query_id=query_id,
        temperature=0.0,
        use_json_mode=True,
    )
    name_to_role = {col.get("name"): role for col, role in columns_with_role if col.get("name")}
    parse_error = None
    try:
        parsed = parse_json_response(response, expected_type=None)
        save_llm_call_output(
            node_name=node_name,
            query_id=query_id,
            raw_response=response,
            parsed=parsed,
            call_suffix=call_suffix,
        )
    except (json.JSONDecodeError, ValueError) as e:
        parse_error = e
        parsed = try_repair_truncated_analytical_selection(response) or {}
        save_llm_call_output(
            node_name=node_name,
            query_id=query_id,
            raw_response=response,
            parsed=parsed,
            extra={"parse_error": str(e)},
            call_suffix=call_suffix,
        )
        if not parsed:
            logger.warning(
                "%s %s Chunk LLM response parse failed: %s | response_len=%s | response_snippet=%s",
                COLUMN_SELECT_TAG, node_name, parse_error, len(response or ""), (response or "").strip()[:400],
            )
    # LLM sometimes returns a raw JSON array; normalize to dimensions + measures_by_category shape
    if isinstance(parsed, list):
        normalized_items: List[Dict[str, Any]] = []
        for x in parsed:
            if not x:
                continue
            if isinstance(x, dict):
                name = x.get("name") or x.get("column")
                if name:
                    normalized_items.append({
                        "name": str(name),
                        "reasoning": (x.get("reasoning") or x.get("reason")) or "",
                    })
            else:
                normalized_items.append({"name": str(x), "reasoning": ""})
        parsed = {"dimensions": normalized_items, "measures_by_category": {}}
        logger.info(
            "%s %s Chunk LLM returned a list (not dict); wrapped in dimensions (%s items)",
            COLUMN_SELECT_TAG, node_name, len(normalized_items),
        )
    if not isinstance(parsed, dict):
        logger.warning("%s %s Chunk parsed result is not a dict or list (type=%s); returning 0 cols", COLUMN_SELECT_TAG, node_name, type(parsed).__name__)
        return {"dimensions": [], "column_reasons": {}, "measures_by_category": {}}
    # No related data for user query → signal so caller can end flow with message + suggested_queries
    no_related = parsed.get("no_related_data") in (True, "true", "yes")
    if no_related:
        user_message = (parsed.get("user_message") or "There is no related data for your query in this data source. Try one of the suggestions below.").strip()
        raw_sug = parsed.get("suggested_queries")
        suggested_queries: List[str] = []
        if isinstance(raw_sug, list):
            suggested_queries = [str(s).strip() for s in raw_sug if s][:5]
        if not suggested_queries:
            suggested_queries = ["Revenue by plant for last quarter", "Spend by category YTD", "Top 5 products by sales this year"]
        logger.info(
            "%s %s No related data for query (chunk) — user_message=%s, suggestions=%s",
            COLUMN_SELECT_TAG, node_name, user_message[:60], len(suggested_queries),
        )
        return {
            "_no_related_data": True,
            "user_message": user_message,
            "suggested_queries": suggested_queries,
            "dimensions": [],
            "column_reasons": {},
            "measures_by_category": {},
        }
    # New format: dimensions (list) + measures_by_category (group-wise measures only)
    dims, column_reasons, dim_priorities = _extract_dimensions_from_parsed(parsed)
    measures_by_cat, cat_priorities = _normalize_measures_by_category_from_llm(parsed.get("measures_by_category"))
    meas = _flatten_measure_names_from_by_category(measures_by_cat)
    # Fallback: if LLM returned old format (selected_columns) or no dimensions/measures
    if not dims and not meas:
        selected_names, col_reasons = _extract_selected_columns_from_parsed(parsed)
        if selected_names:
            dims = [n for n in selected_names if name_to_role.get(n) == "dimension"]
            meas = [n for n in selected_names if name_to_role.get(n) == "measure"]
            if col_reasons and isinstance(col_reasons, dict):
                column_reasons = col_reasons
        if not dims and not meas:
            dims_names, meas_names = _extract_dimensions_measures_from_parsed(parsed)
            dims, meas = dims_names, meas_names
    # Per-chunk fallback: if batch had columns but LLM returned none, use all from this chunk
    if not dims and not meas and columns_with_role:
        dims = [col.get("name") for col, role in columns_with_role if role == "dimension" and col.get("name")]
        meas = [col.get("name") for col, role in columns_with_role if role == "measure" and col.get("name")]
        if dims or meas:
            logger.info("%s %s Chunk returned 0 cols → per-chunk fallback: using all %s dims, %s meas from batch", COLUMN_SELECT_TAG, node_name, len(dims), len(meas))
    if not dims and not meas and columns_with_role and response:
        logger.warning(
            "%s %s Chunk returned 0 cols (batch had %s). parsed_keys=%s | response_snippet=%s",
            COLUMN_SELECT_TAG, node_name, len(columns_with_role), list(parsed.keys()) if isinstance(parsed, dict) else "n/a", (response or "").strip()[:400],
        )
    # If we got measures from fallback (selected_columns or dimensions/measures) but no measures_by_cat, leave measures_by_cat as-is (empty); node will put all in "other"
    if meas and not measures_by_cat:
        measures_by_cat = {"other": meas}
    return {
        "dimensions": [str(n) for n in dims if n],
        "column_reasons": column_reasons if isinstance(column_reasons, dict) else {},
        "measures_by_category": measures_by_cat,
        "category_priorities": cat_priorities if isinstance(cat_priorities, dict) else {},
        "dimension_priorities": dim_priorities if isinstance(dim_priorities, dict) else {},
    }


async def analytical_column_selection_node(
    state: AnalyticsState,
    model: str = None,
) -> Dict[str, Any]:
    """
    Use an LLM to select the dimensions and measures relevant to the user's query.

    This reduces the full analytical schema to only the columns needed for answering
    the query, improving downstream planning accuracy and reducing payload size.

    For non-SAP data sources or when no analytical schema is available, this node
    is a no-op.

    Args:
        state: Current analytics state containing:
            - user_query: Original user query
            - parsed_intent: Query analysis result with intent and depth
            - analytical_dimensions: All extracted dimensions (from prepare_analytical_schema)
            - analytical_measures: All extracted measures (from prepare_analytical_schema)
        model: Optional model name override

    Returns:
        Updated state dictionary with:
            - filtered_analytical_dimensions: LLM-selected relevant dimensions
            - filtered_analytical_measures: LLM-selected relevant measures
    """
    start_time = datetime.now()
    node_name = "analytical_column_selection"

    user_query = state.get("user_query", "")
    parsed_intent = state.get("parsed_intent", {})

    from ..node_timing_registry import get_node_timing_registry
    registry = get_node_timing_registry()
    if registry:
        registry.record_node_start(node_name, start_time)

    analysis_mode = (state.get("analysis_mode") or "normal").strip() or "normal"
    if analysis_mode not in ("normal", "deep_research"):
        analysis_mode = "normal"
    logger.info("%s %s Starting | user_query=%s | analysis_mode=%s", COLUMN_SELECT_TAG, node_name, (user_query or "")[:80], analysis_mode)
    logger.info(f"[{node_name}] Starting LLM-driven analytical column selection (mode={analysis_mode})")

    # ---------------------------------------------------------------
    # Gate: Skip if no analytical schema available
    # ---------------------------------------------------------------
    dimensions = state.get("analytical_dimensions", []) or []
    measures = state.get("analytical_measures", []) or []

    # Ensure each column has label and description from Excel (data/column_metadata/) before
    # the LLM selects columns. prepare_analytical_schema may already have enriched; this is
    # idempotent and ensures we have label/description when schema came from another path.
    # Filtered outputs (filtered_analytical_dimensions/measures) preserve these for downstream nodes.
    try:
        from ..utils.column_metadata_loader import load_column_metadata, enrich_columns_with_metadata
        column_metadata = load_column_metadata()
        if column_metadata:
            enrich_columns_with_metadata(dimensions, column_metadata)
            enrich_columns_with_metadata(measures, column_metadata)
            logger.info(f"[{node_name}] Enriched columns with metadata for {len(column_metadata)} fields")
    except Exception as meta_err:
        logger.debug("Column metadata load/enrich skipped: %s", meta_err)

    if not dimensions and not measures:
        logger.info(f"[{node_name}] No analytical schema available - skipping")
        return {}

    if not user_query:
        logger.warning(f"[{node_name}] No user query - skipping")
        return {}

    # ---------------------------------------------------------------
    # Process Order: hard-coded columns (no LLM call for column selection)
    # ---------------------------------------------------------------
    def _view_key(col: Dict[str, Any]) -> str:
        return (col.get("view_name") or "").strip() or "_default"

    view_names_for_po = sorted(
        set(_view_key(d) for d in dimensions) | set(_view_key(m) for m in measures)
    )
    is_process_order_view = (
        state.get("use_process_order_hardcoded_columns") is True
        or any(
            "Process_Order" in (v or "") or "ProcessOrder" in (v or "")
            for v in view_names_for_po
        )
    )
    if is_process_order_view:
        logger.info(
            f"[{node_name}] Process Order view detected — using hard-coded dimensions/measures (LLM column selection commented out)"
        )
        name_to_dim = {(d.get("name") or "").strip(): d for d in dimensions if (d.get("name") or "").strip()}
        name_to_meas = {(m.get("name") or "").strip(): m for m in measures if (m.get("name") or "").strip()}
        filtered_dims: List[Dict[str, Any]] = []
        for name in PROCESS_ORDER_DIMENSIONS:
            if name in name_to_dim:
                d = dict(name_to_dim[name])
                d.setdefault("selection_reasoning", "Process Order hard-coded")
                filtered_dims.append(d)
            else:
                filtered_dims.append({
                    "name": name,
                    "label": name.replace("_", " ").title(),
                    "data_type": "Edm.Date" if name == PROCESS_ORDER_DATE_COLUMN else "Edm.String",
                    "view_name": view_names_for_po[0] if view_names_for_po else "_default",
                    "selection_reasoning": "Process Order hard-coded",
                })
        filtered_meas: List[Dict[str, Any]] = []
        for name in PROCESS_ORDER_MEASURES:
            if name in name_to_meas:
                m = dict(name_to_meas[name])
                m.setdefault("selection_reasoning", "Process Order hard-coded")
                filtered_meas.append(m)
            else:
                filtered_meas.append({
                    "name": name,
                    "label": name.replace("_", " ").title(),
                    "data_type": "Edm.Decimal",
                    "view_name": view_names_for_po[0] if view_names_for_po else "_default",
                    "selection_reasoning": "Process Order hard-coded",
                })
        measures_by_group = _build_measures_by_category_from_llm(
            filtered_meas, {"Process Order": [m.get("name") for m in filtered_meas if m.get("name")]}
        )
        flat_measures_from_group = [m for lst in measures_by_group.values() for m in lst]
        today = date.today().isoformat()
        analytical_date_filter = {
            "date_column": PROCESS_ORDER_DATE_COLUMN,
            "start_date": today,
            "end_date": today,
            "value_filters": [],
        }
        logger.info(
            f"[{node_name}] Date filter: {PROCESS_ORDER_DATE_COLUMN} from current date ({today})"
        )
        return {
            "filtered_analytical_dimensions": filtered_dims,
            "filtered_analytical_measures_by_group": measures_by_group,
            "filtered_analytical_measures": flat_measures_from_group,
            "analytical_date_filter": analytical_date_filter,
            "category_priorities": {},
            "dimension_priorities": {},
        }

    # ---------------------------------------------------------------
    # Small schema: skip LLM
    # ---------------------------------------------------------------
    total_columns = len(dimensions) + len(measures)
    if total_columns <= 15:
        logger.info(
            f"[{node_name}] Schema is small ({total_columns} columns) - passing all columns through"
        )
        # Ensure selection_reasoning key exists for next node (empty when no LLM selection)
        for d in dimensions:
            d.setdefault("selection_reasoning", "")
        for m in measures:
            m.setdefault("selection_reasoning", "")
        measures_by_group = _build_measures_by_category_from_llm(measures, {})
        logger.info(
            f"[{node_name}] Category-wise measures: {', '.join(f'{k}={len(v)}' for k, v in measures_by_group.items() if v)}"
        )
        flat_measures_from_group = [m for lst in measures_by_group.values() for m in lst]
        return {
            "filtered_analytical_dimensions": dimensions,
            "filtered_analytical_measures_by_group": measures_by_group,
            "filtered_analytical_measures": flat_measures_from_group,
            "category_priorities": {},
            "dimension_priorities": {},
        }

    total_columns = len(dimensions) + len(measures)
    total_dims = len(dimensions)
    total_meas = len(measures)
    logger.info(f"[{node_name}] Total dimensions: {total_dims}, total measures: {total_meas} (no pre-filter)")

    # Group columns by view so we run separate LLM calls per view (multi-view support)
    def _view_key(col: Dict[str, Any]) -> str:
        return (col.get("view_name") or "").strip() or "_default"

    view_to_columns: Dict[str, List[Tuple[Dict[str, Any], str]]] = {}
    for d in dimensions:
        vk = _view_key(d)
        view_to_columns.setdefault(vk, []).append((d, "dimension"))
    for m in measures:
        vk = _view_key(m)
        view_to_columns.setdefault(vk, []).append((m, "measure"))

    view_names = sorted(k for k in view_to_columns.keys() if k != "_default") or (["_default"] if "_default" in view_to_columns else [])
    if view_names != ["_default"] and len(view_names) > 1:
        logger.info(f"[{node_name}] Multi-view: {len(view_names)} view(s) — separate LLM call(s) per view: {view_names}")

    try:
        llm_client = state.get("llm_client") or AzureOpenAIClient()
        from config.settings import settings
        model_name = model or settings.analytics_analytical_column_selection_model
        query_id = state.get("query_id")

        all_filtered_dims: List[Dict[str, Any]] = []
        all_filtered_meas: List[Dict[str, Any]] = []
        merged_column_reasons: Dict[str, str] = {}
        merged_measures_by_category: Dict[str, List[str]] = {}
        merged_category_priorities: Dict[str, int] = {}
        merged_dim_priorities: Dict[str, int] = {}

        for view_key in sorted(view_to_columns.keys()):
            columns_with_role = view_to_columns[view_key]
            view_label = view_key if view_key != "_default" else None
            view_dims = [d for d in dimensions if _view_key(d) == view_key]
            view_meas = [m for m in measures if _view_key(m) == view_key]
            n_view = len(columns_with_role)
            use_single = n_view <= SINGLE_CALL_MAX_COLUMNS

            selected_dims_raw: List[str] = []
            selected_meas_raw: List[str] = []

            if use_single:
                logger.info(f"[{node_name}] View {view_label or '(default)'}: single LLM call ({n_view} columns)")
                res = await _call_llm_for_columns_chunk(
                    columns_with_role=columns_with_role,
                    user_query=user_query,
                    parsed_intent=parsed_intent,
                    llm_client=llm_client,
                    model_name=model_name,
                    node_name=node_name,
                    query_id=query_id,
                    chunk_hint=None,
                    view_name=view_label,
                    call_suffix=f"col_{view_label or 'default'}",
                    analysis_mode=analysis_mode,
                )
                selected_dims_raw = res.get("dimensions", [])
                selected_meas_raw = _flatten_measure_names_from_by_category(res.get("measures_by_category") or {})
                for cat, names in (res.get("measures_by_category") or {}).items():
                    if cat and names:
                        merged_measures_by_category.setdefault(cat, []).extend(names)
                # Collect category priorities
                for cat, p in (res.get("category_priorities") or {}).items():
                    if cat and p is not None:
                        merged_category_priorities[cat] = p
                # Collect dimension priorities
                for dim, p in (res.get("dimension_priorities") or {}).items():
                    if dim and p is not None:
                        merged_dim_priorities[dim] = p
                for col_name, reason in (res.get("column_reasons") or {}).items():
                    if col_name and reason and col_name not in merged_column_reasons:
                        merged_column_reasons[col_name] = reason
                if res.get("_no_related_data"):
                    logger.info("%s %s No related data (single-call view) — ending flow", COLUMN_SELECT_TAG, node_name)
                    return {
                        "data_sufficiency_result": {
                            "can_answer": False,
                            "reason_if_not": "Column selection: no dimension/measure match for user query.",
                            "summary_of_what_we_have": "Available dimensions and measures in schema.",
                            "user_message": res.get("user_message") or "There is no related data for your query in this data source. Try one of the suggestions below.",
                            "suggested_queries": res.get("suggested_queries") or [],
                        },
                        "filtered_analytical_dimensions": [],
                        "filtered_analytical_measures": [],
                        "filtered_analytical_measures_by_group": {},
                        "category_priorities": {},
                        "dimension_priorities": {},
                    }
            else:
                chunks = [
                    columns_with_role[i : i + COLUMNS_PER_CHUNK]
                    for i in range(0, len(columns_with_role), COLUMNS_PER_CHUNK)
                ]
                logger.info(
                    f"[{node_name}] View {view_label or '(default)'}: {n_view} columns in {len(chunks)} chunk(s)"
                )
                for j, chunk in enumerate(chunks):
                    res = await _call_llm_for_columns_chunk(
                        columns_with_role=chunk,
                        user_query=user_query,
                        parsed_intent=parsed_intent,
                        llm_client=llm_client,
                        model_name=model_name,
                        node_name=node_name,
                        query_id=query_id,
                        chunk_hint=f"View {view_label or 'default'} chunk {j + 1}/{len(chunks)} ({len(chunk)} columns)",
                        view_name=view_label,
                        call_suffix=f"col_{view_label or 'default'}_c{j}",
                        analysis_mode=analysis_mode,
                    )
                    selected_dims_raw.extend(res.get("dimensions", []))
                    selected_meas_raw.extend(_flatten_measure_names_from_by_category(res.get("measures_by_category") or {}))
                    for cat, names in (res.get("measures_by_category") or {}).items():
                        if cat and names:
                            merged_measures_by_category.setdefault(cat, []).extend(names)
                    # Collect category priorities
                    for cat, p in (res.get("category_priorities") or {}).items():
                        if cat and p is not None:
                            merged_category_priorities[cat] = p
                    # Collect dimension priorities
                    for dim, p in (res.get("dimension_priorities") or {}).items():
                        if dim and p is not None:
                            merged_dim_priorities[dim] = p
                    for col_name, reason in (res.get("column_reasons") or {}).items():
                        if col_name and reason and col_name not in merged_column_reasons:
                            merged_column_reasons[col_name] = reason
                    if res.get("_no_related_data"):
                        logger.info("%s %s No related data (chunk) — ending flow", COLUMN_SELECT_TAG, node_name)
                        return {
                            "data_sufficiency_result": {
                                "can_answer": False,
                                "reason_if_not": "Column selection: no dimension/measure match for user query.",
                                "summary_of_what_we_have": "Available dimensions and measures in schema.",
                                "user_message": res.get("user_message") or "There is no related data for your query in this data source. Try one of the suggestions below.",
                                "suggested_queries": res.get("suggested_queries") or [],
                            },
                            "filtered_analytical_dimensions": [],
                            "filtered_analytical_measures": [],
                            "filtered_analytical_measures_by_group": {},
                        }

            # Dedupe within view
            seen_d: Set[str] = set()
            seen_m: Set[str] = set()
            selected_dims_raw = [n for n in selected_dims_raw if n and n not in seen_d and not seen_d.add(n)]
            selected_meas_raw = [n for n in selected_meas_raw if n and n not in seen_m and not seen_m.add(n)]

            # Validate against this view's columns only (keeps view_name on returned dicts)
            fd, invalid_d = _validate_selection(selected_dims_raw, view_dims, "dimension", node_name)
            fm, invalid_m = _validate_selection(selected_meas_raw, view_meas, "measure", node_name)
            if invalid_d or invalid_m:
                logger.warning(
                    f"[{node_name}] [{LLM_COL_VALIDATE_TAG}] View {view_label or 'default'}: invalid skipped dims={invalid_d}, meas={invalid_m}"
                )
            dim_order = {d.get("name", ""): i for i, d in enumerate(view_dims) if d.get("name")}
            meas_order = {m.get("name", ""): i for i, m in enumerate(view_meas) if m.get("name")}
            fd = sorted(fd, key=lambda d: dim_order.get(d.get("name", ""), len(view_dims)))
            fm = sorted(fm, key=lambda m: meas_order.get(m.get("name", ""), len(view_meas)))
            for d in fd:
                d.setdefault("selection_reasoning", merged_column_reasons.get(d.get("name", ""), "") or "")
            for m in fm:
                m.setdefault("selection_reasoning", merged_column_reasons.get(m.get("name", ""), "") or "")
            all_filtered_dims.extend(fd)
            all_filtered_meas.extend(fm)

        filtered_dims = all_filtered_dims
        filtered_meas = all_filtered_meas
        merged_dims = len(filtered_dims)
        merged_meas = len(filtered_meas)
        logger.info(
            f"[{node_name}] Merged from all views: {merged_dims} dimensions, {merged_meas} measures (before retry/fallback)"
        )
        if merged_dims == 0 and merged_meas == 0:
            logger.warning(
                "%s %s MERGED_EMPTY — all view(s) returned no columns; will retry with full schema then keyword fallback if needed",
                COLUMN_SELECT_TAG, node_name,
            )

        dim_name_to_index = {d.get("name", ""): i for i, d in enumerate(dimensions) if d.get("name")}
        meas_name_to_index = {m.get("name", ""): i for i, m in enumerate(measures) if m.get("name")}

        # Retry only when merged result is empty (single full-schema call, no view separation)
        if (not filtered_dims and not filtered_meas) and (dimensions or measures):
            all_columns_with_role: List[Tuple[Dict[str, Any], str]] = []
            for _vk in sorted(view_to_columns.keys()):
                all_columns_with_role.extend(view_to_columns[_vk])
            logger.warning(
                "%s %s Merged selection empty — retrying with FULL schema (all %s columns)",
                COLUMN_SELECT_TAG, node_name, len(all_columns_with_role),
            )
            logger.info(f"[{node_name}] Merged selection empty or invalid - single retry with full schema")
            try:
                retry_res = await _call_llm_for_columns_chunk(
                    columns_with_role=all_columns_with_role,
                    user_query=user_query,
                    parsed_intent=parsed_intent,
                    llm_client=llm_client,
                    model_name=model_name,
                    node_name=node_name,
                    query_id=query_id,
                    chunk_hint="retry (full schema)",
                    view_name=None,
                    call_suffix="col_retry",
                    analysis_mode=analysis_mode,
                )
                if retry_res.get("_no_related_data"):
                    logger.info("%s %s No related data (retry) — ending flow", COLUMN_SELECT_TAG, node_name)
                    return {
                        "data_sufficiency_result": {
                            "can_answer": False,
                            "reason_if_not": "Column selection: no dimension/measure match for user query.",
                            "summary_of_what_we_have": "Available dimensions and measures in schema.",
                            "user_message": retry_res.get("user_message") or "There is no related data for your query in this data source. Try one of the suggestions below.",
                            "suggested_queries": retry_res.get("suggested_queries") or [],
                        },
                        "filtered_analytical_dimensions": [],
                        "filtered_analytical_measures": [],
                        "filtered_analytical_measures_by_group": {},
                        "category_priorities": {},
                        "dimension_priorities": {},
                    }
                rd = [n for n in retry_res.get("dimensions", []) if n]
                rm = _flatten_measure_names_from_by_category(retry_res.get("measures_by_category") or {})
                if rd or rm:
                    merged_measures_by_category = _normalize_measures_by_category_from_llm(retry_res.get("measures_by_category"))
                    retry_reasons = retry_res.get("column_reasons") or {}
                    filtered_dims, _ = _validate_selection(rd, dimensions, "dimension", node_name)
                    filtered_meas, _ = _validate_selection(rm, measures, "measure", node_name)
                    filtered_dims = sorted(filtered_dims, key=lambda d: dim_name_to_index.get(d.get("name", ""), len(dimensions)))
                    filtered_meas = sorted(filtered_meas, key=lambda m: meas_name_to_index.get(m.get("name", ""), len(measures)))
                    for d in filtered_dims:
                        name = d.get("name", "")
                        if name:
                            d["selection_reasoning"] = retry_reasons.get(name, "") or ""
                    for m in filtered_meas:
                        name = m.get("name", "")
                        if name:
                            m["selection_reasoning"] = retry_reasons.get(name, "") or ""
                    logger.info("%s %s Retry SUCCESS: %s dims, %s measures", COLUMN_SELECT_TAG, node_name, len(filtered_dims), len(filtered_meas))
                else:
                    logger.warning("%s %s Retry returned 0 dims, 0 measures — will use keyword fallback", COLUMN_SELECT_TAG, node_name)
            except Exception as retry_exc:
                logger.warning("%s %s Retry FAILED: %s — proceeding to keyword fallback", COLUMN_SELECT_TAG, node_name, retry_exc)
                # Leave filtered_dims/filtered_meas empty so fallback below fills them

        # Fallback: use columns that match user query keywords; allow max number that support the query
        _MIN_FALLBACK_DIMS = 10
        _MIN_FALLBACK_MEAS = 10
        _MAX_FALLBACK_DIMS = 80
        _MAX_FALLBACK_MEAS = 60
        if (not filtered_dims and dimensions) or (not filtered_meas and measures):
            fallback_dims, fallback_meas = _fallback_columns_by_query_keywords(
                dimensions,
                measures,
                user_query,
                parsed_intent,
                node_name,
                min_dims=_MIN_FALLBACK_DIMS,
                max_dims=_MAX_FALLBACK_DIMS,
                min_meas=_MIN_FALLBACK_MEAS,
                max_meas=_MAX_FALLBACK_MEAS,
            )
            if not filtered_dims and dimensions:
                filtered_dims = fallback_dims
                for d in filtered_dims:
                    d.setdefault("selection_reasoning", "")
                logger.warning(
                    "%s %s KEYWORD_FALLBACK (dimensions): using %s dims matching query keywords (e.g. revenue, plant)",
                    COLUMN_SELECT_TAG, node_name, len(filtered_dims),
                )
            if not filtered_meas and measures:
                filtered_meas = fallback_meas
                for m in filtered_meas:
                    m.setdefault("selection_reasoning", "")
                logger.warning(
                    "%s %s KEYWORD_FALLBACK (measures): using %s measures matching query keywords",
                    COLUMN_SELECT_TAG, node_name, len(filtered_meas),
                )

        duration = (datetime.now() - start_time).total_seconds()
        combined_count = len(filtered_dims) + len(filtered_meas)
        logger.info(
            "%s %s RESULT: %s dimensions, %s measures (total %s columns) in %.2fs",
            COLUMN_SELECT_TAG, node_name, len(filtered_dims), len(filtered_meas), combined_count, duration,
        )
        logger.info(
            f"[{node_name}] Column selection complete | "
            f"Selected: {len(filtered_dims)} dims, {len(filtered_meas)} measures "
            f"(combined {combined_count} cols for next node) | "
            f"from {len(dimensions)} dims, {len(measures)} measures | "
            f"Duration: {duration:.2f}s"
        )

        if filtered_dims:
            dim_names = [d.get("label", d.get("name", "")) for d in filtered_dims[:6]]
            logger.info(f"[{node_name}] Selected dimensions: {', '.join(dim_names)}")
        if filtered_meas:
            meas_names = [m.get("label", m.get("name", "")) for m in filtered_meas[:6]]
            logger.info(f"[{node_name}] Selected measures: {', '.join(meas_names)}")

        # Group measures by category from LLM (measures_by_category); fallback "other" if LLM did not group.
        measures_by_group = _build_measures_by_category_from_llm(filtered_meas, merged_measures_by_category)
        categories_with_measures = [k for k, v in measures_by_group.items() if v]
        logger.info(
            f"[{node_name}] Category-wise measures: {', '.join(f'{k}={len(measures_by_group[k])}' for k in categories_with_measures) or 'none'}"
        )
        # State: group-wise is canonical; flat list derived from it (each measure has "category").
        flat_measures_from_group = [m for lst in measures_by_group.values() for m in lst]

        # DATE / FISCAL FILTER FLOW: same logic as simple flow (run_analytical_date_fiscal_filter).
        out = {
            "filtered_analytical_dimensions": filtered_dims,
            "filtered_analytical_measures_by_group": measures_by_group,
            "filtered_analytical_measures": flat_measures_from_group,
            "category_priorities": merged_category_priorities if isinstance(merged_category_priorities, dict) else {},
            "dimension_priorities": merged_dim_priorities if isinstance(merged_dim_priorities, dict) else {},
        }
        try:
            filter_updates = await run_analytical_date_fiscal_filter(
                state, filtered_dims, filtered_meas, node_name
            )
            out.update(filter_updates)
        except Exception as date_section_exc:
            logger.warning(
                f"[{node_name}] Date/fiscal filter section failed: {date_section_exc}"
            )
        return out

    except Exception as exc:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(
            f"[{node_name}] LLM column selection failed after {duration:.2f}s: {exc}",
            exc_info=True,
        )
        # Graceful fallback: pass through all columns
        logger.info(f"[{node_name}] Falling back to passing all columns through")
        for d in dimensions:
            d.setdefault("selection_reasoning", "")
        for m in measures:
            m.setdefault("selection_reasoning", "")
        measures_by_group = _build_measures_by_category_from_llm(measures, {})
        flat_measures_from_group = [m for lst in measures_by_group.values() for m in lst]
        return {
            "filtered_analytical_dimensions": dimensions,
            "filtered_analytical_measures_by_group": measures_by_group,
            "filtered_analytical_measures": flat_measures_from_group,
            "category_priorities": {},
            "dimension_priorities": {},
        }
