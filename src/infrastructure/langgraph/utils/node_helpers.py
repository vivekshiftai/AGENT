"""Utility functions for LangGraph nodes."""
import json
import logging
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

logger = logging.getLogger(__name__)


def extract_date_filters_from_state(state: Dict[str, Any]) -> Dict[str, Any]:
    """Extract date filter information from state (plan, generated_queries, operation_plan).

    Returns:
        Dictionary with date filter information:
        - date_range: {start_date, end_date, date_column} if found
        - filter_applied: Boolean indicating if date filter was applied
        - filter_source: Where the filter was found (plan, queries, operation_plan)
    """
    date_filter_info = {
        "date_range": None,
        "filter_applied": False,
        "filter_source": None,
    }

    # Check plan for date filters
    plan = state.get("plan", {})
    if plan and isinstance(plan, dict):
        tables = plan.get("tables", {})
        views = plan.get("views", {})
        plan_items = tables if tables else views
        for item_name, item_data in (plan_items or {}).items():
            if isinstance(item_data, dict):
                filters = item_data.get("filters", [])
                for filter_item in filters if isinstance(filters, list) else [filters]:
                    if isinstance(filter_item, dict):
                        filter_field = filter_item.get("field", "")
                        filter_value = filter_item.get("value", "")
                        filter_operator = filter_item.get("operator", "")
                        if any(kw in filter_field.lower() for kw in ["date", "time", "created", "updated", "fiscal"]):
                            if filter_operator in ["ge", ">=", "gte"] and "start" not in (date_filter_info.get("date_range") or {}):
                                date_filter_info["date_range"] = {
                                    "start_date": str(filter_value),
                                    "date_column": filter_field,
                                }
                                date_filter_info["filter_applied"] = True
                                date_filter_info["filter_source"] = "plan"
                            elif filter_operator in ["le", "<=", "lte"]:
                                if date_filter_info["date_range"]:
                                    date_filter_info["date_range"]["end_date"] = str(filter_value)
                                else:
                                    date_filter_info["date_range"] = {
                                        "end_date": str(filter_value),
                                        "date_column": filter_field,
                                    }
                                date_filter_info["filter_applied"] = True
                                date_filter_info["filter_source"] = "plan"

    # Check generated_queries (SAP OData queries) for date filters
    generated_queries = state.get("generated_queries", "")
    if generated_queries:
        try:
            queries_data = json.loads(generated_queries) if isinstance(generated_queries, str) else generated_queries
            queries = queries_data.get("queries", []) if isinstance(queries_data, dict) else (queries_data if isinstance(queries_data, list) else [])
            for query in queries:
                if isinstance(query, dict):
                    filter_expr = query.get("filter", "")
                    if filter_expr:
                        date_pattern = r'(\w+)\s+(ge|>=)\s+[\'"]?(\d{4}-\d{2}-\d{2})[\'"]?\s+and\s+\1\s+(le|lt|<=|<)\s+[\'"]?(\d{4}-\d{2}-\d{2})[\'"]?'
                        match = re.search(date_pattern, filter_expr, re.IGNORECASE)
                        if match:
                            date_filter_info["date_range"] = {
                                "start_date": match.group(3),
                                "end_date": match.group(5),
                                "date_column": match.group(1),
                            }
                            date_filter_info["filter_applied"] = True
                            date_filter_info["filter_source"] = "generated_queries"
                            break
        except Exception as e:
            logger.debug("Failed to parse generated_queries for date filters: %s", e)

    # Check operation_plan for date filters
    operation_plan = state.get("operation_plan", {})
    if operation_plan and isinstance(operation_plan, dict):
        for agg_key, agg_spec in (operation_plan.get("aggregations") or {}).items():
            if isinstance(agg_spec, dict):
                filter_spec = agg_spec.get("filter", "")
                if filter_spec and any(kw in str(filter_spec).lower() for kw in ["date", "time", "created", "updated", "fiscal"]):
                    date_filter_info["filter_applied"] = True
                    if not date_filter_info["filter_source"]:
                        date_filter_info["filter_source"] = "operation_plan"
                    dates = re.findall(r'(\d{4}-\d{2}-\d{2})', str(filter_spec))
                    if len(dates) >= 2:
                        date_filter_info["date_range"] = {
                            "start_date": dates[0],
                            "end_date": dates[-1],
                            "date_column": "filtered",
                        }
                    break

    return date_filter_info


def _strip_markdown_code_block(text: str) -> Tuple[str, str]:
    """
    Extract content from the first markdown code block (```...```) and optional language tag.
    Shared by parse_json_response, try_repair_truncated_json, try_repair_truncated_analytical_selection,
    and _extract_queries_from_response to avoid duplicate logic.

    Returns:
        (content: str, language: str) — content is stripped; language is "json", "sql", or "".
    """
    cleaned = (text or "").strip()
    if not cleaned.startswith("```"):
        return (cleaned, "")
    parts = cleaned.split("```")
    if len(parts) < 2:
        return (cleaned, "")
    content = parts[1].strip()
    lang = ""
    if content.lower().startswith("json"):
        content = content[4:].strip()
        lang = "json"
    elif content.lower().startswith("sql"):
        content = content[3:].strip()
        lang = "sql"
    return (content.strip(), lang)


# Log tag for LLM column validation — grep for this to find where wrong column names appear
LLM_COL_VALIDATE_TAG = "LLM_COL_VALIDATE"


def clean_sql_plan(sql_plan: Dict[str, Any]) -> Dict[str, Any]:
    """
    Clean SQL plan by removing null, empty, or unnecessary fields.
    This reduces cognitive load on the LLM by only including relevant information.
    
    Args:
        sql_plan: Raw SQL plan dictionary
        
    Returns:
        Cleaned SQL plan with only necessary fields
    """
    cleaned = {}
    
    # Always include required fields
    if 'columns' in sql_plan:
        cleaned['columns'] = sql_plan['columns']
    
    # Include filters only if not empty
    filters = sql_plan.get('filters', [])
    if filters and len(filters) > 0:
        cleaned['filters'] = filters
    
    # Include aggregation only if present
    aggregation = sql_plan.get('aggregation')
    if aggregation:
        cleaned['aggregation'] = aggregation
    
    # Include group_by only if present and not empty
    group_by = sql_plan.get('group_by')
    if group_by and len(group_by) > 0:
        cleaned['group_by'] = group_by
    
    # Include order_by only if present and not empty
    order_by = sql_plan.get('order_by')
    if order_by and len(order_by) > 0:
        cleaned['order_by'] = order_by
    
    # Include limit only if present
    limit = sql_plan.get('limit')
    if limit is not None:
        cleaned['limit'] = limit
    
    # Include value_formats only if present and not empty
    value_formats = sql_plan.get('value_formats', {})
    if value_formats and len(value_formats) > 0:
        cleaned['value_formats'] = value_formats
    
    return cleaned


def parse_json_response(response: str, expected_type: Optional[type] = None) -> Any:
    """
    Parse JSON response from LLM, handling markdown code blocks and extra text.

    Args:
        response: Raw response string from LLM
        expected_type: Expected type (dict, list, etc.) for validation

    Returns:
        Parsed JSON object

    Raises:
        json.JSONDecodeError: If JSON parsing fails
        ValueError: If response doesn't match expected type
    """
    # Ensure response is a string
    if response is None:
        raise ValueError("Response is None")
    elif hasattr(response, 'empty'):  # Check if it's a DataFrame/Series
        raise ValueError(f"Response appears to be a DataFrame/Series, got {type(response)}")
    elif not isinstance(response, str):
        raise ValueError(f"Response must be a string, got {type(response)}")

    if not response or len(response.strip()) == 0:
        raise ValueError("Empty response")

    cleaned_response, _ = _strip_markdown_code_block(response)

    # Try to find the first complete JSON object/array (prefer outermost: object before array when both exist)
    # This handles cases where Claude adds explanations after the JSON, and objects with nested arrays
    # (e.g. {"fiscal_column": "...", "value_filters": [...]} — we must return the object, not the inner array)
    def find_complete_json(text: str) -> Optional[str]:
        """Find the first complete JSON object or array in text. Prefer root object over root array when both exist."""
        obj_start = text.find("{")
        arr_start = text.find("[")

        def extract_object(start: int) -> Optional[str]:
            brace_count = 0
            in_string = False
            escape_next = False
            for i in range(start, len(text)):
                c = text[i]
                if escape_next:
                    escape_next = False
                    continue
                if c == "\\":
                    escape_next = True
                    continue
                if c == '"' and not escape_next:
                    in_string = not in_string
                    continue
                if not in_string:
                    if c == "{":
                        brace_count += 1
                    elif c == "}":
                        brace_count -= 1
                        if brace_count == 0:
                            return text[start : i + 1]
            return None

        def extract_array(start: int) -> Optional[str]:
            bracket_count = 0
            in_string = False
            escape_next = False
            for i in range(start, len(text)):
                c = text[i]
                if escape_next:
                    escape_next = False
                    continue
                if c == "\\":
                    escape_next = True
                    continue
                if c == '"' and not escape_next:
                    in_string = not in_string
                    continue
                if not in_string:
                    if c == "[":
                        bracket_count += 1
                    elif c == "]":
                        bracket_count -= 1
                        if bracket_count == 0:
                            return text[start : i + 1]
            return None

        # Prefer whichever starts first (outermost structure)
        if obj_start >= 0 and (arr_start < 0 or obj_start <= arr_start):
            result = extract_object(obj_start)
            if result:
                return result
        if arr_start >= 0:
            result = extract_array(arr_start)
            if result:
                return result
        if obj_start >= 0:
            result = extract_object(obj_start)
            if result:
                return result
        return None
    
    # Try to extract complete JSON
    json_text = find_complete_json(cleaned_response)
    if json_text:
        cleaned_response = json_text
    else:
        # Fallback to old method if complete JSON not found
        if "{" in cleaned_response and "}" in cleaned_response:
            start_idx = cleaned_response.find("{")
            end_idx = cleaned_response.rfind("}") + 1
            if start_idx >= 0 and end_idx > start_idx:
                cleaned_response = cleaned_response[start_idx:end_idx]
        elif "[" in cleaned_response and "]" in cleaned_response:
            start_idx = cleaned_response.find("[")
            end_idx = cleaned_response.rfind("]") + 1
            if start_idx >= 0 and end_idx > start_idx:
                cleaned_response = cleaned_response[start_idx:end_idx]
    
    # Remove trailing commas before } or ] (invalid JSON but some LLMs emit them)
    cleaned_response = re.sub(r",\s*([}\]])", r"\1", cleaned_response)

    # Parse JSON
    try:
        parsed = json.loads(cleaned_response)
        
        # Validate type if expected (expected_type must be a type/tuple/union valid for isinstance)
        if expected_type:
            try:
                if not isinstance(parsed, expected_type):
                    actual_type_name = type(parsed).__name__
                    expected_type_name = getattr(expected_type, "__name__", str(expected_type))
                    raise ValueError(f"Response is not of expected type {expected_type_name}, got {actual_type_name}")
            except TypeError:
                # expected_type was not valid for isinstance (e.g. string passed by mistake) — skip validation
                pass
        return parsed
        
    except json.JSONDecodeError as e:
        raise


def parse_json_response_required_dict(
    response: str,
    node_name: str = "",
    extract_from_list: bool = False,
) -> Dict[str, Any]:
    """
    Parse LLM JSON response and return a dict. If extract_from_list is True and
    the parsed value is a list, the first dict element is returned.
    Returns empty dict on any failure.
    """
    if response is None or not isinstance(response, str) or not str(response).strip():
        return {}
    try:
        parsed = parse_json_response(response, expected_type=None)
        if isinstance(parsed, dict):
            return parsed
        if extract_from_list and isinstance(parsed, list):
            for item in parsed:
                if isinstance(item, dict):
                    return item
        return {}
    except Exception:
        return {}


def extract_and_parse_json(
    response: Any,
    node_name: str = "",
) -> Optional[Union[Dict[str, Any], List[Any]]]:
    """
    Extract and parse JSON from LLM response. Returns None on failure.
    Uses parse_json_response internally; safe for use when optional parsing is needed.
    """
    if response is None:
        return None
    if hasattr(response, "empty"):
        return None
    if not isinstance(response, str):
        response = str(response)
    if not response or not response.strip():
        return None
    try:
        return parse_json_response(response, expected_type=None)
    except Exception:
        return None


def save_prompt_to_json(
    node_name: str,
    system_prompt: str,
    user_prompt: str,
    query_id: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
    call_suffix: Optional[str] = None,
) -> Optional[Path]:
    """
    Save LLM call input (system + user prompt) to prompts/input/{node_name}_{query_id}[_{call_suffix}].json.
    Used by chart_preplan, chart_plan, and other nodes. Delegates to save_llm_call_input.
    Returns the path if saved, None otherwise. Logs and swallows errors.
    """
    from .llm_call_io import save_llm_call_input
    path = save_llm_call_input(
        node_name=node_name,
        query_id=query_id,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        extra=extra,
        call_suffix=call_suffix,
    )
    if path:
        logger.info(f"[{node_name}] Saved prompts to {path}")
    return path


def try_repair_truncated_json(response: str) -> Optional[Any]:
    """
    Attempt to parse JSON when the response may be truncated (e.g. LLM hit max_tokens).
    Strips markdown code blocks, then closes any unclosed ] and } and parses.
    Returns parsed object or None if repair/parse fails.
    """
    if not response or not response.strip():
        return None
    cleaned, _ = _strip_markdown_code_block(response)
    missing_braces = cleaned.count("{") - cleaned.count("}")
    missing_brackets = cleaned.count("[") - cleaned.count("]")
    if missing_braces > 0 or missing_brackets > 0:
        try:
            reconstructed = cleaned + "]" * missing_brackets + "}" * missing_braces
            return json.loads(reconstructed)
        except json.JSONDecodeError:
            return None
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        return None


def normalize_sql_plan_to_tables_format(parsed: Any, selected_tables: List[str]) -> Dict[str, Any]:
    """
    Normalize LLM SQL plan output to the standard format: {"tables": {table: {...}}, "join_keys": []}.
    Handles: parsed as list, parsed as dict with "tables", or single-table format at root.
    Ensures all selected_tables are present in plan["tables"].
    """
    sql_plan: Dict[str, Any]
    if isinstance(parsed, list):
        if parsed and isinstance(parsed[0], dict):
            sql_plan = parsed[0]
        else:
            sql_plan = {"columns": parsed if all(isinstance(x, str) for x in parsed) else []}
    elif isinstance(parsed, dict):
        sql_plan = parsed
    else:
        sql_plan = {"tables": {t: {"columns": ["*"], "filters": []} for t in selected_tables}, "join_keys": []}
        return sql_plan

    if isinstance(sql_plan, dict) and "tables" in sql_plan:
        tables_in_plan = sql_plan["tables"]
        if not isinstance(tables_in_plan, dict):
            sql_plan["tables"] = {t: {} for t in selected_tables} if isinstance(tables_in_plan, list) else {t: {"columns": ["*"], "filters": []} for t in selected_tables}
        else:
            plan_table_names = set(sql_plan["tables"].keys())
            missing = set(selected_tables) - plan_table_names
            for table in missing:
                sql_plan["tables"][table] = {"columns": ["*"], "filters": []}
        if "join_keys" not in sql_plan:
            sql_plan["join_keys"] = []
        return sql_plan

    if "columns" in sql_plan or any(k in sql_plan for k in ["filters", "aggregation", "group_by", "order_by", "limit"]):
        single_plan = {k: v for k, v in sql_plan.items() if k not in ["tables", "join_keys"]}
        if not single_plan:
            single_plan = {"columns": ["*"], "filters": []}
        return {
            "tables": {table: single_plan.copy() for table in selected_tables},
            "join_keys": [],
        }
    return {
        "tables": {table: {"columns": ["*"], "filters": []} for table in selected_tables},
        "join_keys": [],
    }


def try_repair_truncated_analytical_selection(response: str) -> Optional[Dict[str, Any]]:
    """
    Attempt to recover partial JSON from a truncated LLM response (e.g. hit max_tokens).
    Accepts format: {"dimensions": ["Name1", ...], "measures": ["Name1", ...]} or legacy
    {"selected_dimensions": [...], "selected_measures": [...]}. First tries closing open
    brackets; if that fails, extracts names (strings or "name" fields) with regex.
    """
    if not response or not response.strip():
        return None
    cleaned, _ = _strip_markdown_code_block(response)
    open_braces = cleaned.count("{") - cleaned.count("}")
    open_brackets = cleaned.count("[") - cleaned.count("]")
    if open_braces >= 0 and open_brackets >= 0:
        try:
            repaired = cleaned + "]" * open_brackets + "}" * open_braces
            return json.loads(repaired)
        except json.JSONDecodeError:
            pass
    # Extract from "dimensions" / "measures" (array of strings) or "selected_dimensions" / "selected_measures" (objects with "name")
    out: Dict[str, Any] = {}
    for key, alt_key in [("dimensions", "selected_dimensions"), ("measures", "selected_measures")]:
        match = re.search(rf'"{key}"\s*:\s*\[', cleaned, re.IGNORECASE) or re.search(rf'"{alt_key}"\s*:\s*\[', cleaned, re.IGNORECASE)
        if not match:
            continue
        start = match.end()
        end = len(cleaned)
        for other in ["dimensions", "selected_dimensions", "measures", "selected_measures"]:
            if other == key or other == alt_key:
                continue
            other_m = re.search(rf'"{other}"\s*:\s*\[', cleaned[start:], re.IGNORECASE)
            if other_m:
                end = start + other_m.start()
                break
        section = cleaned[start:end]
        names: List[str] = []
        skip = {"name", "label", "reason", "dimensions", "measures", "selected_dimensions", "selected_measures"}
        for m in re.finditer(r'"([^"]+)"', section):
            val = m.group(1).strip()
            if val and val not in skip and not val.startswith("EXACT_") and any(c.isalnum() or c == "_" for c in val):
                names.append(val)
        if names:
            out[key] = names
    if out.get("dimensions") or out.get("measures"):
        return out
    # Combined format: selected_columns (single array of names)
    sel_match = re.search(r'"selected_columns"\s*:\s*\[', cleaned, re.IGNORECASE) or re.search(r'"columns"\s*:\s*\[', cleaned, re.IGNORECASE)
    if sel_match:
        start = sel_match.end()
        end = len(cleaned)
        for other in ["dimensions", "measures", "selected_dimensions", "selected_measures"]:
            other_m = re.search(rf'"{other}"\s*:\s*\[', cleaned[start:], re.IGNORECASE)
            if other_m:
                end = start + other_m.start()
                break
        section = cleaned[start:end]
        names = []
        skip = {"name", "label", "reason", "dimension", "measure"}
        for m in re.finditer(r'"([^"]+)"', section):
            val = m.group(1).strip()
            if val and val not in skip and any(c.isalnum() or c == "_" for c in val):
                names.append(val)
        if names:
            return {"selected_columns": names}
    # Legacy: selected_* with "name" fields
    out = {"selected_dimensions": [], "selected_measures": []}
    dim_match = re.search(r'"selected_dimensions"\s*:\s*\[', cleaned, re.IGNORECASE)
    meas_match = re.search(r'"selected_measures"\s*:\s*\[', cleaned, re.IGNORECASE)
    if dim_match:
        start = dim_match.end()
        end = meas_match.start() if meas_match else len(cleaned)
        for m in re.finditer(r'"name"\s*:\s*"([^"]*)"', cleaned[start:end]):
            out["selected_dimensions"].append({"name": m.group(1)})
    if meas_match:
        start = meas_match.end()
        for m in re.finditer(r'"name"\s*:\s*"([^"]*)"', cleaned[start:]):
            out["selected_measures"].append({"name": m.group(1)})
    if out["selected_dimensions"] or out["selected_measures"]:
        return out
    return None


# ---------------------------------------------------------------------------
# Common LLM column validation — use everywhere we check LLM-suggested column names
# ---------------------------------------------------------------------------


def log_llm_invalid_columns(
    source: str,
    node_name: str,
    invalid_columns: List[str],
    available_sample: Optional[List[str]] = None,
    context: Optional[str] = None,
) -> None:
    """
    Log invalid LLM-suggested column names in a consistent format so we can grep
    logs to see where wrong column names are coming from.

    Args:
        source: Logical source of the columns (e.g. "chart_preplan.metrics", "sap_fetch_plan.columns").
        node_name: Name of the node (e.g. "analytical_fetch_plan", "sap_fetch_plan").
        invalid_columns: List of column names that were suggested but not valid.
        available_sample: Optional sample of valid column names (for debugging).
        context: Optional extra context (e.g. chart_id, view_name).
    """
    if not invalid_columns:
        return
    sample = (available_sample or [])[:20]
    msg = (
        f"[{LLM_COL_VALIDATE_TAG}] source={source} node={node_name} "
        f"invalid_columns={invalid_columns}"
    )
    if context:
        msg += f" context={context}"
    if sample:
        msg += f" available_sample={sample}"
    logger.warning(msg)


def validate_llm_columns(
    suggested_col_names: List[Union[str, Any]],
    available_col_names: List[str],
    source: str,
    node_name: str,
    *,
    case_insensitive: bool = True,
    available_objects: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[List[Union[str, Dict[str, Any]]], List[str], Dict[str, str]]:
    """
    Validate LLM-suggested column names against the list of actual column names.
    Use this wherever we consume column names from LLM output so invalid names
    are logged in one consistent format (grep for LLM_COL_VALIDATE).

    Args:
        suggested_col_names: Column names from LLM (strings, or dicts with "name" key).
        available_col_names: Actual column names from schema/frame.
        source: Logical source (e.g. "analytical_column_selection.dimensions").
        node_name: Node name for logging.
        case_insensitive: If True, match case-insensitively and return actual casing.
        available_objects: If provided, return full dicts for validated names
            (list of dicts with "name" key); otherwise return name strings.

    Returns:
        (validated_list, invalid_list, corrections_map)
        - validated_list: Correct names (or full dicts from available_objects when given).
        - invalid_list: Suggested names that were not found.
        - corrections_map: {suggested: actual} for case-only corrections.
    """
    validated: List[Union[str, Dict[str, Any]]] = []
    invalid_list: List[str] = []
    corrections: Dict[str, str] = {}

    available_set = set(available_col_names)
    available_lower_to_actual: Dict[str, str] = {}
    if case_insensitive:
        for c in available_col_names:
            if c:
                available_lower_to_actual[c.lower().strip()] = c
    by_name = {obj["name"]: obj for obj in (available_objects or []) if obj.get("name")}

    for item in suggested_col_names:
        name = (
            item if isinstance(item, str) else (item.get("name", "") if isinstance(item, dict) else "")
        )
        if not name or not isinstance(name, str):
            continue
        name = name.strip()
        if not name:
            continue

        if name in available_set:
            if available_objects and name in by_name:
                validated.append(dict(by_name[name]))
            else:
                validated.append(name)
            continue

        if case_insensitive:
            key = name.lower()
            actual = available_lower_to_actual.get(key)
            if actual:
                corrections[name] = actual
                if available_objects and actual in by_name:
                    validated.append(dict(by_name[actual]))
                else:
                    validated.append(actual)
                continue

        invalid_list.append(name)

    if invalid_list:
        sample = list(available_set)[:20] if available_set else (list(available_lower_to_actual.values())[:20])
        log_llm_invalid_columns(
            source=source,
            node_name=node_name,
            invalid_columns=invalid_list,
            available_sample=sample,
        )
    if corrections:
        logger.info(
            f"[{LLM_COL_VALIDATE_TAG}] source={source} node={node_name} "
            f"case_corrections={corrections}"
        )

    return validated, invalid_list, corrections


def _extract_queries_from_response(response: str) -> str:
    """
    Extract SQL queries from LLM response, handling markdown code blocks and JSON formats.
    
    Args:
        response: Raw response string from LLM
        
    Returns:
        JSON string with queries array
    """
    if not response:
        return json.dumps({"queries": []})

    cleaned, lang = _strip_markdown_code_block(response)
    if lang == "sql":
        return json.dumps({"queries": [cleaned]})

    # If it's already valid JSON, return as-is
    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, dict) and "queries" in parsed:
            return cleaned
        elif isinstance(parsed, list):
            return json.dumps({"queries": parsed})
        elif isinstance(parsed, str):
            return json.dumps({"queries": [parsed]})
    except json.JSONDecodeError:
        pass
    
    # If it looks like SQL, wrap it
    if any(keyword in cleaned.upper() for keyword in ["SELECT", "FROM", "WHERE", "GROUP BY"]):
        return json.dumps({"queries": [cleaned]})
    
    # Default: return as single query
    return json.dumps({"queries": [cleaned]})


def build_aggregation_details(
    operation_plan: Optional[Dict[str, Any]],
    selected_tables: Optional[List[str]] = None,
    allowed_keys: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Build a concise aggregation summary so callers can display which operations,
    tables, and columns were used to compute metrics/charts.
    """
    if not operation_plan:
        return None

    def should_include(key: str) -> bool:
        return not allowed_keys or key in allowed_keys

    agg_details: List[Dict[str, Any]] = []
    derived_details: List[Dict[str, str]] = []
    tables: Set[str] = set()
    columns: Set[str] = set()

    aggregations = operation_plan.get("aggregations") or {}
    for key, value in aggregations.items():
        if not should_include(key):
            continue

        column = value.get("column")
        table = value.get("table")
        if not table and column and "." in column:
            table = column.split(".")[0]

        if table:
            tables.add(table)
        if column:
            columns.add(column)

        source = f"{table}.{column.split('.')[-1]}" if table and column else column or table
        operation = value.get("agg", "aggregation")
        group_by = value.get("group_by")

        description = (
            f"{operation.upper()} on {column or 'field'}"
            + (f" grouped by {group_by}" if group_by else "")
        )

        agg_details.append(
            {
                "key": key,
                "operation": operation,
                "column": column,
                "table": table,
                "groupBy": group_by,
                "source": source,
                "description": description,
            }
        )

    derived = operation_plan.get("derived") or {}
    for key, formula in derived.items():
        if not should_include(key):
            continue
        derived_details.append(
            {
                "key": key,
                "formula": formula if isinstance(formula, str) else json.dumps(formula),
            }
        )

    if selected_tables:
        for table in selected_tables:
            if table:
                tables.add(table)

    if not agg_details and not derived_details and not tables and not columns:
        return None

    return {
        "aggregations": agg_details or None,
        "derivedMetrics": derived_details or None,
        "tables": list(tables) if tables else None,
        "columns": list(columns) if columns else None,
    }


# Max data points per axis to include in LLM chart payload (avoids token overflow)
_MAX_CHART_POINTS_FOR_LLM = 100


def extract_chart_data_for_llm(
    prepared_charts: List[Dict[str, Any]],
    max_points: int = _MAX_CHART_POINTS_FOR_LLM,
) -> List[Dict[str, Any]]:
    """
    Extract chart data for summary/intelligence LLM: title + x-axis values + y-axis values only.
    No filters, sort, aggregation, or other metadata.

    Input: prepared_charts — the charts (chart data) from state, as sent by the chart_preparation node.

    Returns list of:
      - { "title": str, "x_values": [...], "y_values": [...] } for single-series charts, or
      - { "title": str, "x_values": [...], "y_series": [ { "label": str, "values": [...] }, ... ] } for multi-series.
    """
    import logging
    _logger = logging.getLogger(__name__)
    
    out: List[Dict[str, Any]] = []
    if not prepared_charts:
        _logger.warning("[extract_chart_data_for_llm] No prepared_charts provided (empty or None)")
        return out

    _logger.info(f"[extract_chart_data_for_llm] Processing {len(prepared_charts)} charts")
    
    for idx, chart in enumerate(prepared_charts):
        if not isinstance(chart, dict):
            _logger.warning(f"[extract_chart_data_for_llm] Chart {idx} is not a dict, skipping")
            continue
        title = chart.get("title") or "Unknown Chart"
        data = chart.get("data", [])
        if not data:
            _logger.warning(f"[extract_chart_data_for_llm] Chart '{title}' has no data, skipping")
            continue
        x_field = chart.get("x_field") or chart.get("group_by") or ""
        y_field = chart.get("y_field", "")
        data_sources = chart.get("data_sources", [])
        
        _logger.debug(f"[extract_chart_data_for_llm] Chart '{title}': x_field='{x_field}', y_field='{y_field}', data_rows={len(data)}, data_sources={len(data_sources) if data_sources else 0}")
        
        # Log first row keys for debugging
        if data and isinstance(data[0], dict):
            first_row_keys = list(data[0].keys())
            _logger.debug(f"[extract_chart_data_for_llm] Chart '{title}' first row keys: {first_row_keys[:10]}")

        # X values (categories or dates)
        x_values: List[Any] = []
        for row in data[:max_points]:
            if not isinstance(row, dict):
                continue
            x_val = row.get(x_field) or row.get("group") or row.get("category")
            if x_val is not None:
                x_values.append(str(x_val) if not isinstance(x_val, (int, float)) else x_val)

        if not x_values:
            _logger.warning(f"[extract_chart_data_for_llm] Chart '{title}' has no x_values extracted (x_field='{x_field}'), skipping")
            continue

        # Single y series: try y_field first; chart_preparation often stores data under a cleaned display key
        if y_field and data:
            y_values: List[Any] = []
            for row in data[:max_points]:
                if isinstance(row, dict):
                    v = row.get(y_field)
                    if v is not None:
                        try:
                            y_values.append(float(v) if isinstance(v, (int, float)) else v)
                        except (ValueError, TypeError):
                            y_values.append(v)
            extracted_via_fallback = False
            if not y_values and data and isinstance(data[0], dict):
                # Fallback: chart_preparation uses display_field (_clean_series_label) as key, not raw y_field
                first_row = data[0]
                x_keys = {x_field, "group", "category"}
                value_keys = [
                    k for k, v in first_row.items()
                    if k not in x_keys and v is not None and isinstance(v, (int, float))
                ]
                if len(value_keys) == 1:
                    col = value_keys[0]
                    for row in data[:max_points]:
                        if isinstance(row, dict):
                            v = row.get(col)
                            if v is not None:
                                try:
                                    y_values.append(float(v) if isinstance(v, (int, float)) else v)
                                except (ValueError, TypeError):
                                    y_values.append(v)
                    if y_values:
                        _logger.info(f"[extract_chart_data_for_llm] Chart '{title}' y_values from inferred key '{col}' (y_field '{y_field}' not in data keys)")
                elif len(value_keys) > 1:
                    y_series_inferred = []
                    for col in value_keys:
                        values = []
                        for row in data[:max_points]:
                            if isinstance(row, dict):
                                v = row.get(col)
                                if v is not None and isinstance(v, (int, float)):
                                    values.append(float(v))
                        if values:
                            y_series_inferred.append({"label": col, "values": values})
                    if y_series_inferred:
                        metrics_in_chart = [s["label"] for s in y_series_inferred]
                        item = {"title": title, "x_values": x_values, "y_series": y_series_inferred, "metrics_in_chart": metrics_in_chart}
                        if chart.get("reasoning"):
                            item["reasoning"] = chart.get("reasoning")
                        out.append(item)
                        extracted_via_fallback = True
                        _logger.info(f"[extract_chart_data_for_llm] Chart '{title}' extracted with {len(y_series_inferred)} series (inferred from data keys)")
            if y_values:
                metrics_in_chart = [y_field] if y_field else [title]
                item = {"title": title, "x_values": x_values, "y_values": y_values, "metrics_in_chart": metrics_in_chart}
                if chart.get("reasoning"):
                    item["reasoning"] = chart.get("reasoning")
                out.append(item)
                _logger.info(f"[extract_chart_data_for_llm] Chart '{title}' extracted with {len(y_values)} y_values (single series)")
            elif not extracted_via_fallback:
                _logger.warning(f"[extract_chart_data_for_llm] Chart '{title}' has y_field='{y_field}' but no y_values extracted (data keys: {list(data[0].keys()) if data and isinstance(data[0], dict) else []})")
            continue

        # Multi-series: data_sources
        if data_sources and isinstance(data_sources, list):
            y_series: List[Dict[str, Any]] = []
            for ds in data_sources:
                if not isinstance(ds, dict):
                    continue
                label = ds.get("label") or ds.get("field") or ""
                col = ds.get("field") or ds.get("label")
                if not col:
                    continue
                values: List[Any] = []
                for row in data[:max_points]:
                    if isinstance(row, dict):
                        v = row.get(col)
                        if v is not None:
                            try:
                                values.append(float(v) if isinstance(v, (int, float)) else v)
                            except (ValueError, TypeError):
                                values.append(v)
                if values:
                    y_series.append({"label": label or col, "values": values})
            if y_series:
                metrics_in_chart = [ds.get("label") or ds.get("field") or "" for ds in y_series if ds.get("label") or ds.get("field")]
                if not metrics_in_chart:
                    metrics_in_chart = [title]
                item = {"title": title, "x_values": x_values, "y_series": y_series, "metrics_in_chart": metrics_in_chart}
                if chart.get("reasoning"):
                    item["reasoning"] = chart.get("reasoning")
                out.append(item)
                _logger.info(f"[extract_chart_data_for_llm] Chart '{title}' extracted with {len(y_series)} series (from data_sources)")
            else:
                _logger.warning(f"[extract_chart_data_for_llm] Chart '{title}' has data_sources but no y_series extracted")
            continue

        # Multi value columns (no y_field, no data_sources): infer from first row
        first_row = data[0] if data else {}
        if isinstance(first_row, dict):
            value_cols = [
                k for k, v in first_row.items()
                if k != x_field and isinstance(v, (int, float))
            ]
            if value_cols:
                y_series = []
                for col in value_cols:
                    values = []
                    for row in data[:max_points]:
                        if isinstance(row, dict):
                            v = row.get(col)
                            if v is not None and isinstance(v, (int, float)):
                                values.append(v)
                    if values:
                        y_series.append({"label": col, "values": values})
                if y_series:
                    metrics_in_chart = [s.get("label", "") for s in y_series if s.get("label")]
                    if not metrics_in_chart:
                        metrics_in_chart = value_cols[:10]
                    item = {"title": title, "x_values": x_values, "y_series": y_series, "metrics_in_chart": metrics_in_chart}
                    if chart.get("reasoning"):
                        item["reasoning"] = chart.get("reasoning")
                    out.append(item)
                    _logger.info(f"[extract_chart_data_for_llm] Chart '{title}' extracted with {len(y_series)} series (inferred from numeric cols)")

    _logger.info(f"[extract_chart_data_for_llm] Successfully extracted {len(out)} chart_data items from {len(prepared_charts)} prepared_charts")
    return out


def filter_charts_with_all_zeros(charts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    NOTE: Chart filtering by "all zero values" is disabled for now.
    This function currently returns the charts unchanged so that even
    all-zero charts are available to the caller / frontend.

    If you need to re-enable this behaviour, restore the earlier
    implementation from version control.
    """
    if not charts:
        return charts

    logger.info(
        "🔍 [Utils] Skipping 'all zero values' chart filtering - returning %d chart(s) unchanged",
        len(charts),
    )
    return charts


# ---------------------------------------------------------------------------
# Category assignment for charts and metrics (measure-based grouping)
# ---------------------------------------------------------------------------

def build_measure_to_category(state: Dict[str, Any]) -> Dict[str, str]:
    """
    Build measure name -> category from state (filtered_analytical_measures or
    filtered_analytical_measures_by_group). Used to assign category to charts
    and metrics based on which measures they use.
    """
    by_group = state.get("filtered_analytical_measures_by_group") or {}
    if by_group:
        out: Dict[str, str] = {}
        for cat, measures in by_group.items():
            if not cat or not measures:
                continue
            for m in measures:
                if isinstance(m, dict):
                    name = (m.get("name") or "").strip()
                    if name:
                        out[name] = cat
        return out
    flat = state.get("filtered_analytical_measures") or []
    out = {}
    for m in flat:
        if isinstance(m, dict) and m.get("category"):
            name = (m.get("name") or "").strip()
            if name:
                out[name] = str(m["category"]).strip()
    return out


def build_metric_to_measures_from_analysis_plan(analysis_plan: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    Build metric_key (normalized) -> list of measure column names from analysis_plan
    (calculation_strategy.single_number_kpis and trend_metrics with columns_used).
    """
    import re
    out: Dict[str, List[str]] = {}
    if not analysis_plan or not isinstance(analysis_plan, dict):
        return out
    inner = analysis_plan.get("analysis_plan", analysis_plan)
    if not isinstance(inner, dict):
        return out
    cs = inner.get("calculation_strategy") or {}
    if not isinstance(cs, dict):
        return out
    for kpi in (cs.get("single_number_kpis") or []):
        if not isinstance(kpi, dict):
            continue
        name = (kpi.get("metric_name") or "").strip()
        key = re.sub(r"\s+", "_", name.lower()) if name else ""
        cols = kpi.get("columns_used") or []
        if key and isinstance(cols, list):
            measure_names = []
            for c in cols:
                if isinstance(c, str) and c.strip():
                    # column may be "ViewName.MeasureName"
                    measure_names.append(c.split(".")[-1].strip() if "." in c else c.strip())
            if measure_names:
                out[key] = measure_names
    for trend in (cs.get("trend_metrics") or []):
        if not isinstance(trend, dict):
            continue
        name = (trend.get("metric_name") or "").strip()
        key = re.sub(r"\s+", "_", name.lower()) if name else ""
        cols = trend.get("columns_used") or []
        if key and isinstance(cols, list):
            measure_names = []
            for c in cols:
                if isinstance(c, str) and c.strip():
                    measure_names.append(c.split(".")[-1].strip() if "." in c else c.strip())
            if measure_names:
                out[key] = measure_names
    return out


def assign_chart_category(chart: Dict[str, Any], measure_to_category: Dict[str, str]) -> str:
    """
    Determine chart category from measures used in its aggregations.
    If chart uses measures from one category only, return that category.
    If chart uses measures from multiple categories, return "Comparison".
    If no measure mapping, return chart.get("category") or "other".
    """
    if not measure_to_category:
        return (chart.get("category") or "other").strip()
    categories: Set[str] = set()
    aggs = chart.get("aggregations") or chart.get("operational_plan", {}).get("aggregations") or {}
    for agg_key, spec in (aggs if isinstance(aggs, dict) else {}).items():
        if not isinstance(spec, dict):
            continue
        col = (spec.get("column") or "").strip()
        if not col:
            continue
        measure_name = col.split(".")[-1] if "." in col else col
        if measure_name in measure_to_category:
            categories.add(measure_to_category[measure_name])
    if not categories:
        return (chart.get("category") or "other").strip()
    if len(categories) == 1:
        return list(categories)[0]
    return "Comparison"


def assign_result_category(
    result: Dict[str, Any],
    metric_to_measures: Dict[str, List[str]],
    measure_to_category: Dict[str, str],
) -> str:
    """
    Determine computation_result category from metric's columns_used (via analysis_plan).
    If metric uses measures from one category, return that category; if multiple, "Comparison".
    """
    if not measure_to_category:
        return (result.get("category") or "other").strip()
    metric_id = (result.get("metric") or "").strip()
    if not metric_id:
        return (result.get("category") or "other").strip()
    import re
    metric_key = re.sub(r"\s+", "_", metric_id.lower())
    measure_names = metric_to_measures.get(metric_key) or metric_to_measures.get(metric_id.lower())
    if not measure_names:
        return (result.get("category") or "other").strip()
    categories = {measure_to_category.get(m) for m in measure_names if measure_to_category.get(m)}
    if not categories:
        return (result.get("category") or "other").strip()
    if len(categories) == 1:
        return list(categories)[0]
    return "Comparison"


def build_analytical_schema_context(
    state: Dict[str, Any],
    node_name: str = "unknown",
) -> str:
    """
    Build a formatted analytical schema context string from filtered
    dimensions and measures stored in state.

    Only returns content for SAP / analytical views.  For every other
    data-source type it returns an empty string so existing prompt logic
    is used unchanged.

    The output explicitly lists dimension columns (for group_by / x-axis /
    breakdowns) and measure columns (for values / aggregation) so the LLM
    can use them directly in chart or KPI plans.

    Args:
        state: Current analytics state (AnalyticsState / dict)
        node_name: Node name used for logging

    Returns:
        Formatted schema context string, or "" if not applicable
    """
    # Gate: only for SAP / analytical views
    data_source_config = state.get("data_source_config", {})
    ds_type = (data_source_config.get("type", "") if data_source_config else "").lower()
    if ds_type not in ("sap", "sap_datasphere"):
        return ""

    filtered_dims: List[Dict[str, Any]] = state.get("filtered_analytical_dimensions", [])
    filtered_meas: List[Dict[str, Any]] = state.get("filtered_analytical_measures", [])

    if not filtered_dims and not filtered_meas:
        return ""

    parts: List[str] = []
    parts.append("The columns below were pre-selected for this query by the column-selection step.")
    parts.append("Each column includes a \"Why selected\" note explaining why it was chosen over similar alternatives.")
    parts.append("IMPORTANT: When you use a column in your plan, you MUST incorporate its \"Why selected\" reasoning into your own reasoning field — rewrite it in clear, user-friendly language (e.g. \"We use Net Revenue rather than Gross Revenue because it reflects actual income after deductions\"). The user sees your reasoning in the UI.")
    parts.append("")

    # ── Dimension columns ──
    if filtered_dims:
        parts.append(f"DIMENSION COLUMNS ({len(filtered_dims)}):")
        parts.append("  Categories, hierarchies, and time periods.")
        parts.append("  Use for group_by / x-axis / breakdowns.")
        parts.append("")
        for dim in filtered_dims:
            name = dim.get("name", "")
            label = dim.get("label", name)
            data_type = dim.get("data_type", "")
            view = dim.get("view_name", "")
            line = f"  - {view}.{name}  |  Label: \"{label}\"  |  Type: {data_type}"
            reason = (dim.get("selection_reasoning") or "").strip()
            if reason:
                line += f"  |  Why selected: {reason}"
            parts.append(line)
        parts.append("")

    # ── Measure columns ──
    if filtered_meas:
        parts.append(f"MEASURE COLUMNS ({len(filtered_meas)}):")
        parts.append("  Numeric values (amounts, quantities, counts).")
        parts.append("  Use for aggregation (SUM, AVG, COUNT, etc.).")
        parts.append("")
        for meas in filtered_meas:
            name = meas.get("name", "")
            label = meas.get("label", name)
            data_type = meas.get("data_type", "")
            view = meas.get("view_name", "")
            line = f"  - {view}.{name}  |  Label: \"{label}\"  |  Type: {data_type}"
            reason = (meas.get("selection_reasoning") or "").strip()
            if reason:
                line += f"  |  Why selected: {reason}"
            parts.append(line)

    context = "\n".join(parts)
    logger.info(
        f"[{node_name}] Analytical schema context built (SAP): "
        f"{len(filtered_dims)} dimensions, {len(filtered_meas)} measures"
    )
    return context


# ---------------------------------------------------------------------------
# Dataset key resolution for dimension-based fetch
# ---------------------------------------------------------------------------

def resolve_dataset_key(
    raw_dataframes: Dict[str, Any],
    table_name: str,
    group_by_column: Optional[str] = None,
    analytical_dataset_mapping: Optional[Dict[str, Any]] = None,
    chart_id: Optional[str] = None,
) -> Optional[str]:
    """Find the correct dataset key in ``raw_dataframes`` for a given table/view.

    Analytical fetch instructions store data under tagged keys such as
    ``ViewName__by_Plant`` or ``ViewName__totals``.  This helper resolves the
    correct key using multiple strategies:

    1. **Direct match** — ``table_name`` exists directly in ``raw_dataframes``
       (standard relational path, no dimension splitting).
    2. **Chart mapping** — ``analytical_dataset_mapping.charts[chart_id]``
       gives the exact dataset key.
    3. **Dimension pattern** — ``{table_name}__by_{group_by_column}`` matches
       a key in ``raw_dataframes`` (same format as analytical_fetch_plan fetch_id
       and polars_engine._resolve_table_key_for_aggregation).
    4. **Prefix fallback** — any key starting with ``{table_name}__`` (first
       match).

    Args:
        raw_dataframes: Current ``raw_dataframes`` dict from state.
        table_name: Original view/table name (e.g. ``"ZSalesView"``).
        group_by_column: Dimension column used for grouping / x-axis
            (e.g. ``"Plant"``).  May be in ``view.column`` format.
        analytical_dataset_mapping: Optional mapping from chart/metric IDs to
            dataset keys.
        chart_id: Optional chart ID for mapping lookup.

    Returns:
        The matching key in ``raw_dataframes``, or ``None`` if no match found.
    """
    if not raw_dataframes:
        return None

    # Normalise: strip view. prefix from group_by if present
    if group_by_column and "." in group_by_column:
        group_by_column = group_by_column.split(".", 1)[1]

    # 1. Direct match
    if table_name in raw_dataframes:
        return table_name

    # 2. Case-insensitive direct match
    lower_lookup = {k.lower(): k for k in raw_dataframes}
    if table_name.lower() in lower_lookup:
        return lower_lookup[table_name.lower()]

    # 3. Chart mapping
    if analytical_dataset_mapping and chart_id:
        mapped = analytical_dataset_mapping.get("charts", {}).get(chart_id)
        if mapped and mapped in raw_dataframes:
            logger.debug(f"[resolve_dataset_key] Chart mapping: chart_id={chart_id} → {mapped}")
            return mapped

    # 4. Dimension pattern: ViewName__by_GroupByCol
    if group_by_column:
        pattern_key = f"{table_name}__by_{group_by_column}"
        if pattern_key in raw_dataframes:
            logger.debug(f"[resolve_dataset_key] Dimension pattern match: {pattern_key}")
            return pattern_key
        # Case-insensitive
        if pattern_key.lower() in lower_lookup:
            return lower_lookup[pattern_key.lower()]

    # 5. Totals pattern: ViewName__totals
    totals_key = f"{table_name}__totals"
    if totals_key in raw_dataframes:
        logger.debug(f"[resolve_dataset_key] Totals pattern match: {totals_key}")
        return totals_key

    # 6. Prefix fallback: any key starting with ViewName__
    prefix = f"{table_name}__"
    for k in raw_dataframes:
        if k.startswith(prefix):
            logger.debug(f"[resolve_dataset_key] Prefix fallback: {k}")
            return k
    # Case-insensitive prefix
    prefix_lower = prefix.lower()
    for k_lower, k_orig in lower_lookup.items():
        if k_lower.startswith(prefix_lower):
            logger.debug(f"[resolve_dataset_key] Prefix fallback (case-insensitive): {k_orig}")
            return k_orig

    logger.warning(
        f"[resolve_dataset_key] No dataset found for table='{table_name}', "
        f"group_by='{group_by_column}', chart_id='{chart_id}'. "
        f"Available keys: {list(raw_dataframes.keys())[:15]}"
    )
    return None


# Keywords for revenue/business priority when LLM matching fails (shared by analytical_summary and graph)
REVENUE_PRIORITY_KEYWORDS = [
    "revenue", "profit", "income", "sales", "expense", "cost",
    "total", "net", "gross", "margin", "amount", "paid", "unpaid",
    "count", "average", "sum", "value", "actual", "units",
]


def _score_metric_for_revenue_priority(metric_name: str) -> int:
    """Score a metric name for revenue/business priority (higher = more relevant to show when LLM fails)."""
    if not metric_name or not isinstance(metric_name, str):
        return 0
    metric_lower = metric_name.lower()
    score = 0
    for keyword in REVENUE_PRIORITY_KEYWORDS:
        if keyword in metric_lower:
            if keyword in ("revenue", "profit", "income", "sales"):
                score += 10
            elif keyword in ("expense", "cost", "margin"):
                score += 8
            elif keyword in ("total", "net", "gross", "amount"):
                score += 6
            else:
                score += 3
    return score


def select_revenue_priority_metrics(
    items: List[Any],
    max_count: int = 15,
    metric_key: str = "metric",
) -> List[Any]:
    """
    When LLM matching fails, select up to max_count metrics by priority (revenue/sales/cost/key totals).
    items: list of dicts with metric_key (e.g. "metric") or list of metric name strings.
    Returns items (dicts or names) sorted by priority, capped at max_count.
    """
    if not items:
        return []
    # Normalize to (name, original_item) for sorting
    name_item_pairs: List[Tuple[str, Any]] = []
    for x in items:
        if isinstance(x, dict):
            name = (x.get(metric_key) or "").strip()
        elif isinstance(x, str):
            name = x.strip()
        else:
            continue
        if name:
            name_item_pairs.append((name, x))
    if not name_item_pairs:
        return []
    sorted_pairs = sorted(
        name_item_pairs,
        key=lambda p: _score_metric_for_revenue_priority(p[0]),
        reverse=True,
    )
    selected = [p[1] for p in sorted_pairs[:max_count]]
    return selected


# Alias for generic use (same logic, neutral name)
select_priority_metrics = select_revenue_priority_metrics


# ---------------------------------------------------------------------------
# Schema / plan column helpers (shared by chart_plan, chart_preplan, etc.)
# ---------------------------------------------------------------------------


def get_column_type(
    table_name: str,
    col_name: str,
    table_schemas: Dict[str, List[Dict[str, Any]]],
) -> str:
    """Return data type for table_name.col_name from table_schemas, or 'string' if unknown.
    table_schemas: dict mapping table name to list of {name, type, ...} column entries."""
    schema_list = table_schemas.get(table_name) if table_schemas else None
    if not schema_list:
        return "string"
    for entry in schema_list:
        if isinstance(entry, dict) and entry.get("name") == col_name:
            return entry.get("type", "string") or "string"
    return "string"


def find_date_field_in_table(table_name: str, table_data: Dict[str, Any]) -> Optional[str]:
    """
    Find a date/timestamp field in a table.
    table_data[table_name] = {"schema": {"columns": {col: {"type": ...}}}, "sample_data": [...]}.
    Returns first column that looks like date (by type or name or sample value).
    """
    if not table_name or not table_data or table_name not in table_data:
        return None
    table_info = table_data[table_name]
    schema = table_info.get("schema", {})
    columns = schema.get("columns", {})
    if isinstance(columns, dict):
        for col_name, col_info in columns.items():
            if isinstance(col_info, dict):
                col_type = (col_info.get("type") or "").lower()
                if "date" in col_type or "time" in col_type or "timestamp" in col_type or "temporal" in col_type:
                    return col_name
            col_lower = (col_name or "").lower()
            if any(k in col_lower for k in ["date", "time", "timestamp", "created", "updated", "period"]):
                return col_name
    elif isinstance(columns, list):
        for col_name in columns:
            col_lower = (str(col_name) or "").lower()
            if any(k in col_lower for k in ["date", "time", "timestamp", "created", "updated", "period"]):
                return col_name
    sample_data = table_info.get("sample_data", [])
    if sample_data and isinstance(sample_data[0], dict):
        for col_name in sample_data[0].keys():
            col_lower = (str(col_name) or "").lower()
            if any(k in col_lower for k in ["date", "time", "timestamp", "created", "updated", "period"]):
                sample_val = sample_data[0].get(col_name)
                if sample_val:
                    val_str = str(sample_val)
                    if any(p in val_str for p in ["-", "/", "T", ":"]):
                        return col_name
                    if isinstance(sample_val, (date, datetime)):
                        return col_name
    return None


def find_date_field_from_tables(
    table_names: List[str], table_data: Dict[str, Any]
) -> Optional[str]:
    """Find a date field from the first table in table_names that has one."""
    for table_name in table_names:
        field = find_date_field_in_table(table_name, table_data)
        if field:
            return field
    return None


def build_column_types_for_preplan(
    chart_preplan: List[Dict[str, Any]],
    table_schemas: Dict[str, List[Dict[str, Any]]],
    header: str = "Columns and types (use only date/datetime/timestamp columns for date range filters):",
) -> str:
    """
    Build a string listing each table.column from the preplan with its data type.
    Used so chart plan (or other) LLM applies filters by type (e.g. date filters only on date columns).
    """
    seen: Set[str] = set()
    lines: List[str] = []
    for chart in chart_preplan or []:
        if not isinstance(chart, dict):
            continue
        group_by = chart.get("group_by")
        if isinstance(group_by, str) and "." in group_by:
            if group_by not in seen:
                seen.add(group_by)
                tname, _, cname = group_by.partition(".")
                lines.append(f"  {group_by} ({get_column_type(tname, cname, table_schemas)})")
        for m in chart.get("metrics") or []:
            if not isinstance(m, dict):
                continue
            col = m.get("column")
            if isinstance(col, str) and "." in col and col not in seen:
                seen.add(col)
                tname, _, cname = col.partition(".")
                lines.append(f"  {col} ({get_column_type(tname, cname, table_schemas)})")
    if not lines:
        return ""
    return header + "\n" + "\n".join(lines)


def check_timeline_availability(
    user_query: str,
    available_date_ranges: Dict[str, Dict[str, Any]],
) -> Optional[str]:
    """
    Check if user query mentions years that are outside available data range.

    Args:
        user_query: User's query text.
        available_date_ranges: Dict of {table_name: {min_date, max_date, date_columns}}.

    Returns:
        Warning message if timeline mismatch (mentioned years not in data), None otherwise.
    """
    if not available_date_ranges or not (user_query or "").strip():
        return None
    full_year_pattern = r"\b(19\d{2}|20\d{2})\b"
    year_matches = re.findall(full_year_pattern, user_query)
    mentioned_years = {int(y) for y in year_matches}
    all_min_dates: List[date] = []
    all_max_dates: List[date] = []
    for table_name, date_info in available_date_ranges.items():
        min_date_str = date_info.get("min_date", "")
        max_date_str = date_info.get("max_date", "")
        if min_date_str and max_date_str:
            try:
                min_date_str = (min_date_str or "").replace("Z", "+00:00")
                max_date_str = (max_date_str or "").replace("Z", "+00:00")
                all_min_dates.append(datetime.fromisoformat(min_date_str).date())
                all_max_dates.append(datetime.fromisoformat(max_date_str).date())
            except Exception as e:
                logger.debug("Failed to parse date range for %s: %s", table_name, e)
                continue
    if not all_min_dates or not all_max_dates:
        return None
    overall_min = min(all_min_dates)
    overall_max = max(all_max_dates)
    available_years = set(range(overall_min.year, overall_max.year + 1))
    if not mentioned_years:
        return None
    missing_years = mentioned_years - available_years
    if not missing_years:
        return None
    year_list = sorted(available_years)
    if len(year_list) == 1:
        year_range_str = str(year_list[0])
    elif len(year_list) == 2:
        year_range_str = f"{year_list[0]} and {year_list[1]}"
    else:
        year_range_str = f"{year_list[0]} to {year_list[-1]}"
    return (
        f"I don't have data for the requested timeline. "
        f"The data I have is from {overall_min.strftime('%B %d, %Y')} to {overall_max.strftime('%B %d, %Y')} "
        f"(years {year_range_str}). "
        f"Please ask about data within this range."
    )

