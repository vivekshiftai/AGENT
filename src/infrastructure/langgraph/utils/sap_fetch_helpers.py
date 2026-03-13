"""Shared helpers for SAP relational and analytical data fetch (OData $select, filters, date columns, fiscal periods)."""
import json
import logging
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


def strip_base_url_from_api_url(full_url: Optional[str]) -> str:
    """Return only the API endpoint (path + query string), without scheme or host.
    Used so frontend receives 'SQL queries' as a list of OData endpoints without the base URL.
    """
    if not full_url or not isinstance(full_url, str) or not full_url.strip():
        return ""
    try:
        parsed = urlparse(full_url.strip())
        path = parsed.path or ""
        query = ("?" + parsed.query) if parsed.query else ""
        return path + query
    except Exception:
        return full_url.strip()


def api_urls_to_generated_queries(api_urls_by_view: Dict[str, str]) -> str:
    """Build generated_queries JSON (same format as SQL flow) from SAP api_urls_by_view.
    Each entry is the API endpoint only (path + query), no base URL.
    Frontend can show these as 'SQL queries' for SAP flow.
    """
    if not api_urls_by_view:
        return json.dumps({"queries": []})
    endpoints = []
    for _key, full_url in api_urls_by_view.items():
        endpoint = strip_base_url_from_api_url(full_url)
        if endpoint:
            endpoints.append(endpoint)
    return json.dumps({"queries": endpoints})


def clean_odata_select(select_str: Optional[str]) -> Optional[str]:
    """Clean $select for OData API: comma-separated list only, no spaces.
    SAP expects ODataIdentifier tokens; spaces after commas break the URI parser.
    """
    if not select_str or not isinstance(select_str, str):
        return select_str
    parts = [p.strip() for p in select_str.split(",") if p.strip()]
    if not parts:
        return None
    return ",".join(parts)


def normalize_odata_rows_for_polars(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Normalize OData row dicts so Polars can build a schema without mixed-type errors.
    Replaces string 'null'/'NULL' and '' with None. Returns a new list; does not mutate input.
    """
    if not rows:
        return rows
    normalized = []
    for row in rows:
        if not isinstance(row, dict):
            normalized.append(row)
            continue
        out = {}
        for k, v in row.items():
            if isinstance(v, str):
                out[k] = None if (v.strip().lower() == "null" or v == "") else v
            else:
                out[k] = v
        normalized.append(out)
    return normalized


def get_allowed_columns_for_view(state: Dict[str, Any], view_name: str) -> Optional[Set[str]]:
    """Build the set of allowed column names for a view before API call.
    Uses filtered_analytical_dimensions/measures when present (by view_name), else sap_view_schemas.
    Returns None if no allowed list can be built (caller may skip filtering).
    """
    filtered_dims = state.get("filtered_analytical_dimensions", []) or []
    filtered_meas = state.get("filtered_analytical_measures", []) or []
    if filtered_dims or filtered_meas:
        allowed = set()
        for d in filtered_dims:
            name = d.get("name", "")
            v = d.get("view_name")
            if name and (v is None or v == view_name):
                allowed.add(name)
        for m in filtered_meas:
            name = m.get("name", "")
            v = m.get("view_name")
            if name and (v is None or v == view_name):
                allowed.add(name)
        if allowed:
            return allowed
    schemas = state.get("sap_view_schemas", {}) or {}
    schema_info = schemas.get(view_name) if isinstance(schemas, dict) else None
    if isinstance(schema_info, dict):
        cols = schema_info.get("columns", [])
        names = set()
        for c in cols:
            if isinstance(c, dict) and c.get("name"):
                names.add(c["name"])
        if names:
            return names
    return None


def filter_columns_for_api_call(
    select_columns: List[str],
    filters: List[Dict[str, Any]],
    allowed_set: Set[str],
    view_name: str,
    node_name: str,
) -> Tuple[List[str], List[Dict[str, Any]]]:
    """Keep only select_columns and filter columns that are in allowed_set; remove the rest.
    Returns (filtered_select_columns, filtered_filters). Logs removed columns.
    """
    allowed_lower = {c.lower(): c for c in allowed_set}
    kept_select = []
    removed_select = []
    for col in select_columns:
        if not col:
            continue
        key = col.strip().lower()
        if key in allowed_lower:
            kept_select.append(allowed_lower[key])
        else:
            removed_select.append(col)
    kept_filters = []
    removed_filter_cols = []
    for f in filters:
        if not isinstance(f, dict):
            continue
        col = (f.get("column") or "").strip()
        if not col:
            kept_filters.append(f)
            continue
        key = col.lower()
        if key in allowed_lower:
            kept = dict(f)
            kept["column"] = allowed_lower[key]
            kept_filters.append(kept)
        else:
            removed_filter_cols.append(col)
    if removed_select or removed_filter_cols:
        logger.info(
            "[%s] Column check for '%s': removed from $select: %s; removed filter columns: %s",
            node_name, view_name, removed_select or "none", removed_filter_cols or "none",
        )
    return kept_select, kept_filters


def date_like_column_name(col: str) -> bool:
    """Return True if column name looks like a date/fiscal column (for default filter)."""
    if not col or not isinstance(col, str):
        return False
    c = col.lower().strip()
    if not c:
        return False
    if c in ("_0calday", "calendar_day", "calendar_day_date", "posting_date", "created on", "created_on"):
        return True
    if "fiscal" in c or "fiscper" in c:
        return True
    if "calendar" in c or "calday" in c:
        return True
    if "date" in c and ("posting" in c or "created" in c or "calendar" in c or "order" in c):
        return True
    if ("year" in c or "month" in c) and ("fiscal" in c or "calendar" in c):
        return True
    return False


def _column_type_in_schema(view_name: str, column_name: str, schemas: Optional[Dict[str, Any]]) -> Optional[str]:
    """Return the schema type (e.g. Edm.Date, Edm.Int64) for a column in a view, or None."""
    if not schemas or not column_name:
        return None
    schema_info = schemas.get(view_name)
    if not schema_info or not isinstance(schema_info, dict):
        return None
    for col in schema_info.get("columns", []) or []:
        if isinstance(col, dict) and (col.get("name") or "").strip() == column_name.strip():
            return (col.get("type") or col.get("data_type") or "").strip()
    return None


def pick_date_column_and_default_range(
    select_columns: List[str],
    view_name: str,
    schemas: Optional[Dict[str, Any]],
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Pick a date column for default $filter only when it is Edm.Date in schema.
    Do NOT use fiscal/Int64 columns with date literals (they need input_parameters / sap_fiscal_filter).
    Returns (date_column, start_date, end_date) with ISO dates, or (None, None, None) if no suitable column.
    """
    today = datetime.now().date()
    start_ytd = today.replace(month=1, day=1)
    start_str = start_ytd.isoformat()
    end_str = today.isoformat()

    # Only consider columns that are actually Edm.Date in schema (never Edm.Int64 fiscal)
    date_cols = extract_date_columns_from_schema(view_name, schemas) if schemas else []
    if not date_cols:
        return (None, None, None)

    # Prefer a date column that is in the select list
    for col in (select_columns or []):
        col_type = _column_type_in_schema(view_name, col, schemas)
        if col_type and "Edm.Date" in col_type and col in date_cols:
            logger.info(
                "[sap_analytical_fetch] Using date column from select (Edm.Date) for default filter: '%s' (range %s to %s)",
                col, start_str, end_str,
            )
            return (col, start_str, end_str)
    for col in date_cols:
        if col in (select_columns or []) or not select_columns:
            logger.info(
                "[sap_analytical_fetch] Using date column from schema for default filter: '%s' (range %s to %s)",
                col, start_str, end_str,
            )
            return (col, start_str, end_str)
    col = date_cols[0]
    logger.info(
        "[sap_analytical_fetch] Using first schema date column for default filter: '%s' (range %s to %s)",
        col, start_str, end_str,
    )
    return (col, start_str, end_str)


def extract_date_columns_from_schema(view_name: str, schemas: Dict[str, Any]) -> List[str]:
    """Extract date columns from schema for a view by checking for Edm.Date type."""
    if not schemas:
        return []
    schema_info = schemas.get(view_name)
    if not schema_info or not isinstance(schema_info, dict):
        return []
    schema_columns = schema_info.get("columns", [])
    date_columns = []
    for col in schema_columns:
        if isinstance(col, dict):
            col_name = col.get("name", "")
            col_type = col.get("type", "") or col.get("data_type", "")
            if col_name and col_type and "Edm.Date" in str(col_type):
                date_columns.append(col_name)
                logger.debug("[SAP Fetch] Found date column '%s' (type: %s) in view '%s'", col_name, col_type, view_name)
    if date_columns:
        logger.info("[SAP Fetch] View '%s': Found %s date column(s): %s", view_name, len(date_columns), date_columns)
    else:
        logger.warning("[SAP Fetch] View '%s': No date columns found in schema", view_name)
    return date_columns


def extract_date_columns_by_view(schemas: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    Extract date column names per view from schema dict (Edm.Date).
    Returns Dict mapping view_name to list of date column names; only includes views that have date columns.
    """
    out: Dict[str, List[str]] = {}
    if not schemas:
        return out
    for view_name, schema_info in schemas.items():
        if not isinstance(schema_info, dict):
            continue
        date_cols: List[str] = []
        for col in schema_info.get("columns", []) or []:
            if isinstance(col, dict):
                name = col.get("name", "")
                typ = (col.get("type") or col.get("data_type") or "").strip()
                if name and "Edm.Date" in typ:
                    date_cols.append(name)
        if date_cols:
            out[view_name] = date_cols
    return out


_FISCAL_COL_NAMES = {
    "Fiscal_Qrtr_Fisca",
    "Fiscal_Week_Fisca",
    "Fiscal_Year_Fisca",
    "Fiscal_Hier_Key",
    "Fiscal_Period1_Fisca",
}

_FISCAL_NAME_KEYWORDS = {"fiscal", "fiscper", "fisc"}


def is_fiscal_column(col_name: str, col_type: str = "") -> bool:
    """Return True if the column is a fiscal time column (Int64 with fiscal-related name)."""
    if not col_name:
        return False
    name_lower = col_name.lower()
    if col_name in _FISCAL_COL_NAMES:
        return True
    type_ok = not col_type or "Int" in col_type
    return type_ok and any(kw in name_lower for kw in _FISCAL_NAME_KEYWORDS)


def extract_fiscal_columns_from_schema(view_name: str, schemas: Dict[str, Any]) -> List[Dict[str, str]]:
    """Extract fiscal columns (Edm.Int64 with fiscal-related names) from schema for a view.
    Returns list of dicts with 'name' and 'type' keys.
    """
    if not schemas:
        return []
    schema_info = schemas.get(view_name)
    if not schema_info or not isinstance(schema_info, dict):
        return []
    result: List[Dict[str, str]] = []
    for col in schema_info.get("columns", []):
        if not isinstance(col, dict):
            continue
        col_name = col.get("name", "")
        col_type = col.get("type", "") or col.get("data_type", "")
        if col_name and is_fiscal_column(col_name, col_type):
            result.append({"name": col_name, "type": col_type})
    if result:
        logger.info("[SAP Fetch] View '%s': Found %s fiscal column(s): %s",
                     view_name, len(result), [c["name"] for c in result])
    return result


def extract_fiscal_columns_by_view(schemas: Dict[str, Any]) -> Dict[str, List[Dict[str, str]]]:
    """Extract fiscal column dicts per view from schema dict.
    Returns Dict mapping view_name → list of {name, type}.
    Only includes views that have fiscal columns.
    """
    out: Dict[str, List[Dict[str, str]]] = {}
    if not schemas:
        return out
    for view_name in schemas:
        cols = extract_fiscal_columns_from_schema(view_name, schemas)
        if cols:
            out[view_name] = cols
    return out


def compute_fiscal_value(year: int, period: int) -> int:
    """Compute fiscal period integer value: year * 1000 + period (e.g. 2026011 = year 2026, week 11)."""
    return year * 1000 + period


def date_to_fiscal_week(d: date) -> Tuple[int, int]:
    """Convert a date to (fiscal_year, fiscal_week). Uses ISO week numbering."""
    iso_year, iso_week, _ = d.isocalendar()
    return iso_year, iso_week


def date_to_fiscal_period(d: date) -> Tuple[int, int]:
    """Convert a calendar date to (fiscal_year, fiscal_period).
    Fiscal year runs April–March and is named by the ending March year.
    FY2026 = April 2025 to March 2026.  Period 1 = April, period 12 = March.
    """
    y, m = d.year, d.month
    if m >= 4:
        return y + 1, m - 3
    else:
        return y, m + 9


def date_to_fiscal_quarter(d: date) -> Tuple[int, int]:
    """Convert a calendar date to (fiscal_year, fiscal_quarter).
    Uses April–March fiscal year (named by ending March year).
    Q1 = periods 1–3 (Apr–Jun), Q2 = 4–6 (Jul–Sep), Q3 = 7–9 (Oct–Dec), Q4 = 10–12 (Jan–Mar).
    """
    fy, fp = date_to_fiscal_period(d)
    return fy, (fp - 1) // 3 + 1


def compute_fiscal_range_for_dates(
    start_date: str,
    end_date: str,
    fiscal_granularity: str = "period",
) -> Tuple[int, int]:
    """Compute fiscal start and end integer values from ISO date strings.

    Args:
        start_date: ISO date string (YYYY-MM-DD)
        end_date: ISO date string (YYYY-MM-DD)
        fiscal_granularity: 'week', 'period' (month), 'quarter', or 'year'

    Returns:
        (start_value, end_value) as integers (e.g. 2026001, 2026011)
    """
    try:
        sd = date.fromisoformat(start_date)
        ed = date.fromisoformat(end_date)
    except (ValueError, TypeError):
        today = date.today()
        sd = today.replace(month=1, day=1)
        ed = today

    if fiscal_granularity == "year":
        return sd.year, ed.year

    if fiscal_granularity == "quarter":
        sy, sq = date_to_fiscal_quarter(sd)
        ey, eq = date_to_fiscal_quarter(ed)
        return compute_fiscal_value(sy, sq), compute_fiscal_value(ey, eq)

    if fiscal_granularity == "period":
        sy, sp = date_to_fiscal_period(sd)
        ey, ep = date_to_fiscal_period(ed)
        return compute_fiscal_value(sy, sp), compute_fiscal_value(ey, ep)

    # Default: week
    sy, sw = date_to_fiscal_week(sd)
    ey, ew = date_to_fiscal_week(ed)
    return compute_fiscal_value(sy, sw), compute_fiscal_value(ey, ew)


def compute_ytd_fiscal_range(fiscal_granularity: str = "period") -> Tuple[int, int]:
    """Compute fiscal YTD range (fiscal year period 1 to current fiscal period).
    Fiscal year runs April–March, named by ending March year.
    E.g. today = 2026-03-04 → FY2026, period 12 → YTD = 2026001 to 2026012.
    """
    today = date.today()
    fy, fp = date_to_fiscal_period(today)

    if fiscal_granularity == "year":
        return fy, fy

    if fiscal_granularity == "quarter":
        fq = (fp - 1) // 3 + 1
        return compute_fiscal_value(fy, 1), compute_fiscal_value(fy, fq)

    # Default: period (month)
    return compute_fiscal_value(fy, 1), compute_fiscal_value(fy, fp)


def choose_fiscal_granularity(user_query: str, parsed_intent: Optional[Dict[str, Any]] = None) -> str:
    """Determine fiscal granularity from user query and intent.
    Returns 'period' (default — monthly), 'quarter', or 'year'.
    Since Fiscal_Period1_Fisca is the only supported input parameter and it
    represents monthly periods, the default granularity is 'period'.
    """
    q = (user_query or "").lower()
    intent_text = ""
    if isinstance(parsed_intent, dict):
        intent_text = (parsed_intent.get("intent_explanation") or "")[:500].lower()
    combined = q + " " + intent_text

    if any(kw in combined for kw in ("year over year", "yoy", "yearly", "annual", "years comparison", "year comparison", "fiscal year")):
        return "year"
    if any(kw in combined for kw in ("quarter", "quarterly", "q1", "q2", "q3", "q4")):
        return "quarter"
    return "period"


def pick_fiscal_input_parameter_column(fiscal_cols: List[Dict[str, str]], granularity: str = "week") -> Optional[str]:
    """Pick the best fiscal column to use as the URL input parameter based on granularity.
    Prefers Fiscal_Period1_Fisca for week/period, Fiscal_Year_Fisca for year, Fiscal_Qrtr_Fisca for quarter.
    """
    if not fiscal_cols:
        return None
    names = {c["name"] for c in fiscal_cols}

    if granularity == "year" and "Fiscal_Year_Fisca" in names:
        return "Fiscal_Year_Fisca"
    if granularity == "quarter" and "Fiscal_Qrtr_Fisca" in names:
        return "Fiscal_Qrtr_Fisca"
    if "Fiscal_Period1_Fisca" in names:
        return "Fiscal_Period1_Fisca"
    if "Fiscal_Week_Fisca" in names:
        return "Fiscal_Week_Fisca"
    return fiscal_cols[0]["name"]


SAP_FISCAL_INPUT_PARAMETER = "Fiscal_Period1_Fisca"


def build_fiscal_input_parameters(
    fiscal_column: str,
    start_value: int,
    end_value: int,
) -> Dict[str, str]:
    """Build SAP input_parameters dict for fiscal period filtering.
    Uses SAP 'BT' (Between) operator for ranges and 'EQ' for single periods:
      - Single period (start_value == end_value): {'Fiscal_Period1_Fisca': 'EQ 2026003'}
      - Range: {'Fiscal_Period1_Fisca': 'BT 2026001,2026010'}

    Always uses Fiscal_Period1_Fisca as the URL parameter key regardless of
    which fiscal column is used for data analysis — SAP analytical views only
    support Fiscal_Period1_Fisca as an input parameter.
    """
    if start_value == end_value:
        return {SAP_FISCAL_INPUT_PARAMETER: f"EQ {start_value}"}
    return {SAP_FISCAL_INPUT_PARAMETER: f"BT {start_value},{end_value}"}


def build_date_filter_expression(
    date_filters: List[Dict[str, Any]],
    view_name: str,
    node_name: str,
) -> Optional[str]:
    """Build OData filter expression from date filters, grouping ranges with OR and conditions with AND."""
    if not date_filters:
        return None
    filters_by_column: Dict[str, List[Dict[str, Any]]] = {}
    for filter_dict in date_filters:
        column = filter_dict.get("column", "")
        if column:
            filters_by_column.setdefault(column, []).append(filter_dict)

    column_expressions = []
    has_multiple_columns = len(filters_by_column) > 1

    for column, column_filters in filters_by_column.items():
        filter_conditions = []
        for filter_dict in column_filters:
            odata_syntax = filter_dict.get("odata_syntax")
            if odata_syntax:
                filter_conditions.append(odata_syntax)
            else:
                operator = filter_dict.get("operator", "=")
                value = filter_dict.get("value", "")
                op_map = {"=": "eq", "!=": "ne", "<>": "ne", ">": "gt", ">=": "ge", "<": "lt", "<=": "le"}
                odata_op = op_map.get(operator.lower(), "eq")
                date_pattern = r"^\d{4}-\d{2}-\d{2}"
                if isinstance(value, str) and re.match(date_pattern, value):
                    filter_conditions.append(f"{column} {odata_op} {value}")
                elif isinstance(value, (int, float)):
                    filter_conditions.append(f"{column} {odata_op} {value}")
                else:
                    filter_conditions.append(f"{column} {odata_op} '{value}'")

        if not filter_conditions:
            continue

        range_expressions = []
        i = 0
        while i < len(filter_conditions):
            condition = filter_conditions[i]
            ge_match = re.search(rf"{re.escape(column)}\s+ge\s+([^\s]+)", condition)
            if ge_match:
                ge_value = ge_match.group(1).strip("'\"")
                range_start = condition
                range_end = None
                for j in range(i + 1, len(filter_conditions)):
                    le_match = re.search(rf"{re.escape(column)}\s+le\s+([^\s]+)", filter_conditions[j])
                    if le_match:
                        le_value = le_match.group(1).strip("'\"")
                        if le_value >= ge_value:
                            range_end = filter_conditions[j]
                            i = j + 1
                            break
                if range_end:
                    range_expressions.append(f"({range_start} and {range_end})")
                else:
                    range_expressions.append(condition)
                    i += 1
            else:
                le_match = re.search(rf"{re.escape(column)}\s+le\s+([^\s]+)", condition)
                if le_match:
                    range_expressions.append(condition)
                else:
                    range_expressions.append(condition)
                i += 1

        if len(range_expressions) > 1:
            column_expr = " or ".join(range_expressions)
            column_expressions.append(f"({column_expr})" if has_multiple_columns else column_expr)
        elif len(range_expressions) == 1:
            column_expressions.append(range_expressions[0])

    if column_expressions:
        result = " and ".join(column_expressions)
        logger.info(
            "[%s] Built date filter for '%s': %s",
            node_name, view_name, result[:300] + "..." if len(result) > 300 else result,
        )
        return result
    return None
