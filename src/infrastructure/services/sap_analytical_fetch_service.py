"""SAP analytical view fetch: direct $select/$filter/$orderby, no $count. Paginates with $top/$skip until empty."""
import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import polars as pl

from config.settings import settings
from infrastructure.langgraph.utils.sap_fetch_helpers import (
    api_urls_to_generated_queries,
    clean_odata_select,
    extract_date_columns_from_schema,
    filter_columns_for_api_call,
    get_allowed_columns_for_view,
    normalize_odata_rows_for_polars,
    pick_date_column_and_default_range,
)

from .datasphere_service import get_datasphere_service

logger = logging.getLogger(__name__)

MAX_ROWS_PER_PAGE = settings.sap_rows_per_page
ANALYTICAL_PAGE_SIZE = getattr(settings, "sap_analytical_page_size", None) or MAX_ROWS_PER_PAGE


async def fetch_analytical_view_data(
    datasphere_service: Any,
    user_id: str,
    view_name: str,
    select: str,
    filter_expr: Optional[str],
    orderby: Optional[str],
    data_url: Optional[str],
    space_id: Optional[str],
    token: str,
    fetch_id: str,
    sap_view_schemas: Optional[Dict[str, Any]] = None,
    input_parameters: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[pl.LazyFrame], Optional[str], Dict[str, Any]]:
    """Fetch data from a SAP analytical view using $select/$filter/$orderby (no $count).
    When input_parameters is provided, the URL path includes fiscal period parameters
    (e.g. /_ViewName(Fiscal_Period1_Fisca='BT 2026001,2026011')/Set).
    Returns (lazy_frame, api_url, fetch_status).
    """
    node_name = "sap_analytical_fetch"
    select = clean_odata_select(select)

    logger.info("[%s] ═══════════════════════════════════════════════", node_name)
    logger.info("[%s] Starting analytical fetch: '%s'", node_name, fetch_id)
    logger.info("[%s]   View:    %s", node_name, view_name)
    logger.info("[%s]   Select:  %s%s", node_name, (select[:200] if select else "none"), "..." if select and len(select) > 200 else "")
    logger.info("[%s]   Filter:  %s", node_name, (filter_expr[:200] + "..." if filter_expr and len(filter_expr) > 200 else (filter_expr or "none")))
    logger.info("[%s]   OrderBy: %s", node_name, orderby or "none")
    if input_parameters:
        logger.info("[%s]   InputParams: %s", node_name, input_parameters)
    logger.info("[%s] ═══════════════════════════════════════════════", node_name)

    if not orderby and sap_view_schemas:
        date_cols = extract_date_columns_from_schema(view_name, sap_view_schemas)
        if date_cols:
            orderby = date_cols[0]
            logger.info("[%s]   Derived orderby from schema: %s", node_name, orderby)
    if orderby and select:
        select_parts = [p.strip() for p in select.split(",") if p.strip()]
        if orderby not in select_parts:
            select = clean_odata_select(f"{orderby},{select}")
            logger.info("[%s]   Added orderby column to select: %s", node_name, orderby)

    all_rows: List[Dict[str, Any]] = []
    api_url = None
    page = 0
    page_size = ANALYTICAL_PAGE_SIZE
    total_api_calls = 0

    try:
        while True:
            page += 1
            skip = (page - 1) * page_size
            logger.info("[%s]   Page %s: Fetching (top=%s, skip=%s) ...", node_name, page, f"{page_size:,}", f"{skip:,}")

            result = await datasphere_service.execute_odata_query(
                user_id=user_id,
                view_name=view_name,
                select=select,
                filter=filter_expr,
                top=page_size,
                skip=skip,
                orderby=orderby,
                data_url=data_url,
                space_id=space_id,
                token=token,
                input_parameters=input_parameters,
            )
            total_api_calls += 1
            if not api_url and result.api_url:
                api_url = result.api_url
            rows_returned = len(result.data) if result.data else 0
            logger.info("[%s]   Page %s: Got %s rows", node_name, page, f"{rows_returned:,}")

            if rows_returned == 0:
                break
            all_rows.extend(result.data)
            if rows_returned < page_size:
                break
            if page >= 100:
                logger.warning("[%s]   ⚠️ Reached 100-page cap for '%s' — stopping pagination", node_name, fetch_id)
                break

        total_rows = len(all_rows)
        logger.info("[%s] ✅ Fetch complete: '%s' — %s total rows in %s API call(s)", node_name, fetch_id, f"{total_rows:,}", total_api_calls)

        if total_rows == 0:
            fetch_status = {"planned_rows": 0, "actual_rows": 0, "failed_chunks": 0, "total_chunks": total_api_calls, "message": None}
            return pl.LazyFrame(), api_url, fetch_status

        all_rows = normalize_odata_rows_for_polars(all_rows)
        try:
            df = pl.LazyFrame(all_rows, infer_schema_length=None)
            schema_names = df.collect_schema().names()
            logger.info("[%s] ✅ Created LazyFrame: %s rows, %s columns", node_name, f"{total_rows:,}", len(schema_names))
        except Exception as e:
            try:
                df = pl.DataFrame(all_rows, infer_schema_length=None).lazy()
                schema_names = df.collect_schema().names()
                logger.info("[%s] ✅ Created LazyFrame via DataFrame fallback: %s rows, %s columns", node_name, f"{total_rows:,}", len(schema_names))
            except Exception as e2:
                try:
                    all_cols = sorted(set(k for row in all_rows if isinstance(row, dict) for k in row))
                    if not all_cols:
                        raise ValueError("No columns in rows")
                    schema_utf8 = {c: pl.Utf8 for c in all_cols}
                    str_rows = []
                    for row in all_rows:
                        if not isinstance(row, dict):
                            str_rows.append({c: None for c in all_cols})
                            continue
                        str_rows.append({c: (str(row[c]) if row.get(c) is not None else None) for c in all_cols})
                    df = pl.DataFrame(str_rows, schema=schema_utf8, orient="row").lazy()
                    logger.info("[%s] ✅ Created LazyFrame via Utf8-schema fallback: %s rows, %s columns", node_name, f"{total_rows:,}", len(all_cols))
                except Exception as e3:
                    logger.error("[%s] ❌ Failed to create LazyFrame for '%s': %s", node_name, fetch_id, e)
                    return None, api_url, {
                        "planned_rows": total_rows, "actual_rows": 0, "failed_chunks": 1, "total_chunks": total_api_calls,
                        "message": f"LazyFrame creation failed: {str(e)[:200]}",
                    }
        fetch_status = {"planned_rows": total_rows, "actual_rows": total_rows, "failed_chunks": 0, "total_chunks": total_api_calls, "message": None}
        return df, api_url, fetch_status
    except Exception as e:
        logger.error("[%s] ❌ Analytical fetch FAILED for '%s': %s", node_name, fetch_id, e)
        return None, api_url, {
            "planned_rows": 0, "actual_rows": 0, "failed_chunks": 1, "total_chunks": total_api_calls,
            "message": f"Fetch failed: {str(e)[:200]}",
        }


async def execute_analytical_fetch(
    state: Dict[str, Any],
    instructions: List[Dict[str, Any]],
    start_time: datetime,
    node_name: str,
) -> Dict[str, Any]:
    """Execute all analytical fetch instructions in parallel. Returns state update dict."""
    logger.info("[%s] ════════════════════════════════════════════════════", node_name)
    logger.info("[%s] ANALYTICAL FETCH — %s instruction(s)", node_name, len(instructions))
    logger.info("[%s] ════════════════════════════════════════════════════", node_name)

    _llm_df = state.get("analytical_date_filter")
    _instr_with_filters = sum(1 for i in instructions if i.get("filters"))
    logger.info("[%s] [DATE FILTER FLOW] instructions_with_filters=%s/%s, state.analytical_date_filter=%s", node_name, _instr_with_filters, len(instructions), "set" if (_llm_df and _llm_df.get("date_column")) else "missing")

    datasphere_service = get_datasphere_service()
    user_id = state.get("user_id", "unknown")
    token = state.get("sap_access_token")
    if not token:
        logger.error("[%s] ❌ No SAP access token found", node_name)
        return {"errors": ["No SAP access token"], "status": "error"}

    sap_datasphere_assets = state.get("sap_datasphere_assets", {})
    sap_view_schemas = state.get("sap_view_schemas", {})
    assets_dict = sap_datasphere_assets.get("assets", {}) if isinstance(sap_datasphere_assets, dict) else {}

    existing_raw = state.get("raw_dataframes", {})
    raw_dataframes = existing_raw.copy() if existing_raw else {}
    table_data: Dict[str, list] = {}
    data_fetch_status = {"by_view": {}, "has_partial_fetch": False, "total_planned_rows": 0, "total_actual_rows": 0}
    view_stats: List[Dict[str, Any]] = []
    api_urls_by_view: Dict[str, str] = {}

    for idx, instr in enumerate(instructions, 1):
        dim_label = instr.get("dimension") or "(totals)"
        logger.info("[%s]   Instruction %s/%s: fetch_id=%s, view=%s, dim=%s, measures=%s", node_name, idx, len(instructions), instr.get("fetch_id"), instr.get("source_view"), dim_label, instr.get("measures", []))

    async def _fetch_single_instruction(instr: Dict[str, Any], idx: int) -> Dict[str, Any]:
        fetch_id = instr.get("fetch_id", f"unknown_{idx}")
        source_view = instr.get("source_view", "")
        select_columns = instr.get("select_columns", [])
        filters = instr.get("filters", [])
        instr_input_params = instr.get("input_parameters")

        if not source_view:
            return {"fetch_id": fetch_id, "status": "failed", "error": "No source_view"}
        if not select_columns:
            return {"fetch_id": fetch_id, "status": "failed", "error": "No select_columns"}
        if fetch_id in raw_dataframes:
            return {"fetch_id": fetch_id, "status": "skipped"}

        asset_info = assets_dict.get(source_view)
        if asset_info is None:
            return {"fetch_id": fetch_id, "status": "failed", "error": f"Asset '{source_view}' not found"}
        if isinstance(asset_info, dict):
            data_url = asset_info.get("data_url")
            space_id = asset_info.get("space_id")
        else:
            data_url = getattr(asset_info, "data_url", None)
            space_id = getattr(asset_info, "space_id", None)
            if hasattr(asset_info, "to_dict"):
                ad = asset_info.to_dict()
                data_url = ad.get("data_url")
                space_id = ad.get("space_id")
        if not data_url:
            return {"fetch_id": fetch_id, "status": "failed", "error": "No data_url"}

        # Restrict $select to selected columns only (filtered_analytical_dimensions/measures)
        allowed_set = get_allowed_columns_for_view(state, source_view)
        if allowed_set:
            select_columns, filters = filter_columns_for_api_call(select_columns, filters, allowed_set, source_view, node_name)
            if not select_columns:
                return {"fetch_id": fetch_id, "status": "failed", "error": "No valid columns after column check"}
            logger.info("[%s] Fetching only selected columns for '%s': $select has %s column(s)", node_name, source_view, len(select_columns))
        select_str = clean_odata_select(",".join(select_columns))

        filter_parts = []
        for f in filters:
            if isinstance(f, dict):
                syntax = f.get("odata_syntax")
                if syntax:
                    filter_parts.append(syntax)
                else:
                    col, op, val = f.get("column", ""), f.get("operator", "eq"), f.get("value", "")
                    if col and val:
                        filter_parts.append(f"{col} {op} {val}")
        filter_expr = " and ".join(filter_parts) if filter_parts else None

        # When fiscal input_parameters are set, skip date filter fallback (fiscal period is the time filter)
        if not filter_expr and not instr_input_params:
            llm_date = state.get("analytical_date_filter")
            if llm_date and isinstance(llm_date, dict):
                dc = (llm_date.get("date_column") or "").strip()
                start = (llm_date.get("start_date") or "").strip()
                end = (llm_date.get("end_date") or "").strip()
                if dc and start and end:
                    filter_expr = f"{dc} ge {start} and {dc} le {end}"
            if not filter_expr:
                dc, start_ytd, end_ytd = pick_date_column_and_default_range(select_columns, source_view, sap_view_schemas)
                if dc and start_ytd and end_ytd:
                    filter_expr = f"{dc} ge {start_ytd} and {dc} le {end_ytd}"

        # If no instruction-level input_parameters, check state for fiscal filter
        if not instr_input_params:
            fiscal_filter = state.get("sap_fiscal_filter")
            if fiscal_filter and isinstance(fiscal_filter, dict) and fiscal_filter.get("input_parameters"):
                instr_input_params = fiscal_filter["input_parameters"]
                logger.info(
                    "[%s] Using state fiscal filter as input_parameters for '%s': %s",
                    node_name, fetch_id, instr_input_params,
                )

        dimension = instr.get("dimension")
        orderby = dimension

        df, url, fetch_stat = await fetch_analytical_view_data(
            datasphere_service=datasphere_service,
            user_id=user_id,
            view_name=source_view,
            select=select_str,
            filter_expr=filter_expr,
            orderby=orderby,
            data_url=data_url,
            space_id=space_id,
            token=token,
            fetch_id=fetch_id,
            sap_view_schemas=sap_view_schemas,
            input_parameters=instr_input_params,
        )
        return {"fetch_id": fetch_id, "df": df, "api_url": url, "fetch_status": fetch_stat, "status": "success" if df is not None else "failed"}

    tasks = [_fetch_single_instruction(instr, idx) for idx, instr in enumerate(instructions, 1)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for idx, result in enumerate(results, 1):
        if isinstance(result, Exception):
            fetch_id = instructions[idx - 1].get("fetch_id", f"unknown_{idx}")
            logger.error("[%s] ❌ Instruction %s ('%s') raised exception: %s", node_name, idx, fetch_id, result)
            view_stats.append({"view_name": fetch_id, "rows": 0, "columns": 0, "status": "failed (exception)"})
            continue
        fetch_id = result.get("fetch_id", f"unknown_{idx}")
        status = result.get("status", "unknown")
        if status == "skipped":
            view_stats.append({"view_name": fetch_id, "rows": 0, "columns": 0, "status": "skipped"})
            continue
        if status == "failed":
            logger.error("[%s] ❌ Instruction '%s' failed: %s", node_name, fetch_id, result.get("error", "unknown error"))
            view_stats.append({"view_name": fetch_id, "rows": 0, "columns": 0, "status": f"failed: {(result.get('error') or '')[:100]}"})
            continue
        df = result.get("df")
        api_url = result.get("api_url")
        fetch_stat = result.get("fetch_status", {})
        if df is not None:
            raw_dataframes[fetch_id] = df
            table_data[fetch_id] = []
            if api_url:
                api_urls_by_view[fetch_id] = api_url
            if fetch_stat:
                data_fetch_status["by_view"][fetch_id] = fetch_stat
                data_fetch_status["total_planned_rows"] += fetch_stat.get("planned_rows", 0)
                data_fetch_status["total_actual_rows"] += fetch_stat.get("actual_rows", 0)
            try:
                row_count = df.select(pl.len()).collect().item()
                col_count = len(df.collect_schema().names())
                view_stats.append({"view_name": fetch_id, "rows": row_count, "columns": col_count, "status": "success"})
            except Exception:
                view_stats.append({"view_name": fetch_id, "rows": 0, "columns": 0, "status": "success (stats unavailable)"})
        else:
            view_stats.append({"view_name": fetch_id, "rows": 0, "columns": 0, "status": "failed (no data)"})

    duration = (datetime.now() - start_time).total_seconds()
    logger.info("[%s] ANALYTICAL FETCH SUMMARY (%ss) — %s instructions, %s successful", node_name, f"{duration:.2f}", len(instructions), len([s for s in view_stats if s["status"] == "success"]))

    if not raw_dataframes:
        logger.error("[%s] ❌ No data fetched from any instruction", node_name)
        return {"errors": ["No data fetched from SAP analytical views"], "status": "error"}

    # Same format as SQL flow: frontend shows these as "SQL queries" (endpoint only, no base URL)
    generated_queries = api_urls_to_generated_queries(api_urls_by_view)

    out: Dict[str, Any] = {
        "raw_dataframes": raw_dataframes,
        "table_data": table_data,
        "data_fetch_status": data_fetch_status,
        "api_urls_by_view": api_urls_by_view,
        "generated_queries": generated_queries,
        "status": "data_fetched",
    }
    applied = state.get("applied_date_filters")
    if not applied or not applied.get("filter_applied"):
        llm_date = state.get("analytical_date_filter")
        if llm_date and isinstance(llm_date, dict):
            dc = (llm_date.get("date_column") or "").strip()
            start = (llm_date.get("start_date") or "").strip()
            end = (llm_date.get("end_date") or "").strip()
            if dc and start and end:
                from datetime import date as date_type
                try:
                    today = date_type.today()
                    yyyy = str(today.year)
                    time_period_description = f"YTD ({start} to {end})" if (start == f"{yyyy}-01-01" and end == today.isoformat()) else f"{start} to {end}"
                except Exception:
                    time_period_description = f"{start} to {end}"
                out["applied_date_filters"] = {
                    "filter_applied": True,
                    "date_range": {"start_date": start, "end_date": end, "date_column": dc},
                    "filter_source": "sap_data_fetch",
                    "time_period_description": time_period_description,
                }
    return out
