"""Analytical summary node – LLM-assisted summarizer for production planning analysis (Pipeline A)."""
from typing import Dict, Any, List, Optional, Tuple
import asyncio
import logging
from datetime import datetime

from ...llm.azure_openai import AzureOpenAIClient
from ..state import AnalyticsState
from ..prompts import (
    ANALYTICAL_OVERALL_SUMMARY_SYSTEM_PROMPT,
    ANALYTICAL_GROUP_SUMMARY_SYSTEM_PROMPT,
    get_analytical_overall_summary_user_prompt,
    get_analytical_group_summary_user_prompt,
    SIMPLE_FLOW_SUMMARY_SYSTEM_PROMPT,
    get_simple_flow_summary_user_prompt,
    SIMPLE_FLOW_NO_DATA_AGENT_SYSTEM_PROMPT,
    get_simple_flow_no_data_agent_user_prompt,
)
from ..utils import (
    check_timeline_availability,
    extract_date_filters_from_state,
    extract_chart_data_for_llm,
    parse_json_response_required_dict,
    save_llm_call_input,
    save_llm_call_output,
    truncate_metrics_by_priority_for_prompt,
    truncate_charts_for_prompt,
    validate_prompt_tokens,
    DEFAULT_MAX_INPUT_PROMPT_TOKENS,
)
from config.settings import settings

logger = logging.getLogger(__name__)

_VALID_METRIC_STATUS = ("completed", "computed")

_PRODUCTION_SUGGESTED_QUERIES = [
    "Which machines have the lowest utilization this week?",
    "Show me all delayed production orders",
    "What are the current bottlenecks in the schedule?",
    "Compare cycle times across work centers",
    "What is the on-time delivery rate for the last month?",
    "Show throughput trends by production line",
]


def _filter_metrics_to_lightweight(
    all_metrics: List[Dict[str, Any]], node_name: str
) -> List[Dict[str, Any]]:
    """Filter metrics to lightweight scalars (metric + value only) for the LLM prompt."""
    lightweight: List[Dict[str, Any]] = []
    skipped_non_completed = 0
    skipped_complex = 0
    skipped_missing = 0

    for result in all_metrics:
        if not isinstance(result, dict):
            continue
        if result.get("value") is None:
            skipped_missing += 1
            continue
        status = (result.get("status") or "").strip().lower()
        if status and status not in _VALID_METRIC_STATUS:
            skipped_non_completed += 1
            continue
        value = result.get("value")
        if isinstance(value, (list, dict)):
            skipped_complex += 1
            continue
        lightweight.append({"metric": result.get("metric"), "value": value})

    if all_metrics and not lightweight:
        for result in all_metrics:
            if (
                isinstance(result, dict)
                and result.get("value") is not None
                and result.get("metric") is not None
                and not isinstance(result.get("value"), (list, dict))
            ):
                lightweight.append({"metric": result["metric"], "value": result["value"]})
        if lightweight:
            logger.info(f"[{node_name}] Relaxed metric filter: kept {len(lightweight)} metrics")

    logger.info(
        f"[{node_name}] Metric filter: total={len(all_metrics)}, kept={len(lightweight)}, "
        f"skipped(status={skipped_non_completed}, complex={skipped_complex}, missing={skipped_missing})"
    )
    return lightweight


def _extract_gantt_summary(gantt_data: Any) -> Dict[str, Any]:
    """Extract high-level machine/job counts from gantt_data.

    Supports both formats:
    - ``{"machines": [{"machineId": ..., "jobs": [...]}], ...}`` (primary)
    - ``{"charts": [{"machines": [...]}, ...], "machines": [...]}`` (multiple Gantt charts by measure)
    """
    summary: Dict[str, Any] = {"available": False}
    if not gantt_data or not isinstance(gantt_data, dict):
        return summary

    try:
        machines_list = gantt_data.get("machines") or []
        if not machines_list and gantt_data.get("charts"):
            # Use first chart's machines for summary
            first = gantt_data["charts"][0] if gantt_data["charts"] else None
            if isinstance(first, dict):
                machines_list = first.get("machines") or []
        if not machines_list:
            logger.debug("[analytical_summary] gantt_data present but no machines found")
            return summary

        total_jobs = 0
        machine_ids: list = []
        for machine in machines_list:
            if not isinstance(machine, dict):
                continue
            mid = machine.get("machineId", "Unknown")
            machine_ids.append(mid)
            jobs = machine.get("jobs") or []
            total_jobs += len(jobs)

        summary = {
            "available": True,
            "unique_machines": len(machine_ids),
            "total_jobs": total_jobs,
            "machine_ids": machine_ids[:20],
            "plan_reason": gantt_data.get("plan_reason") or "",
            "chart_plan": gantt_data.get("chart_plan") or [],
        }
        logger.info(
            f"[analytical_summary] Gantt data: {len(machine_ids)} machines, "
            f"{total_jobs} jobs"
        )
    except Exception as e:
        logger.warning(f"[analytical_summary] Failed to parse gantt_data: {e}")
    return summary


def _build_gantt_context_text(gantt_summary: Dict[str, Any]) -> str:
    """Build a human-readable snippet about Gantt data for inclusion in the summary."""
    if not gantt_summary.get("available"):
        return ""
    machines = gantt_summary.get("unique_machines", 0)
    jobs = gantt_summary.get("total_jobs", 0)
    machine_ids = gantt_summary.get("machine_ids") or []
    parts = [
        f"Gantt schedule data: {jobs} jobs across {machines} machines."
    ]
    plan_reason = (gantt_summary.get("plan_reason") or "").strip()
    if plan_reason:
        parts.append(f"Plan reason for this time period: {plan_reason}")
    chart_plan = gantt_summary.get("chart_plan") or []
    if chart_plan:
        views = [c.get("title") or c.get("id", "") for c in chart_plan if isinstance(c, dict)]
        if views:
            parts.append(f"Available views (all in sync): {', '.join(views)}.")
    if machine_ids:
        display_ids = machine_ids[:5]
        extra = f" (+{len(machine_ids) - 5} more)" if len(machine_ids) > 5 else ""
        parts.append(f"Machines: {', '.join(str(m) for m in display_ids)}{extra}.")
    return " ".join(parts)


def _select_production_priority_metrics(
    metrics: List[Dict[str, Any]], max_count: int = 15, metric_key: str = "metric",
) -> List[Dict[str, Any]]:
    """Select and rank metrics by production-planning relevance."""
    priority_keywords = [
        "utilization", "capacity", "oee", "throughput", "cycle_time", "cycle time",
        "lead_time", "lead time", "on_time", "on-time", "delay", "bottleneck",
        "downtime", "setup_time", "setup time", "scrap", "yield", "efficiency",
        "backlog", "wip", "work_in_progress", "schedule_adherence", "adherence",
        "makespan", "takt", "availability", "performance", "quality",
    ]

    def _score(m: Dict[str, Any]) -> int:
        name = str(m.get(metric_key, "")).lower()
        for i, kw in enumerate(priority_keywords):
            if kw in name:
                return i
        return len(priority_keywords) + 1

    ranked = sorted(metrics, key=_score)
    return ranked[:max_count]


def _build_analysis_summary_output(
    summary_text: str,
    summaries_by_group: Optional[List[Dict[str, Any]]],
    metrics_to_display: List[str],
    date_filter_info: Optional[Dict[str, Any]],
    data_fetch_status: Optional[Dict[str, Any]],
    confidence: str = "medium",
    confidence_reason: str = "",
    suggested_queries: Optional[List[str]] = None,
    gantt_context: str = "",
) -> Dict[str, Any]:
    """Build the analysis_summary dict with production context, date_filter, and data-issue notes."""
    effective_text = summary_text or "Production planning analysis completed based on the provided data."

    if gantt_context and gantt_context not in effective_text:
        effective_text = f"{effective_text.rstrip()}\n\n**Schedule Overview:** {gantt_context}"

    analysis_summary: Dict[str, Any] = {
        "summary_text": effective_text,
        "confidence": confidence,
        "confidence_reason": confidence_reason,
        "metrics_to_display": metrics_to_display or [],
    }
    if summaries_by_group:
        analysis_summary["summaries_by_group"] = summaries_by_group

    if suggested_queries:
        analysis_summary["suggested_queries"] = suggested_queries

    if date_filter_info and date_filter_info.get("filter_applied"):
        dr = date_filter_info.get("date_range") or {}
        analysis_summary["date_filter"] = {
            "applied": True,
            "start_date": dr.get("start_date", ""),
            "end_date": dr.get("end_date", ""),
            "date_column": dr.get("date_column", "date"),
            "source": date_filter_info.get("filter_source", "unknown"),
        }
    else:
        analysis_summary["date_filter"] = {"applied": False}

    _has_issues = bool(
        data_fetch_status
        and (
            data_fetch_status.get("has_partial_fetch")
            or any(v.get("message") for v in (data_fetch_status.get("by_view") or {}).values())
        )
    )
    if _has_issues:
        data_note = (
            "\n\n**Note:** This analysis is based on the data we were able to retrieve; "
            "some data may be missing (e.g. partial fetch from source)."
        )
        st = (analysis_summary.get("summary_text") or "").rstrip()
        if st and "some data may be missing" not in st and "partial fetch" not in st:
            analysis_summary["summary_text"] = st + data_note

    return analysis_summary


def _normalize_llm_summary_response(response: str, node_name: str) -> Dict[str, Any]:
    """Parse LLM response and normalize to a dict with summary_text, confidence, confidence_reason."""
    parsed = parse_json_response_required_dict(
        response or "", node_name=node_name, extract_from_list=True
    )
    if not parsed:
        return {
            "summary_text": "Production planning analysis completed based on the provided data.",
            "confidence": "low",
            "confidence_reason": "LLM response could not be parsed as JSON.",
        }

    if isinstance(parsed, list):
        if parsed and isinstance(parsed[0], dict) and ("label" in parsed[0] or "value" in parsed[0]):
            parts = [f"{it.get('label','')}: {it.get('value','')}" for it in parsed[:5]
                     if isinstance(it, dict) and it.get("label") and it.get("value")]
            text = "Based on the production analysis: " + "; ".join(parts) if parts else "Analysis completed."
            return {"summary_text": text, "confidence": "medium", "confidence_reason": "Converted list of insights."}
        merged: Dict[str, Any] = {}
        for item in parsed:
            if isinstance(item, dict):
                merged.update(item)
        parsed = merged

    if not isinstance(parsed, dict):
        return {
            "summary_text": "Production planning analysis completed based on the provided data.",
            "confidence": "low",
            "confidence_reason": "Unexpected response type.",
        }

    if not parsed.get("summary_text"):
        parsed["summary_text"] = (
            parsed.get("summary")
            or parsed.get("executive_summary")
            or "Production planning analysis completed based on the provided data."
        )
    parsed.setdefault("confidence", "medium")
    parsed.setdefault("confidence_reason", "")
    return parsed


async def _call_no_data_agent_llm(
    state: AnalyticsState,
    model: str,
    node_name: str,
    column_names: List[str],
    zero_rows: bool,
) -> Optional[Dict[str, Any]]:
    """Call the no-data agent LLM and return analysis_summary dict or None on failure."""
    user_query = state.get("user_query", "")
    parsed_intent = state.get("parsed_intent") or {}
    intent_explanation = parsed_intent.get("intent_explanation") if isinstance(parsed_intent, dict) else None
    user_prompt = get_simple_flow_no_data_agent_user_prompt(
        user_query=user_query,
        column_names=column_names,
        intent_explanation=intent_explanation,
        zero_rows=zero_rows,
    )
    llm_client = state.get("llm_client") or AzureOpenAIClient()
    query_id = state.get("query_id")
    try:
        response = await llm_client._call_llm_unified(
            model=model,
            system_prompt=SIMPLE_FLOW_NO_DATA_AGENT_SYSTEM_PROMPT,
            user_prompt=user_prompt,
            node_name=node_name,
            query_id=query_id,
            temperature=0.5,
            use_json_mode=True,
        )
        analysis_summary = parse_json_response_required_dict(
            response or "", node_name=node_name, extract_from_list=True
        )
        if analysis_summary and analysis_summary.get("summary_text"):
            return {
                "summary_text": (analysis_summary.get("summary_text") or "").strip(),
                "confidence": analysis_summary.get("confidence", "low"),
                "confidence_reason": analysis_summary.get("confidence_reason") or "No data, agent response",
            }
    except Exception as e:
        logger.warning(f"[{node_name}] No-data agent LLM failed: {e}")
    return None


async def analytical_summary_node(state: AnalyticsState, model: str = None) -> Dict[str, Any]:
    """Generate production planning summary using LLM.

    Summarises machine utilization, production order status, bottlenecks,
    schedule adherence, throughput, and cycle times.  Reads ``gantt_data``
    from state (when available) to reference machine/job counts.

    Two summary strategies:
    - **Overall / single-category**: Uses the primary model with all metrics + charts.
    - **Per-group (multiple categories)**: Uses a lighter model per category, primary
      model for the "other" bucket.

    Returns dict with analysis_summary, suggested_metrics, status.
    """
    start_time = datetime.now()
    node_name = "analytical_summary"

    from ..node_timing_registry import get_node_timing_registry
    registry = get_node_timing_registry()

    if registry:
        if not registry.try_start_node(node_name, start_time):
            logger.info(f"[{node_name}] Skipping duplicate invocation (already completed or in progress)")
            existing_summary = state.get("analysis_summary") or {}
            existing_metrics = state.get("suggested_metrics") or []
            if existing_summary or existing_metrics:
                return {
                    "analysis_summary": existing_summary,
                    "suggested_metrics": existing_metrics,
                }
            return {}

    logger.info(f"[{node_name}] Starting production planning summary node")

    # Fallback duplicate detection
    if state.get("analysis_summary") and isinstance(state.get("analysis_summary"), dict):
        existing_summary = state.get("analysis_summary", {})
        if existing_summary.get("summary_text") or existing_summary.get("suggested_metrics"):
            logger.warning(f"[{node_name}] Analytical summary already completed (state check) – skipping")
            return {
                "analysis_summary": existing_summary,
                "suggested_metrics": state.get("suggested_metrics", []),
            }

    if state.get("errors"):
        logger.warning(f"[{node_name}] Errors detected in state – skipping analytical summary")
        return {}

    # --- Read gantt_data from state and build context ---
    gantt_data = state.get("gantt_data")
    gantt_summary = _extract_gantt_summary(gantt_data)
    gantt_context = _build_gantt_context_text(gantt_summary)
    if gantt_summary.get("available"):
        logger.info(
            f"[{node_name}] Gantt context built: {gantt_summary.get('unique_machines', 0)} machines, "
            f"{gantt_summary.get('unique_jobs', 0)} jobs"
        )
    else:
        logger.info(f"[{node_name}] No gantt_data available in state")

    # --- No data available shortcut ---
    if state.get("no_data_available"):
        logger.warning(f"[{node_name}] No data available flag set – calling no-data agent LLM")
        dims = state.get("filtered_analytical_dimensions") or state.get("analytical_dimensions") or []
        meas = state.get("filtered_analytical_measures") or state.get("analytical_measures") or []
        dim_names = [d.get("name") or d.get("label") or "" for d in dims if isinstance(d, dict)]
        meas_names = [m.get("name") or m.get("label") or "" for m in meas if isinstance(m, dict)]
        column_names = [x for x in dim_names + meas_names if x and isinstance(x, str)]
        model_name = model or settings.analytics_analytical_summary_model
        summary_dict = await _call_no_data_agent_llm(
            state, model_name, node_name, column_names=column_names, zero_rows=True
        )
        if summary_dict:
            summary_dict["suggested_queries"] = _PRODUCTION_SUGGESTED_QUERIES[:3]
            return {"analysis_summary": summary_dict, "no_data_available": True, "status": "completed"}
        return {
            "analysis_summary": {
                "summary_text": "",
                "confidence": "low",
                "confidence_reason": "",
                "suggested_queries": _PRODUCTION_SUGGESTED_QUERIES[:3],
            },
            "no_data_available": True,
            "status": "completed",
        }

    user_query = state.get("user_query", "")
    parsed_intent = state.get("parsed_intent")
    computation_results = state.get("computation_results", [])
    prepared_charts = state.get("prepared_charts", [])
    available_date_ranges = state.get("available_date_ranges", {})

    all_metrics = computation_results or []
    logger.info(f"[{node_name}] {len(all_metrics)} metrics, {len(prepared_charts)} charts")

    timeline_warning = None
    if available_date_ranges and user_query:
        timeline_warning = check_timeline_availability(user_query, available_date_ranges)
        if timeline_warning:
            logger.warning(f"[{node_name}] Timeline mismatch detected: {timeline_warning}")

    # --- Simple flow (no computation results / charts) ---
    orchestrator_decision = (state.get("orchestrator_decision") or "").strip().lower()
    raw_dataframes = state.get("raw_dataframes") or {}
    if orchestrator_decision == "simple" and not all_metrics and not prepared_charts:
        total_rows = 0
        all_columns: List[str] = []
        sample_rows: List[Dict[str, Any]] = []
        _max_sample = 2500
        for key, df in raw_dataframes.items():
            if df is None:
                continue
            if hasattr(df, "collect") and callable(df.collect):
                try:
                    c = df.collect()
                    n = int(c.height) if hasattr(c, "height") else len(c)
                    total_rows += n
                    if hasattr(c, "columns"):
                        all_columns.extend(c.columns)
                    if n > 0 and len(sample_rows) < _max_sample:
                        try:
                            if hasattr(c, "to_dicts") and callable(c.to_dicts):
                                for row in c.to_dicts():
                                    sample_rows.append(row)
                                    if len(sample_rows) >= _max_sample:
                                        break
                            elif hasattr(c, "rows") and callable(c.rows):
                                for row in c.rows(named=True):
                                    sample_rows.append(row if isinstance(row, dict) else dict(row))
                                    if len(sample_rows) >= _max_sample:
                                        break
                            else:
                                logger.warning(f"[{node_name}] No to_dicts/rows on collected frame for '{key}'")
                        except Exception as e:
                            logger.warning(f"[{node_name}] Could not extract sample rows from '{key}': {e}")
                except Exception as e:
                    logger.warning(f"[{node_name}] Could not collect LazyFrame for '{key}': {e}")
                    if hasattr(df, "height"):
                        total_rows += int(df.height)
            elif hasattr(df, "height"):
                total_rows += int(df.height)
            elif hasattr(df, "columns"):
                all_columns.extend(df.columns)
        cols_preview = list(dict.fromkeys(all_columns))[:30]

        if total_rows > 0 and not sample_rows:
            logger.warning(f"[{node_name}] Simple flow: {total_rows} rows but sample_rows empty – LLM may lack data context")
        elif sample_rows:
            logger.info(f"[{node_name}] Simple flow: passing {len(sample_rows)} sample row(s) (total_rows={total_rows})")

        if not raw_dataframes or total_rows == 0:
            dims = state.get("filtered_analytical_dimensions") or []
            meas = state.get("filtered_analytical_measures") or []
            dim_names = [d.get("name") or d.get("label") or "" for d in dims if isinstance(d, dict)]
            meas_names = [m.get("name") or m.get("label") or "" for m in meas if isinstance(m, dict)]
            column_names_from_state = [x for x in dim_names + meas_names if x and isinstance(x, str)]
            intent_explanation = (parsed_intent or {}).get("intent_explanation") if parsed_intent else None
            user_prompt = get_simple_flow_no_data_agent_user_prompt(
                user_query=user_query,
                column_names=column_names_from_state,
                intent_explanation=intent_explanation,
                zero_rows=bool(raw_dataframes and total_rows == 0),
            )
            model_name = model or settings.analytics_analytical_summary_model
            llm_client = state.get("llm_client") or AzureOpenAIClient()
            query_id = state.get("query_id")
            try:
                response = await llm_client._call_llm_unified(
                    model=model_name,
                    system_prompt=SIMPLE_FLOW_NO_DATA_AGENT_SYSTEM_PROMPT,
                    user_prompt=user_prompt,
                    node_name=node_name,
                    query_id=query_id,
                    temperature=0.5,
                    use_json_mode=True,
                )
                analysis_summary = parse_json_response_required_dict(
                    response or "", node_name=node_name, extract_from_list=True
                )
                if analysis_summary and analysis_summary.get("summary_text"):
                    summary_text = analysis_summary.get("summary_text", "").strip()
                    logger.info(f"[{node_name}] Simple flow: LLM agent response (no data)")
                    return {
                        "analysis_summary": {
                            "summary_text": summary_text,
                            "confidence": analysis_summary.get("confidence", "low"),
                            "confidence_reason": analysis_summary.get("confidence_reason") or "Simple flow: no production data, agent response",
                            "suggested_queries": _PRODUCTION_SUGGESTED_QUERIES[:3],
                        },
                        "no_data_available": True,
                        "status": "completed",
                    }
            except Exception as e:
                logger.warning(f"[{node_name}] Simple flow no-data agent LLM failed: {e}, using fallback")
            return {
                "analysis_summary": {
                    "summary_text": "",
                    "confidence": "low",
                    "confidence_reason": "",
                    "suggested_queries": _PRODUCTION_SUGGESTED_QUERIES[:3],
                },
                "no_data_available": True,
                "status": "completed",
            }

        from datetime import date as _date
        intent_explanation = (parsed_intent or {}).get("intent_explanation") if parsed_intent else None
        user_prompt = get_simple_flow_summary_user_prompt(
            user_query=user_query,
            total_rows=total_rows,
            column_names=cols_preview,
            intent_explanation=intent_explanation,
            sample_rows=sample_rows if sample_rows else None,
            current_date_iso=_date.today().isoformat(),
        )
        model_name = model or settings.analytics_analytical_summary_model
        llm_client = state.get("llm_client") or AzureOpenAIClient()
        query_id = state.get("query_id")
        try:
            response = await llm_client._call_llm_unified(
                model=model_name,
                system_prompt=SIMPLE_FLOW_SUMMARY_SYSTEM_PROMPT,
                user_prompt=user_prompt,
                node_name=node_name,
                query_id=query_id,
                temperature=0.5,
                use_json_mode=True,
            )
            analysis_summary = parse_json_response_required_dict(
                response or "", node_name=node_name, extract_from_list=True
            )
            if analysis_summary and analysis_summary.get("summary_text"):
                summary_text = analysis_summary.get("summary_text", "").strip()
                if gantt_context and gantt_context not in summary_text:
                    summary_text = f"{summary_text}\n\n**Schedule Overview:** {gantt_context}"
                logger.info(f"[{node_name}] Simple flow: LLM production summary generated ({total_rows} rows)")
                return {
                    "analysis_summary": {
                        "summary_text": summary_text,
                        "confidence": analysis_summary.get("confidence", "medium"),
                        "confidence_reason": analysis_summary.get("confidence_reason") or "Simple flow: production summary from fetched data",
                        "suggested_queries": _PRODUCTION_SUGGESTED_QUERIES[:4],
                    },
                    "status": "completed",
                }
        except Exception as e:
            logger.warning(f"[{node_name}] Simple flow LLM summary failed: {e}, using fallback")

        summary_text = (
            f"Retrieved {total_rows:,} row(s) of production data. "
            f"Columns available: {', '.join(cols_preview[:20])}{'...' if len(cols_preview) > 20 else ''}."
        )
        if gantt_context:
            summary_text = f"{summary_text}\n\n**Schedule Overview:** {gantt_context}"
        logger.info(f"[{node_name}] Simple flow summary (fallback): {total_rows} rows")
        return {
            "analysis_summary": {
                "summary_text": summary_text,
                "confidence": "medium",
                "confidence_reason": "Simple flow: summary based on fetched production data",
                "suggested_queries": _PRODUCTION_SUGGESTED_QUERIES[:3],
            },
            "status": "completed",
        }

    # --- Computation-based flow (metrics + charts) ---
    if not all_metrics and not prepared_charts:
        logger.warning(f"[{node_name}] No metrics and no prepared charts – calling no-data agent LLM")
        dims = state.get("filtered_analytical_dimensions") or state.get("analytical_dimensions") or []
        meas = state.get("filtered_analytical_measures") or state.get("analytical_measures") or []
        dim_names = [d.get("name") or d.get("label") or "" for d in dims if isinstance(d, dict)]
        meas_names = [m.get("name") or m.get("label") or "" for m in meas if isinstance(m, dict)]
        column_names = [x for x in dim_names + meas_names if x and isinstance(x, str)]
        model_name = model or settings.analytics_analytical_summary_model
        summary_dict = await _call_no_data_agent_llm(
            state, model_name, node_name, column_names=column_names, zero_rows=True
        )
        if summary_dict:
            summary_dict["suggested_queries"] = _PRODUCTION_SUGGESTED_QUERIES[:3]
            return {"analysis_summary": summary_dict, "no_data_available": True, "status": "completed"}
        return {
            "analysis_summary": {
                "summary_text": "",
                "confidence": "low",
                "confidence_reason": "",
                "suggested_queries": _PRODUCTION_SUGGESTED_QUERIES[:3],
            },
            "no_data_available": True,
            "status": "completed",
        }

    try:
        logger.info(f"[{node_name}] Generating production planning summary using computation metrics")

        llm_client = state.get("llm_client") or AzureOpenAIClient()
        query_id = state.get("query_id")

        lightweight_metrics = _filter_metrics_to_lightweight(all_metrics, node_name)

        date_filter_info = state.get("applied_date_filters") or extract_date_filters_from_state(state)
        if date_filter_info.get("filter_applied"):
            logger.info(f"[{node_name}] Date filters: {date_filter_info.get('date_range')} (source: {date_filter_info.get('filter_source', 'unknown')})")
        data_fetch_status = state.get("data_fetch_status")

        categories_from_metrics = {r.get("category") or "other" for r in all_metrics if isinstance(r, dict)}
        categories_from_charts = {ch.get("category") or "other" for ch in (prepared_charts or []) if isinstance(ch, dict)}

        category_priorities = state.get("category_priorities") or {}

        def _get_cat_priority(cat: str) -> int:
            if not cat:
                return 99
            if str(cat).strip().lower() == "other":
                return 98
            try:
                return int(category_priorities.get(cat, 5))
            except (TypeError, ValueError):
                return 5

        all_categories = sorted(
            (categories_from_metrics | categories_from_charts),
            key=lambda c: (_get_cat_priority(c), str(c).strip().lower()),
        )

        overall_model = model or settings.analytics_analytical_summary_model
        group_model = getattr(settings, "analytics_analytical_group_summary_model", None) or overall_model

        if len(all_categories) > 1 and (lightweight_metrics or prepared_charts):
            # --- Per-group summary path ---
            logger.info(f"[{node_name}] Running per-group production summary for {len(all_categories)} categories: {all_categories}")

            async def _summary_for_category(cat: str) -> Tuple[str, str, List[str]]:
                is_other = str(cat).strip().lower() == "other"

                if is_other:
                    cat_lightweight = lightweight_metrics
                    cat_chart_data = extract_chart_data_for_llm(prepared_charts or []) if prepared_charts else []
                else:
                    cat_metrics = [r for r in all_metrics if isinstance(r, dict) and ((r.get("category") or "other") == cat)]
                    cat_charts = [ch for ch in (prepared_charts or []) if isinstance(ch, dict) and ((ch.get("category") or "other") == cat)]
                    cat_lightweight = _filter_metrics_to_lightweight(cat_metrics, node_name)
                    cat_chart_data = extract_chart_data_for_llm(cat_charts) if cat_charts else []

                logger.info(f"[{node_name}] Category {cat!r}: {len(cat_lightweight)} metrics, {len(cat_chart_data)} charts")

                if not cat_lightweight and not cat_chart_data:
                    return (cat, "No production metrics or chart data available for this category.", [])

                if is_other:
                    sys_prompt = ANALYTICAL_OVERALL_SUMMARY_SYSTEM_PROMPT
                    cat_model = overall_model
                    user_prompt_c = get_analytical_overall_summary_user_prompt(
                        user_query=user_query,
                        computation_results=cat_lightweight,
                        parsed_intent=parsed_intent,
                        timeline_warning=timeline_warning,
                        date_filter_info=date_filter_info,
                        data_fetch_status=data_fetch_status,
                        chart_data=cat_chart_data or None,
                        overview_only=True,
                    )
                else:
                    sys_prompt = ANALYTICAL_GROUP_SUMMARY_SYSTEM_PROMPT
                    cat_model = group_model
                    user_prompt_c = get_analytical_group_summary_user_prompt(
                        user_query=user_query,
                        category_name=cat,
                        computation_results=cat_lightweight,
                        chart_data=cat_chart_data or None,
                        parsed_intent=parsed_intent,
                        timeline_warning=timeline_warning,
                        date_filter_info=date_filter_info,
                        data_fetch_status=data_fetch_status,
                    )

                token_count, within_limit = validate_prompt_tokens(
                    sys_prompt, user_prompt_c, max_tokens=DEFAULT_MAX_INPUT_PROMPT_TOKENS, model=cat_model,
                )
                if not within_limit:
                    rebuild_fn = (
                        (lambda m: get_analytical_overall_summary_user_prompt(
                            user_query=user_query, computation_results=m,
                            parsed_intent=parsed_intent, timeline_warning=timeline_warning,
                            date_filter_info=date_filter_info, data_fetch_status=data_fetch_status,
                            chart_data=cat_chart_data or None,
                            overview_only=True,
                        )) if is_other else
                        (lambda m: get_analytical_group_summary_user_prompt(
                            user_query=user_query, category_name=cat,
                            computation_results=m, chart_data=cat_chart_data or None,
                            parsed_intent=parsed_intent, timeline_warning=timeline_warning,
                            date_filter_info=date_filter_info, data_fetch_status=data_fetch_status,
                        ))
                    )
                    cat_lightweight, user_prompt_c, token_count = truncate_metrics_by_priority_for_prompt(
                        metrics=cat_lightweight,
                        build_user_prompt=rebuild_fn,
                        system_prompt=sys_prompt,
                        max_tokens=DEFAULT_MAX_INPUT_PROMPT_TOKENS,
                        min_metrics=5,
                        model=cat_model,
                    )
                    logger.info(f"[{node_name}] Category {cat!r} truncated to {len(cat_lightweight)} metrics; tokens={token_count}")

                if token_count > DEFAULT_MAX_INPUT_PROMPT_TOKENS and cat_chart_data:
                    _m = cat_lightweight
                    chart_rebuild_fn = (
                        (lambda m, c: get_analytical_overall_summary_user_prompt(
                            user_query=user_query, computation_results=m,
                            parsed_intent=parsed_intent, timeline_warning=timeline_warning,
                            date_filter_info=date_filter_info, data_fetch_status=data_fetch_status,
                            chart_data=c or None,
                            overview_only=True,
                        )) if is_other else
                        (lambda m, c: get_analytical_group_summary_user_prompt(
                            user_query=user_query, category_name=cat,
                            computation_results=m, chart_data=c or None,
                            parsed_intent=parsed_intent, timeline_warning=timeline_warning,
                            date_filter_info=date_filter_info, data_fetch_status=data_fetch_status,
                        ))
                    )
                    cat_chart_data, user_prompt_c, token_count = truncate_charts_for_prompt(
                        chart_data=cat_chart_data,
                        metrics=_m,
                        build_user_prompt=chart_rebuild_fn,
                        system_prompt=sys_prompt,
                        max_tokens=DEFAULT_MAX_INPUT_PROMPT_TOKENS,
                        min_charts=2,
                        model=cat_model,
                    )
                    logger.info(f"[{node_name}] Category {cat!r} truncated charts to {len(cat_chart_data)}; tokens={token_count}")

                try:
                    response = await llm_client._call_llm_unified(
                        model=cat_model,
                        system_prompt=sys_prompt,
                        user_prompt=user_prompt_c,
                        node_name=node_name,
                        query_id=query_id,
                        temperature=0.5,
                        use_json_mode=True,
                    )
                    parsed = parse_json_response_required_dict(response or "", node_name=node_name, extract_from_list=True)
                    text = (parsed.get("summary_text") or "").strip() if parsed else ""
                    mtd = parsed.get("metrics_to_display", []) if parsed else []
                    return (cat, text, mtd if isinstance(mtd, list) else [])
                except Exception as e:
                    logger.warning(f"[{node_name}] Per-group production summary for {cat!r} failed: {e}")
                    return (cat, "", [])

            max_parallel = getattr(settings, "analytics_max_parallel_chart_groups", 4) or 4
            if max_parallel < 1:
                max_parallel = 1

            results_per_group: List[Tuple[str, str, List[str]]] = []
            for i in range(0, len(all_categories), max_parallel):
                batch = all_categories[i : i + max_parallel]
                batch_results = await asyncio.gather(*[_summary_for_category(cat) for cat in batch])
                results_per_group.extend(batch_results)

            summaries_by_group = [
                {"category": "other", "group": "other", "text": t or ""}
                if str(c).strip().lower() == "other"
                else {"category": c, "group": c, "text": t or ""}
                for c, t, _ in results_per_group
            ]

            overview_parts = []
            for cat, text, _ in results_per_group:
                if text and text.strip():
                    overview_parts.append(f"**{cat}**\n{text.strip()}")
            summary_text = "\n\n".join(overview_parts).strip() if overview_parts else "Production planning analysis completed."

            metrics_to_display: List[str] = []
            seen_metric: set = set()
            for _, _, mtd in results_per_group:
                for m in mtd:
                    if m and m not in seen_metric:
                        metrics_to_display.append(m)
                        seen_metric.add(m)
            if not metrics_to_display:
                metrics_to_display = [m.get("metric") for m in lightweight_metrics if m.get("metric")][:20]
            if not metrics_to_display and lightweight_metrics:
                selected = _select_production_priority_metrics(lightweight_metrics, max_count=15, metric_key="metric")
                metrics_to_display = [m.get("metric") for m in selected if m.get("metric")]

            analysis_summary = _build_analysis_summary_output(
                summary_text=summary_text,
                summaries_by_group=summaries_by_group,
                metrics_to_display=metrics_to_display,
                date_filter_info=date_filter_info,
                data_fetch_status=data_fetch_status,
                confidence="medium",
                confidence_reason="Per-group production planning summaries generated.",
                suggested_queries=_PRODUCTION_SUGGESTED_QUERIES[:4],
                gantt_context=gantt_context,
            )
            if registry:
                registry.record_node_completion(node_name)
            group_names = [s.get("group") or s.get("category") or "?" for s in summaries_by_group]
            group_details = ", ".join(f"{g}({len((s.get('text') or ''))} chars)" for g, s in zip(group_names, summaries_by_group))
            logger.info(
                f"[{node_name}] Per-group production summary done: {len(summaries_by_group)} groups, "
                f"{len(metrics_to_display)} metrics | groups: {group_details}"
            )
            return {
                "analysis_summary": analysis_summary,
                "suggested_metrics": metrics_to_display,
                "applied_date_filters": date_filter_info,
                "status": "completed",
            }

        # --- Single-category summary path ---
        logger.info(f"[{node_name}] Single production summary path ({len(lightweight_metrics)} metrics, {len(prepared_charts or [])} charts)")
        chart_data_for_llm = extract_chart_data_for_llm(prepared_charts) if prepared_charts else []
        system_prompt = ANALYTICAL_OVERALL_SUMMARY_SYSTEM_PROMPT
        user_prompt = get_analytical_overall_summary_user_prompt(
            user_query=user_query,
            computation_results=lightweight_metrics,
            parsed_intent=parsed_intent,
            timeline_warning=timeline_warning,
            date_filter_info=date_filter_info,
            data_fetch_status=data_fetch_status,
            chart_data=chart_data_for_llm or None,
        )

        token_count, within_limit = validate_prompt_tokens(
            system_prompt, user_prompt, max_tokens=DEFAULT_MAX_INPUT_PROMPT_TOKENS, model=overall_model,
        )
        if not within_limit:
            lightweight_metrics, user_prompt, token_count = truncate_metrics_by_priority_for_prompt(
                metrics=lightweight_metrics,
                build_user_prompt=lambda m: get_analytical_overall_summary_user_prompt(
                    user_query=user_query, computation_results=m,
                    parsed_intent=parsed_intent, timeline_warning=timeline_warning,
                    date_filter_info=date_filter_info, data_fetch_status=data_fetch_status,
                    chart_data=chart_data_for_llm or None,
                ),
                system_prompt=system_prompt,
                max_tokens=DEFAULT_MAX_INPUT_PROMPT_TOKENS,
                min_metrics=5,
                model=overall_model,
            )
            logger.info(f"[{node_name}] Single summary truncated to {len(lightweight_metrics)} metrics; tokens={token_count}")

        if token_count > DEFAULT_MAX_INPUT_PROMPT_TOKENS and chart_data_for_llm:
            _m = lightweight_metrics
            chart_data_for_llm, user_prompt, token_count = truncate_charts_for_prompt(
                chart_data=chart_data_for_llm,
                metrics=_m,
                build_user_prompt=lambda m, c: get_analytical_overall_summary_user_prompt(
                    user_query=user_query, computation_results=m,
                    parsed_intent=parsed_intent, timeline_warning=timeline_warning,
                    date_filter_info=date_filter_info, data_fetch_status=data_fetch_status,
                    chart_data=c or None,
                ),
                system_prompt=system_prompt,
                max_tokens=DEFAULT_MAX_INPUT_PROMPT_TOKENS,
                min_charts=3,
                model=overall_model,
            )
            logger.info(f"[{node_name}] Single summary truncated charts to {len(chart_data_for_llm)}; tokens={token_count}")

        save_llm_call_input(
            node_name=node_name, query_id=query_id,
            system_prompt=system_prompt, user_prompt=user_prompt,
            extra={"model": overall_model},
        )
        response = await llm_client._call_llm_unified(
            model=overall_model,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            node_name=node_name,
            query_id=query_id,
            temperature=0.5,
            use_json_mode=True,
        )

        analysis_summary = _normalize_llm_summary_response(response, node_name)
        save_llm_call_output(
            node_name=node_name, query_id=query_id,
            raw_response=response, parsed=analysis_summary,
        )

        for _unwanted in ("key_insights", "financial_analysis", "recommendations",
                          "suggested_charts", "data_quality_assessment", "suggested_follow_up_queries"):
            analysis_summary.pop(_unwanted, None)

        metrics_to_display = analysis_summary.get("metrics_to_display", [])
        if not metrics_to_display and lightweight_metrics:
            selected = _select_production_priority_metrics(lightweight_metrics, max_count=15, metric_key="metric")
            metrics_to_display = [m.get("metric") for m in selected if m.get("metric")]
            analysis_summary["metrics_to_display"] = metrics_to_display

        st = (analysis_summary.get("summary_text") or "").strip()
        analysis_summary["summaries_by_group"] = [
            {"category": "other", "group": "other", "text": st or "Production planning analysis completed based on the provided data."}
        ]

        final_summary = _build_analysis_summary_output(
            summary_text=analysis_summary.get("summary_text", ""),
            summaries_by_group=analysis_summary.get("summaries_by_group"),
            metrics_to_display=metrics_to_display,
            date_filter_info=date_filter_info,
            data_fetch_status=data_fetch_status,
            confidence=analysis_summary.get("confidence", "medium"),
            confidence_reason=analysis_summary.get("confidence_reason", ""),
            suggested_queries=_PRODUCTION_SUGGESTED_QUERIES[:4],
            gantt_context=gantt_context,
        )

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"[{node_name}] Production summary generated | {duration:.2f}s | {len(st)} chars | {len(metrics_to_display)} metrics")

        if registry:
            registry.record_node_completion(node_name)
        return {
            "analysis_summary": final_summary,
            "suggested_metrics": metrics_to_display,
            "applied_date_filters": date_filter_info,
            "status": "completed",
        }

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"[{node_name}] Production planning summary failed after {duration:.2f}s: {str(e)}", exc_info=True)
        if registry:
            registry.record_node_completion(node_name)
        return {
            "analysis_summary": {
                "summary_text": "Production planning summary generation failed.",
                "confidence": "low",
                "confidence_reason": f"Error: {str(e)}",
                "summaries_by_group": [
                    {"category": "other", "group": "other", "text": "Production planning summary generation failed."}
                ],
                "suggested_queries": _PRODUCTION_SUGGESTED_QUERIES[:3],
            },
            "suggested_metrics": [],
            "errors": [f"Production planning summary failed: {str(e)}"],
            "status": "completed",
        }
