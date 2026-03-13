"""Token counting and validation for LLM prompts. Reusable across summary, intelligence, and other nodes."""
from typing import Optional, Tuple, List, Dict, Any, Callable
import logging

from .node_helpers import _score_metric_for_revenue_priority

logger = logging.getLogger(__name__)

# Default encoding for OpenAI GPT-4 / o1 / cl100k-based models
_DEFAULT_ENCODING = "cl100k_base"
# Max input tokens — 110K leaves room for output (~16K) and API overhead (~2K) within 128K models.
DEFAULT_MAX_INPUT_PROMPT_TOKENS = 110_000

# Track whether tiktoken is available so we log only once
_tiktoken_available: Optional[bool] = None


def count_tokens(text: str, model: Optional[str] = None) -> int:
    """
    Count token length of text for the given model.

    Uses tiktoken when available (o200k_base for gpt-4o, cl100k_base for others).
    Falls back to len(text)//3 (conservative) if tiktoken is not installed.

    Args:
        text: Input string to count (e.g. system or user prompt).
        model: Optional model name (e.g. "gpt-4o", "claude-sonnet-4-5") to pick encoding.

    Returns:
        Estimated or actual token count.
    """
    global _tiktoken_available
    if not text:
        return 0
    try:
        import tiktoken
        # gpt-4o uses o200k_base; older GPT-4 / Claude proxied through OpenAI use cl100k_base
        encoding_name = _DEFAULT_ENCODING
        if model and ("4o" in model or "gpt-4o" in model):
            encoding_name = "o200k_base"
        enc = tiktoken.get_encoding(encoding_name)
        if _tiktoken_available is None:
            _tiktoken_available = True
            logger.info(f"[token_utils] Using tiktoken ({encoding_name}) for token counting")
        return len(enc.encode(text))
    except Exception as e:
        if _tiktoken_available is None:
            _tiktoken_available = False
            logger.warning(f"[token_utils] tiktoken not available: {e}; using char/3 estimate (conservative)")
    # Conservative fallback: ~3.2 chars per token for English; use 3 to overestimate safely
    return (len(text) + 2) // 3


def get_prompt_token_count(
    system_prompt: str,
    user_prompt: str,
    model: Optional[str] = None,
) -> int:
    """
    Total token count for the full input (system + user) sent to the LLM.

    Use this before calling the LLM to validate or truncate when over model limits.

    Args:
        system_prompt: System message content.
        user_prompt: User message content.
        model: Optional model name for encoding.

    Returns:
        Total tokens for system_prompt + user_prompt.
    """
    return count_tokens(system_prompt or "", model) + count_tokens(user_prompt or "", model)


def validate_prompt_tokens(
    system_prompt: str,
    user_prompt: str,
    max_tokens: int = DEFAULT_MAX_INPUT_PROMPT_TOKENS,
    model: Optional[str] = None,
) -> Tuple[int, bool]:
    """
    Check whether the combined prompt is within the allowed token limit.

    Returns:
        (token_count, within_limit).
    """
    total = get_prompt_token_count(system_prompt, user_prompt, model)
    return total, total <= max_tokens


def sort_metrics_by_priority(
    metrics: List[Dict[str, Any]],
    metric_key: str = "metric",
) -> List[Dict[str, Any]]:
    """
    Sort metrics by revenue/business priority (highest first).
    Use when truncating: drop from the end to remove least-priority metrics.

    Args:
        metrics: List of dicts with at least metric_key (e.g. "metric").
        metric_key: Key holding the metric name used for scoring.

    Returns:
        New list sorted by _score_metric_for_revenue_priority descending.
    """
    if not metrics:
        return []
    return sorted(
        metrics,
        key=lambda m: _score_metric_for_revenue_priority(
            (m.get(metric_key) or "") if isinstance(m, dict) else ""
        ),
        reverse=True,
    )


def truncate_metrics_by_priority_for_prompt(
    metrics: List[Dict[str, Any]],
    build_user_prompt: Callable[[List[Dict[str, Any]]], str],
    system_prompt: str,
    max_tokens: int = DEFAULT_MAX_INPUT_PROMPT_TOKENS,
    min_metrics: int = 5,
    metric_key: str = "metric",
    model: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], str, int]:
    """
    If the prompt (system + user built from metrics) exceeds max_tokens, truncate
    by removing the least-priority metrics until under the limit.

    Args:
        metrics: List of metric dicts (e.g. [{"metric": "...", "value": ...}]).
        build_user_prompt: Function that takes a list of metric dicts and returns the user prompt string.
        system_prompt: System prompt (included in token count).
        max_tokens: Maximum allowed input tokens.
        min_metrics: Stop truncating when we have this many metrics (even if still over limit).
        metric_key: Key used for priority scoring.
        model: Optional model for tokenizer.

    Returns:
        (truncated_metrics, final_user_prompt, token_count).
    """
    sorted_metrics = sort_metrics_by_priority(metrics, metric_key=metric_key)
    user_prompt = build_user_prompt(sorted_metrics)
    token_count = get_prompt_token_count(system_prompt, user_prompt, model)

    while token_count > max_tokens and len(sorted_metrics) > min_metrics:
        sorted_metrics = sorted_metrics[:-1]
        user_prompt = build_user_prompt(sorted_metrics)
        token_count = get_prompt_token_count(system_prompt, user_prompt, model)
        logger.info(
            f"[token_utils] Truncated to {len(sorted_metrics)} metrics; token_count={token_count}"
        )

    if token_count > max_tokens:
        logger.warning(
            f"[token_utils] Still over limit after truncation ({token_count} > {max_tokens}, "
            f"min_metrics={min_metrics}); using {len(sorted_metrics)} metrics"
        )
    return (sorted_metrics, user_prompt, token_count)


def truncate_charts_for_prompt(
    chart_data: List[Dict[str, Any]],
    metrics: List[Dict[str, Any]],
    build_user_prompt: Callable[[List[Dict[str, Any]], List[Dict[str, Any]]], str],
    system_prompt: str,
    max_tokens: int = DEFAULT_MAX_INPUT_PROMPT_TOKENS,
    min_charts: int = 3,
    model: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], str, int]:
    """
    If the prompt is still over max_tokens after metrics truncation, remove charts
    from the end (least important last) until under the limit.

    Args:
        chart_data: List of chart dicts (title + x_values + y_values).
        metrics: Already-truncated metrics list.
        build_user_prompt: fn(metrics, chart_data) -> user_prompt string.
        system_prompt: System prompt (included in token count).
        max_tokens: Maximum allowed input tokens.
        min_charts: Stop truncating at this many charts even if still over.
        model: Optional model for tokenizer.

    Returns:
        (truncated_chart_data, final_user_prompt, token_count).
    """
    if not chart_data:
        user_prompt = build_user_prompt(metrics, chart_data)
        return chart_data, user_prompt, get_prompt_token_count(system_prompt, user_prompt, model)

    charts = list(chart_data)
    user_prompt = build_user_prompt(metrics, charts)
    token_count = get_prompt_token_count(system_prompt, user_prompt, model)

    while token_count > max_tokens and len(charts) > min_charts:
        charts = charts[:-1]
        user_prompt = build_user_prompt(metrics, charts)
        token_count = get_prompt_token_count(system_prompt, user_prompt, model)
        logger.info(f"[token_utils] Truncated charts to {len(charts)}; token_count={token_count}")

    if token_count > max_tokens:
        logger.warning(
            f"[token_utils] Still over limit after chart truncation ({token_count} > {max_tokens}, "
            f"min_charts={min_charts}); using {len(charts)} charts"
        )
    return charts, user_prompt, token_count
