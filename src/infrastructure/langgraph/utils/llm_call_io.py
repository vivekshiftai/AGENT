"""Common util to save LLM call input and output for every node.

Saves to:
  prompts/
    input/   -> {node_name}_{query_id}[_{call_suffix}].json  (system_prompt, user_prompt, extra)
    output/  -> {node_name}_{query_id}[_{call_suffix}].json  (raw_response, parsed, extra)

Used by chart_plan, chart_preplan, and all other nodes that make LLM calls.
"""
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def _project_root() -> Path:
    """Return analytics-backend project root (parent of src)."""
    current_file = Path(__file__).resolve()
    # llm_call_io.py is at .../analytics-backend/src/infrastructure/langgraph/utils/llm_call_io.py
    # -> parent x5 = analytics-backend
    return current_file.parent.parent.parent.parent.parent


def _prompts_dir() -> Path:
    """Return prompts base dir: project_root / prompts. Creates input and output subdirs."""
    base = _project_root() / "prompts"
    (base / "input").mkdir(parents=True, exist_ok=True)
    (base / "output").mkdir(parents=True, exist_ok=True)
    return base


def _safe_filename(node_name: str, query_id: Optional[str], call_suffix: Optional[str]) -> str:
    """Build filename: node_name_queryid or node_name_queryid_suffix. Sanitized for filesystem."""
    safe_node = (node_name or "node").replace("/", "_").replace("\\", "_").strip()[:64]
    qid = (query_id or "no_id").replace("/", "_").replace("\\", "_").strip()[:64]
    name = f"{safe_node}_{qid}"
    if call_suffix:
        suffix = str(call_suffix).replace("/", "_").replace("\\", "_").strip()[:32]
        name = f"{name}_{suffix}"
    return name + ".json"


def save_llm_call_input(
    node_name: str,
    query_id: Optional[str],
    system_prompt: str,
    user_prompt: str,
    extra: Optional[Dict[str, Any]] = None,
    call_suffix: Optional[str] = None,
) -> Optional[Path]:
    """
    Save LLM call input (system + user prompt) to prompts/input/{node_name}_{query_id}[_{call_suffix}].json.
    Returns path if saved, None otherwise. Logs and swallows errors.
    """
    try:
        base = _prompts_dir()
        path = base / "input" / _safe_filename(node_name, query_id, call_suffix)
        payload: Dict[str, Any] = {
            "node": node_name,
            "query_id": query_id,
            "timestamp": datetime.now().isoformat(),
            "system_prompt": system_prompt,
            "user_prompt": user_prompt,
        }
        if extra:
            payload["extra"] = extra
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False, default=str)
        logger.debug(f"[{node_name}] Saved LLM input to {path}")
        return path
    except Exception as e:
        logger.warning(f"[{node_name}] Failed to save LLM input JSON: {e}")
        return None


def save_llm_call_output(
    node_name: str,
    query_id: Optional[str],
    raw_response: Optional[str],
    parsed: Any = None,
    extra: Optional[Dict[str, Any]] = None,
    call_suffix: Optional[str] = None,
) -> Optional[Path]:
    """
    Save LLM call output to prompts/output/{node_name}_{query_id}[_{call_suffix}].json.
    raw_response: raw string from LLM. parsed: parsed JSON/dict if available.
    Returns path if saved, None otherwise. Logs and swallows errors.
    """
    try:
        base = _prompts_dir()
        path = base / "output" / _safe_filename(node_name, query_id, call_suffix)
        payload: Dict[str, Any] = {
            "node": node_name,
            "query_id": query_id,
            "timestamp": datetime.now().isoformat(),
            "raw_response": raw_response if isinstance(raw_response, str) else str(raw_response) if raw_response is not None else None,
        }
        if parsed is not None:
            payload["parsed"] = parsed
        if extra:
            payload["extra"] = extra
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False, default=str)
        logger.debug(f"[{node_name}] Saved LLM output to {path}")
        return path
    except Exception as e:
        logger.warning(f"[{node_name}] Failed to save LLM output JSON: {e}")
        return None
