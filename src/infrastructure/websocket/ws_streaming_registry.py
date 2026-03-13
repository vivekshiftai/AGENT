"""Registry to make the active WebSocket manager accessible from the LLM client layer.

This allows _call_llm_unified (and the provider-specific methods it delegates to)
to forward LLM tokens to the UI in real time without threading ws_manager through
every node function signature.

Pattern matches token_usage_registry.py / node_timing_registry.py.
"""
import asyncio
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

_ws_manager: Optional[Any] = None


def get_ws_streaming_manager():
    """Return the active QueryWebSocketManager (or None when not inside a WS query)."""
    return _ws_manager


def set_ws_streaming_manager(manager) -> None:
    """Set the active QueryWebSocketManager for the current query."""
    global _ws_manager
    _ws_manager = manager


def clear_ws_streaming_manager() -> None:
    """Clear the registry (called when the query/connection ends)."""
    global _ws_manager
    _ws_manager = None


async def forward_llm_chunk(text: str, node_name: str = "", chunk_index: int = 0) -> bool:
    """Forward a single LLM text chunk to the UI via the registered ws_manager.

    Returns True if sent, False if no manager is registered or send failed.
    Safe to call even when no WebSocket session is active.
    """
    mgr = _ws_manager
    if mgr is None:
        return False
    try:
        return await mgr.send_llm_stream_chunk(text, node_name, chunk_index)
    except Exception as e:
        logger.debug(f"[WS Stream] Failed to forward chunk for {node_name}: {e}")
        return False


async def forward_llm_stream_end(node_name: str = "", full_text: str = "") -> bool:
    """Signal end-of-stream to the UI via the registered ws_manager."""
    mgr = _ws_manager
    if mgr is None:
        return False
    try:
        return await mgr.send_llm_stream_end(node_name, full_text)
    except Exception as e:
        logger.debug(f"[WS Stream] Failed to send stream-end for {node_name}: {e}")
        return False


async def forward_llm_thinking_chunk(text: str, node_name: str = "", chunk_index: int = 0) -> bool:
    """Forward a single LLM thinking/reasoning chunk to the UI."""
    mgr = _ws_manager
    if mgr is None:
        return False
    try:
        return await mgr.send_llm_thinking_chunk(text, node_name, chunk_index)
    except Exception as e:
        logger.debug(f"[WS Stream] Failed to forward thinking chunk for {node_name}: {e}")
        return False


async def forward_llm_thinking_end(node_name: str = "", full_thinking: str = "") -> bool:
    """Signal end-of-thinking-stream to the UI."""
    mgr = _ws_manager
    if mgr is None:
        return False
    try:
        return await mgr.send_llm_thinking_end(node_name, full_thinking)
    except Exception as e:
        logger.debug(f"[WS Stream] Failed to send thinking-end for {node_name}: {e}")
        return False
