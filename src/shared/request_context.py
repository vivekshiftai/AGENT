"""
Request/query ID and user ID context for request-lifecycle tracing.

Uses contextvars so the same request/query ID and user_id are available across
the entire request (and any async tasks spawned from it) without passing them
explicitly. Also keeps thread-local copies so the logger's filter sees them
even when asyncio task context differs (e.g. create_task). Middleware or
WebSocket handler set IDs at request start; logging shows them on every line.
"""
from contextvars import ContextVar
from typing import Optional
import threading

# ContextVar is async-safe: each request/task gets its own context copy.
_request_id_var: ContextVar[Optional[str]] = ContextVar(
    "request_id",
    default=None,
)
_user_id_var: ContextVar[Optional[str]] = ContextVar(
    "user_id",
    default=None,
)

# Thread-local fallback so the same thread (e.g. all asyncio tasks on main thread) sees the IDs
_local = threading.local()


def _get_local_request_id() -> Optional[str]:
    return getattr(_local, "request_id", None)


def _get_local_user_id() -> Optional[str]:
    return getattr(_local, "user_id", None)


def get_request_id() -> Optional[str]:
    """Return the current request/query ID, or None if not set (e.g. outside request scope)."""
    rid = _request_id_var.get()
    if rid is not None:
        return rid
    return _get_local_request_id()


def get_user_id() -> Optional[str]:
    """Return the current user ID, or None if not set (e.g. outside request scope)."""
    uid = _user_id_var.get()
    if uid is not None:
        return uid
    return _get_local_user_id()


def set_request_id(request_id: str) -> None:
    """
    Set the request/query ID for the current context and current thread.
    Called by middleware at request start or by WebSocket handler when a query starts.
    """
    _request_id_var.set(request_id)
    _local.request_id = request_id


def set_user_id(user_id: str) -> None:
    """
    Set the user ID for the current context and current thread.
    Called by WebSocket handler when a query starts so logs show user_id.
    """
    _user_id_var.set(user_id)
    _local.user_id = user_id


def set_request_context(query_id: Optional[str] = None, user_id: Optional[str] = None) -> None:
    """
    Set both query_id and user_id in context in one call.
    Pass None for any value to leave that part of context unchanged.
    """
    if query_id is not None:
        set_request_id(query_id)
    if user_id is not None:
        set_user_id(user_id)


def clear_request_id() -> None:
    """
    Clear the request/query ID so it does not leak to other requests.
    Called by middleware in a finally block after handling the request.
    """
    try:
        _request_id_var.set(None)
    except LookupError:
        pass
    try:
        _local.request_id = None
    except Exception:
        pass


def clear_user_id() -> None:
    """Clear the user ID so it does not leak to other requests."""
    try:
        _user_id_var.set(None)
    except LookupError:
        pass
    try:
        _local.user_id = None
    except Exception:
        pass


def clear_request_context() -> None:
    """Clear both query_id and user_id from context."""
    clear_request_id()
    clear_user_id()
