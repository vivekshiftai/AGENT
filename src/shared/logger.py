"""Logging configuration.

Query/request ID and user ID tracing: when request_context is set (by middleware or
WebSocket handler), every log record gets query_id and user_id injected via
RequestIdFilter so each line is traceable. The formatter shows [query_id=...] [user_id=...];
when not set, "-" is used. Do not add query_id/user_id manually in log messages—they
are added once here for every log line.
"""
import logging
import sys
import os
from typing import Any


def _get_request_context_ids():
    """Read query_id and user_id from the same request_context module used by the app (shared.request_context).
    Prefer absolute import so we use the same ContextVar/thread-local instance as routes and graph.
    """
    try:
        from shared.request_context import get_request_id, get_user_id
    except ImportError:
        try:
            from .request_context import get_request_id, get_user_id
        except Exception:
            return None, None
    try:
        return get_request_id(), get_user_id()
    except Exception:
        return None, None


class RequestIdFilter(logging.Filter):
    """
    Injects the current request/query ID and user ID (from contextvars + thread-local)
    into every log record so every line shows the same IDs without changing existing log calls.
    Uses the same request_context module as the app (shared.request_context) so context is visible.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        rid, uid = _get_request_context_ids()
        record.request_id = rid if rid else "-"
        record.user_id = uid if uid else "-"
        return True


def configure_logging(log_level: str = "INFO") -> None:
    """Configure logging with plain text formatting, UTF-8 support, and request ID tracing."""
    # Remove any existing handlers to avoid duplicates
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Create console handler with UTF-8 encoding for Windows compatibility
    console_handler = logging.StreamHandler(sys.stdout)

    # Ensure UTF-8 encoding on Windows
    if os.name == 'nt':  # Windows
        try:
            # Try to set console to UTF-8 mode
            import subprocess
            subprocess.run(['chcp', '65001'], shell=True, capture_output=True)
        except Exception:
            pass

        # Set UTF-8 encoding for the handler
        try:
            console_handler.stream.reconfigure(encoding='utf-8')
        except AttributeError:
            # Python < 3.7 doesn't have stream.reconfigure
            pass

    # Formatter includes query_id and user_id so every line is traceable; "-" when not set
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - [query_id=%(request_id)s] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler.setFormatter(formatter)

    # Add filter that injects request_id and user_id from contextvars into each record
    console_handler.addFilter(RequestIdFilter())

    # Configure root logger
    root_logger.setLevel(getattr(logging, log_level.upper()))
    root_logger.addHandler(console_handler)
    
    # Suppress verbose Azure SDK logging (set to WARNING to reduce noise)
    azure_loggers = [
        'azure.core.pipeline.policies.http_logging_policy',
        'azure.core.pipeline.policies',
        'azure.identity',
        'azure.keyvault',
        'azure.core',
        'urllib3',
        'httpx',
    ]
    for logger_name in azure_loggers:
        azure_logger = logging.getLogger(logger_name)
        azure_logger.setLevel(logging.WARNING)  # Only show WARNING and above
        azure_logger.propagate = True  # Still propagate to root logger


def get_logger(name: str) -> Any:
    """Get a configured logger instance."""
    return logging.getLogger(name)

