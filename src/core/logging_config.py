"""Logging configuration for the application."""
import logging
import sys

from src.core.config import settings


def setup_logging() -> None:
    """Configure application logging."""
    level = getattr(logging, (settings.log_level or "INFO").upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
    )
    logging.getLogger().setLevel(level)
