"""
Data Access Layer - Production Planning System (backward compatibility).

Use this as the entry point for connecting to data sources and retrieving data.
For the full application, run: python src/main.py

Example usage:
    from src.index import ConnectorFactory, DataRepository, get_datasphere_service
"""
import logging
import sys
from pathlib import Path

_src_dir = Path(__file__).parent
if str(_src_dir.parent) not in sys.path:
    sys.path.insert(0, str(_src_dir.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

from src.core.config import settings
from src.core.logging_config import setup_logging

setup_logging()
logging.getLogger().setLevel(getattr(logging, (settings.log_level or "INFO").upper(), logging.INFO))

from src.infrastructure.connectors.connector_factory import ConnectorFactory
from src.domain.repositories.data_repository import DataRepository
from src.infrastructure.external_services.datasphere_service import get_datasphere_service

__all__ = [
    "ConnectorFactory",
    "DataRepository",
    "get_datasphere_service",
]
