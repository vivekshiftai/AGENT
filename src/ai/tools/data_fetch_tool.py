"""Tool: fetch data via FetchDataUseCase (for agent use)."""
import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def data_fetch_tool(
    queries: List[str],
    data_repository: Any,
    cached_dataframes: Dict[str, Any] = None,
) -> Dict[str, Any]:
    """Execute queries using the given DataRepository."""
    from src.application.use_cases.fetch_data_usecase import FetchDataUseCase

    use_case = FetchDataUseCase(data_repository)
    return use_case.execute(queries=queries, cached_dataframes=cached_dataframes or {})
