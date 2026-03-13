"""Registry for accumulating LLM token usage across a query session for batch database updates."""
from typing import Dict, Any, List, Optional
import threading
import logging

logger = logging.getLogger(__name__)

# Thread-local storage for token usage accumulation
_thread_local = threading.local()


def get_token_usage_registry() -> Optional['TokenUsageRegistry']:
    """Get the token usage registry for the current thread/query."""
    return getattr(_thread_local, 'token_usage_registry', None)


def set_token_usage_registry(registry: 'TokenUsageRegistry') -> None:
    """Set the token usage registry for the current thread/query."""
    _thread_local.token_usage_registry = registry


def clear_token_usage_registry() -> None:
    """Clear the token usage registry for the current thread/query."""
    if hasattr(_thread_local, 'token_usage_registry'):
        delattr(_thread_local, 'token_usage_registry')


class TokenUsageRegistry:
    """
    Registry to accumulate LLM token usage records for batch database updates.
    
    This allows token usage to be collected in memory during query execution
    and saved to the database in a single batch operation at the end.
    """
    
    def __init__(self, query_id: str):
        """Initialize registry for a specific query."""
        self.query_id = query_id
        self.usage_records: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
    
    def add_usage(
        self,
        provider: str,
        model: str,
        node_name: str,
        input_tokens: int,
        output_tokens: int,
        total_tokens: int,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Add a token usage record to the registry.
        
        Thread-safe accumulation for batch database update.
        """
        with self._lock:
            self.usage_records.append({
                "provider": provider,
                "model": model,
                "node_name": node_name,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "total_tokens": total_tokens,
                "config": config or {},
            })
            logger.debug(f"[TokenRegistry] Added usage for {node_name}: {total_tokens} tokens (query: {self.query_id})")
    
    def get_all_records(self) -> List[Dict[str, Any]]:
        """Get all accumulated usage records."""
        with self._lock:
            return self.usage_records.copy()
    
    def clear(self) -> None:
        """Clear all accumulated records."""
        with self._lock:
            self.usage_records.clear()

