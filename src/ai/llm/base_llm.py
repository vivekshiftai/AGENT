"""Base interface for LLM clients."""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class BaseLLM(ABC):
    """Abstract base for all LLM clients used by AI nodes."""

    @abstractmethod
    def invoke(
        self,
        messages: List[Dict[str, str]],
        **kwargs: Any,
    ) -> str:
        """Send messages and return assistant text content."""
        pass

    @abstractmethod
    async def ainvoke(
        self,
        messages: List[Dict[str, str]],
        **kwargs: Any,
    ) -> str:
        """Async: send messages and return assistant text content."""
        pass
