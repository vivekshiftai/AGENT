"""
Single place to obtain the LLM client. Initializes once and returns the same instance everywhere.
Uses UnifiedLLMClient: routes by model name (claude* → Claude, else → Azure OpenAI / OpenAI).
"""
import logging
from typing import Optional

from src.ai.llm.base_llm import BaseLLM
from src.ai.llm.unified_llm_client import UnifiedLLMClient

logger = logging.getLogger(__name__)

_llm_client: Optional[BaseLLM] = None


def get_llm_client() -> BaseLLM:
    """Return the shared LLM client. One instance; routing by model name (Claude vs OpenAI/Azure)."""
    global _llm_client
    if _llm_client is not None:
        return _llm_client
    _llm_client = UnifiedLLMClient()
    logger.info("LLM client initialized: unified (Claude + OpenAI/Azure, route by model)")
    return _llm_client


def set_llm_client(client: Optional[BaseLLM]) -> None:
    """Override the shared client (e.g. for tests). Pass None to reset to lazy default."""
    global _llm_client
    _llm_client = client
