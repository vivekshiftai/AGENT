"""LLM abstraction. Use get_llm_client() everywhere for a single shared client (unified, route by model)."""

from src.ai.llm.base_llm import BaseLLM
from src.ai.llm.client_factory import get_llm_client, set_llm_client
from src.ai.llm.ClaudeClient import ClaudeClient
from src.ai.llm.openai_client import OpenAILLMClient
from src.ai.llm.unified_llm_client import UnifiedLLMClient

__all__ = [
    "BaseLLM",
    "get_llm_client",
    "set_llm_client",
    "ClaudeClient",
    "OpenAILLMClient",
    "UnifiedLLMClient",
]
