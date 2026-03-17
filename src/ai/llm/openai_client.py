"""OpenAI-compatible LLM client for AI nodes."""
import logging
from typing import Any, Dict, List, Optional

from src.ai.llm.base_llm import BaseLLM
from src.core.config import settings

logger = logging.getLogger(__name__)


class OpenAILLMClient(BaseLLM):
    """LLM client using OpenAI API (or compatible endpoint)."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        model: Optional[str] = None,
    ):
        self._api_key = api_key or settings.openai_api_key
        self._base_url = base_url or settings.openai_base_url
        self._model = model or settings.openai_model

    def invoke(
        self,
        messages: List[Dict[str, str]],
        **kwargs: Any,
    ) -> str:
        """Synchronous invoke."""
        try:
            from openai import OpenAI

            client = OpenAI(
                api_key=self._api_key or "sk-dummy",
                base_url=self._base_url,
            )
            resp = client.chat.completions.create(
                model=self._model,
                messages=messages,
                **kwargs,
            )
            if resp.choices:
                return (resp.choices[0].message.content or "").strip()
            return ""
        except Exception as e:
            logger.warning("OpenAILLMClient.invoke failed: %s", e)
            return f"[LLM unavailable: {e}]"

    async def ainvoke(
        self,
        messages: List[Dict[str, str]],
        **kwargs: Any,
    ) -> str:
        """Async invoke."""
        try:
            from openai import AsyncOpenAI

            client = AsyncOpenAI(
                api_key=self._api_key or "sk-dummy",
                base_url=self._base_url,
            )
            resp = await client.chat.completions.create(
                model=self._model,
                messages=messages,
                **kwargs,
            )
            if resp.choices:
                return (resp.choices[0].message.content or "").strip()
            return ""
        except Exception as e:
            logger.warning("OpenAILLMClient.ainvoke failed: %s", e)
            return f"[LLM unavailable: {e}]"
