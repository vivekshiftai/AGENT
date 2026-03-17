"""
Unified LLM client: routes by model name (claude* → Claude, else → Azure OpenAI / OpenAI).
Use get_llm_client() to get the single shared instance. Supports snippet-style calls via
call_llm_unified(model, system_prompt, user_prompt, ...).
"""
import logging
from typing import Any, Dict, List, Optional

from src.ai.llm.base_llm import BaseLLM
from src.ai.llm.ClaudeClient import ClaudeClient
from src.ai.llm.openai_client import OpenAILLMClient
from src.core.config import settings
from src.core.exceptions import LLMException

logger = logging.getLogger(__name__)


def _is_claude_model(model: Optional[str]) -> bool:
    """Route to Claude when model name contains 'claude'."""
    if not model:
        return False
    return "claude" in (model or "").lower()


class UnifiedLLMClient(BaseLLM):
    """
    Single client that routes to Claude or OpenAI/Azure by model name.
    Use the same client everywhere; pass model= per call to choose provider.
    """

    def __init__(self):
        self._claude: Optional[ClaudeClient] = None
        self._openai: Optional[OpenAILLMClient] = None

    def _get_claude(self) -> ClaudeClient:
        if self._claude is None:
            self._claude = ClaudeClient()
        return self._claude

    def _get_openai(self) -> OpenAILLMClient:
        if self._openai is None:
            # Prefer Azure when configured
            if settings.azure_openai_endpoint and settings.azure_openai_api_key:
                from openai import AzureOpenAI, AsyncAzureOpenAI
                self._openai = _AzureOpenAILLMClient()
            else:
                self._openai = OpenAILLMClient()
        return self._openai

    def invoke(
        self,
        messages: List[Dict[str, str]],
        **kwargs: Any,
    ) -> str:
        """Invoke LLM; route by kwargs.get('model'). Default model: Claude if key set, else OpenAI."""
        model = kwargs.get("model") or self._default_model()
        if _is_claude_model(model):
            return self._get_claude().invoke(messages, **kwargs)
        return self._get_openai().invoke(messages, **kwargs)

    async def ainvoke(
        self,
        messages: List[Dict[str, str]],
        **kwargs: Any,
    ) -> str:
        """Async invoke; route by kwargs.get('model')."""
        model = kwargs.get("model") or self._default_model()
        if _is_claude_model(model):
            return await self._get_claude().ainvoke(messages, **kwargs)
        return await self._get_openai().ainvoke(messages, **kwargs)

    def _default_model(self) -> str:
        if (settings.claude_api_key or settings.anthropic_api_key):
            return settings.anthropic_model or "claude-sonnet-4-6"
        return settings.azure_openai_deployment_name or settings.openai_model or "gpt-4o-mini"

    def call_llm_unified(
        self,
        model: str,
        system_prompt: str,
        user_prompt: str,
        *,
        node_name: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        default_max_tokens: int = 16000,
        use_json_mode: bool = False,
    ) -> str:
        """
        Synchronous unified call: routes to Claude or OpenAI by model name.
        Matches the pattern: model + system_prompt + user_prompt.
        """
        sys_content = (system_prompt or "").strip()
        if use_json_mode and _is_claude_model(model):
            sys_content = sys_content + "\n\nRespond with valid JSON only; no markdown or extra text." if sys_content else "Respond with valid JSON only; no markdown or extra text."
        messages: List[Dict[str, str]] = []
        if sys_content:
            messages.append({"role": "system", "content": sys_content})
        messages.append({"role": "user", "content": (user_prompt or "").strip()})
        kwargs: Dict[str, Any] = {
            "model": model,
            "max_tokens": max_tokens or default_max_tokens,
        }
        if temperature is not None:
            kwargs["temperature"] = temperature
        if use_json_mode and not _is_claude_model(model):
            kwargs["response_format"] = {"type": "json_object"}
        return self.invoke(messages, **kwargs)

    async def call_llm_unified_async(
        self,
        model: str,
        system_prompt: str,
        user_prompt: str,
        *,
        node_name: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        default_max_tokens: int = 4096,
        use_json_mode: bool = False,
    ) -> str:
        """Async unified call: routes to Claude or OpenAI by model name."""
        sys_content = (system_prompt or "").strip()
        if use_json_mode and _is_claude_model(model):
            sys_content = sys_content + "\n\nRespond with valid JSON only; no markdown or extra text." if sys_content else "Respond with valid JSON only; no markdown or extra text."
        messages: List[Dict[str, str]] = []
        if sys_content:
            messages.append({"role": "system", "content": sys_content})
        messages.append({"role": "user", "content": (user_prompt or "").strip()})
        kwargs: Dict[str, Any] = {
            "model": model,
            "max_tokens": max_tokens or default_max_tokens,
        }
        if temperature is not None:
            kwargs["temperature"] = temperature
        if use_json_mode and not _is_claude_model(model):
            kwargs["response_format"] = {"type": "json_object"}
        return await self.ainvoke(messages, **kwargs)


class _AzureOpenAILLMClient(BaseLLM):
    """Thin wrapper using Azure OpenAI; same invoke/ainvoke interface."""

    def __init__(self):
        from openai import AzureOpenAI, AsyncAzureOpenAI
        self._sync_client = AzureOpenAI(
            api_key=settings.azure_openai_api_key,
            api_version=settings.azure_openai_api_version,
            azure_endpoint=settings.azure_openai_endpoint,
        )
        self._async_client = AsyncAzureOpenAI(
            api_key=settings.azure_openai_api_key,
            api_version=settings.azure_openai_api_version,
            azure_endpoint=settings.azure_openai_endpoint,
        )
        self._model = settings.azure_openai_deployment_name
        self._temperature = settings.azure_openai_temperature
        self._max_tokens = settings.azure_openai_max_tokens

    def _model_for_call(self, kwargs: Dict[str, Any]) -> str:
        return (kwargs.get("model") or self._model or "gpt-4o").strip()

    def invoke(self, messages: List[Dict[str, str]], **kwargs: Any) -> str:
        try:
            model = self._model_for_call(kwargs)
            req: Dict[str, Any] = {
                "model": model,
                "messages": messages,
                "temperature": kwargs.get("temperature", self._temperature),
                "max_tokens": kwargs.get("max_tokens", self._max_tokens),
            }
            if kwargs.get("response_format"):
                req["response_format"] = kwargs["response_format"]
            resp = self._sync_client.chat.completions.create(**req)
            if resp.choices:
                return (resp.choices[0].message.content or "").strip()
            return ""
        except Exception as e:
            logger.warning("Azure OpenAI invoke failed: %s", e)
            raise LLMException(f"Azure OpenAI call failed: {e}") from e

    async def ainvoke(self, messages: List[Dict[str, str]], **kwargs: Any) -> str:
        try:
            model = self._model_for_call(kwargs)
            req: Dict[str, Any] = {
                "model": model,
                "messages": messages,
                "temperature": kwargs.get("temperature", self._temperature),
                "max_tokens": kwargs.get("max_tokens", self._max_tokens),
            }
            if kwargs.get("response_format"):
                req["response_format"] = kwargs["response_format"]
            resp = await self._async_client.chat.completions.create(**req)
            if resp.choices:
                return (resp.choices[0].message.content or "").strip()
            return ""
        except Exception as e:
            logger.warning("Azure OpenAI ainvoke failed: %s", e)
            raise LLMException(f"Azure OpenAI call failed: {e}") from e
