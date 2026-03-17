"""Claude (Anthropic or AnthropicFoundry) LLM client. Supports custom endpoint via claude_endpoint."""
import logging
from typing import Any, Dict, List, Optional

from src.ai.llm.base_llm import BaseLLM
from src.core.config import settings

logger = logging.getLogger(__name__)


def _claude_api_key() -> str:
    return (settings.claude_api_key or settings.anthropic_api_key or "").strip()


def _claude_endpoint() -> str:
    base = (settings.claude_endpoint or "").rstrip("/")
    if not base:
        return ""
    if "/anthropic" in base:
        return base
    return f"{base}/anthropic"


def _build_claude_client_sync():
    """Build sync Claude client: AnthropicFoundry if endpoint set, else Anthropic."""
    import anthropic
    api_key = _claude_api_key()
    endpoint = _claude_endpoint()
    if endpoint and api_key:
        try:
            # AnthropicFoundry for custom/base URL (e.g. proxy or Azure Foundry)
            foundry = getattr(anthropic, "AnthropicFoundry", None)
            if foundry is not None:
                return foundry(api_key=api_key, base_url=endpoint)
        except Exception as e:
            logger.debug("AnthropicFoundry not available or failed: %s, using Anthropic", e)
    return anthropic.Anthropic(api_key=api_key) if api_key else None


def _build_claude_client_async():
    """Build async Claude client: AnthropicFoundry if endpoint set, else AsyncAnthropic."""
    import anthropic
    api_key = _claude_api_key()
    endpoint = _claude_endpoint()
    if endpoint and api_key:
        try:
            async_foundry = getattr(anthropic, "AsyncAnthropicFoundry", None)
            if async_foundry is not None:
                return async_foundry(api_key=api_key, base_url=endpoint)
        except Exception as e:
            logger.debug("AsyncAnthropicFoundry not available or failed: %s, using AsyncAnthropic", e)
    return anthropic.AsyncAnthropic(api_key=api_key) if api_key else None


class ClaudeClient(BaseLLM):
    """LLM client using Anthropic Claude API (or AnthropicFoundry when claude_endpoint is set)."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
    ):
        self._api_key = api_key or _claude_api_key()
        self._model = (model or settings.anthropic_model or "claude-sonnet-4-6").strip()

    def _model_for_call(self, kwargs: Dict[str, Any]) -> str:
        return (kwargs.get("model") or self._model).strip()

    def invoke(
        self,
        messages: List[Dict[str, str]],
        **kwargs: Any,
    ) -> str:
        """Synchronous invoke. Pass model= in kwargs for per-call model override."""
        try:
            client = _build_claude_client_sync()
            if not client:
                return "[LLM unavailable: Claude API key not set]"
            system = None
            api_messages = []
            for m in messages:
                role = (m.get("role") or "user").lower()
                content = (m.get("content") or "").strip()
                if role == "system":
                    system = content
                    continue
                if role in ("user", "assistant"):
                    api_messages.append({"role": role, "content": content})
            if not api_messages:
                return ""
            model = self._model_for_call(kwargs)
            resp = client.messages.create(
                model=model,
                max_tokens=kwargs.get("max_tokens", 1024),
                system=system,
                messages=api_messages,
            )
            if resp.content and len(resp.content) > 0:
                block = resp.content[0]
                text = getattr(block, "text", None) or str(block)
                return (text or "").strip()
            return ""
        except Exception as e:
            logger.warning("ClaudeClient.invoke failed: %s", e)
            return f"[LLM unavailable: {e}]"

    async def ainvoke(
        self,
        messages: List[Dict[str, str]],
        **kwargs: Any,
    ) -> str:
        """Async invoke. Pass model= in kwargs for per-call model override."""
        try:
            client = _build_claude_client_async()
            if not client:
                return "[LLM unavailable: Claude API key not set]"
            system = None
            api_messages = []
            for m in messages:
                role = (m.get("role") or "user").lower()
                content = (m.get("content") or "").strip()
                if role == "system":
                    system = content
                    continue
                if role in ("user", "assistant"):
                    api_messages.append({"role": role, "content": content})
            if not api_messages:
                return ""
            model = self._model_for_call(kwargs)
            resp = await client.messages.create(
                model=model,
                max_tokens=kwargs.get("max_tokens", 1024),
                system=system,
                messages=api_messages,
            )
            if resp.content and len(resp.content) > 0:
                block = resp.content[0]
                text = getattr(block, "text", None) or str(block)
                return (text or "").strip()
            return ""
        except Exception as e:
            logger.warning("ClaudeClient.ainvoke failed: %s", e)
            return f"[LLM unavailable: {e}]"
