"""Azure OpenAI and Claude unified client wrapper."""
from typing import List, Dict, Any, Optional
from openai import AzureOpenAI, AsyncAzureOpenAI, OpenAI, AsyncOpenAI
import logging
import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
from anthropic import AnthropicFoundry
from config.settings import settings
from shared.exceptions import LLMException
from ..langgraph.prompts import (
    SQL_GENERATION_SYSTEM_PROMPT,
    get_sql_generation_user_prompt,
)

logger = logging.getLogger(__name__)


class AzureOpenAIClient:
    """Unified LLM client wrapper that routes to Azure OpenAI or Claude endpoints based on model name."""
    
    def __init__(self):
        """Initialize unified LLM client."""
        # Initialize Azure OpenAI client (for OpenAI models)
        if settings.azure_openai_endpoint and settings.azure_openai_api_key:
            self.openai_client = AsyncAzureOpenAI(
                api_key=settings.azure_openai_api_key,
                api_version=settings.azure_openai_api_version,
                azure_endpoint=settings.azure_openai_endpoint,
            )
        else:
            self.openai_client = None
            logger.warning("Azure OpenAI configuration is missing. OpenAI models will not work.")
        
        # Claude configuration (for Claude models) - match chatbot pattern
        self.claude_api_key = settings.claude_api_key
        claude_endpoint_base = settings.claude_endpoint.rstrip('/')
        # Claude endpoint format: {base}/anthropic (full URL for AnthropicFoundry base_url)
        # If endpoint already contains /anthropic, use as-is, otherwise append it
        if "/anthropic" in claude_endpoint_base:
            self.claude_endpoint = claude_endpoint_base
        else:
            self.claude_endpoint = f"{claude_endpoint_base}/anthropic"
        self._claude_client = None
        
        if not self.claude_api_key:
            logger.warning("Claude API key not provided. Claude models will not work. Set CLAUDE_API_KEY environment variable.")
        
        # DeepSeek configuration (backup for Sonnet)
        # DeepSeek uses the same API key as Azure OpenAI (GPT-4o)
        self.deepseek_api_key = settings.azure_openai_api_key  # Use same key as GPT-4o
        self.deepseek_endpoint = settings.deepseek_endpoint.rstrip('/')
        self.deepseek_model_name = settings.deepseek_model_name
        self.deepseek_deployment_name = settings.deepseek_deployment_name
        self._deepseek_client = None
        
        if not self.deepseek_api_key:
            logger.warning("DeepSeek fallback will not work. Azure OpenAI API key is required (same as GPT-4o).")
        
        self.deployment_name = settings.azure_openai_deployment_name
        self.temperature = settings.azure_openai_temperature
        self.max_tokens = settings.azure_openai_max_tokens
        
        logger.info(f"Unified LLM client initialized - OpenAI endpoint: {settings.azure_openai_endpoint}, Claude endpoint: {self.claude_endpoint}, DeepSeek endpoint: {self.deepseek_endpoint}")

    def _log_token_utilization(
        self,
        provider: str,
        model: str,
        node_name: Optional[str],
        query_id: Optional[str],
        usage: Optional[Dict[str, Any]],
        temperature: Optional[float],
        max_tokens: Optional[int],
        use_json_mode: bool,
    ) -> None:
        """
        Log token utilization and configuration for an LLM call in a structured way.

        Every node that calls `_call_llm_unified` will automatically emit
        a single `[LLM_USAGE]` JSON line that can be used for per-node analytics.
        """
        try:
            # Normalize common usage fields if available so downstream analytics
            # can rely on consistent keys across providers.
            normalized_usage: Dict[str, Any] = {}
            if usage:
                # Azure OpenAI-style usage
                if "prompt_tokens" in usage or "completion_tokens" in usage or "total_tokens" in usage:
                    normalized_usage["input_tokens"] = usage.get("prompt_tokens")
                    normalized_usage["output_tokens"] = usage.get("completion_tokens")
                    normalized_usage["total_tokens"] = usage.get("total_tokens")
                # Anthropic / Claude-style usage
                if "input_tokens" in usage or "output_tokens" in usage:
                    normalized_usage.setdefault("input_tokens", usage.get("input_tokens"))
                    normalized_usage.setdefault("output_tokens", usage.get("output_tokens"))
                    if "total_tokens" in usage:
                        normalized_usage.setdefault("total_tokens", usage.get("total_tokens"))

            # Prepare normalized numeric token counts for DB as well
            input_tokens = int((normalized_usage.get("input_tokens") or 0) or 0)
            output_tokens = int((normalized_usage.get("output_tokens") or 0) or 0)
            total_tokens = int((normalized_usage.get("total_tokens") or 0) or (input_tokens + output_tokens))

            payload: Dict[str, Any] = {
                "event": "llm_token_usage",
                "provider": provider,
                "node_name": node_name or "unknown",
                "model": model,
                "timestamp": datetime.utcnow().isoformat(),
                "config": {
                    "temperature": temperature,
                    "max_tokens": max_tokens,
                    "use_json_mode": use_json_mode,
                },
                "usage_raw": usage,
                "usage_normalized": normalized_usage or None,
            }

            # Single-line JSON payload for easy parsing and downstream aggregation
            logger.info(f"[LLM_USAGE] {json.dumps(payload, default=str)}")

            # Accumulate token usage in registry for batch database update at query completion
            # All token usage is saved in a single batch operation after query completes
            try:
                from .token_usage_registry import get_token_usage_registry
                registry = get_token_usage_registry()
                
                if registry:
                    # Accumulate in registry for batch update at query completion
                    registry.add_usage(
                        provider=provider,
                        model=model,
                        node_name=node_name or "unknown",
                        input_tokens=input_tokens,
                        output_tokens=output_tokens,
                        total_tokens=total_tokens,
                        config={
                            "temperature": temperature,
                            "max_tokens": max_tokens,
                            "use_json_mode": use_json_mode,
                        },
                    )
                    logger.debug(f"[LLM] Token usage accumulated in registry for {node_name} (will be saved in batch)")
                else:
                    # No registry available - log warning but don't save (batch update will handle it)
                    logger.debug(f"[LLM] No token usage registry available for {node_name}, token usage will not be saved individually")
            except Exception as registry_error:
                logger.debug(f"Failed to accumulate token usage in registry: {registry_error}")
        except Exception as e:
            # Never let analytics logging break the main flow
            logger.debug(f"Failed to log LLM token utilization: {e}")

    def _is_claude_model(self, model: str) -> bool:
        """Check if model name indicates Claude model (only Claude models, not o-series which are OpenAI)."""
        if not model:
            return False
        # Only Claude models use Claude API - o3, o1, gpt models are OpenAI models
        return "claude" in model.lower()
    
    def _is_deepseek_model(self, model: str) -> bool:
        """Check if model name indicates DeepSeek model."""
        if not model:
            return False
        return "deepseek" in model.lower() or model == self.deepseek_deployment_name
    
    def _get_deepseek_client(self) -> AsyncOpenAI:
        """Get AsyncOpenAI client for DeepSeek API calls. Uses same API key as Azure OpenAI (GPT-4o)."""
        if self._deepseek_client is None:
            if not self.deepseek_api_key:
                raise LLMException("DeepSeek API key not provided. Azure OpenAI API key is required (same as GPT-4o).")
            self._deepseek_client = AsyncOpenAI(
                base_url=self.deepseek_endpoint,
                api_key=self.deepseek_api_key  # Same API key as Azure OpenAI
            )
        return self._deepseek_client
    
    def _get_claude_client(self) -> AnthropicFoundry:
        """Get AnthropicFoundry client for Claude API calls."""
        if self._claude_client is None:
            if not self.claude_api_key:
                raise LLMException("Claude API key not provided. Set CLAUDE_API_KEY environment variable.")
            self._claude_client = AnthropicFoundry(
                api_key=self.claude_api_key,
                base_url=self.claude_endpoint
            )
        return self._claude_client
    
    async def _call_claude_api(
        self,
        model: str,
        system_prompt: str,
        user_prompt: str,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        use_json_mode: bool = False,
        node_name: Optional[str] = None,
    ) -> tuple[str, Optional[Dict[str, Any]]]:
        """
        Call Claude API using AnthropicFoundry SDK with real-time streaming
        of both thinking blocks and text blocks.
        
        When settings.llm_thinking_enabled is True, extended thinking is enabled
        and thinking tokens are streamed via LLM_THINKING_STREAM messages.
        Text tokens are always streamed via LLM_STREAM messages.
        """
        if not self.claude_api_key:
            raise LLMException("Claude API key not provided. Set CLAUDE_API_KEY environment variable.")
        
        client = self._get_claude_client()
        
        messages = [{"role": "user", "content": user_prompt}]
        max_tokens_val = max_tokens or 16000
        temperature_val = temperature if temperature is not None else 0.0
        
        request_params = {
            "model": model,
            "messages": messages,
            "max_tokens": max_tokens_val,
            "temperature": temperature_val
        }
        
        if system_prompt:
            request_params["system"] = system_prompt
        
        if use_json_mode:
            json_instruction = "IMPORTANT: You must respond with valid JSON only. Do not include any markdown formatting, code blocks, or explanations outside the JSON."
            if system_prompt:
                request_params["system"] = f"{system_prompt}\n\n{json_instruction}"
            else:
                request_params["system"] = json_instruction
        
        # Enable extended thinking when configured
        thinking_enabled = settings.llm_thinking_enabled
        if thinking_enabled:
            budget = settings.llm_thinking_budget_tokens
            request_params["thinking"] = {"type": "enabled", "budget_tokens": budget}
            request_params["temperature"] = 1  # Required by Anthropic when thinking is on
            logger.info(f"[Claude API] Extended thinking enabled: budget_tokens={budget}")
        
        try:
            logger.info(f"Calling Claude API (streaming, thinking={'on' if thinking_enabled else 'off'}): model={model}")
            logger.debug(f"Request params: model={model}, max_tokens={max_tokens_val}, temperature={request_params['temperature']}, use_json_mode={use_json_mode}")
            
            import queue as _queue
            import threading
            from ..websocket.ws_streaming_registry import (
                get_ws_streaming_manager, forward_llm_chunk, forward_llm_stream_end,
                forward_llm_thinking_chunk, forward_llm_thinking_end,
            )

            ws_mgr = get_ws_streaming_manager()
            chunk_q: _queue.Queue = _queue.Queue()
            _SENTINEL = object()

            def _stream_claude_sync():
                """Iterate raw stream events to capture both thinking_delta and
                text_delta, then get the final message for usage data."""
                try:
                    with client.messages.stream(**request_params) as stream:
                        for event in stream:
                            event_type = getattr(event, "type", None)
                            if event_type == "content_block_delta":
                                delta = getattr(event, "delta", None)
                                if delta is None:
                                    continue
                                delta_type = getattr(delta, "type", None)
                                if delta_type == "thinking_delta":
                                    chunk_q.put(("thinking", getattr(delta, "thinking", "")))
                                elif delta_type == "text_delta":
                                    chunk_q.put(("text", getattr(delta, "text", "")))
                            # Ignore other event types (content_block_start, stop, signature_delta, etc.)
                        final_msg = stream.get_final_message()
                        chunk_q.put(("__final__", final_msg))
                except Exception as e:
                    chunk_q.put(e)
                finally:
                    chunk_q.put(_SENTINEL)

            thread = threading.Thread(target=_stream_claude_sync, daemon=True)
            thread.start()

            accumulated_text = ""
            accumulated_thinking = ""
            message = None
            text_chunk_index = 0
            thinking_chunk_index = 0

            while True:
                while chunk_q.empty():
                    await asyncio.sleep(0.01)
                item = chunk_q.get()

                if item is _SENTINEL:
                    break
                if isinstance(item, Exception):
                    logger.error(f"[Claude API] Stream error: {item}")
                    raise LLMException(f"Claude API streaming failed: {item}") from item
                if isinstance(item, tuple) and item[0] == "__final__":
                    message = item[1]
                    continue

                chunk_type, chunk_text = item
                if chunk_type == "thinking":
                    accumulated_thinking += chunk_text
                    if ws_mgr:
                        await forward_llm_thinking_chunk(chunk_text, node_name or "claude", thinking_chunk_index)
                        thinking_chunk_index += 1
                elif chunk_type == "text":
                    accumulated_text += chunk_text
                    if ws_mgr:
                        await forward_llm_chunk(chunk_text, node_name or "claude", text_chunk_index)
                        text_chunk_index += 1

            # Signal end of streams
            if ws_mgr:
                if thinking_chunk_index > 0:
                    await forward_llm_thinking_end(node_name or "claude", accumulated_thinking)
                    logger.debug(f"[Claude API] Streamed {thinking_chunk_index} thinking chunks for node={node_name}")
                if text_chunk_index > 0:
                    await forward_llm_stream_end(node_name or "claude", accumulated_text)
                    logger.debug(f"[Claude API] Streamed {text_chunk_index} text chunks for node={node_name}")

            logger.debug(f"[Claude API] Streaming completed: text={len(accumulated_text)} chars, thinking={len(accumulated_thinking)} chars")

            usage_data: Optional[Dict[str, Any]] = None
            if message and hasattr(message, "usage") and message.usage is not None:
                try:
                    usage_attr = message.usage
                    if hasattr(usage_attr, "model_dump"):
                        usage_data = usage_attr.model_dump()
                    elif isinstance(usage_attr, dict):
                        usage_data = usage_attr
                    else:
                        usage_data = dict(usage_attr)
                except Exception as usage_error:
                    logger.debug(f"Failed to extract Claude usage data: {usage_error}")

            content = accumulated_text.strip()
            if not content and message:
                if hasattr(message, 'content') and message.content:
                    for block in message.content:
                        block_type = getattr(block, 'type', None)
                        if block_type == 'text':
                            content = getattr(block, 'text', '') or ''
                            content = content.strip()
                            break

            if not content:
                raise LLMException("Claude API returned empty content")

            logger.debug(f"Claude API response received: {len(content)} characters")
            return content, usage_data

        except LLMException:
            raise
        except Exception as e:
            logger.error(f"Error calling Claude API: {str(e)}", exc_info=True)
            raise LLMException(f"Claude API call failed: {str(e)}") from e
    
    async def _call_deepseek_api(
        self,
        model: str,
        system_prompt: str,
        user_prompt: str,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        use_json_mode: bool = False,
        node_name: Optional[str] = None,
    ) -> tuple[str, Optional[Dict[str, Any]]]:
        """
        Call DeepSeek API with real-time streaming of both reasoning_content
        (chain-of-thought) and content (final answer).
        
        DeepSeek R1 returns reasoning_content in streaming deltas before the
        final content. Both are forwarded to the UI via the ws_streaming_registry.
        """
        if not self.deepseek_api_key:
            raise LLMException("DeepSeek API key not provided. Set DEEPSEEK_API_KEY environment variable.")
        
        from ..websocket.ws_streaming_registry import (
            get_ws_streaming_manager, forward_llm_chunk, forward_llm_stream_end,
            forward_llm_thinking_chunk, forward_llm_thinking_end,
        )

        client = self._get_deepseek_client()
        
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": user_prompt})
        
        if use_json_mode:
            json_instruction = "IMPORTANT: You must respond with valid JSON only. Do not include any markdown formatting, code blocks, or explanations outside the JSON."
            if system_prompt:
                if messages and messages[0].get("role") == "system":
                    messages[0]["content"] = f"{system_prompt}\n\n{json_instruction}"
                else:
                    messages.insert(0, {"role": "system", "content": json_instruction})
            else:
                messages.insert(0, {"role": "system", "content": json_instruction})
        
        max_tokens_val = max_tokens or 16000
        temperature_val = temperature if temperature is not None else 0.0
        
        request_params = {
            "model": model,
            "messages": messages,
            "max_tokens": max_tokens_val,
            "temperature": temperature_val,
            "stream": True,
        }
        
        try:
            logger.info(f"Calling DeepSeek API (streaming): model={model}, endpoint={self.deepseek_endpoint}")
            logger.debug(f"Request params: model={model}, max_tokens={max_tokens_val}, temperature={temperature_val}, use_json_mode={use_json_mode}")

            ws_mgr = get_ws_streaming_manager()
            accumulated_content = ""
            accumulated_reasoning = ""
            usage_data: Optional[Dict[str, Any]] = None
            text_chunk_index = 0
            thinking_chunk_index = 0

            response = await client.chat.completions.create(**request_params)

            async for chunk in response:
                if chunk.choices and chunk.choices[0].delta:
                    delta = chunk.choices[0].delta

                    # DeepSeek R1 reasoning_content (chain-of-thought)
                    reasoning = getattr(delta, "reasoning_content", None)
                    if reasoning:
                        accumulated_reasoning += reasoning
                        if ws_mgr:
                            await forward_llm_thinking_chunk(reasoning, node_name or "deepseek", thinking_chunk_index)
                            thinking_chunk_index += 1

                    # Regular content (final answer)
                    if delta.content:
                        accumulated_content += delta.content
                        if ws_mgr:
                            await forward_llm_chunk(delta.content, node_name or "deepseek", text_chunk_index)
                            text_chunk_index += 1

                if getattr(chunk, "usage", None) is not None:
                    try:
                        usage_attr = chunk.usage
                        if hasattr(usage_attr, "model_dump"):
                            usage_data = usage_attr.model_dump()
                        elif isinstance(usage_attr, dict):
                            usage_data = usage_attr
                        else:
                            usage_data = dict(usage_attr)
                    except Exception as usage_error:
                        logger.debug(f"Failed to extract DeepSeek usage data: {usage_error}")

            if ws_mgr:
                if thinking_chunk_index > 0:
                    await forward_llm_thinking_end(node_name or "deepseek", accumulated_reasoning)
                    logger.debug(f"[DeepSeek API] Streamed {thinking_chunk_index} reasoning chunks for node={node_name}")
                if text_chunk_index > 0:
                    await forward_llm_stream_end(node_name or "deepseek", accumulated_content)
                    logger.debug(f"[DeepSeek API] Streamed {text_chunk_index} text chunks for node={node_name}")

            logger.debug(f"[DeepSeek API] Streaming completed: text={len(accumulated_content)} chars, reasoning={len(accumulated_reasoning)} chars")

            if not accumulated_content:
                logger.error(f"DeepSeek returned empty response")
                raise LLMException("DeepSeek returned empty response")

            logger.debug(f"DeepSeek API response received: {len(accumulated_content)} characters")
            return accumulated_content.strip(), usage_data
            
        except LLMException:
            raise
        except Exception as e:
            logger.error(f"Error calling DeepSeek API: {str(e)}", exc_info=True)
            raise LLMException(f"DeepSeek API call failed: {str(e)}") from e
    
    async def _call_llm_unified(
        self,
        model: str,
        system_prompt: str,
        user_prompt: str,
        node_name: Optional[str] = None,
        query_id: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        default_max_tokens: int = 16000,
        use_json_mode: bool = False
    ) -> str:
        """
        Unified method to call either Claude or OpenAI API based on model type (matching chatbot pattern).

        Args:
            model: Model name (Claude or OpenAI)
            system_prompt: System prompt
            user_prompt: User prompt
            node_name: Optional node name for logging (nodes save prompts via save_llm_call_input to prompts/input/)
            temperature: Optional temperature override
            max_tokens: Optional max tokens override
            default_max_tokens: Default max tokens if not specified
            use_json_mode: Whether to use JSON mode

        Returns:
            Response content string
        """
        logger.info(f"Calling LLM for node: {node_name or 'unknown'}, model: {model}")

        if self._is_claude_model(model):
            logger.info(f"Routing to Claude API for model: {model}")
            content, usage = await self._call_claude_api(
                model=model,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=temperature,
                max_tokens=max_tokens or default_max_tokens,
                use_json_mode=use_json_mode,
                node_name=node_name,
            )
            self._log_token_utilization(
                provider="claude",
                model=model,
                node_name=node_name,
                query_id=query_id,
                usage=usage,
                temperature=temperature if temperature is not None else 0.0,
                max_tokens=max_tokens or default_max_tokens,
                use_json_mode=use_json_mode,
            )
            return content
        elif self._is_deepseek_model(model):
            logger.info(f"Routing to DeepSeek API for model: {model}")
            content, usage = await self._call_deepseek_api(
                model=model,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=temperature,
                max_tokens=max_tokens or default_max_tokens,
                use_json_mode=use_json_mode,
                node_name=node_name,
            )
            self._log_token_utilization(
                provider="deepseek",
                model=model,
                node_name=node_name,
                query_id=query_id,
                usage=usage,
                temperature=temperature if temperature is not None else 0.0,
                max_tokens=max_tokens or default_max_tokens,
                use_json_mode=use_json_mode,
            )
            return content
        else:
            logger.info(f"Routing to OpenAI API for model: {model}")
            content, usage = await self._call_openai_api(
                model=model,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=temperature,
                max_tokens=max_tokens or default_max_tokens,
                use_json_mode=use_json_mode,
                node_name=node_name,
            )
            self._log_token_utilization(
                provider="openai",
                model=model,
                node_name=node_name,
                query_id=query_id,
                usage=usage,
                temperature=temperature if temperature is not None else self.temperature,
                max_tokens=max_tokens or default_max_tokens,
                use_json_mode=use_json_mode,
            )
            return content
    
    async def _call_openai_api(
        self,
        model: str,
        system_prompt: str,
        user_prompt: str,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        use_json_mode: bool = False,
        node_name: Optional[str] = None,
    ) -> tuple[str, Optional[Dict[str, Any]]]:
        """
        Call Azure OpenAI API with real-time token streaming.
        
        Tokens are forwarded to the UI via the ws_streaming_registry as they arrive.
        """
        if not self.openai_client:
            raise LLMException("Azure OpenAI client not initialized. Please set AZURE_OPENAI_ENDPOINT and AZURE_OPENAI_API_KEY.")
        
        from ..websocket.ws_streaming_registry import get_ws_streaming_manager, forward_llm_chunk, forward_llm_stream_end

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]
        
        is_o_model = "o3" in model.lower() or "o1" in model.lower()
        max_tokens_param = "max_completion_tokens" if is_o_model else "max_tokens"

        request_params = {
            "model": model,
            "messages": messages,
            max_tokens_param: max_tokens or self.max_tokens,
            "stream": True,
            "stream_options": {"include_usage": True},
        }
        
        if not is_o_model:
            request_params["temperature"] = temperature if temperature is not None else self.temperature
        
        if use_json_mode and "o1" not in model.lower() and "o3" not in model.lower() and "gpt" in model.lower():
            request_params["response_format"] = {"type": "json_object"}
        
        logger.debug(f"Calling Azure OpenAI (streaming) - Model: {model}, Messages: {len(messages)}")

        ws_mgr = get_ws_streaming_manager()
        accumulated_content = ""
        usage_data: Optional[Dict[str, Any]] = None
        chunk_index = 0

        response = await self.openai_client.chat.completions.create(**request_params)

        async for chunk in response:
            if chunk.choices and chunk.choices[0].delta and chunk.choices[0].delta.content:
                text = chunk.choices[0].delta.content
                accumulated_content += text
                if ws_mgr:
                    await forward_llm_chunk(text, node_name or "openai", chunk_index)
                    chunk_index += 1

            if getattr(chunk, "usage", None) is not None:
                try:
                    usage_attr = chunk.usage
                    if hasattr(usage_attr, "model_dump"):
                        usage_data = usage_attr.model_dump()
                    elif isinstance(usage_attr, dict):
                        usage_data = usage_attr
                    else:
                        usage_data = dict(usage_attr)
                except Exception as usage_error:
                    logger.debug(f"Failed to extract Azure OpenAI usage data: {usage_error}")

        if ws_mgr and chunk_index > 0:
            await forward_llm_stream_end(node_name or "openai", accumulated_content)
            logger.debug(f"[OpenAI API] Streamed {chunk_index} chunks to UI for node={node_name}")

        if not accumulated_content:
            logger.error(f"Azure OpenAI returned empty response")
            raise LLMException("Azure OpenAI returned empty response")

        logger.debug(f"Azure OpenAI response received - Token usage: {usage_data}")
        return accumulated_content.strip(), usage_data
    
    async def chat_completion(
        self,
        messages: List[Dict[str, str]],
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        model: Optional[str] = None,
    ) -> str:
        """
        Get chat completion from Azure OpenAI or Claude (unified routing based on model name).
        Legacy method - prefer using _call_llm_unified() for new code.
        
        Args:
            messages: List of message dictionaries with 'role' and 'content'
            temperature: Override default temperature
            max_tokens: Override default max_tokens
            model: Model name (node-specific model) - routes to Claude if contains "claude"
            
        Returns:
            Response content string
            
        Raises:
            LLMException: If API call fails
        """
        try:
            deployment = model or self.deployment_name
            temp = temperature if temperature is not None else self.temperature
            tokens = max_tokens if max_tokens is not None else self.max_tokens
            
            # Extract system and user prompts from messages
            system_prompt = ""
            user_prompt = ""
            for msg in messages:
                if msg.get("role") == "system":
                    system_prompt = msg.get("content", "")
                elif msg.get("role") == "user":
                    user_prompt = msg.get("content", "")
            
            # Use unified method
            return await self._call_llm_unified(
                model=deployment,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                node_name=None,
                temperature=temp,
                max_tokens=tokens,
                default_max_tokens=tokens,
                use_json_mode=False
            )
            
        except Exception as e:
            logger.error(f"LLM API call failed: {str(e)}", exc_info=True)
            raise LLMException(f"LLM API call failed: {str(e)}") from e
    
    async def generate_sql(
        self,
        query: str,
        schema_info: Dict[str, Any],
        examples: Optional[List[Dict[str, str]]] = None,
    ) -> str:
        """
        Generate SQL from natural language query.
        
        Args:
            query: Natural language query
            schema_info: Database schema information
            examples: Optional few-shot examples
            
        Returns:
            Generated SQL query string
        """
        # Use prompts from prompts.py (matching chatbot pattern)
        system_prompt = SQL_GENERATION_SYSTEM_PROMPT
        
        # Format schema context
        schema_text = self._format_schema(schema_info)
        examples_text = self._format_examples(examples) if examples else ""
        schema_context = f"{schema_text}\n\n{examples_text}".strip()
        
        # Use prompt helper function from prompts.py
        # Note: This is a legacy method - new code should use SQL plan
        user_prompt = get_sql_generation_user_prompt(
            sql_plan=None,  # No SQL plan for this legacy method
            database_name=None
        )
        
        # Use unified method for proper routing
        sql = await self._call_llm_unified(
            model=self.deployment_name,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            node_name=None,
            temperature=0.0,
            default_max_tokens=4000,
            use_json_mode=False  # SQL queries should be plain text
        )
        
        # Clean up SQL - remove markdown code blocks if present
        sql = sql.strip()
        if sql.startswith("```"):
            sql = sql.split("```")[1]
            if sql.startswith("sql"):
                sql = sql[3:]
            sql = sql.strip()
        
        return sql
    
    def _format_schema(self, schema_info: Dict[str, Any]) -> str:
        """Format schema information for prompt."""
        lines = []
        for table_name, columns in schema_info.items():
            lines.append(f"Table: {table_name}")
            for col_name, col_type in columns.items():
                lines.append(f"  - {col_name}: {col_type}")
        return "\n".join(lines)
    
    def _format_examples(self, examples: List[Dict[str, str]]) -> str:
        """Format few-shot examples for prompt."""
        lines = ["Examples:"]
        for ex in examples:
            lines.append(f"Query: {ex['query']}")
            lines.append(f"SQL: {ex['sql']}\n")
        return "\n".join(lines)
    
    def _format_dict(self, data: Dict[str, Any], indent: int = 0) -> str:
        """Format dictionary for prompt."""
        lines = []
        indent_str = "  " * indent
        for key, value in data.items():
            if isinstance(value, dict):
                lines.append(f"{indent_str}{key}:")
                lines.append(self._format_dict(value, indent + 1))
            elif isinstance(value, list):
                lines.append(f"{indent_str}{key}: {value}")
            else:
                lines.append(f"{indent_str}{key}: {value}")
        return "\n".join(lines)

    # -------------------------------------------------------------------------
    # Token-level streaming generators (yield text chunks as they arrive)
    # -------------------------------------------------------------------------

    async def stream_llm_response(
        self,
        model: str,
        system_prompt: str,
        user_prompt: str,
        node_name: Optional[str] = None,
        query_id: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
    ):
        """
        Async generator that yields text chunks as they arrive from the LLM.
        
        Use for user-facing narrative text where you want real-time streaming
        to the UI. Do NOT use for JSON-mode structured output (use _call_llm_unified
        for those — JSON tokens are meaningless to users).
        
        Usage:
            async for chunk in llm_client.stream_llm_response(model=..., ...):
                await ws_manager.send_llm_stream_chunk(chunk)
        """
        logger.info(f"[LLM Streaming] Starting token stream for node={node_name or 'unknown'}, model={model}")

        if self._is_claude_model(model):
            async for chunk in self._stream_claude_tokens(
                model=model,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=temperature,
                max_tokens=max_tokens or 16000,
            ):
                yield chunk
        elif self._is_deepseek_model(model):
            async for chunk in self._stream_openai_compatible_tokens(
                client=self._get_deepseek_client(),
                model=model,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=temperature,
                max_tokens=max_tokens or 16000,
            ):
                yield chunk
        else:
            if not self.openai_client:
                raise LLMException("Azure OpenAI client not initialized.")
            async for chunk in self._stream_openai_compatible_tokens(
                client=self.openai_client,
                model=model,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=temperature,
                max_tokens=max_tokens or self.max_tokens,
            ):
                yield chunk

    async def _stream_claude_tokens(
        self,
        model: str,
        system_prompt: str,
        user_prompt: str,
        temperature: Optional[float] = None,
        max_tokens: int = 16000,
    ):
        """Yield text chunks from Claude streaming API via a background thread."""
        import queue
        import threading

        client = self._get_claude_client()
        messages = [{"role": "user", "content": user_prompt}]
        temperature_val = temperature if temperature is not None else 0.0

        request_params = {
            "model": model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature_val,
        }
        if system_prompt:
            request_params["system"] = system_prompt

        chunk_queue: queue.Queue = queue.Queue()
        _SENTINEL = object()

        def _run_stream():
            try:
                with client.messages.stream(**request_params) as stream:
                    for text in stream.text_stream:
                        chunk_queue.put(text)
            except Exception as e:
                chunk_queue.put(e)
            finally:
                chunk_queue.put(_SENTINEL)

        thread = threading.Thread(target=_run_stream, daemon=True)
        thread.start()

        while True:
            while chunk_queue.empty():
                await asyncio.sleep(0.01)
            item = chunk_queue.get()
            if item is _SENTINEL:
                break
            if isinstance(item, Exception):
                logger.error(f"[LLM Streaming] Claude stream error: {item}")
                raise LLMException(f"Claude streaming failed: {item}") from item
            yield item

    async def _stream_openai_compatible_tokens(
        self,
        client,
        model: str,
        system_prompt: str,
        user_prompt: str,
        temperature: Optional[float] = None,
        max_tokens: int = 16000,
    ):
        """Yield text chunks from OpenAI-compatible streaming API (Azure OpenAI / DeepSeek)."""
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": user_prompt})

        is_o_model = "o3" in model.lower() or "o1" in model.lower()
        max_tokens_param = "max_completion_tokens" if is_o_model else "max_tokens"

        request_params = {
            "model": model,
            "messages": messages,
            max_tokens_param: max_tokens,
            "stream": True,
        }
        if not is_o_model:
            request_params["temperature"] = temperature if temperature is not None else self.temperature

        logger.debug(f"[LLM Streaming] OpenAI-compatible stream starting: model={model}")
        response = await client.chat.completions.create(**request_params)

        async for chunk in response:
            if chunk.choices and chunk.choices[0].delta and chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content

