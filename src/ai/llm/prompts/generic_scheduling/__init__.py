"""Generic LLM scheduling prompts for non-CHG datasources (LLMSchedulingService)."""
from src.ai.llm.prompts.generic_scheduling.system_prompt import (
    SCHEDULING_OPTIMIZER_SYSTEM,
    MULTI_MACHINE_SCHEDULING_SYSTEM,
)
from src.ai.llm.prompts.generic_scheduling.user_prompt import (
    build_scheduling_user_prompt,
    build_multi_machine_prompt,
)

__all__ = [
    "SCHEDULING_OPTIMIZER_SYSTEM",
    "MULTI_MACHINE_SCHEDULING_SYSTEM",
    "build_scheduling_user_prompt",
    "build_multi_machine_prompt",
]
