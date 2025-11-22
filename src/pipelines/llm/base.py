"""LLM connector abstractions and factories."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

DEFAULT_LLM_PROVIDER = "openai"
DEFAULT_LLM_MODEL = "gpt-5-mini"

# Documented options for the OpenAI connector; we do not hard-fail outside
# this list so new models can be introduced without code changes.
ALLOWED_OPENAI_MODELS = (
    "gpt-5-mini",  # default bulk summarization tier
    "gpt-5.1",  # hero / premium tier
    "gpt-4o-mini",  # ultra-cheap debug tier
)


class LLMResponseError(RuntimeError):
    """Raised when an LLM response cannot be parsed or recovered."""


@dataclass(slots=True)
class LLMConfig:
    """Serializable configuration for LLM providers."""

    provider: str
    model: str
    reasoning_effort: str | None = None
    max_output_tokens: int | None = None


class LLMClient(Protocol):
    """Protocol describing the expected LLM connector surface area."""

    config: LLMConfig

    def summarize_entity(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """Return a structured summary for a single canonical entity."""
        ...


def create_llm_client_from_env(
    provider_override: str | None = None,
    model_override: str | None = None,
    reasoning_override: str | None = None,
    max_output_override: int | None = None,
) -> LLMClient:
    """Instantiate the configured client via environment + overrides."""

    provider = (
        provider_override or _env("TB_LLM_PROVIDER") or DEFAULT_LLM_PROVIDER
    )
    model = (model_override or _env("TB_LLM_MODEL")) or DEFAULT_LLM_MODEL
    reasoning = reasoning_override or _env("TB_LLM_REASONING")
    max_output = max_output_override or _env_int("TB_LLM_MAX_OUTPUT_TOKENS")

    config = LLMConfig(
        provider=provider.lower(),
        model=model,
        reasoning_effort=reasoning,
        max_output_tokens=max_output,
    )

    if config.provider == "openai":
        from pipelines.llm.openai_client import OpenAILLMClient

        return OpenAILLMClient(config=config)

    msg = (
        "Unsupported LLM provider '{provider}'. Configure TB_LLM_PROVIDER or "
        "pass --llm-provider."
    ).format(provider=config.provider)
    raise RuntimeError(msg)


def _env(name: str) -> str | None:
    return os.environ.get(name)


def _env_int(name: str) -> int | None:
    value = os.environ.get(name)
    if value is None:
        return None
    try:
        return int(value)
    except ValueError as exc:  # pragma: no cover - defensive guard
        msg = f"Environment variable {name} must be an integer"
        raise RuntimeError(msg) from exc


__all__ = [
    "ALLOWED_OPENAI_MODELS",
    "LLMClient",
    "LLMConfig",
    "LLMResponseError",
    "create_llm_client_from_env",
]
