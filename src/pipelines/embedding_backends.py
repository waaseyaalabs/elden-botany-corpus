# pyright: reportMissingImports=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false
# pyright: reportMissingTypeStubs=false

"""Shared embedding backend utilities for lore pipelines."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from importlib import import_module
from typing import Any, Literal, Protocol, cast

ProviderLiteral = Literal["local", "openai"]


class EmbeddingEncoder(Protocol):
    """Minimal protocol for embedding providers."""

    def encode(self, texts: Sequence[str]) -> list[list[float]]:
        """Encode a list of texts into dense vectors."""

        raise NotImplementedError


@dataclass(slots=True)
class EncoderConfig:
    """Configuration parameters for building an encoder."""

    provider: ProviderLiteral
    model_name: str
    batch_size: int
    openai_api_key: str | None = None


def create_encoder(config: EncoderConfig) -> EmbeddingEncoder:
    """Instantiate an embedding encoder for the requested provider."""

    if config.provider == "local":
        return _LocalSentenceTransformerEncoder(
            model_name=config.model_name,
            batch_size=config.batch_size,
        )

    if config.provider == "openai":
        if not config.openai_api_key:
            msg = "OPENAI_API_KEY is required when provider=openai"
            raise ValueError(msg)
        return _OpenAIEncoder(
            model_name=config.model_name,
            batch_size=config.batch_size,
            api_key=config.openai_api_key,
        )

    msg = f"Unsupported embedding provider: {config.provider}"
    raise ValueError(msg)


class _LocalSentenceTransformerEncoder:
    """SentenceTransformer-backed encoder."""

    def __init__(self, model_name: str, batch_size: int) -> None:
        try:
            sentence_transformers = import_module("sentence_transformers")
        except ImportError as err:  # pragma: no cover - import guard
            raise ImportError(
                "sentence-transformers is not installed. "
                "Install with 'poetry add sentence-transformers'"
            ) from err

        self._model = sentence_transformers.SentenceTransformer(model_name)
        self._batch_size = batch_size

    def encode(self, texts: Sequence[str]) -> list[list[float]]:
        if not texts:
            return []

        vectors: Any = self._model.encode(
            list(texts),
            batch_size=self._batch_size,
            show_progress_bar=False,
            convert_to_numpy=True,
        )
        return cast(list[list[float]], vectors.tolist())


class _OpenAIEncoder:
    """OpenAI Embeddings API backed encoder."""

    def __init__(self, model_name: str, batch_size: int, api_key: str) -> None:
        try:
            openai_module = import_module("openai")
        except ImportError as err:  # pragma: no cover - import guard
            raise ImportError(
                (
                    "openai package is not installed. "
                    "Install with 'poetry add openai'"
                )
            ) from err

        OpenAI = cast(Any, openai_module.OpenAI)
        self._model_name = model_name
        self._batch_size = batch_size
        self._client = OpenAI(api_key=api_key)

    def encode(self, texts: Sequence[str]) -> list[list[float]]:
        if not texts:
            return []

        batches: list[list[float]] = []
        for start in range(0, len(texts), self._batch_size):
            batch = list(texts[start:start + self._batch_size])
            response = self._client.embeddings.create(
                input=batch,
                model=self._model_name,
            )
            batch_vectors = [
                [float(value) for value in item.embedding]
                for item in response.data
            ]
            batches.extend(batch_vectors)

        return batches
