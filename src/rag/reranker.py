"""Reranking utilities for lore retrieval."""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from types import ModuleType
from typing import TYPE_CHECKING, Any, Protocol, cast

from corpus.config import settings

SentenceCrossEncoder = Any

try:  # pragma: no cover - optional dependency
    import sentence_transformers as _sentence_transformers
except ImportError:  # pragma: no cover - handled at runtime
    sentence_transformers: ModuleType | None = None
else:
    sentence_transformers = _sentence_transformers

if sentence_transformers is not None:
    CrossEncoder: type[SentenceCrossEncoder] | None = cast(
        type[SentenceCrossEncoder],
        getattr(sentence_transformers, "CrossEncoder", None),
    )
else:
    CrossEncoder = None

if TYPE_CHECKING:  # pragma: no cover
    from rag.query import LoreMatch

LOGGER = logging.getLogger(__name__)


class RerankerProtocol(Protocol):
    """Protocol implemented by rerankers that reorder retrieved matches."""

    candidate_pool_size: int | None

    def rerank(
        self,
        query: str,
        matches: Sequence[LoreMatch],
    ) -> list[LoreMatch]:
        """Reorder the provided matches and return a new list."""

        raise NotImplementedError


@dataclass(slots=True)
class RerankerConfig:
    """Configuration for reranker implementations."""

    model_name: str
    batch_size: int = 16
    candidate_pool_size: int = 50
    max_passages: int | None = None
    device: str | None = None


@dataclass(slots=True)
class IdentityReranker:
    """No-op reranker that keeps the original ordering."""

    name: str = "identity"
    candidate_pool_size: int | None = None

    def rerank(
        self,
        _: str,
        matches: Sequence[LoreMatch],
    ) -> list[LoreMatch]:
        return list(matches)


_CrossEncoderFactory = Callable[[str, str | None], SentenceCrossEncoder]


@dataclass(slots=True)
class CrossEncoderReranker:
    """Cross-encoder reranker powered by sentence-transformers."""

    config: RerankerConfig
    name: str = "cross_encoder"
    candidate_pool_size: int | None = field(init=False)
    _model: SentenceCrossEncoder | None = field(default=None, init=False)
    _model_factory: _CrossEncoderFactory | None = field(
        default=None,
        init=False,
    )

    def __post_init__(self) -> None:
        pool = max(0, self.config.candidate_pool_size)
        self.candidate_pool_size = pool or None

    def rerank(
        self,
        query: str,
        matches: Sequence[LoreMatch],
    ) -> list[LoreMatch]:
        if not matches:
            return []

        model = self._load_model()
        limit = min(
            len(matches),
            self.config.max_passages or self.config.candidate_pool_size,
        )
        limit = max(1, limit)
        scored_slice = list(matches[:limit])
        pairs = [(query, candidate.text) for candidate in scored_slice]
        scores = model.predict(pairs, batch_size=self.config.batch_size)

        scored_pairs: list[tuple[float, LoreMatch]] = []
        for candidate, score in zip(scored_slice, scores, strict=False):
            candidate.reranker_score = float(score)
            _append_note(candidate, f"reranker:{self.name}")
            scored_pairs.append((candidate.reranker_score, candidate))

        scored_pairs.sort(key=lambda item: item[0], reverse=True)
        reranked = [candidate for _, candidate in scored_pairs]
        reranked.extend(matches[limit:])
        return reranked

    def _load_model(self) -> SentenceCrossEncoder:
        if self._model is not None:
            return self._model

        factory = self._model_factory or _default_cross_encoder_factory
        self._model = factory(self.config.model_name, self.config.device)
        LOGGER.info(
            "Loaded cross-encoder reranker model %s (device=%s)",
            self.config.model_name,
            self.config.device or "auto",
        )
        return self._model


def _append_note(match: LoreMatch, note: str) -> None:
    if not note:
        return
    existing = getattr(match, "ordering_notes", "")
    match.ordering_notes = (
        f"{existing}; {note}".strip("; ") if existing else note
    )


def _default_cross_encoder_factory(
    model_name: str,
    device: str | None,
) -> SentenceCrossEncoder:  # pragma: no cover - thin wrapper
    if CrossEncoder is None:  # pragma: no cover - defensive
        msg = (
            "sentence-transformers is required for cross-encoder reranking. "
            "Install the 'embeddings-local' extra or add sentence-"
            "transformers to your environment."
        )
        raise ImportError(msg)
    kwargs = {"model_name": model_name}
    if device:
        kwargs["device"] = device
    return CrossEncoder(**kwargs)


def load_reranker(
    name: str | None,
    config: RerankerConfig | None = None,
) -> RerankerProtocol:
    """Return a reranker implementation by name."""

    normalized = (name or settings.reranker_name or "identity").lower()
    if normalized in {"identity", "none"}:
        return IdentityReranker()
    if normalized == "cross_encoder":
        resolved = config or RerankerConfig(
            model_name=settings.reranker_model,
            batch_size=settings.reranker_batch_size,
            candidate_pool_size=settings.reranker_candidate_pool,
        )
        return CrossEncoderReranker(config=resolved)

    msg = f"Unknown reranker: {name}"
    raise ValueError(msg)


__all__ = [
    "CrossEncoderReranker",
    "IdentityReranker",
    "RerankerConfig",
    "RerankerProtocol",
    "load_reranker",
]
