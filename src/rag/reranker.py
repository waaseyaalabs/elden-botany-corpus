"""Simple reranking interfaces for lore retrieval."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:  # pragma: no cover
    from rag.query import LoreMatch


class RerankerProtocol(Protocol):
    """Protocol implemented by rerankers that reorder retrieved matches."""

    def rerank(self, matches: Sequence["LoreMatch"]) -> list["LoreMatch"]:
        """Reorder the provided matches and return a new list."""

        raise NotImplementedError


@dataclass(slots=True)
class IdentityReranker:
    """No-op reranker that keeps the original ordering."""

    name: str = "identity"

    def rerank(self, matches: Sequence["LoreMatch"]) -> list["LoreMatch"]:
        return list(matches)


def load_reranker(name: str | None) -> RerankerProtocol:
    """Return a reranker implementation by name."""

    normalized = (name or "identity").lower()
    if normalized in {"identity", "none"}:
        return IdentityReranker()

    msg = f"Unknown reranker: {name}"
    raise ValueError(msg)


__all__ = ["IdentityReranker", "RerankerProtocol", "load_reranker"]
