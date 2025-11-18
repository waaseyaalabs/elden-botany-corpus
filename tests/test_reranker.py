# pyright: reportMissingImports=false
# pyright: reportMissingModuleSource=false
# pyright: reportMissingTypeStubs=false

from __future__ import annotations

import pytest

from rag.query import LoreMatch  # type: ignore[import]
from rag.reranker import (  # type: ignore[import]
    IdentityReranker,
    load_reranker,
)


def _sample_matches() -> list[LoreMatch]:
    return [
        LoreMatch(
            lore_id="lore-1",
            text="First match",
            score=0.9,
            canonical_id="canon-1",
            category="item",
            text_type="description",
            source="test",
        ),
        LoreMatch(
            lore_id="lore-2",
            text="Second match",
            score=0.7,
            canonical_id="canon-2",
            category="weapon",
            text_type="effect",
            source="test",
        ),
    ]


def test_identity_reranker_returns_copy() -> None:
    matches = _sample_matches()
    reranked = IdentityReranker().rerank(matches)

    assert reranked == matches
    assert reranked is not matches


def test_load_reranker_handles_none_alias() -> None:
    reranker = load_reranker("none")
    assert isinstance(reranker, IdentityReranker)


def test_load_reranker_validates_names() -> None:
    with pytest.raises(ValueError):
        load_reranker("unknown")
