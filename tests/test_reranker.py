# pyright: reportMissingImports=false
# pyright: reportMissingModuleSource=false
# pyright: reportMissingTypeStubs=false

from __future__ import annotations

import pytest
from rag.query import LoreMatch  # type: ignore[import]
from rag.reranker import (  # type: ignore[import]
    CrossEncoderReranker,
    IdentityReranker,
    RerankerConfig,
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
    reranked = IdentityReranker().rerank("query", matches)

    assert reranked == matches
    assert reranked is not matches


def test_load_reranker_handles_none_alias() -> None:
    reranker = load_reranker("none")
    assert isinstance(reranker, IdentityReranker)


def test_load_reranker_validates_names() -> None:
    with pytest.raises(ValueError):
        load_reranker("unknown")


class _FakeCrossEncoder:
    def __init__(self, scores: list[float]) -> None:
        self._scores = scores
        self.calls: list[tuple[str, str]] = []

    def predict(
        self,
        pairs: list[tuple[str, str]],
        *,
        batch_size: int,
    ) -> list[float]:
        del batch_size
        self.calls.extend(pairs)
        return self._scores[: len(pairs)]


def test_cross_encoder_reranker_orders_by_model_scores() -> None:
    matches = _sample_matches() + [
        LoreMatch(
            lore_id="lore-3",
            text="Third match",
            score=0.6,
            canonical_id="canon-3",
            category="spell",
            text_type="dialogue",
            source="test",
        )
    ]
    config = RerankerConfig(
        model_name="unit-test",
        batch_size=2,
        candidate_pool_size=3,
    )
    reranker = CrossEncoderReranker(config=config)
    fake_model = _FakeCrossEncoder([0.2, 0.9, 0.5])

    def _factory(*_: object) -> _FakeCrossEncoder:
        return fake_model

    reranker._model_factory = _factory  # type: ignore[attr-defined]

    reordered = reranker.rerank("query", matches)

    assert [match.lore_id for match in reordered[:3]] == [
        "lore-2",
        "lore-3",
        "lore-1",
    ]
    assert reranker.candidate_pool_size == 3
