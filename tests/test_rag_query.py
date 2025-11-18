# pyright: reportMissingImports=false
# pyright: reportMissingModuleSource=false
# pyright: reportMissingTypeStubs=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pandas as pd  # type: ignore[import]
import pytest
from rag.query import (  # type: ignore[import]
    FilterExpression,
    LoreMatch,
    query_lore,
)

from pipelines.build_lore_embeddings import (  # type: ignore[import]
    build_lore_embeddings,
)
from pipelines.build_rag_index import (  # type: ignore[import]
    build_rag_index,
)

from .helpers import DeterministicEncoder, write_sample_lore_corpus


class _StubQueryHelper:
    def __init__(self, frame: pd.DataFrame) -> None:
        self._frame = frame
        self.calls: list[int] = []

    def query(
        self,
        _: str,
        *,
        top_k: int,
        filter_by: object | None = None,
    ) -> pd.DataFrame:
        self.calls.append(top_k)
        limit = min(len(self._frame), top_k)
        return self._frame.head(limit).copy()


def _build_match_frame(total: int) -> pd.DataFrame:
    rows = []
    for index in range(total):
        rows.append(
            {
                "lore_id": f"lore-{index}",
                "text": f"Sample text {index}",
                "score": 1.0 - index * 0.01,
                "canonical_id": f"canon-{index}",
                "category": "item",
                "text_type": "description",
                "source": "test",
            }
        )
    return pd.DataFrame(rows)


def test_query_lore_defaults_to_10_unique_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = _build_match_frame(20)
    helper = _StubQueryHelper(frame)

    def _fake_loader(**_: object) -> _StubQueryHelper:
        return helper

    monkeypatch.setattr("rag.query.load_query_helper", _fake_loader)

    matches = query_lore("topic")

    assert helper.calls[-1] == 30  # padded top_k
    assert len(matches) == 10


def test_query_lore_honors_top_k_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = _build_match_frame(20)
    helper = _StubQueryHelper(frame)

    def _fake_loader(**_: object) -> _StubQueryHelper:
        return helper

    monkeypatch.setattr("rag.query.load_query_helper", _fake_loader)

    matches = query_lore("topic", top_k=4)

    assert helper.calls[-1] == 12
    assert len(matches) == 4


def test_query_lore_deduplicates_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = _build_match_frame(15).to_dict("records")
    rows[1]["text"] = rows[0]["text"]
    rows[3]["text"] = rows[2]["text"]
    frame = pd.DataFrame(rows)
    helper = _StubQueryHelper(frame)

    def _fake_loader(**_: object) -> _StubQueryHelper:
        return helper

    monkeypatch.setattr("rag.query.load_query_helper", _fake_loader)

    matches = query_lore("topic", top_k=5)

    texts = [match.text for match in matches]
    assert len(matches) == 5
    assert texts.count(rows[0]["text"]) == 1
    assert texts.count(rows[2]["text"]) == 1


def _build_rag_fixture(
    base_dir: Path,
    *,
    extra_rows: Sequence[dict[str, str]] | None = None,
) -> tuple[Path, Path, Path, DeterministicEncoder]:
    lore_path = write_sample_lore_corpus(base_dir)
    if extra_rows:
        frame = pd.read_parquet(lore_path)
        extra_frame = pd.DataFrame(extra_rows, columns=frame.columns)
        frame = pd.concat([frame, extra_frame], ignore_index=True)
        frame.to_parquet(lore_path, index=False)
    embeddings_path = (
        base_dir / "data" / "embeddings" / "lore_embeddings.parquet"
    )
    index_path = base_dir / "data" / "embeddings" / "faiss_index.bin"
    metadata_path = base_dir / "data" / "embeddings" / "rag_metadata.parquet"
    info_path = base_dir / "data" / "embeddings" / "rag_index_meta.json"

    encoder = DeterministicEncoder(dim=4)
    build_lore_embeddings(
        lore_path=lore_path,
        output_path=embeddings_path,
        provider="local",
        model_name="test-model",
        batch_size=2,
        encoder=encoder,
    )
    build_rag_index(
        embeddings_path=embeddings_path,
        index_path=index_path,
        metadata_path=metadata_path,
        info_path=info_path,
    )
    return index_path, metadata_path, info_path, encoder


def test_query_lore_returns_structured_matches(tmp_path: Path) -> None:
    (
        index_path,
        metadata_path,
        info_path,
        encoder,
    ) = _build_rag_fixture(tmp_path)

    matches = query_lore(
        "Moonblade",
        top_k=2,
        filters={"category": "weapon"},
        index_path=index_path,
        metadata_path=metadata_path,
        info_path=info_path,
        encoder=encoder,
    )

    assert matches
    assert isinstance(matches[0], LoreMatch)
    assert matches[0].category == "weapon"
    assert matches[0].score > 0


def test_query_lore_handles_missing_filters(tmp_path: Path) -> None:
    (
        index_path,
        metadata_path,
        info_path,
        encoder,
    ) = _build_rag_fixture(tmp_path)

    matches = query_lore(
        "bloom",
        top_k=3,
        filters={"category": "item", "unsupported": "foo"},
        index_path=index_path,
        metadata_path=metadata_path,
        info_path=info_path,
        encoder=encoder,
    )

    assert matches
    assert all(match.category == "item" for match in matches)


def test_query_lore_supports_exclusion_filters(tmp_path: Path) -> None:
    (
        index_path,
        metadata_path,
        info_path,
        encoder,
    ) = _build_rag_fixture(tmp_path)

    filters = [
        FilterExpression(
            column="text_type",
            values=("description",),
            operator="exclude",
        )
    ]

    matches = query_lore(
        "weapon",
        top_k=3,
        filters=filters,
        index_path=index_path,
        metadata_path=metadata_path,
        info_path=info_path,
        encoder=encoder,
    )

    assert matches
    assert all(match.text_type != "description" for match in matches)

