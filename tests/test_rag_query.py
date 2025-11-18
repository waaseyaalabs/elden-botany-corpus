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
from rag.query import (  # type: ignore[import]
    FilterExpression,
    LoreMatch,
    _deduplicate_frame,  # noqa: SLF001
    query_lore,
)

from pipelines.build_lore_embeddings import (  # type: ignore[import]
    build_lore_embeddings,
)
from pipelines.build_rag_index import (  # type: ignore[import]
    build_rag_index,
)

from .helpers import DeterministicEncoder, write_sample_lore_corpus


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


def test_deduplicate_frame_moves_duplicates_to_end() -> None:
    frame = pd.DataFrame(
        [
            {
                "lore_id": "lore-1",
                "text": "Crimson Bloom restores a trickle of HP.",
                "score": 0.9,
            },
            {
                "lore_id": "lore-dup",
                "text": "Crimson Bloom restores a trickle of HP.",
                "score": 0.85,
            },
            {
                "lore_id": "lore-2",
                "text": "Moonblade cleaves with frostlit arcs.",
                "score": 0.8,
            },
        ]
    )

    deduped = _deduplicate_frame(frame)
    texts = list(deduped["text"])

    assert texts[0] == "Crimson Bloom restores a trickle of HP."
    assert texts[1] == "Moonblade cleaves with frostlit arcs."
    assert texts[-1] == "Crimson Bloom restores a trickle of HP."
