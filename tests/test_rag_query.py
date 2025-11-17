# pyright: reportMissingImports=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false

from __future__ import annotations

from pathlib import Path

from rag.query import LoreMatch, query_lore

from pipelines.build_lore_embeddings import build_lore_embeddings
from pipelines.build_rag_index import build_rag_index

from .helpers import DeterministicEncoder, write_sample_lore_corpus


def _build_rag_fixture(
    base_dir: Path,
) -> tuple[Path, Path, Path, DeterministicEncoder]:
    lore_path = write_sample_lore_corpus(base_dir)
    embeddings_path = base_dir / "data" / "embeddings" / "lore_embeddings.parquet"
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
