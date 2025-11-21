# pyright: reportMissingImports=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false

from __future__ import annotations

import json
from pathlib import Path

from corpus.config import settings

from pipelines.build_lore_embeddings import build_lore_embeddings
from pipelines.build_rag_index import (
    FilterClause,
    build_rag_index,
    query_index,
)
from tests.helpers import DeterministicEncoder, write_sample_lore_corpus


def test_build_rag_index_produces_artifacts(tmp_path: Path) -> None:
    lore_path = write_sample_lore_corpus(tmp_path)
    embeddings_path = (
        tmp_path / "data" / "embeddings" / "lore_embeddings.parquet"
    )
    index_path = tmp_path / "data" / "embeddings" / "faiss_index.bin"
    metadata_path = tmp_path / "data" / "embeddings" / "rag_metadata.parquet"
    info_path = tmp_path / "data" / "embeddings" / "rag_index_meta.json"

    encoder = DeterministicEncoder(dim=4)
    build_lore_embeddings(
        lore_path=lore_path,
        output_path=embeddings_path,
        provider="local",
        model_name="test-model",
        batch_size=2,
        encoder=encoder,
    )

    metadata = build_rag_index(
        embeddings_path=embeddings_path,
        index_path=index_path,
        metadata_path=metadata_path,
        info_path=info_path,
    )

    assert index_path.exists()
    assert metadata_path.exists()
    assert info_path.exists()
    assert len(metadata) == 3
    assert set(metadata["category"]) == {"item", "weapon", "boss"}
    info_payload = json.loads(info_path.read_text(encoding="utf-8"))
    assert info_payload["reranker"]["default_name"] == settings.reranker_name


def test_query_index_supports_filters(tmp_path: Path) -> None:
    lore_path = write_sample_lore_corpus(tmp_path)
    embeddings_path = (
        tmp_path / "data" / "embeddings" / "lore_embeddings.parquet"
    )
    index_path = tmp_path / "data" / "embeddings" / "faiss_index.bin"
    metadata_path = tmp_path / "data" / "embeddings" / "rag_metadata.parquet"
    info_path = tmp_path / "data" / "embeddings" / "rag_index_meta.json"

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

    filters = {"category": FilterClause(include={"weapon"})}

    results = query_index(
        "Moonblade",
        top_k=2,
        filter_by=filters,
        index_path=index_path,
        metadata_path=metadata_path,
        info_path=info_path,
        provider="local",
        model_name="test-model",
        batch_size=1,
        encoder=encoder,
    )

    assert len(results) == 1
    assert results.iloc[0]["category"] == "weapon"
    assert results.iloc[0]["score"] > 0
