# pyright: reportMissingImports=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false

from __future__ import annotations

from pathlib import Path

import pandas as pd  # type: ignore[import]
import pytest

from pipelines.build_lore_embeddings import (
    EmbeddingGenerationError,
    build_lore_embeddings,
)
from tests.helpers import DeterministicEncoder, write_sample_lore_corpus


def test_build_lore_embeddings_writes_artifact(tmp_path: Path) -> None:
    lore_path = write_sample_lore_corpus(tmp_path)
    output_path = tmp_path / "data" / "embeddings" / "lore_embeddings.parquet"
    encoder = DeterministicEncoder(dim=3)

    df = build_lore_embeddings(
        lore_path=lore_path,
        output_path=output_path,
        provider="local",
        model_name="test-model",
        batch_size=2,
        encoder=encoder,
    )

    assert output_path.exists()
    assert len(df) == 3
    assert set(df["embedding_model"]) == {"test-model"}
    assert set(df["embedding_provider"]) == {"local"}

    dimensions = {len(vec) for vec in df["embedding"]}
    assert dimensions == {3}

    first_expected = encoder.encode([df.iloc[0]["text"]])[0]
    assert df.iloc[0]["embedding"] == first_expected


def test_build_lore_embeddings_drops_empty_rows(tmp_path: Path) -> None:
    path = tmp_path / "data" / "curated" / "lore_corpus.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "lore_id": "npc:1",
                "canonical_id": "npc:1",
                "category": "npc",
                "text_type": "quote",
                "source": "github_api",
                "text": "Greetings, Tarnished.",
            },
            {
                "lore_id": "npc:2",
                "canonical_id": "npc:2",
                "category": "npc",
                "text_type": "quote",
                "source": "github_api",
                "text": "   ",
            },
        ]
    ).to_parquet(path, index=False)

    encoder = DeterministicEncoder(dim=2)
    df = build_lore_embeddings(
        lore_path=path,
        output_path=tmp_path / "embeddings.parquet",
        provider="local",
        model_name="tiny",
        batch_size=1,
        encoder=encoder,
        dry_run=True,
    )

    assert len(df) == 1
    assert df.iloc[0]["lore_id"] == "npc:1"


def test_build_lore_embeddings_requires_text(tmp_path: Path) -> None:
    path = tmp_path / "missing.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        columns=[
            "lore_id",
            "canonical_id",
            "category",
            "text_type",
            "source",
            "text",
        ]
    ).to_parquet(path, index=False)

    encoder = DeterministicEncoder()
    with pytest.raises(EmbeddingGenerationError):
        build_lore_embeddings(
            lore_path=path,
            output_path=tmp_path / "unused.parquet",
            provider="local",
            model_name="test",
            batch_size=1,
            encoder=encoder,
        )
