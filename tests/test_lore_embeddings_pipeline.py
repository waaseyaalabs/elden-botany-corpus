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
    assert set(df["embedding_strategy"]) == {"weighted_text_types_v1"}
    assert any("effect" in comps for comps in df["text_type_components"])

    dimensions = {len(vec) for vec in df["embedding"]}
    assert dimensions == {3}


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
    assert df.iloc[0]["canonical_id"] == "npc:1"
    assert df.iloc[0]["lore_id"].startswith("npc:1::")


def test_weighting_prefers_narrative_sections(tmp_path: Path) -> None:
    lore_path = tmp_path / "data" / "curated" / "lore_corpus.parquet"
    lore_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "lore_id": "foo-desc",
                "canonical_id": "foo",
                "category": "item",
                "text_type": "description",
                "source": "unit",
                "text": "Long form lore body.",
            },
            {
                "lore_id": "foo-effect",
                "canonical_id": "foo",
                "category": "item",
                "text_type": "effect",
                "source": "unit",
                "text": "Deals a tiny amount of damage.",
            },
        ]
    ).to_parquet(lore_path, index=False)

    encoder = DeterministicEncoder(dim=2)
    df = build_lore_embeddings(
        lore_path=lore_path,
        output_path=tmp_path / "embeddings.parquet",
        provider="local",
        model_name="tiny",
        batch_size=1,
        encoder=encoder,
        dry_run=True,
        text_type_weights={"description": 2.0, "effect": 0.5},
    )

    assert len(df) == 1
    row = df.iloc[0]
    assert row["text_type"] == "description"
    assert row["text_type_components"] == "description|effect"
    assert row["text"].split("\n\n")[0].startswith("Description:")
    assert "Effect" in row["text"]
    assert row["weight_config_path"] == "<inline-text-type-weights>"


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
