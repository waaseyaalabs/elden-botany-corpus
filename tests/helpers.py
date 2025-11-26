"""Shared test helpers for the Elden Botany corpus."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from corpus.community_schema import MotifCategory, MotifEntry, MotifTaxonomy

REQUIRED_LORE_COLUMNS = (
    "lore_id",
    "canonical_id",
    "raw_canonical_id",
    "category",
    "text_type",
    "source",
    "text",
)


@dataclass
class DeterministicEncoder:
    """Small encoder that avoids network calls in the embedding tests."""

    dim: int = 4

    def encode(self, texts: Sequence[str]) -> list[list[float]]:
        """Return pseudo-random vectors derived from the text content."""

        if self.dim <= 0:
            msg = "Encoder dimension must be greater than zero"
            raise ValueError(msg)

        vectors: list[list[float]] = []
        for text in texts:
            checksum = sum(ord(char) for char in text)
            vector = [
                ((checksum + index * 31) % 997) / 997.0
                for index in range(self.dim)
            ]
            vectors.append(vector)
        return vectors


def write_sample_lore_corpus(base_dir: Path) -> Path:
    """Create a miniature lore_corpus.parquet fixture for embedding tests."""

    curated_dir = base_dir / "data" / "curated"
    curated_dir.mkdir(parents=True, exist_ok=True)
    lore_path = curated_dir / "lore_corpus.parquet"

    sample_rows = [
        {
            "lore_id": "lore-item",
            "canonical_id": "item-001",
            "raw_canonical_id": "item-001",
            "category": "item",
            "text_type": "description",
            "source": "test",
            "text": "Crimson Bloom restores a trickle of HP.",
        },
        {
            "lore_id": "lore-weapon",
            "canonical_id": "weapon-001",
            "raw_canonical_id": "weapon-001",
            "category": "weapon",
            "text_type": "description",
            "source": "test",
            "text": "Moonblade cleaves with frostlit arcs.",
        },
        {
            "lore_id": "lore-weapon-effect",
            "canonical_id": "weapon-001",
            "raw_canonical_id": "weapon-001",
            "category": "weapon",
            "text_type": "effect",
            "source": "test",
            "text": "Fires arcs of moonlit frost dealing magic damage.",
        },
        {
            "lore_id": "lore-boss",
            "canonical_id": "boss-001",
            "raw_canonical_id": "boss-001",
            "category": "boss",
            "text_type": "bio",
            "source": "test",
            "text": "Messmer the Impaler wields living flame.",
        },
    ]

    frame = pd.DataFrame(sample_rows, columns=REQUIRED_LORE_COLUMNS)
    frame.to_parquet(lore_path, index=False)
    return lore_path


def sample_taxonomy() -> MotifTaxonomy:
    """Return a compact taxonomy used across analysis-layer tests."""

    categories = [
        MotifCategory(
            slug="narrative",
            label="Narrative",
            description="Test motifs",
            motifs=[
                MotifEntry(
                    slug="scarlet_rot",
                    label="Scarlet Rot",
                    description="Blooming decay",
                    category="narrative",
                    synonyms=["rot", "scarlet"],
                    narrative_signals=["rot"],
                ),
                MotifEntry(
                    slug="dream_cycle",
                    label="Dream Cycle",
                    description="Dream-laden journeys",
                    category="narrative",
                    synonyms=["dream", "mirror"],
                    narrative_signals=["dream"],
                ),
            ],
        )
    ]
    return MotifTaxonomy(version=1, categories=categories)


__all__ = [
    "DeterministicEncoder",
    "write_sample_lore_corpus",
    "sample_taxonomy",
]
