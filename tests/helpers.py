# pyright: reportMissingImports=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false

"""Test helpers for lore embedding and indexing pipelines."""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

import pandas as pd  # type: ignore[import]


def write_sample_lore_corpus(base_dir: Path) -> Path:
    """Create a minimal lore_corpus.parquet for pipeline tests."""

    rows = [
        {
            "lore_id": "item:1-desc",
            "canonical_id": "item:1",
            "category": "item",
            "text_type": "description",
            "source": "kaggle_base",
            "text": "Verdant charm glows softly.",
        },
        {
            "lore_id": "weapon:2-skill",
            "canonical_id": "weapon:2",
            "category": "weapon",
            "text_type": "skill",
            "source": "github_api",
            "text": "Moonblade cleaves the sky.",
        },
        {
            "lore_id": "boss:3-quote",
            "canonical_id": "boss:3",
            "category": "boss",
            "text_type": "quote",
            "source": "impalers",
            "text": "We are the last bloom.",
        },
    ]

    output = base_dir / "data" / "curated" / "lore_corpus.parquet"
    output.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(output, index=False)
    return output


class DeterministicEncoder:
    """Simple embedding stub for tests."""

    def __init__(self, dim: int = 4) -> None:
        self._dim = dim

    def encode(self, texts: Sequence[str]) -> list[list[float]]:
        return [self._encode_single(text) for text in texts]

    def _encode_single(self, text: str) -> list[float]:
        base = sum(ord(char) for char in text) % 127
        return [float(base + offset) / 100 for offset in range(self._dim)]
