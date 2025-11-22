"""Tests for the narrative summarizer pipeline."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from pipelines.narrative_summarizer import (
    NarrativeSummariesConfig,
    NarrativeSummariesPipeline,
)
from pipelines.npc_motif_graph import (
    NPCMotifGraphConfig,
    NPCMotifGraphPipeline,
)
from tests.helpers import sample_taxonomy


def _write_lore_fixture(path: Path) -> None:
    frame = pd.DataFrame(
        [
            {
                "lore_id": "lore-010",
                "canonical_id": "npc:melina",
                "category": "npc",
                "text_type": "dialogue",
                "text": "The scarlet rot takes root in every oath I keep.",
                "source": "test",
            },
            {
                "lore_id": "lore-011",
                "canonical_id": "npc:melina",
                "category": "npc",
                "text_type": "dialogue",
                "text": "In dreams I walk the mirrored paths again.",
                "source": "test",
            },
        ]
    )
    frame.to_parquet(path, index=False)


def test_narrative_summaries_pipeline(tmp_path: Path) -> None:
    taxonomy = sample_taxonomy()
    curated_path = tmp_path / "lore.parquet"
    _write_lore_fixture(curated_path)

    graph_dir = tmp_path / "graph"
    NPCMotifGraphPipeline(
        config=NPCMotifGraphConfig(
            curated_path=curated_path,
            output_dir=graph_dir,
            categories=("npc",),
        ),
        taxonomy=taxonomy,
    ).run()

    pipeline = NarrativeSummariesPipeline(
        config=NarrativeSummariesConfig(
            graph_dir=graph_dir,
            output_dir=tmp_path / "summaries",
            max_motifs=2,
            max_quotes=1,
        ),
        taxonomy=taxonomy,
    )
    artifacts = pipeline.run()

    payload = json.loads(artifacts.summaries_json.read_text())
    assert payload["summaries"], "Expected at least one summary"
    entry = payload["summaries"][0]
    assert entry["top_motifs"][0]["slug"] == "scarlet_rot"
    assert artifacts.markdown_path.exists()
