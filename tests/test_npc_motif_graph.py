"""Tests covering the NPC motif graph pipeline."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from pipelines.npc_motif_graph import (
    NPCMotifGraphConfig,
    NPCMotifGraphPipeline,
    load_graph,
)
from tests.helpers import sample_taxonomy


def _write_lore_fixture(path: Path) -> None:
    frame = pd.DataFrame(
        [
            {
                "lore_id": "lore-001",
                "canonical_id": "npc:melina",
                "category": "npc",
                "text_type": "dialogue",
                "text": "The scarlet rot blooms beneath twin branches.",
                "source": "test",
            },
            {
                "lore_id": "lore-002",
                "canonical_id": "npc:melina",
                "category": "npc",
                "text_type": "dialogue",
                "text": "I dreamed of mirrored cycles and quiet vows.",
                "source": "test",
            },
        ]
    )
    frame.to_parquet(path, index=False)


def test_npc_motif_graph_pipeline_builds_network(tmp_path: Path) -> None:
    curated_path = tmp_path / "lore.parquet"
    _write_lore_fixture(curated_path)
    output_dir = tmp_path / "analysis"

    pipeline = NPCMotifGraphPipeline(
        config=NPCMotifGraphConfig(
            curated_path=curated_path,
            output_dir=output_dir,
            categories=("npc",),
        ),
        taxonomy=sample_taxonomy(),
    )
    artifacts = pipeline.run()

    entity_frame = pd.read_parquet(artifacts.entity_summary)
    assert entity_frame.loc[0, "canonical_id"] == "npc:melina"
    assert int(entity_frame.loc[0, "unique_motifs"]) == 2

    motif_stats = pd.read_parquet(artifacts.entity_motif_stats)
    required_motif_cols = {
        "canonical_id",
        "motif_slug",
        "motif_label",
        "motif_category",
        "hit_count",
        "unique_lore",
    }
    assert required_motif_cols.issubset(set(motif_stats.columns))
    motif_row = motif_stats.loc[
        motif_stats["motif_slug"] == "scarlet_rot"
    ].iloc[0]
    assert motif_row["motif_label"] == "Scarlet Rot"
    assert int(motif_row["hit_count"]) == 1

    lore_hits = pd.read_parquet(artifacts.lore_hits)
    required_hit_cols = {
        "lore_id",
        "canonical_id",
        "motif_slug",
        "motif_label",
        "motif_category",
        "text",
    }
    assert required_hit_cols.issubset(set(lore_hits.columns))
    assert lore_hits["text"].str.contains("rot", case=False).any()

    report = json.loads(artifacts.report_path.read_text())
    assert report["summary"]["entities"] == 1
    assert len(report["sample_queries"]) >= 2
    assert set(report["artifacts"]).issuperset(
        {
            "graph",
            "graphml",
            "entity_summary",
            "entity_motif_stats",
            "lore_hits",
        }
    )

    graph = load_graph(artifacts.graph_path)
    assert "scarlet_rot" in graph.nodes
    assert graph.has_edge("npc:melina", "scarlet_rot")
