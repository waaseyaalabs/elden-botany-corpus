"""Tests for the motif clustering helpers (deterministic + summaries)."""

from __future__ import annotations

import math

import pandas as pd
import pytest

from pipelines.motif_clustering import (
    MotifClusteringConfig,
    MotifClusteringPipeline,
    select_exemplars,
    summarize_clusters,
)


@pytest.fixture()
def tiny_lore_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "lore_id": ["l-1", "l-2", "l-3"],
            "canonical_id": ["npc:a", "npc:b", "npc:c"],
            "text": [
                "Scarlet rot blooms in the Haligtree",
                "The rot tide rises under Messmer",
                "A gentle breeze carries golden pollen",
            ],
            "category": ["npc", "npc", "npc"],
            "text_type": ["description", "dialogue", "dialogue"],
        }
    )


def test_sample_frame_is_deterministic(tiny_lore_frame: pd.DataFrame) -> None:
    config = MotifClusteringConfig(max_rows=2, random_seed=1)
    pipeline = MotifClusteringPipeline(config)
    sampled_first = pipeline.sample_frame(tiny_lore_frame)
    sampled_second = pipeline.sample_frame(tiny_lore_frame)
    assert sampled_first.equals(sampled_second)


def test_select_exemplars_orders_by_probability() -> None:
    frame = pd.DataFrame(
        {
            "lore_id": ["a", "b", "c"],
            "canonical_id": ["x", "y", "z"],
            "text": ["foo", "bar", "baz"],
            "category": ["npc", "npc", "npc"],
            "text_type": ["dialogue", "dialogue", "dialogue"],
            "cluster_probability": [0.1, 0.9, 0.7],
        }
    )
    exemplars = select_exemplars(frame, limit=2)
    assert [item["lore_id"] for item in exemplars] == ["b", "c"]
    probability = float(exemplars[0]["probability"])
    assert math.isclose(probability, 0.9, rel_tol=1e-6)


def test_summarize_clusters_builds_density_table() -> None:
    frame = pd.DataFrame(
        {
            "lore_id": ["l1", "l2", "l3"],
            "canonical_id": ["npc:1", "npc:2", "npc:3"],
            "text": [
                "Scarlet rot consumes everything",
                "Messmer commands sacred flame",
                "Rot and flame entwine",
            ],
            "category": ["npc", "npc", "npc"],
            "text_type": ["dialogue", "dialogue", "dialogue"],
            "cluster_id": [0, 0, 1],
            "cluster_probability": [0.9, 0.7, 0.8],
        }
    )
    motif_hits = pd.DataFrame(
        {
            "scarlet_rot": [True, False, True],
            "flame": [False, True, True],
        }
    )
    coverage = pd.DataFrame(
        {
            "motif_slug": ["scarlet_rot", "flame"],
            "label": ["Scarlet Rot", "Sacred Flame"],
            "category": ["cosmic", "elemental"],
            "coverage_pct": [5.0, 10.0],
        }
    )

    density, summaries = summarize_clusters(
        frame,
        motif_hits,
        coverage,
        exemplars=2,
    )

    assert set(density["motif_slug"]) == {"scarlet_rot", "flame"}
    assert any(cluster["cluster_id"] == 0 for cluster in summaries)
    scarlet_row = density.loc[
        (density["cluster_id"] == 0) & (density["motif_slug"] == "scarlet_rot")
    ].iloc[0]
    assert scarlet_row["cluster_match_count"] == 1
    cluster_pct = float(scarlet_row["cluster_pct"])
    assert math.isclose(cluster_pct, 50.0, rel_tol=1e-6)
