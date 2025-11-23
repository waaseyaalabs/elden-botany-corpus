from __future__ import annotations

import pandas as pd

from scripts import corpus_stats


def test_summarize_coverage_computes_expected_totals() -> None:
    frame = pd.DataFrame(
        [
            {
                "motif_slug": "alpha",
                "label": "Alpha",
                "match_count": 5,
                "coverage_pct": 10.0,
                "category": "sky",
            },
            {
                "motif_slug": "beta",
                "label": "",
                "match_count": 0,
                "coverage_pct": 0.0,
                "category": "earth",
            },
            {
                "motif_slug": "gamma",
                "label": "Gamma",
                "match_count": 2,
                "coverage_pct": 5.0,
                "category": "sea",
            },
        ]
    )

    stats = corpus_stats.summarize_coverage(frame)

    assert stats.total_motifs == 3
    assert stats.zero_match == 1
    assert stats.under_five == 2
    assert stats.avg_matches == 2.33
    assert stats.sample_zero_labels == ["beta"]
    assert stats.top_motifs[0] == ("Alpha", 5)


def test_detect_sparse_entities_filters_below_threshold() -> None:
    entity_counts = {"a": 5, "b": 11, "c": 3}

    sparse = corpus_stats.detect_sparse_entities(entity_counts, threshold=10)

    assert sparse == [("c", 3), ("a", 5)]


def test_list_zero_match_motifs_respects_limit() -> None:
    frame = pd.DataFrame(
        [
            {
                "motif_slug": f"motif_{idx}",
                "label": "",
                "match_count": 0,
                "coverage_pct": 0.0,
                "category": "cat",
            }
            for idx in range(6)
        ]
    )

    results = corpus_stats.list_zero_match_motifs(frame, limit=3)

    assert len(results) == 3
    assert all(label.startswith("motif_") for label, _ in results)
