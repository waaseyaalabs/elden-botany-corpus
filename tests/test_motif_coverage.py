from __future__ import annotations

import pandas as pd
from corpus.community_schema import load_motif_taxonomy

from pipelines.motif_coverage import compute_motif_coverage


def _row_for_slug(rows, slug):
    for row in rows:
        if row.motif_slug == slug:
            return row
    raise AssertionError(f"Motif slug {slug} not found in coverage rows")


def test_compute_motif_coverage_counts_matches() -> None:
    frame = pd.DataFrame(
        {
            "text": [
                "Flame and storm converge in oathkeeper lore.",
                "Twin mirrors hold frost secrets.",
            ],
            "canonical_id": ["entry_a", "entry_b"],
        }
    )

    taxonomy = load_motif_taxonomy()
    rows = compute_motif_coverage(frame, taxonomy)

    flame = _row_for_slug(rows, "flame")
    assert flame.match_count == 1
    assert abs(flame.coverage_pct - 50.0) < 0.01

    twin = _row_for_slug(rows, "twin")
    assert twin.match_count == 1
    assert twin.sample_ids == ["entry_b"]
