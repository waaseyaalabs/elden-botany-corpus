from __future__ import annotations

import pandas as pd
from corpus.community_schema import load_motif_taxonomy
from pandas.testing import assert_frame_equal

from pipelines.motif_coverage import (
    _load_curated_frame,
    compute_motif_coverage,
)


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


def test_load_curated_frame_falls_back_to_csv(tmp_path, monkeypatch) -> None:
    parquet_path = tmp_path / "unified.parquet"
    parquet_path.write_text("not a parquet file", encoding="utf-8")
    csv_path = tmp_path / "unified.csv"
    csv_path.write_text("text,canonical_id\nhello,entry_a\n", encoding="utf-8")

    def fake_read_parquet(path: object) -> pd.DataFrame:
        raise OSError("boom")

    def fake_polars_roundtrip(path: object) -> pd.DataFrame:
        raise RuntimeError("polars boom")

    captured: dict[str, object] = {}
    real_read_csv = pd.read_csv

    def fake_read_csv(path: object) -> pd.DataFrame:
        captured["path"] = path
        return real_read_csv(csv_path)

    monkeypatch.setattr(pd, "read_parquet", fake_read_parquet)
    monkeypatch.setattr(
        "pipelines.motif_coverage._polars_roundtrip",
        fake_polars_roundtrip,
    )
    monkeypatch.setattr(pd, "read_csv", fake_read_csv)

    frame = _load_curated_frame(parquet_path, csv_path)
    assert captured["path"] == str(csv_path)
    assert frame["canonical_id"].tolist() == ["entry_a"]


def test_load_curated_frame_uses_polars_roundtrip(
    tmp_path, monkeypatch
) -> None:
    parquet_path = tmp_path / "unified.parquet"
    parquet_path.write_text("not a parquet file", encoding="utf-8")
    csv_path = tmp_path / "unified.csv"
    csv_path.write_text("placeholder", encoding="utf-8")

    def fake_read_parquet(path: object) -> pd.DataFrame:
        raise OSError("boom")

    expected = pd.DataFrame({"text": ["hi"], "canonical_id": ["entry_a"]})
    called: dict[str, int] = {"count": 0}

    def fake_polars_roundtrip(path: object) -> pd.DataFrame:
        called["count"] += 1
        return expected

    def fake_read_csv(path: object) -> pd.DataFrame:  # pragma: no cover
        raise AssertionError("CSV fallback should not be used")

    monkeypatch.setattr(pd, "read_parquet", fake_read_parquet)
    monkeypatch.setattr(
        "pipelines.motif_coverage._polars_roundtrip",
        fake_polars_roundtrip,
    )
    monkeypatch.setattr(pd, "read_csv", fake_read_csv)

    frame = _load_curated_frame(parquet_path, csv_path)
    assert called["count"] == 1
    assert_frame_equal(frame, expected)
