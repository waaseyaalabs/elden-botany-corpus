"""Tests for quality report generation."""

from pathlib import Path

import polars as pl  # type: ignore[import]
from corpus.curate import CorpusCurator  # type: ignore[import]
from corpus.quality import QualityReporter  # type: ignore[import]


def test_quality_reporter_generates_files(tmp_path: Path) -> None:
    """QualityReporter should emit JSON and HTML summaries with stats."""

    df = pl.DataFrame(
        {
            "name": ["Alpha", "Beta", None, "Alpha"],
            "hit_points": [100, 200, 150, None],
        }
    )

    reporter = QualityReporter(
        output_dir=tmp_path / "quality",
        base_dir=tmp_path,
    )

    summary = reporter.profile("unified", df)

    json_report = tmp_path / "quality" / "unified.json"
    html_report = tmp_path / "quality" / "unified.html"

    assert json_report.exists()
    assert html_report.exists()
    assert summary["reports"]["json"] == "quality/unified.json"
    assert summary["columns"]["name"]["null_percent"] == 25.0
    assert summary["columns"]["hit_points"]["summary"]["max"] == 200


def test_curator_records_quality_metadata(tmp_path: Path) -> None:
    """CorpusCurator should attach report metadata for datasets."""

    curator = CorpusCurator(output_dir=tmp_path)
    df = pl.DataFrame({"entity_type": ["weapon", "armor"], "value": [1, 2]})

    curator._record_quality_report("demo", df)

    reports = curator.metadata.metadata["quality_reports"]
    assert "demo" in reports
    assert reports["demo"]["reports"]["json"].endswith("demo.json")
