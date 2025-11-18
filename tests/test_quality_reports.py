"""Tests for automated quality report generation."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl  # type: ignore[import]
from corpus.curate import CorpusCurator  # type: ignore[import]
from corpus.quality import QualityReporter  # type: ignore[import]


def test_quality_reporter_generates_profiles(tmp_path: Path) -> None:
    """QualityReporter should emit JSON/HTML artifacts and rich metadata."""

    df = pl.DataFrame(
        {
            "name": ["Sword", "Shield", None, "Sword"],
            "hit_points": [100, 200, 150, None],
        }
    )

    reporter = QualityReporter(
        output_dir=tmp_path / "quality",
        relative_root=tmp_path,
    )

    summary = reporter.generate_report("unified", df)

    json_path = tmp_path / summary["json_report"]
    html_path = tmp_path / summary["html_report"]

    assert json_path.exists()
    assert html_path.exists()

    payload = json.loads(json_path.read_text())
    assert payload["row_count"] == 4
    assert payload["columns"]["hit_points"]["summary"]["max"] == 200
    assert summary["reports"]["json"] == summary["json_report"]
    assert summary["rows"] == payload["row_count"]

    alias = reporter.profile("alias", df)
    assert alias["json_report"].endswith("alias.json")


def test_curator_records_quality_reports(tmp_path: Path) -> None:
    """CorpusCurator should capture report metadata for all exports."""

    curator = CorpusCurator(output_dir=tmp_path, enable_quality_reports=True)

    df = pl.DataFrame(
        {
            "entity_type": ["weapon", "weapon", "boss"],
            "name": ["Sword", "Axe", "Margit"],
            "meta_json": [{"tier": 1}, {"tier": 2}, {"tier": 5}],
            "sources": [["kaggle"], ["github"], ["kaggle"]],
        }
    )

    curator.export_unified(df)
    curator.export_by_entity_type(df)

    reports = curator.metadata.metadata["quality_reports"]
    assert set(reports) == {"unified", "weapon", "boss"}

    unified_report = reports["unified"]
    assert unified_report["rows"] == 3
    assert unified_report["reports"]["json"] == unified_report["json_report"]
    assert (tmp_path / unified_report["json_report"]).exists()

    schema_versions = curator.metadata.metadata["schema_versions"]
    assert schema_versions["weapon"]["tag"] == "weapons_v1"
    assert schema_versions["boss"]["tag"] == "bosses_v1"
