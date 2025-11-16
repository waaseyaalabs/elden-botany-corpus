"""Tests for automated quality report generation."""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

corpus_curate = importlib.import_module("corpus.curate")
corpus_quality = importlib.import_module("corpus.quality")

pl = pytest.importorskip("polars")
CorpusCurator = corpus_curate.CorpusCurator
QualityReporter = corpus_quality.QualityReporter


def test_quality_reporter_generates_profiles(tmp_path: Path) -> None:
    """QualityReporter should emit JSON + HTML artifacts with stats."""

    df = pl.DataFrame(
        {
            "name": ["Sword", "Shield", None],
            "rarity": ["common", "rare", "rare"],
            "weight": [8.5, 12.0, None],
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
    assert payload["row_count"] == 3
    assert payload["column_count"] == 3
    assert any(col["name"] == "rarity" for col in payload["columns"])


def test_curator_records_quality_reports(tmp_path: Path) -> None:
    """CorpusCurator should capture report metadata for all exports."""

    curated_dir = tmp_path / "curated"
    curator = CorpusCurator(
        output_dir=curated_dir,
        enable_quality_reports=True,
    )

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
    assert unified_report["json_report"].startswith("quality/")

    report_path = curated_dir / unified_report["json_report"]
    assert report_path.exists()
    assert json.loads(report_path.read_text())["dataset"] == "unified"
