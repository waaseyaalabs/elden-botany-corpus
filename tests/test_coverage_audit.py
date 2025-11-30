"""Tests for NPC summary coverage utilities."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from pipelines.coverage_audit import compare_summary_coverage


def _write_parquet(path: Path, rows: list[dict[str, object]]) -> None:
    frame = pd.DataFrame(rows)
    frame.to_parquet(path, index=False)


def test_compare_summary_coverage_detects_missing(tmp_path: Path) -> None:
    lore_path = tmp_path / "lore.parquet"
    _write_parquet(
        lore_path,
        [
            {
                "canonical_id": "npc:melina",
                "category": "npc",
                "text": "I am Melina",
            },
            {
                "canonical_id": "npc:millicent",
                "category": "npc",
                "text": "Rot takes root",
            },
        ],
    )

    summary_path = tmp_path / "summaries.parquet"
    _write_parquet(
        summary_path,
        [
            {"canonical_id": "npc:melina"},
            {"canonical_id": "npc:unknown"},
        ],
    )

    report = compare_summary_coverage(lore_path, summary_path)
    assert report.curated_entities == 2
    assert report.summarized_entities == 2
    assert tuple(report.missing_ids) == ("npc:millicent",)
    assert tuple(report.extra_ids) == ("npc:unknown",)


def test_compare_summary_coverage_respects_alias_map(tmp_path: Path) -> None:
    lore_path = tmp_path / "lore.parquet"
    _write_parquet(
        lore_path,
        [
            {
                "canonical_id": "npc:renna",
                "category": "npc",
                "text": "Call me Renna",
            }
        ],
    )

    summary_json = tmp_path / "summaries.json"
    summary_json.write_text(
        """
        {
            "summaries": [
                {"canonical_id": "npc:ranni"}
            ]
        }
        """.strip()
    )

    alias_path = tmp_path / "aliases.csv"
    alias_path.write_text("alias_id,canonical_id\nnpc:renna,npc:ranni\n")

    report = compare_summary_coverage(
        lore_path,
        summary_json,
        alias_table=alias_path,
    )

    assert report.missing_ids == ()
    assert report.extra_ids == ()
