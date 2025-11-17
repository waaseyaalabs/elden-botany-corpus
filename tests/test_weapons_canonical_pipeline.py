# pyright: reportMissingImports=false

from __future__ import annotations

import csv
import json
from pathlib import Path

import pandas as pd
import pytest

from pipelines.build_weapons_canonical import build_weapons_canonical
from pipelines.io.kaggle_base_loader import load_kaggle_base_weapons


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_kaggle_base_loader_parses_weapon_stats(tmp_path: Path) -> None:
    """Base loader should parse damage, requirements, and scaling."""

    raw_root = tmp_path / "raw"
    rows: list[dict[str, object]] = [
        {
            "id": "weapon_001",
            "name": "Test Sword",
            "description": "A reliable blade",
            "category": "Straight Sword",
            "weight": "3.5",
            "attack": "[{\"name\": \"Phy\", \"amount\": 113}]",
            "requiredAttributes": "[{\"name\": \"Str\", \"amount\": 9}]",
            "scalesWith": "[{\"name\": \"Str\", \"scaling\": \"D\"}]",
        }
    ]
    _write_csv(raw_root / "kaggle" / "base" / "weapons.csv", rows)

    records = load_kaggle_base_weapons(raw_root)
    assert len(records) == 1

    weapon = records[0]
    assert weapon["damage_physical"] == pytest.approx(113)
    assert weapon["required_str"] == 9
    assert weapon["scaling_str"] == "D"
    assert weapon["weapon_type"] == "sword"


def test_build_weapons_canonical_merges_sources(tmp_path: Path) -> None:
    """Pipeline should merge Kaggle base, DLC, and GitHub sources."""

    raw_root = tmp_path / "raw"

    base_rows: list[dict[str, object]] = [
        {
            "id": "weapon_axe",
            "name": "Hand Axe",
            "description": "Base entry",
            "category": "Axe",
            "weight": "4.0",
            "attack": "[{\"name\": \"Phy\", \"amount\": 100}]",
            "requiredAttributes": "[{\"name\": \"Str\", \"amount\": 8}]",
            "scalesWith": "[{\"name\": \"Str\", \"scaling\": \"C\"}]",
        }
    ]
    _write_csv(raw_root / "kaggle" / "base" / "weapons.csv", base_rows)

    dlc_rows: list[dict[str, object]] = [
        {
            "id": 1,
            "weapon_id": 1,
            "name": "Shadow Saber",
            "description": "DLC weapon",
            "category": "Twinblade",
            "weight": "6.0",
            "dlc": 1,
            "requirements": "{\"Str\": 12, \"Dex\": 18}",
        }
    ]
    _write_csv(
        raw_root / "kaggle" / "dlc" / "eldenringScrap" / "weapons.csv",
        dlc_rows,
    )

    github_payload = {
        "success": True,
        "total": 1,
        "count": 1,
        "source": "https://example.com/api",
        "data": [
            {
                "id": "weapon_axe",
                "name": "Hand Axe",
                "description": "Fallback entry",
                "category": "Axe",
                "weight": 3.8,
                "attack": [{"name": "Phy", "amount": 95}],
                "requiredAttributes": [
                    {"name": "Str", "amount": 7},
                ],
                "scalesWith": [
                    {"name": "Str", "scaling": "D"},
                ],
            }
        ],
    }
    _write_json(raw_root / "github_api" / "weapons.json", github_payload)

    output_path = tmp_path / "curated" / "weapons.parquet"
    df = build_weapons_canonical(
        raw_root=raw_root,
        output_path=output_path,
        dry_run=False,
    )

    assert output_path.exists()
    assert set(df["name"]) == {"Hand Axe", "Shadow Saber"}
    hand_axe = df[df["name"] == "Hand Axe"].iloc[0]
    assert hand_axe["source"] == "kaggle_base"
    assert hand_axe["damage_physical"] == pytest.approx(100)

    saber = df[df["name"] == "Shadow Saber"].iloc[0]
    assert bool(saber["is_dlc"]) is True
    assert saber["weapon_type"] == "spear"  # Twinblade mapped to spear

    parquet_df = pd.read_parquet(output_path)
    assert len(parquet_df) == 2
