# pyright: reportGeneralTypeIssues=false, reportMissingImports=false
# pyright: reportMissingTypeStubs=false, reportMissingModuleSource=false

from __future__ import annotations

import csv
import importlib
import json
from math import isclose
from pathlib import Path
from typing import Any


def _load_attr(module_path: str, attr: str) -> Any:
    module = importlib.import_module(module_path)
    return getattr(module, attr)


build_armor_canonical = _load_attr(
    "pipelines.build_armor_canonical",
    "build_armor_canonical",
)
build_bosses_canonical = _load_attr(
    "pipelines.build_bosses_canonical",
    "build_bosses_canonical",
)
build_items_canonical = _load_attr(
    "pipelines.build_items_canonical",
    "build_items_canonical",
)
build_spells_canonical = _load_attr(
    "pipelines.build_spells_canonical",
    "build_spells_canonical",
)
load_github_api_armor = _load_attr(
    "pipelines.io.github_api_loader",
    "load_github_api_armor",
)
load_github_api_bosses = _load_attr(
    "pipelines.io.github_api_loader",
    "load_github_api_bosses",
)
load_github_api_items = _load_attr(
    "pipelines.io.github_api_loader",
    "load_github_api_items",
)
load_github_api_spells = _load_attr(
    "pipelines.io.github_api_loader",
    "load_github_api_spells",
)


def _write_csv(
    path: Path,
    rows: list[dict[str, object]],
    *,
    fieldnames: list[str] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    header = fieldnames
    if header is None:
        if rows:
            header = sorted({key for row in rows for key in row})
        else:
            header = ["name"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _provenance_sources(row: Any) -> set[str]:
    payload = json.loads(row["provenance"])
    return {entry.get("source") for entry in payload}


def test_load_github_api_items_parses_numeric_fields(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    payload: dict[str, Any] = {
        "source": "https://example.com/items",
        "data": [
            {
                "id": "item_1",
                "name": "Rune Arc",
                "type": "consumable",
                "description": "Boosts yourself",
                "weight": 0.3,
                "sellPrice": 500,
                "maxAmount": 20,
                "rarity": "rare",
                "effect": "Raises stats",
                "obtainedFrom": "Leyndell",
                "image": "https://example.com/rune_arc.png",
            }
        ],
    }
    _write_json(raw_root / "github_api" / "items.json", payload)

    records = load_github_api_items(raw_root)
    assert len(records) == 1
    record = records[0]
    assert record["source"] == "github_api"
    assert record["source_priority"] == 3
    assert record["category"] == "consumable"
    assert isclose(float(record["weight"]), 0.3, rel_tol=1e-6)
    assert record["sell_price"] == 500


def test_load_github_api_bosses_maps_fields(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    payload: dict[str, Any] = {
        "source": "https://example.com/bosses",
        "data": [
            {
                "id": "boss_grafted",
                "name": "Grafted Scion",
                "description": "Tutorial menace",
                "region": "Limgrave",
                "location": "Chapel of Anticipation",
                "drops": ["Golden Seed"],
                "healthPoints": 1800,
                "image": "https://example.com/scion.png",
            }
        ],
    }
    _write_json(raw_root / "github_api" / "bosses.json", payload)

    records = load_github_api_bosses(raw_root)
    assert len(records) == 1
    record = records[0]
    assert record["region"] == "Limgrave"
    assert record["health_points"] == 1800
    assert record["source_id"] == "boss_grafted"


def test_load_github_api_armor_maps_stats(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    payload: dict[str, Any] = {
        "source": "https://example.com/armor",
        "data": [
            {
                "id": "armor_knight",
                "name": "Knight Helm",
                "description": "Reliable protection",
                "category": "head",
                "weight": 4.6,
                "dmgNegation": [{"name": "Phy", "amount": 6.8}],
                "resistance": [{"name": "Imm.", "amount": 70}],
            }
        ],
    }
    _write_json(raw_root / "github_api" / "armors.json", payload)

    records = load_github_api_armor(raw_root)
    assert len(records) == 1
    record = records[0]
    assert record["armor_type"] == "head"
    assert isclose(float(record["defense_physical"]), 6.8, rel_tol=1e-6)
    assert record["resistance_immunity"] == 70


def test_load_github_api_spells_parses_requirements(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    incantations_payload: dict[str, Any] = {
        "source": "https://example.com/incantations",
        "data": [
            {
                "id": "spell_flame",
                "name": "Flame Sling",
                "type": "incantation",
                "cost": 10,
                "slots": 1,
                "requires": [
                    {"name": "Faith", "amount": 9},
                    {"name": "Arcane", "amount": 0},
                ],
            }
        ],
    }
    sorceries_payload: dict[str, Any] = {
        "source": "https://example.com/sorceries",
        "data": [
            {
                "id": "spell_pebble",
                "name": "Glintstone Pebble",
                "type": "sorcery",
                "cost": 7,
                "slots": 1,
                "requires": [{"name": "Intelligence", "amount": 10}],
            }
        ],
    }
    _write_json(
        raw_root / "github_api" / "incantations.json",
        incantations_payload,
    )
    _write_json(
        raw_root / "github_api" / "sorceries.json",
        sorceries_payload,
    )

    records = load_github_api_spells(raw_root)
    assert len(records) == 2
    spell = next(entry for entry in records if entry["name"] == "Glintstone Pebble")
    assert spell["spell_type"] == "sorcery"
    assert spell["required_int"] == 10


def test_items_canonical_includes_github_fallback(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    _write_csv(
        raw_root / "kaggle" / "base" / "items.csv",
        [
            {
                "id": "item_golden_seed",
                "name": "Golden Seed",
                "type": "key item",
                "description": "Strengthens flask",
                "weight": 0.2,
                "sellPrice": 100,
                "maxAmount": 5,
                "rarity": "rare",
            }
        ],
    )
    dlc_items_dir = raw_root / "kaggle" / "dlc" / "eldenringScrap" / "items"
    _write_csv(dlc_items_dir / "key_items.csv", [], fieldnames=["name"])

    github_payload: dict[str, Any] = {
        "source": "https://example.com/items",
        "data": [
            {
                "id": "item_golden_seed",
                "name": "Golden Seed",
                "type": "key item",
                "description": "Fallback copy",
                "weight": 0.25,
            },
            {
                "id": "item_starlight",
                "name": "Starlight Shard",
                "type": "consumable",
                "description": "Restores FP",
                "weight": 0.1,
                "maxAmount": 99,
            },
        ],
    }
    _write_json(raw_root / "github_api" / "items.json", github_payload)

    df = build_items_canonical(
        raw_root=raw_root,
        output_path=tmp_path / "curated" / "items.parquet",
        dry_run=False,
    )

    assert set(df["name"]) == {"Golden Seed", "Starlight Shard"}

    golden = df[df["name"] == "Golden Seed"].iloc[0]
    assert golden["source"] == "kaggle_base"
    assert "github_api" in _provenance_sources(golden)

    shard = df[df["name"] == "Starlight Shard"].iloc[0]
    assert shard["source"] == "github_api"
    assert shard["category"] == "consumable"


def test_bosses_canonical_merges_sources(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    _write_csv(
        raw_root / "kaggle" / "base" / "bosses.csv",
        [
            {
                "id": "boss_margit",
                "name": "Margit the Fell Omen",
                "description": "Gatekeeper",
                "region": "Limgrave",
                "location": "Stormhill",
                "drops": "Talisman Pouch",
                "healthPoints": 4200,
            }
        ],
    )
    _write_csv(
        raw_root / "kaggle" / "dlc" / "eldenringScrap" / "bosses.csv",
        [],
        fieldnames=["name"],
    )

    github_payload: dict[str, Any] = {
        "source": "https://example.com/bosses",
        "data": [
            {
                "id": "boss_margit",
                "name": "Margit the Fell Omen",
                "region": "Stormhill",
                "location": "Castle Gate",
            },
            {
                "id": "boss_divine",
                "name": "Divine Beast Dancing Lion",
                "region": "Shadow Keep",
                "location": "Messmer's Arena",
                "drops": ["Great Grave Glovewort"],
            },
        ],
    }
    _write_json(raw_root / "github_api" / "bosses.json", github_payload)

    df = build_bosses_canonical(
        raw_root=raw_root,
        output_path=tmp_path / "curated" / "bosses.parquet",
        dry_run=False,
    )

    assert set(df["name"]) == {
        "Margit the Fell Omen",
        "Divine Beast Dancing Lion",
    }

    margit = df[df["name"] == "Margit the Fell Omen"].iloc[0]
    assert margit["source"] == "kaggle_base"
    assert "github_api" in _provenance_sources(margit)

    lion = df[df["name"] == "Divine Beast Dancing Lion"].iloc[0]
    assert lion["source"] == "github_api"
    assert lion["region"] == "Shadow Keep"


def test_armor_canonical_keeps_github_only_rows(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    _write_csv(
        raw_root / "kaggle" / "base" / "armors.csv",
        [
            {
                "id": "armor_knight",
                "name": "Knight Helm",
                "description": "Reliable",
                "category": "head",
                "weight": 4.6,
                "dmgNegation": '[{"name": "Phy", "amount": 6.8}]',
                "resistance": '[{"name": "Imm.", "amount": 70}]',
            }
        ],
    )
    _write_csv(
        raw_root / "kaggle" / "dlc" / "eldenringScrap" / "armors.csv",
        [],
        fieldnames=["name"],
    )

    github_payload: dict[str, Any] = {
        "source": "https://example.com/armor",
        "data": [
            {
                "id": "armor_knight",
                "name": "Knight Helm",
                "category": "head",
                "weight": 4.8,
                "dmgNegation": [{"name": "Phy", "amount": 6.5}],
            },
            {
                "id": "armor_sable",
                "name": "Sable Hood",
                "category": "head",
                "weight": 2.1,
                "dmgNegation": [{"name": "Phy", "amount": 3.1}],
                "resistance": [{"name": "Imm.", "amount": 55}],
            },
        ],
    }
    _write_json(raw_root / "github_api" / "armors.json", github_payload)

    df = build_armor_canonical(
        raw_root=raw_root,
        output_path=tmp_path / "curated" / "armor.parquet",
        dry_run=False,
    )

    assert set(df["name"]) == {"Knight Helm", "Sable Hood"}

    sable = df[df["name"] == "Sable Hood"].iloc[0]
    assert sable["source"] == "github_api"
    assert isclose(float(sable["weight"]), 2.1, rel_tol=1e-6)


def test_spells_canonical_handles_mixed_sources(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    _write_csv(
        raw_root / "kaggle" / "base" / "incantations.csv",
        [
            {
                "id": "spell_bestial",
                "name": "Bestial Sling",
                "type": "incantation",
                "description": "Hurls stones",
                "cost": 7,
                "slots": 1,
                "requires": '{"Faith": 10}',
            }
        ],
    )
    _write_csv(
        raw_root / "kaggle" / "base" / "sorceries.csv",
        [],
        fieldnames=["name"],
    )
    dlc_dir = raw_root / "kaggle" / "dlc" / "eldenringScrap"
    _write_csv(dlc_dir / "incantations.csv", [], fieldnames=["name"])
    _write_csv(dlc_dir / "sorceries.csv", [], fieldnames=["name"])

    github_incantations: dict[str, Any] = {
        "source": "https://example.com/incantations",
        "data": [
            {
                "id": "spell_bestial",
                "name": "Bestial Sling",
                "type": "incantation",
                "cost": 9,
                "slots": 1,
                "requires": [{"name": "Faith", "amount": 9}],
            },
            {
                "id": "spell_poison_mist",
                "name": "Poison Mist",
                "type": "incantation",
                "cost": 18,
                "slots": 1,
                "requires": [{"name": "Faith", "amount": 12}],
            },
        ],
    }
    github_sorceries: dict[str, Any] = {
        "source": "https://example.com/sorceries",
        "data": [],
    }
    _write_json(
        raw_root / "github_api" / "incantations.json",
        github_incantations,
    )
    _write_json(
        raw_root / "github_api" / "sorceries.json",
        github_sorceries,
    )

    df = build_spells_canonical(
        raw_root=raw_root,
        output_path=tmp_path / "curated" / "spells.parquet",
        dry_run=False,
    )

    assert set(df["name"]) == {"Bestial Sling", "Poison Mist"}

    bestial = df[df["name"] == "Bestial Sling"].iloc[0]
    assert bestial["source"] == "kaggle_base"
    assert "github_api" in _provenance_sources(bestial)

    poison_mist = df[df["name"] == "Poison Mist"].iloc[0]
    assert poison_mist["source"] == "github_api"
    assert poison_mist["fp_cost"] == 18
