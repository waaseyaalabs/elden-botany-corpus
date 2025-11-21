from __future__ import annotations

import csv
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from pipelines.io.common import (
    build_armor_record,
    build_boss_record,
    build_item_record,
    build_spell_record,
    extract_damage,
    extract_requirements,
    extract_scaling,
    normalize_weapon_type,
    safe_float,
    serialize_payload,
    to_entry_list,
)


def load_kaggle_base_weapons(raw_root: Path) -> list[dict[str, Any]]:
    """Load and normalize Kaggle base-game weapons."""

    weapons_path = raw_root / "kaggle" / "base" / "weapons.csv"
    if not weapons_path.exists():
        message = f"Missing Kaggle base weapons file: {weapons_path}"
        raise FileNotFoundError(message)

    records: list[dict[str, Any]] = []

    rows = _read_csv(weapons_path)

    for row in rows:
        name = row.get("name")
        if not isinstance(name, str) or not name.strip():
            continue

        normalized: dict[str, Any] = {
            "name": name.strip(),
            "description": row.get("description") or None,
            "weapon_type": normalize_weapon_type(row.get("category")),
            "weight": safe_float(row.get("weight")),
            "source": "kaggle_base",
            "source_id": str(row.get("id")),
            "is_dlc": False,
            "source_priority": 2,
            "source_payload": serialize_payload(dict(row)),
        }

        attack_entries = to_entry_list(row.get("attack"))
        requirement_entries = to_entry_list(row.get("requiredAttributes"))
        scaling_entries = to_entry_list(row.get("scalesWith"))

        normalized.update(extract_damage(attack_entries))
        normalized.update(extract_requirements(requirement_entries))
        normalized.update(extract_scaling(scaling_entries))

        records.append(normalized)

    return records


def load_kaggle_base_items(raw_root: Path) -> list[dict[str, Any]]:
    """Load Kaggle base items as canonical-aligned rows."""

    items_path = raw_root / "kaggle" / "base" / "items.csv"
    rows = _read_csv(items_path)

    records: list[dict[str, Any]] = []

    for row in rows:
        name = row.get("name")
        if not isinstance(name, str) or not name.strip():
            continue

        record = build_item_record(
            name=name,
            description=row.get("description"),
            category_raw=row.get("type"),
            weight=row.get("weight"),
            sell_price=_first_present(row, ["sellPrice", "sell_price"]),
            max_stack=_first_present(row, ["maxAmount", "maxHeld", "max_held"]),
            rarity=row.get("rarity"),
            effect=row.get("effect"),
            obtained_from=row.get("obtainedFrom"),
            is_dlc=False,
            source="kaggle_base",
            source_id=str(row.get("id") or name),
            source_priority=2,
            payload=dict(row),
            extra_fields={
                "image": row.get("image") or None,
            },
        )
        records.append(record)

    return records


def load_kaggle_base_bosses(raw_root: Path) -> list[dict[str, Any]]:
    """Load Kaggle base bosses dataset."""

    bosses_path = raw_root / "kaggle" / "base" / "bosses.csv"
    rows = _read_csv(bosses_path)

    records: list[dict[str, Any]] = []

    for row in rows:
        name = row.get("name")
        if not isinstance(name, str) or not name.strip():
            continue

        record = build_boss_record(
            name=name,
            description=row.get("description"),
            region=row.get("region"),
            location=row.get("location"),
            drops=row.get("drops"),
            health_points=row.get("healthPoints"),
            quote=None,
            is_dlc=False,
            source="kaggle_base",
            source_id=str(row.get("id") or name),
            source_priority=2,
            payload=dict(row),
            extra_fields={
                "image": row.get("image") or None,
            },
        )
        records.append(record)

    return records


def load_kaggle_base_armor(raw_root: Path) -> list[dict[str, Any]]:
    """Load Kaggle base armor pieces."""

    armor_path = raw_root / "kaggle" / "base" / "armors.csv"
    rows = _read_csv(armor_path)

    records: list[dict[str, Any]] = []

    for row in rows:
        name = row.get("name")
        if not isinstance(name, str) or not name.strip():
            continue

        damage_entries = to_entry_list(row.get("dmgNegation"))
        resistance_entries = to_entry_list(row.get("resistance"))

        record = build_armor_record(
            name=name,
            description=row.get("description"),
            armor_type=row.get("category"),
            weight=row.get("weight"),
            damage_entries=damage_entries,
            resistance_entries=resistance_entries,
            is_dlc=False,
            source="kaggle_base",
            source_id=str(row.get("id") or name),
            source_priority=2,
            payload=dict(row),
            extra_fields={
                "image": row.get("image") or None,
            },
        )
        records.append(record)

    return records


def load_kaggle_base_spells(raw_root: Path) -> list[dict[str, Any]]:
    """Load sorceries and incantations from Kaggle base dumps."""

    incantations_path = raw_root / "kaggle" / "base" / "incantations.csv"
    sorceries_path = raw_root / "kaggle" / "base" / "sorceries.csv"
    records: list[dict[str, Any]] = []

    records.extend(
        _load_kaggle_spell_rows(
            rows=_read_csv(incantations_path),
            source="kaggle_base",
            source_priority=2,
            spell_type_hint="incantation",
            is_dlc=False,
        )
    )
    records.extend(
        _load_kaggle_spell_rows(
            rows=_read_csv(sorceries_path),
            source="kaggle_base",
            source_priority=2,
            spell_type_hint="sorcery",
            is_dlc=False,
        )
    )

    return records


def _load_kaggle_spell_rows(
    *,
    rows: list[dict[str, Any]],
    source: str,
    source_priority: int,
    spell_type_hint: str,
    is_dlc: bool,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    for row in rows:
        name = row.get("name")
        if not isinstance(name, str) or not name.strip():
            continue

        requirements = to_entry_list(row.get("requires"))
        required_int, required_fai, required_arc = _requirements_from_entries(requirements)

        record = build_spell_record(
            name=name,
            description=row.get("description"),
            spell_type=row.get("type") or spell_type_hint,
            fp_cost=row.get("cost"),
            stamina_cost=row.get("staminaCost"),
            slots_required=row.get("slots"),
            required_int=required_int,
            required_fai=required_fai,
            required_arc=required_arc,
            is_dlc=is_dlc,
            source=source,
            source_id=str(row.get("id") or name),
            source_priority=source_priority,
            payload=dict(row),
            extra_fields={
                "image": row.get("image") or None,
                "effects": row.get("effects") or None,
            },
        )
        records.append(record)

    return records


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        message = f"Missing Kaggle file: {path}"
        raise FileNotFoundError(message)

    with path.open(encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def _first_present(row: Mapping[str, Any], keys: Sequence[str]) -> Any:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return None


def _requirements_from_entries(
    entries: list[dict[str, Any]],
) -> tuple[Any, Any, Any]:
    lookup: dict[str, Any] = {}
    for entry in entries:
        name = str(entry.get("name", "")).strip().lower()
        if not name:
            continue
        lookup[name] = entry.get("amount")

    intelligence = lookup.get("intelligence") or lookup.get("int")
    faith = lookup.get("faith") or lookup.get("fai")
    arcane = lookup.get("arcane") or lookup.get("arc")

    return intelligence, faith, arcane
