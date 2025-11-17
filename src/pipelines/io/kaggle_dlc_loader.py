from __future__ import annotations

import csv
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, cast

from pipelines.io.common import (
    build_armor_record,
    build_boss_record,
    build_item_record,
    build_spell_record,
    extract_requirements,
    extract_scaling,
    normalize_weapon_type,
    parse_structured_data,
    safe_float,
    serialize_payload,
    to_entry_list,
)


def load_kaggle_dlc_weapons(raw_root: Path) -> list[dict[str, Any]]:
    """Load Kaggle DLC weapons and normalize core columns."""

    weapons_path = raw_root / "kaggle" / "dlc" / "eldenringScrap" / "weapons.csv"
    rows = _read_csv(weapons_path)
    records: list[dict[str, Any]] = []

    for row in rows:
        name = row.get("name")
        if not isinstance(name, str) or not name.strip():
            continue

        requirements = to_entry_list(row.get("requirements"))
        scaling_entries = to_entry_list(row.get("scales_with"))

        normalized: dict[str, Any] = {
            "name": name.strip(),
            "description": row.get("description") or None,
            "weapon_type": normalize_weapon_type(row.get("category")),
            "weight": safe_float(row.get("weight")),
            "source": "kaggle_dlc",
            "source_id": str(row.get("weapon_id") or row.get("id")),
            "is_dlc": True,
            "source_priority": 1,
            "source_payload": serialize_payload(dict(row)),
        }

        normalized.update(extract_requirements(requirements))
        normalized.update(extract_scaling(scaling_entries))

        records.append(normalized)

    return records


def load_kaggle_dlc_items(raw_root: Path) -> list[dict[str, Any]]:
    """Load DLC-specific item tables found under the items/ folder."""

    items_dir = raw_root / "kaggle" / "dlc" / "eldenringScrap" / "items"
    if not items_dir.exists():
        message = f"Missing Kaggle DLC items directory: {items_dir}"
        raise FileNotFoundError(message)

    records: list[dict[str, Any]] = []

    for csv_path in sorted(items_dir.glob("*.csv")):
        rows = _read_csv(csv_path)
        category_hint = csv_path.stem.replace("_", " ")

        for idx, row in enumerate(rows):
            name = row.get("name")
            if not isinstance(name, str) or not name.strip():
                continue

            source_id = str(row.get("id") or f"{csv_path.stem}:{idx}")
            record = build_item_record(
                name=name,
                description=row.get("description"),
                category_raw=row.get("type"),
                category_hint=category_hint,
                weight=_first_present(row, ["weight", "Weight"]),
                sell_price=_first_present(row, ["sell_price", "sellPrice", "value", "price"]),
                max_stack=_first_present(
                    row,
                    [
                        "max",
                        "maxHeld",
                        "max_held",
                        "maxAmount",
                    ],
                ),
                rarity=row.get("rarity"),
                effect=row.get("effect"),
                obtained_from=_first_present(row, ["how to acquire", "location", "obtainedFrom"]),
                is_dlc=_bool_from_value(row.get("dlc", 1)),
                source="kaggle_dlc",
                source_id=source_id,
                source_priority=1,
                payload=dict(row),
                extra_fields={
                    "image": row.get("image") or None,
                    "fp_cost": _first_present(row, ["FP cost", "fp_cost"]),
                },
            )
            records.append(record)

    return records


def load_kaggle_dlc_bosses(raw_root: Path) -> list[dict[str, Any]]:
    """Load DLC boss data."""

    bosses_path = raw_root / "kaggle" / "dlc" / "eldenringScrap" / "bosses.csv"
    rows = _read_csv(bosses_path)

    records: list[dict[str, Any]] = []

    for row in rows:
        name = row.get("name")
        if not isinstance(name, str) or not name.strip():
            continue

        drops_payload = row.get("Locations & Drops")
        record = build_boss_record(
            name=name,
            description=row.get("description"),
            region=None,
            location=_deduce_location_from_payload(drops_payload),
            drops=drops_payload,
            health_points=row.get("HP"),
            quote=row.get("blockquote"),
            is_dlc=_bool_from_value(row.get("dlc", 1)),
            source="kaggle_dlc",
            source_id=str(row.get("id") or name),
            source_priority=1,
            payload=dict(row),
            extra_fields={
                "image": row.get("image") or None,
            },
        )
        records.append(record)

    return records


def load_kaggle_dlc_armor(raw_root: Path) -> list[dict[str, Any]]:
    """Load DLC armor pieces."""

    armor_path = raw_root / "kaggle" / "dlc" / "eldenringScrap" / "armors.csv"
    rows = _read_csv(armor_path)

    records: list[dict[str, Any]] = []

    for row in rows:
        name = row.get("name")
        if not isinstance(name, str) or not name.strip():
            continue

        damage_entries = to_entry_list(_first_present(row, ["damage negation", "dmgNegation"]))
        resistance_entries = to_entry_list(row.get("resistance"))

        record = build_armor_record(
            name=name,
            description=row.get("description"),
            armor_type=row.get("type"),
            weight=row.get("weight"),
            damage_entries=damage_entries,
            resistance_entries=resistance_entries,
            is_dlc=_bool_from_value(row.get("dlc", 1)),
            source="kaggle_dlc",
            source_id=str(row.get("id") or name),
            source_priority=1,
            payload=dict(row),
            extra_fields={
                "image": row.get("image") or None,
                "special_effect": row.get("special effect") or None,
                "how_to_acquire": row.get("how to acquire") or None,
                "in_game_section": row.get("in-game section") or None,
            },
        )
        records.append(record)

    return records


def load_kaggle_dlc_spells(raw_root: Path) -> list[dict[str, Any]]:
    """Load DLC incantations and sorceries."""

    base_dir = raw_root / "kaggle" / "dlc" / "eldenringScrap"
    incantations_path = base_dir / "incantations.csv"
    sorceries_path = base_dir / "sorceries.csv"

    records: list[dict[str, Any]] = []

    records.extend(
        _load_kaggle_dlc_spell_rows(
            rows=_read_csv(incantations_path),
            spell_type_hint="incantation",
        )
    )
    records.extend(
        _load_kaggle_dlc_spell_rows(
            rows=_read_csv(sorceries_path),
            spell_type_hint="sorcery",
        )
    )

    return records


def _load_kaggle_dlc_spell_rows(
    *, rows: list[dict[str, Any]], spell_type_hint: str
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    for row in rows:
        name = row.get("name")
        if not isinstance(name, str) or not name.strip():
            continue

        record = build_spell_record(
            name=name,
            description=row.get("description"),
            spell_type=row.get("type") or spell_type_hint,
            fp_cost=_first_present(row, ["FP", "fp", "cost"]),
            stamina_cost=_first_present(row, ["stamina cost", "stamina"]),
            slots_required=_first_present(row, ["slot", "slots"]),
            required_int=row.get("INT"),
            required_fai=row.get("FAI"),
            required_arc=row.get("ARC"),
            is_dlc=_bool_from_value(row.get("dlc", 1)),
            source="kaggle_dlc",
            source_id=str(row.get("id") or name),
            source_priority=1,
            payload=dict(row),
            extra_fields={
                "image": row.get("image") or None,
                "effects": row.get("effect") or None,
                "bonus": row.get("bonus") or None,
                "group": row.get("group") or None,
                "location": row.get("location") or None,
            },
        )
        records.append(record)

    return records


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        message = f"Missing Kaggle DLC file: {path}"
        raise FileNotFoundError(message)

    with path.open(encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def _first_present(row: Mapping[str, Any], keys: Sequence[str]) -> Any:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return None


def _bool_from_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return True
    text = str(value).strip().lower()
    if text in {"0", "false", "no"}:
        return False
    return True


def _deduce_location_from_payload(value: Any) -> str | None:
    data = parse_structured_data(value)
    if isinstance(data, dict) and data:
        keys: list[str] = []
        for key in data:
            text = str(cast(Any, key)).strip()
            if text:
                keys.append(text)
        if keys:
            return ", ".join(keys)
    return None
