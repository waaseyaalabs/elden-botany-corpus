from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, cast

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


def load_github_api_weapons(raw_root: Path) -> list[dict[str, Any]]:
    """Load GitHub API fallback data for weapons."""

    weapons_path = raw_root / "github_api" / "weapons.json"
    source_url, rows = _load_github_payload(weapons_path)
    records: list[dict[str, Any]] = []

    for entry_dict in rows:
        entry_mapping = cast(Mapping[str, Any], entry_dict)
        entry_dict = dict(entry_mapping)

        name = entry_dict.get("name")
        if not isinstance(name, str) or not name.strip():
            continue

        attack_entries = to_entry_list(entry_dict.get("attack"))
        requirements = to_entry_list(entry_dict.get("requiredAttributes"))
        scaling_entries = to_entry_list(
            entry_dict.get("scalesWith") or entry_dict.get("scales_with")
        )

        source_id_raw = (
            entry_dict.get("id")
            or entry_dict.get("slug")
            or name.strip().lower().replace(" ", "-")
        )

        normalized: dict[str, Any] = {
            "name": name.strip(),
            "description": entry_dict.get("description") or None,
            "weapon_type": normalize_weapon_type(entry_dict.get("category")),
            "weight": safe_float(entry_dict.get("weight")),
            "source": "github_api",
            "source_id": str(source_id_raw),
            "is_dlc": False,
            "source_priority": 3,
            "source_payload": serialize_payload(entry_dict),
        }
        if source_url:
            normalized["source_uri"] = source_url

        normalized.update(extract_damage(attack_entries))
        normalized.update(extract_requirements(requirements))
        normalized.update(extract_scaling(scaling_entries))

        records.append(normalized)

    return records


def load_github_api_items(raw_root: Path) -> list[dict[str, Any]]:
    """Load GitHub API items dataset."""

    items_path = raw_root / "github_api" / "items.json"
    source_url, rows = _load_github_payload(items_path)

    records: list[dict[str, Any]] = []

    for row in rows:
        name = row.get("name")
        if not isinstance(name, str) or not name.strip():
            continue

        source_id = row.get("id") or row.get("slug") or name
        extra_fields = {
            "image": row.get("image") or None,
        }
        if source_url:
            extra_fields["source_uri"] = source_url

        record = build_item_record(
            name=name,
            description=row.get("description"),
            category_raw=row.get("type"),
            weight=row.get("weight"),
            sell_price=row.get("sellPrice"),
            max_stack=row.get("maxAmount"),
            rarity=row.get("rarity"),
            effect=row.get("effect"),
            obtained_from=row.get("obtainedFrom"),
            is_dlc=False,
            source="github_api",
            source_id=str(source_id),
            source_priority=3,
            payload=row,
            extra_fields=extra_fields,
        )
        records.append(record)

    return records


def load_github_api_bosses(raw_root: Path) -> list[dict[str, Any]]:
    """Load GitHub API bosses data."""

    bosses_path = raw_root / "github_api" / "bosses.json"
    source_url, rows = _load_github_payload(bosses_path)

    records: list[dict[str, Any]] = []

    for row in rows:
        name = row.get("name")
        if not isinstance(name, str) or not name.strip():
            continue

        extra_fields = {
            "image": row.get("image") or None,
        }
        if source_url:
            extra_fields["source_uri"] = source_url

        record = build_boss_record(
            name=name,
            description=row.get("description"),
            region=row.get("region"),
            location=row.get("location"),
            drops=row.get("drops"),
            health_points=row.get("healthPoints"),
            quote=row.get("quote"),
            is_dlc=False,
            source="github_api",
            source_id=str(row.get("id") or row.get("slug") or name),
            source_priority=3,
            payload=row,
            extra_fields=extra_fields,
        )
        records.append(record)

    return records


def load_github_api_armor(raw_root: Path) -> list[dict[str, Any]]:
    """Load GitHub API armor data."""

    armor_path = raw_root / "github_api" / "armors.json"
    source_url, rows = _load_github_payload(armor_path)

    records: list[dict[str, Any]] = []

    for row in rows:
        name = row.get("name")
        if not isinstance(name, str) or not name.strip():
            continue

        damage_entries = to_entry_list(
            row.get("dmgNegation") or row.get("damage_negation")
        )
        resistance_entries = to_entry_list(row.get("resistance"))

        extra_fields = {
            "image": row.get("image") or None,
        }
        if source_url:
            extra_fields["source_uri"] = source_url

        record = build_armor_record(
            name=name,
            description=row.get("description"),
            armor_type=row.get("category") or row.get("type"),
            weight=row.get("weight"),
            damage_entries=damage_entries,
            resistance_entries=resistance_entries,
            is_dlc=False,
            source="github_api",
            source_id=str(row.get("id") or row.get("slug") or name),
            source_priority=3,
            payload=row,
            extra_fields=extra_fields,
        )
        records.append(record)

    return records


def load_github_api_spells(raw_root: Path) -> list[dict[str, Any]]:
    """Load GitHub API sorceries and incantations."""

    base_dir = raw_root / "github_api"
    records: list[dict[str, Any]] = []

    records.extend(
        _load_github_spell_file(
            base_dir / "incantations.json", spell_type_hint="incantation"
        )
    )
    records.extend(
        _load_github_spell_file(
            base_dir / "sorceries.json", spell_type_hint="sorcery"
        )
    )

    return records


def _load_github_payload(
    path: Path,
) -> tuple[str | None, list[dict[str, Any]]]:
    if not path.exists():
        message = f"Missing GitHub API file: {path}"
        raise FileNotFoundError(message)

    payload = json.loads(path.read_text(encoding="utf-8"))
    data_raw = payload.get("data", [])
    if not isinstance(data_raw, list):
        message = f"GitHub API payload at {path} missing 'data' list"
        raise ValueError(message)

    data: list[object] = data_raw

    rows: list[dict[str, Any]] = []
    for entry in data:
        if not isinstance(entry, Mapping):
            continue
        rows.append(dict(cast(Mapping[str, Any], entry)))
    return payload.get("source"), rows


def _load_github_spell_file(
    path: Path, *, spell_type_hint: str
) -> list[dict[str, Any]]:
    source_url, rows = _load_github_payload(path)

    records: list[dict[str, Any]] = []

    for row in rows:
        name = row.get("name")
        if not isinstance(name, str) or not name.strip():
            continue

        requirements = to_entry_list(row.get("requires"))
        required_int, required_fai, required_arc = _requirements_from_entries(
            requirements
        )

        extra_fields = {
            "image": row.get("image") or None,
            "effects": row.get("effects") or None,
        }
        if source_url:
            extra_fields["source_uri"] = source_url

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
            is_dlc=False,
            source="github_api",
            source_id=str(row.get("id") or row.get("slug") or name),
            source_priority=3,
            payload=row,
            extra_fields=extra_fields,
        )
        records.append(record)

    return records


def _requirements_from_entries(
    entries: Sequence[dict[str, Any]],
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
