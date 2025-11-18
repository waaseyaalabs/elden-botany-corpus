from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from corpus.ingest_carian_fmg import CARIAN_FMG_CANDIDATES
from corpus.models import create_slug, normalize_name_for_matching

from pipelines.io.common import serialize_payload  # type: ignore[import]

FMG_PRIORITY = 4


RecordBuilder = Callable[[str, str | None, int], dict[str, Any]]


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class NameCaptionSpec:
    names_candidates: tuple[str, ...]
    captions_candidates: tuple[str, ...]
    source: str
    record_builder: RecordBuilder
    deduplicate_by_name: bool = False
    optional: bool = False


def load_carian_weapon_fmg(raw_root: Path) -> list[dict[str, Any]]:
    """Load weapon names/captions from the Carian Archive."""

    archive_root = _require_archive_root(raw_root)
    spec = NameCaptionSpec(
        names_candidates=CARIAN_FMG_CANDIDATES["weapon_name"],
        captions_candidates=CARIAN_FMG_CANDIDATES["weapon_caption"],
        source="carian_weapon_fmg",
        record_builder=_weapon_record_builder,
        deduplicate_by_name=True,
    )
    return _load_name_caption_records(archive_root, spec)


def load_carian_armor_fmg(raw_root: Path) -> list[dict[str, Any]]:
    """Load armor captions to enrich canonical armor descriptions."""

    archive_root = _require_archive_root(raw_root)
    spec = NameCaptionSpec(
        names_candidates=CARIAN_FMG_CANDIDATES["protector_name"],
        captions_candidates=CARIAN_FMG_CANDIDATES["protector_caption"],
        source="carian_armor_fmg",
        record_builder=_armor_record_builder,
        deduplicate_by_name=True,
    )
    return _load_name_caption_records(archive_root, spec)


def load_carian_item_fmg(raw_root: Path) -> list[dict[str, Any]]:
    """Aggregate item/talisman/spirit captions from Carian FMGs."""

    archive_root = _require_archive_root(raw_root)
    specs = [
        NameCaptionSpec(
            names_candidates=CARIAN_FMG_CANDIDATES["goods_name"],
            captions_candidates=CARIAN_FMG_CANDIDATES["goods_caption"],
            source="carian_goods_fmg",
            record_builder=_item_record_builder("consumable"),
        ),
        NameCaptionSpec(
            names_candidates=CARIAN_FMG_CANDIDATES["accessory_name"],
            captions_candidates=CARIAN_FMG_CANDIDATES["accessory_caption"],
            source="carian_accessory_fmg",
            record_builder=_item_record_builder("talisman"),
        ),
        NameCaptionSpec(
            names_candidates=CARIAN_FMG_CANDIDATES["gem_name"],
            captions_candidates=CARIAN_FMG_CANDIDATES["gem_caption"],
            source="carian_gem_fmg",
            record_builder=_item_record_builder("spirit"),
        ),
        NameCaptionSpec(
            names_candidates=CARIAN_FMG_CANDIDATES["weapon_skill"],
            captions_candidates=CARIAN_FMG_CANDIDATES["weapon_skill_caption"],
            source="carian_skill_fmg",
            record_builder=_item_record_builder("ash_of_war"),
        ),
    ]

    records: list[dict[str, Any]] = []
    for spec in specs:
        records.extend(_load_name_caption_records(archive_root, spec))
    return records


def load_carian_boss_fmg(raw_root: Path) -> list[dict[str, Any]]:
    """Load boss captions for narrative enrichment."""

    archive_root = _require_archive_root(raw_root)
    spec = NameCaptionSpec(
        names_candidates=CARIAN_FMG_CANDIDATES["boss_name"],
        captions_candidates=CARIAN_FMG_CANDIDATES["boss_caption"],
        source="carian_boss_fmg",
        record_builder=_boss_record_builder,
        optional=True,
    )
    return _load_name_caption_records(archive_root, spec)


def load_carian_spell_fmg(raw_root: Path) -> list[dict[str, Any]]:
    """Load spell names/captions for sorceries and incantations."""

    archive_root = _require_archive_root(raw_root)
    spec = NameCaptionSpec(
        names_candidates=CARIAN_FMG_CANDIDATES["magic_name"],
        captions_candidates=CARIAN_FMG_CANDIDATES["magic_caption"],
        source="carian_spell_fmg",
        record_builder=_spell_record_builder,
    )
    return _load_name_caption_records(archive_root, spec)


def load_carian_dialogue_lines(raw_root: Path) -> list[dict[str, Any]]:
    """Parse TalkMsg/NpcName FMGs into dialogue lines."""

    archive_root = _require_archive_root(raw_root)
    talk_path = _resolve_candidate_path(
        archive_root,
        CARIAN_FMG_CANDIDATES["talk"],
        description="Carian dialogue",
        required=False,
    )
    if talk_path is None:
        LOGGER.warning("Skipping Carian dialogue ingestion; TalkMsg missing")
        return []

    talk_entries = _parse_fmg_file(talk_path)

    npc_resolved = _resolve_candidate_path(
        archive_root,
        CARIAN_FMG_CANDIDATES["npc_name"],
        description="Carian NPC names",
        required=False,
    )
    if npc_resolved is None:
        LOGGER.warning("NpcName FMG missing; continuing without speaker names")
        npc_path = None
        npc_names: dict[int, str] = {}
    else:
        npc_path = npc_resolved
        npc_names = _parse_fmg_file(npc_path)

    lines: list[dict[str, Any]] = []
    for talk_id, text in sorted(talk_entries.items()):
        speaker_id = _resolve_speaker_id(talk_id, npc_names)
        speaker_name = npc_names.get(speaker_id) if speaker_id else None
        if not speaker_name:
            speaker_name = f"Carian Speaker {talk_id}"
        speaker_slug = create_slug(speaker_name)
        lines.append(
            {
                "talk_id": talk_id,
                "text": text,
                "speaker_id": speaker_id,
                "speaker_name": speaker_name,
                "speaker_slug": speaker_slug,
                "source": "carian_dialogue_fmg",
                "payload": serialize_payload(
                    {
                        "talk_path": str(talk_path),
                        "npc_path": str(npc_path) if npc_path else None,
                    }
                ),
            }
        )

    return lines


def _require_archive_root(raw_root: Path) -> Path:
    archive_root = raw_root / "carian_archive"
    if not archive_root.exists():
        message = f"Missing Carian Archive directory: {archive_root}"
        raise FileNotFoundError(message)
    return archive_root


def _load_name_caption_records(
    archive_root: Path,
    spec: NameCaptionSpec,
) -> list[dict[str, Any]]:
    names_path = _resolve_candidate_path(
        archive_root,
        spec.names_candidates,
        description=f"{spec.source} names",
        required=not spec.optional,
    )
    if names_path is None:
        return []

    captions_path = _resolve_candidate_path(
        archive_root,
        spec.captions_candidates,
        description=f"{spec.source} captions",
        required=False,
    )

    names = _parse_fmg_file(names_path)
    captions = _parse_fmg_file(captions_path) if captions_path else {}

    records: list[dict[str, Any]]
    if spec.deduplicate_by_name:
        records = _deduplicate_records(names, captions, spec.record_builder)
    else:
        records = []
        for fmg_id, name in sorted(names.items()):
            description = captions.get(fmg_id)
            record = spec.record_builder(name, description, fmg_id)
            record["_fmg_ids"] = [fmg_id]
            records.append(record)

    finalized: list[dict[str, Any]] = []
    for record in records:
        fmg_ids = record.pop("_fmg_ids", [])
        description_source = record.pop("_description_source", None)
        payload: dict[str, Any] = {
            "names_path": str(names_path),
            "captions_path": str(captions_path) if captions_path else None,
            "fmg_ids": fmg_ids,
            "description_id": description_source,
        }
        if fmg_ids:
            record.setdefault("source_id", str(fmg_ids[0]))
        else:
            record.setdefault("source_id", record.get("name"))
        record["source"] = spec.source
        record["source_priority"] = FMG_PRIORITY
        record["is_dlc"] = False
        record["source_payload"] = serialize_payload(payload)
        finalized.append(record)

    finalized.sort(key=lambda row: row["name"].lower())
    return finalized


def _deduplicate_records(
    names: dict[int, str],
    captions: dict[int, str],
    builder: RecordBuilder,
) -> list[dict[str, Any]]:
    records_by_key: dict[str, dict[str, Any]] = {}

    for fmg_id in sorted(names.keys()):
        name = names[fmg_id]
        match_key = normalize_name_for_matching(name)
        if not match_key:
            continue
        description = captions.get(fmg_id)
        record = records_by_key.get(match_key)
        if record is None:
            record = builder(name, description, fmg_id)
            record["_fmg_ids"] = [fmg_id]
            if description:
                record["_description_source"] = fmg_id
            records_by_key[match_key] = record
            continue

        record["_fmg_ids"].append(fmg_id)
        if description and not record.get("description"):
            record["description"] = description
            record["_description_source"] = fmg_id

    return list(records_by_key.values())


def _weapon_record_builder(
    name: str,
    description: str | None,
    _: int,
) -> dict[str, Any]:
    return {
        "name": name,
        "description": description,
        "weapon_type": "other",
        "weight": None,
    }


def _armor_record_builder(
    name: str,
    description: str | None,
    _: int,
) -> dict[str, Any]:
    return {
        "name": name,
        "description": description,
        "armor_type": "other",
        "weight": None,
    }


def _item_record_builder(category: str) -> RecordBuilder:
    def _builder(name: str, description: str | None, _: int) -> dict[str, Any]:
        return {
            "name": name,
            "description": description,
            "category": category,
            "weight": None,
            "sell_price": 0,
            "max_stack": 1,
            "rarity": None,
            "effect": None,
            "obtained_from": None,
        }

    return _builder


def _boss_record_builder(
    name: str,
    description: str | None,
    _: int,
) -> dict[str, Any]:
    return {
        "name": name,
        "description": description,
        "region": None,
        "location": None,
        "drops": None,
        "health_points": None,
        "quote": None,
    }


def _spell_record_builder(
    name: str,
    description: str | None,
    _: int,
) -> dict[str, Any]:
    return {
        "name": name,
        "description": description,
        "spell_type": "incantation",
        "fp_cost": 0,
        "stamina_cost": 0,
        "slots_required": 1,
        "required_int": 0,
        "required_fai": 0,
        "required_arc": 0,
    }


def _resolve_fmg_path(base_dir: Path, filename: str) -> Path:
    direct = base_dir / filename
    if direct.exists():
        return direct

    matches = sorted(base_dir.rglob(filename))
    if not matches:
        message = f"Missing FMG file {filename} under {base_dir}"
        raise FileNotFoundError(message)

    def _score(path: Path) -> tuple[int, str]:
        lower = str(path).lower()
        return (
            0 if "eng" in lower or "en" in lower else 1,
            str(path),
        )

    matches.sort(key=_score)
    return matches[0]


def _resolve_candidate_path(
    archive_root: Path,
    candidates: tuple[str, ...],
    *,
    description: str,
    required: bool,
) -> Path | None:
    missing: list[str] = []
    for index, candidate in enumerate(candidates):
        try:
            path = _resolve_fmg_path(archive_root, candidate)
        except FileNotFoundError as exc:
            missing.append(str(exc))
            continue
        if index > 0:
            LOGGER.info(
                "Using fallback FMG %s for %s (primary missing)",
                candidate,
                description,
            )
        return path

    if required:
        message = f"Missing FMG candidates for {description}; " f"tried {', '.join(candidates)}"
        raise FileNotFoundError(message)

    LOGGER.warning(
        "No FMG candidates available for %s; tried %s",
        description,
        ", ".join(candidates),
    )
    return None


def _parse_fmg_file(path: Path) -> dict[int, str]:
    if not path.exists():
        message = f"FMG file does not exist: {path}"
        raise FileNotFoundError(message)

    tree = ET.parse(path)
    root = tree.getroot()

    entries: dict[int, str] = {}
    for text_node in root.findall(".//text"):
        node_id_raw = text_node.get("id")
        if not node_id_raw:
            continue
        try:
            node_id = int(node_id_raw)
        except ValueError:
            continue

        value = _normalize_fmg_text(text_node.text or "")
        if not value or value == "%null%":
            continue
        entries[node_id] = value

    return entries


def _normalize_fmg_text(value: str) -> str:
    stripped_lines: list[str] = []
    blank_pending = False
    for line in value.splitlines():
        cleaned = line.strip()
        if not cleaned:
            if stripped_lines and not blank_pending:
                blank_pending = True
            continue
        if blank_pending:
            stripped_lines.append("")
            blank_pending = False
        stripped_lines.append(cleaned)
    text = "\n".join(stripped_lines).strip()
    return text


def _resolve_speaker_id(
    entry_id: int,
    npc_names: dict[int, str],
) -> int | None:
    candidate = entry_id
    while candidate > 0:
        if candidate in npc_names and npc_names[candidate] not in (
            None,
            "%null%",
        ):
            return candidate
        candidate //= 10
    return None
