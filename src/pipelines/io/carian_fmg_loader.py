from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

from corpus.models import normalize_name_for_matching
from pipelines.io.common import serialize_payload

FMG_SOURCE_NAME = "carian_weapon_fmg"
FMG_PRIORITY = 4


def load_carian_weapon_fmg(raw_root: Path) -> list[dict[str, Any]]:
    """Load weapon names and captions from Carian Archive FMG dumps."""

    archive_root = raw_root / "carian_archive"
    if not archive_root.exists():
        message = f"Missing Carian Archive directory: {archive_root}"
        raise FileNotFoundError(message)

    names_path = _resolve_fmg_path(archive_root, "WeaponName.fmg.xml")
    captions_path = _resolve_fmg_path(archive_root, "WeaponCaption.fmg.xml")

    names = _parse_fmg_file(names_path)
    captions = _parse_fmg_file(captions_path)

    records_by_key: dict[str, dict[str, Any]] = {}

    sorted_ids = sorted(names.keys())
    for fmg_id in sorted_ids:
        name = names[fmg_id]
        match_key = normalize_name_for_matching(name)
        if not match_key:
            continue

        description = captions.get(fmg_id)
        entry = records_by_key.get(match_key)
        if entry is None:
            entry = {
                "name": name,
                "description": description,
                "weapon_type": "other",
                "weight": None,
                "source": FMG_SOURCE_NAME,
                "source_id": str(fmg_id),
                "is_dlc": False,
                "source_priority": FMG_PRIORITY,
                "_fmg_ids": [fmg_id],
            }
            if description:
                entry["_description_source"] = fmg_id
            records_by_key[match_key] = entry
            continue

        entry["_fmg_ids"].append(fmg_id)
        if description and not entry.get("description"):
            entry["description"] = description
            entry["_description_source"] = fmg_id

    records: list[dict[str, Any]] = []
    for entry in records_by_key.values():
        payload = {
            "fmg_ids": entry.pop("_fmg_ids", []),
            "description_id": entry.pop("_description_source", None),
            "names_path": str(names_path),
            "captions_path": str(captions_path),
        }
        entry["source_payload"] = serialize_payload(payload)
        records.append(entry)

    records.sort(key=lambda row: row["name"].lower())
    return records


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
    stripped_lines = []
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
