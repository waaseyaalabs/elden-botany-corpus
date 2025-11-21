# pyright: reportMissingImports=false

from __future__ import annotations

import importlib
from pathlib import Path

load_carian_item_fmg = importlib.import_module(
    "pipelines.io.carian_fmg_loader"
).load_carian_item_fmg


def _write_fmg(path: Path, entries: list[tuple[int, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["<fmg>"]
    for entry_id, text in entries:
        lines.append(f'  <text id="{entry_id}">{text}</text>')
    lines.append("</fmg>")
    path.write_text("\n".join(lines), encoding="utf-8")


def _seed_item_support_files(archive_root: Path) -> None:
    filenames = [
        "GoodsName.fmg.xml",
        "GoodsCaption.fmg.xml",
        "AccessoryName.fmg.xml",
        "AccessoryCaption.fmg.xml",
        "GemName.fmg.xml",
        "GemCaption.fmg.xml",
    ]
    for filename in filenames:
        _write_fmg(archive_root / filename, [])


def test_item_loader_uses_alias_for_weapon_skill_names(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    archive_root = raw_root / "carian_archive"
    _seed_item_support_files(archive_root)

    _write_fmg(
        archive_root / "ArtsName.fmg.xml",
        [
            (9000, "Flame Art"),
        ],
    )
    _write_fmg(
        archive_root / "WeaponSkillCaption.fmg.xml",
        [
            (9000, "Ash of War description"),
        ],
    )

    records = load_carian_item_fmg(raw_root)
    skill_records = [
        record for record in records if record["source"] == "carian_skill_fmg"
    ]
    assert len(skill_records) == 1
    skill = skill_records[0]
    assert skill["name"] == "Flame Art"
    assert skill["description"] == "Ash of War description"


def test_item_loader_uses_alias_for_weapon_skill_captions(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"
    archive_root = raw_root / "carian_archive"
    _seed_item_support_files(archive_root)

    _write_fmg(
        archive_root / "WeaponSkillName.fmg.xml",
        [
            (9100, "Frost Stomp"),
        ],
    )
    _write_fmg(
        archive_root / "ArtsCaption.fmg.xml",
        [
            (9100, "Alt caption"),
        ],
    )

    records = load_carian_item_fmg(raw_root)
    skill_records = [
        record for record in records if record["source"] == "carian_skill_fmg"
    ]
    assert len(skill_records) == 1
    skill = skill_records[0]
    assert skill["name"] == "Frost Stomp"
    assert skill["description"] == "Alt caption"
