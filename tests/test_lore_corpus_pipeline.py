# pyright: reportMissingImports=false
# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false
# pyright: reportMissingModuleSource=false
# pyright: reportMissingTypeStubs=false
# pyright: reportUnknownArgumentType=false

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from pipelines.build_lore_corpus import (
    _compute_lore_id,
    _parse_impalers_dump,
    build_lore_corpus,
)


def test_parse_impalers_dump_extracts_entries(tmp_path: Path) -> None:
    html = """
    <h2>AccessoryName_dlc01.fmg</h2>
    <h3>Verdant Charm [10]</h3>
    <p>First line</p>
    <p>Second line</p>
    """
    dump_path = tmp_path / "Master.html"
    dump_path.write_text(html, encoding="utf-8")

    entries = _parse_impalers_dump(dump_path)
    assert len(entries) == 1
    entry = entries[0]
    assert entry["name"] == "Verdant Charm"
    assert entry["entry_id"] == "10"
    assert entry["category"] == "item"
    assert "First line" in "\n".join(entry["paragraphs"])


def test_build_lore_corpus_pipeline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    curated_root = tmp_path / "curated"
    raw_root = tmp_path / "raw"
    impalers_dir = raw_root / "impalers"
    curated_root.mkdir(parents=True)
    impalers_dir.mkdir(parents=True)

    def _write_parquet(path: Path, rows: list[dict[str, object]]) -> None:
        frame = pd.DataFrame(rows)
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(path, index=False)

    base_row = {
        "source": "kaggle_base",
        "source_id": "src",
        "source_priority": 1,
        "provenance": json.dumps([{"source": "kaggle_base"}]),
    }

    _write_parquet(
        curated_root / "items_canonical.parquet",
        [
            {
                **base_row,
                "item_id": 1,
                "canonical_slug": "verdant_charm",
                "name": "Verdant Charm",
                "description": "  Line one.  \n\nLine two.",
                "effect": None,
                "obtained_from": "Quest reward",
            }
        ],
    )

    _write_parquet(
        curated_root / "weapons_canonical.parquet",
        [
            {
                **base_row,
                "weapon_id": 10,
                "canonical_slug": "moon_blade",
                "name": "Moon Blade",
                "description": "Blade text",
                "special_effect": None,
            }
        ],
    )

    _write_parquet(
        curated_root / "armor_canonical.parquet",
        [
            {
                **base_row,
                "armor_id": 2,
                "canonical_slug": "old_armor",
                "name": "Old Armor",
                "description": "Armor lore",
            }
        ],
    )

    _write_parquet(
        curated_root / "bosses_canonical.parquet",
        [
            {
                **base_row,
                "boss_id": 3,
                "canonical_slug": "shadow_beast",
                "name": "Shadow Beast",
                "description": "Boss description",
                "quote": "Roars loudly",
            }
        ],
    )

    _write_parquet(
        curated_root / "spells_canonical.parquet",
        [
            {
                **base_row,
                "spell_id": 4,
                "canonical_slug": "lunar_spell",
                "name": "Lunar Spell",
                "description": "Spell lore",
            }
        ],
    )

    impalers_dir.joinpath("Master.html").write_text(
        """
        <h2>AccessoryName_dlc01.fmg</h2>
        <h3>Verdant Charm [99]</h3>
        <p>Impalers lore snippet.</p>
        """,
        encoding="utf-8",
    )

    dialogue_rows = [
        {
            "talk_id": 5001,
            "text": "Carian voice line",
            "speaker_id": 700,
            "speaker_name": "Primeval Sorcerer",
            "speaker_slug": "primeval_sorcerer",
            "source": "carian_dialogue_fmg",
            "payload": {"names": "NpcName"},
        }
    ]
    monkeypatch.setattr(
        "pipelines.build_lore_corpus.load_carian_dialogue_lines",
        lambda raw_root: dialogue_rows,
    )

    output = curated_root / "lore_corpus.parquet"
    df = build_lore_corpus(
        curated_root=curated_root,
        raw_root=raw_root,
        output_path=output,
        dry_run=False,
    )

    assert output.exists()
    assert not df.empty
    assert set(df["language"]) == {"en"}

    item_rows = df[
        (df["category"] == "item") & (df["text_type"] == "description")
    ]
    description_row = item_rows.iloc[0]
    expected_id = _compute_lore_id(
        description_row["canonical_id"],
        "description",
        description_row["text"],
    )
    assert description_row["lore_id"] == expected_id
    assert "Line one." in description_row["text"]
    assert "\n\n" not in description_row["text"]

    impaler_rows = df[df["source"] == "impalers"]
    assert len(impaler_rows) == 1
    imp_row = impaler_rows.iloc[0]
    assert imp_row["text_type"] == "impalers_excerpt"
    assert imp_row["text"].strip() == "Impalers lore snippet."

    parsed_provenance = json.loads(imp_row["provenance"])
    assert parsed_provenance["source"] == "impalers"

    dialogue_df = df[df["text_type"] == "dialogue"]
    assert len(dialogue_df) == 1
    dialogue_row = dialogue_df.iloc[0]
    assert dialogue_row["category"] == "npc"
    assert dialogue_row["source"] == "carian_dialogue_fmg"
    assert dialogue_row["canonical_id"] == "npc:700"
    assert dialogue_row["raw_canonical_id"] == "npc:700"
