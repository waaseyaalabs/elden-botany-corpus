"""Entity alias normalization tests."""

# pyright: reportPrivateUsage=false, reportGeneralTypeIssues=false

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from pipelines.build_lore_corpus import (
    _apply_entity_aliases,
    _compute_lore_id,
    _load_entity_aliases,
    build_lore_corpus,
)
from pipelines.npc_motif_graph import (
    NPCMotifGraphConfig,
    NPCMotifGraphPipeline,
)
from tests.helpers import sample_taxonomy

ALIAS_PATH = Path("data/reference/entity_aliases.csv")


def _write_minimal_canonical(curated_root: Path) -> None:
    curated_root.mkdir(parents=True, exist_ok=True)

    def _write_parquet(file_name: str, rows: list[dict[str, object]]) -> None:
        frame = pd.DataFrame(rows)
        frame.to_parquet(curated_root / file_name, index=False)

    base_row = {
        "source": "kaggle_base",
        "source_id": "src",
        "source_priority": 1,
        "provenance": json.dumps([{"source": "kaggle_base"}]),
    }

    _write_parquet(
        "items_canonical.parquet",
        [
            {
                **base_row,
                "item_id": 1,
                "canonical_slug": "verdant_charm",
                "name": "Verdant Charm",
                "description": "Lore",
                "effect": None,
                "obtained_from": None,
            }
        ],
    )
    _write_parquet(
        "weapons_canonical.parquet",
        [
            {
                **base_row,
                "weapon_id": 10,
                "canonical_slug": "moon_blade",
                "name": "Moon Blade",
                "description": "Blade text",
                "skill_description": None,
                "special_effect": None,
            }
        ],
    )
    _write_parquet(
        "armor_canonical.parquet",
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
        "bosses_canonical.parquet",
        [
            {
                **base_row,
                "boss_id": 3,
                "canonical_slug": "shadow_beast",
                "name": "Shadow Beast",
                "description": "Boss description",
                "quote": "Roars loudly",
                "lore": None,
                "drops": None,
            }
        ],
    )
    _write_parquet(
        "spells_canonical.parquet",
        [
            {
                **base_row,
                "spell_id": 4,
                "canonical_slug": "lunar_spell",
                "name": "Lunar Spell",
                "description": "Spell lore",
                "effects": None,
            }
        ],
    )


def test_alias_table_contains_required_patterns() -> None:
    assert ALIAS_PATH.exists(), "entity_aliases.csv is missing"
    frame = pd.read_csv(ALIAS_PATH)
    expected = {
        ("npc:carian_speaker_1061*", "npc:ranni"),
        ("npc:carian_speaker_324*", "npc:gideon_ofnir"),
        ("npc:carian_speaker_31003*", "npc:hyetta"),
        ("npc:carian_speaker_10014*", "npc:melina"),
        ("npc:carian_speaker_2054*", "npc:marika"),
        ("npc:carian_speaker_220*", "npc:iron_fist_alexander"),
        ("npc:carian_speaker_309*", "npc:patches"),
    }
    present = {
        (row["raw_id"], row["canonical_id"])
        for row in frame.to_dict("records")
    }
    missing = expected - present
    assert not missing, f"alias table missing entries: {missing}"


def test_build_lore_corpus_applies_entity_aliases(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    curated_root = tmp_path / "curated"
    raw_root = tmp_path / "raw"
    _write_minimal_canonical(curated_root)
    (raw_root / "impalers").mkdir(parents=True, exist_ok=True)
    (raw_root / "impalers" / "Master.html").write_text("", encoding="utf-8")

    dialogue_rows = [
        {
            "talk_id": 106110010,
            "text": "Rot shall guide the age.",
            "speaker_id": None,
            "speaker_name": "Carian Speaker 106110010",
            "speaker_slug": "carian_speaker_106110010",
            "source": "carian_dialogue_fmg",
        },
        {
            "talk_id": 324001000,
            "text": "Dreams favor Gideon the All-knowing.",
            "speaker_id": None,
            "speaker_name": "Carian Speaker 324001000",
            "speaker_slug": "carian_speaker_324001000",
            "source": "carian_dialogue_fmg",
        },
        {
            "talk_id": 310030010,
            "text": "Fingerprint grapes beckon.",
            "speaker_id": None,
            "speaker_name": "Carian Speaker 310030010",
            "speaker_slug": "carian_speaker_310030010",
            "source": "carian_dialogue_fmg",
        },
        {
            "talk_id": 20540100,
            "text": "Return to the bosom of earth.",
            "speaker_id": None,
            "speaker_name": "Carian Speaker 20540100",
            "speaker_slug": "carian_speaker_20540100",
            "source": "carian_dialogue_fmg",
        },
        {
            "talk_id": 100130020,
            "text": "I was born at the foot of the Erdtree.",
            "speaker_id": None,
            "speaker_name": "Carian Speaker 100130020",
            "speaker_slug": "carian_speaker_100130020",
            "source": "carian_dialogue_fmg",
        },
        {
            "talk_id": 220001000,
            "text": "I am Alexander, also known as the Iron Fist.",
            "speaker_id": None,
            "speaker_name": "Carian Speaker 220001000",
            "speaker_slug": "carian_speaker_220001000",
            "source": "carian_dialogue_fmg",
        },
    ]
    monkeypatch.setattr(
        "pipelines.build_lore_corpus.load_carian_dialogue_lines",
        lambda _: dialogue_rows,
    )

    output = curated_root / "lore_corpus.parquet"
    df = build_lore_corpus(
        curated_root=curated_root,
        raw_root=raw_root,
        output_path=output,
        dry_run=False,
    )

    assert output.exists()
    dialogue_df = df[df["text_type"] == "dialogue"].sort_values("text")
    assert set(dialogue_df["canonical_id"]) == {
        "npc:ranni",
        "npc:gideon_ofnir",
        "npc:hyetta",
        "npc:marika",
        "npc:melina",
        "npc:iron_fist_alexander",
    }
    for _, row in dialogue_df.iterrows():
        assert row["raw_canonical_id"].startswith("npc:carian_speaker_")
        expected_lore_id = _compute_lore_id(
            row["canonical_id"],
            row["text_type"],
            row["text"],
        )
        assert row["lore_id"] == expected_lore_id

    marika_row = dialogue_df[
        dialogue_df["canonical_id"] == "npc:marika"
    ].iloc[0]
    assert "npc:carian_speaker_2054" in marika_row["raw_canonical_id"]

    melina_row = dialogue_df[
        dialogue_df["canonical_id"] == "npc:melina"
    ].iloc[0]
    assert "npc:carian_speaker_10013" in melina_row["raw_canonical_id"]


def test_graph_pipeline_aggregates_alias_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    curated_root = tmp_path / "curated"
    raw_root = tmp_path / "raw"
    _write_minimal_canonical(curated_root)
    (raw_root / "impalers").mkdir(parents=True, exist_ok=True)
    (raw_root / "impalers" / "Master.html").write_text("", encoding="utf-8")

    dialogue_rows = [
        {
            "talk_id": 106110010,
            "text": "Rot dreams beneath the moon.",
            "speaker_id": None,
            "speaker_name": "Carian Speaker 106110010",
            "speaker_slug": "carian_speaker_106110010",
            "source": "carian_dialogue_fmg",
        },
        {
            "talk_id": 106210010,
            "text": "Dream anew of scarlet rot.",
            "speaker_id": None,
            "speaker_name": "Carian Speaker 106210010",
            "speaker_slug": "carian_speaker_106210010",
            "source": "carian_dialogue_fmg",
        },
    ]
    monkeypatch.setattr(
        "pipelines.build_lore_corpus.load_carian_dialogue_lines",
        lambda _: dialogue_rows,
    )

    lore_path = curated_root / "lore_corpus.parquet"
    build_lore_corpus(
        curated_root=curated_root,
        raw_root=raw_root,
        output_path=lore_path,
        dry_run=False,
    )

    pipeline = NPCMotifGraphPipeline(
        config=NPCMotifGraphConfig(
            curated_path=lore_path,
            output_dir=tmp_path / "graph",
            categories=("npc",),
        ),
        taxonomy=sample_taxonomy(),
    )
    artifacts = pipeline.run()

    entity_summary = pd.read_parquet(artifacts.entity_summary)
    ranni_row = entity_summary.loc[
        entity_summary["canonical_id"] == "npc:ranni"
    ].iloc[0]
    assert int(ranni_row["lore_count"]) == 2
    assert int(ranni_row["motif_mentions"]) >= 2


def test_entity_aliases_distinguish_melina_and_marika() -> None:
    aliases = _load_entity_aliases(ALIAS_PATH)
    frame = pd.DataFrame(
        [
            {
                "canonical_id": "npc:carian_speaker_20540100",
                "text": "Return to the bosom of earth.",
                "text_type": "dialogue",
            },
            {
                "canonical_id": "npc:carian_speaker_100130010",
                "text": "Only a little further till the foot of the Erdtree.",
                "text_type": "dialogue",
            },
        ]
    )

    replacements = _apply_entity_aliases(frame, aliases)
    assert replacements == 2
    assert set(frame["canonical_id"]) == {"npc:marika", "npc:melina"}
    assert set(frame["raw_canonical_id"]) == {
        "npc:carian_speaker_20540100",
        "npc:carian_speaker_100130010",
    }
