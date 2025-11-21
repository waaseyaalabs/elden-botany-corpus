"""Tests for lineage manifest builder."""

import json
from pathlib import Path

from corpus.lineage import LineageManifestBuilder
from corpus.models import Provenance, RawEntity


def _sample_provenance(
    source: str,
    dataset: str,
    source_file: str,
) -> Provenance:
    return Provenance(
        source=source,
        dataset=dataset,
        source_file=source_file,
        uri=f"{source}://{dataset}/{source_file}",
        sha256="abc123",
    )


def test_lineage_builder_emits_per_entity_manifests(tmp_path: Path) -> None:
    """The builder writes manifests per entity type and records metadata."""
    curated_root = tmp_path / "curated"
    builder = LineageManifestBuilder(
        output_root=curated_root / "lineage",
        relative_root=curated_root,
    )

    weapon = RawEntity(
        entity_type="weapon",
        name="Sword of Night and Flame",
        provenance=[
            _sample_provenance(
                "kaggle_dlc",
                (
                    "pedroaltobelli/ultimate-elden-ring-"
                    "with-shadow-of-the-erdtree-dlc"
                ),
                "weapons.csv",
            )
        ],
    )

    spell = RawEntity(
        entity_type="spell",
        name="Night Comet",
        provenance=[
            _sample_provenance(
                "github_api",
                "fanapis:spells",
                "spells.json",
            )
        ],
    )

    summary = builder.build([weapon, spell])

    assert summary["total_records"] == 2
    assert "weapon" in summary["datasets"]
    assert "spell" in summary["datasets"]
    assert summary["index"].startswith("lineage/")

    weapon_manifest = curated_root / summary["datasets"]["weapon"]["path"]
    spell_manifest = curated_root / summary["datasets"]["spell"]["path"]
    index_manifest = curated_root / summary["index"]

    assert weapon_manifest.exists()
    assert spell_manifest.exists()
    assert index_manifest.exists()

    weapon_records = json.loads(weapon_manifest.read_text())
    assert weapon_records[0]["slug"] == "sword_of_night_and_flame"
    assert weapon_records[0]["sources"][0]["dataset"].startswith(
        "pedroaltobelli/"
    )

    spell_records = json.loads(spell_manifest.read_text())
    assert spell_records[0]["sources"][0]["source_file"] == "spells.json"
