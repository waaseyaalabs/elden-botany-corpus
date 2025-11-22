from __future__ import annotations

from pathlib import Path

from corpus.community_bundle import load_bundle, scaffold_bundle
from corpus.community_schema import SubmissionChannel, load_motif_taxonomy


def test_scaffold_and_load_bundle(tmp_path: Path) -> None:
    taxonomy = load_motif_taxonomy()
    bundle_root = tmp_path / "bundle"
    bundle = scaffold_bundle(
        root=bundle_root,
        canonical_id="sword_of_night_and_flame",
        contributor_handle="lorescribe",
        submission_channel=SubmissionChannel.MANUAL,
        taxonomy=taxonomy,
        motif_tags=["flame", "dream"],
        body="Interpretation of flame and moon",
    )

    loaded = load_bundle(bundle.path, taxonomy=taxonomy)
    assert loaded.annotation.canonical_id == "sword_of_night_and_flame"
    assert loaded.annotation.contributor_handle == "lorescribe"
    assert loaded.annotation.revisions[0].body.startswith("Interpretation")


def test_scaffold_bundle_writes_notes_and_references(tmp_path: Path) -> None:
    taxonomy = load_motif_taxonomy()
    bundle_root = tmp_path / "bundle_notes"
    scaffold_bundle(
        root=bundle_root,
        canonical_id="goldmask",
        contributor_handle="scribe",
        submission_channel=SubmissionChannel.MANUAL,
        taxonomy=taxonomy,
        motif_tags=["oath"],
        body="Lore about oaths",
        notes="Initial reviewer context",
    )

    notes_path = bundle_root / "notes.md"
    assert notes_path.exists()
    assert "Initial reviewer context" in notes_path.read_text(encoding="utf-8")

    references_dir = bundle_root / "references"
    assert references_dir.is_dir()
    assert (references_dir / ".gitkeep").exists()
