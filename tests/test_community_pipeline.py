from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

from corpus.community_bundle import (
    BundleOperation,
    load_bundle,
    scaffold_bundle,
)
from corpus.community_schema import (
    SubmissionChannel,
    load_motif_taxonomy,
    utcnow,
)

from pipelines.community_ingest import CommunityIngestionPipeline


def _make_bundle(root: Path, canonical_id: str) -> Path:
    taxonomy = load_motif_taxonomy()
    bundle = scaffold_bundle(
        root=root,
        canonical_id=canonical_id,
        contributor_handle="tester",
        submission_channel=SubmissionChannel.MANUAL,
        taxonomy=taxonomy,
        motif_tags=["flame"],
        body="Initial lore interpretation",
    )
    return bundle.path


def test_pipeline_ingests_and_updates(tmp_path: Path) -> None:
    bundles_dir = tmp_path / "bundles"
    processed_dir = tmp_path / "processed"
    bundle_path = _make_bundle(bundles_dir / "bundle_a", "test_subject")

    taxonomy = load_motif_taxonomy()
    pipeline = CommunityIngestionPipeline(
        bundles_dir=bundles_dir,
        output_dir=processed_dir,
        taxonomy=taxonomy,
    )

    first_result = pipeline.ingest([bundle_path], actor="tester")
    assert first_result.created == 1

    bundle = load_bundle(bundle_path, taxonomy=taxonomy)
    bundle.annotation.revisions[0].body = "Updated lore interpretation"
    bundle.header.updated_at = utcnow()
    bundle.write()

    second_result = pipeline.ingest([bundle_path], actor="tester")
    assert second_result.updated == 1

    annotations_path = processed_dir / "community_annotations.parquet"
    revisions_path = processed_dir / "community_revisions.parquet"
    assert annotations_path.exists()
    assert revisions_path.exists()


def test_pipeline_detects_stale_conflicts(tmp_path: Path) -> None:
    bundles_dir = tmp_path / "bundles"
    processed_dir = tmp_path / "processed"
    bundle_path = _make_bundle(bundles_dir / "bundle_b", "conflict_subject")
    taxonomy = load_motif_taxonomy()
    pipeline = CommunityIngestionPipeline(
        bundles_dir=bundles_dir,
        output_dir=processed_dir,
        taxonomy=taxonomy,
    )

    pipeline.ingest([bundle_path], actor="tester")

    bundle = load_bundle(bundle_path, taxonomy=taxonomy)
    bundle.annotation.revisions[0].body = "Stale edit"
    bundle.header.updated_at = bundle.header.updated_at - timedelta(days=1)
    bundle.write()

    result = pipeline.ingest([bundle_path], actor="tester")
    assert result.updated == 0
    assert result.conflicts is not None
    assert len(result.conflicts) == 1

    conflict_files = list(pipeline.conflict_dir.glob("*.json"))
    assert conflict_files, "Expected conflict artifact to be written"
    payload = json.loads(conflict_files[0].read_text(encoding="utf-8"))
    assert payload["reason"] == "stale_bundle"


def test_pipeline_handles_delete_operations(tmp_path: Path) -> None:
    bundles_dir = tmp_path / "bundles"
    processed_dir = tmp_path / "processed"
    bundle_path = _make_bundle(bundles_dir / "bundle_c", "delete_subject")
    taxonomy = load_motif_taxonomy()
    pipeline = CommunityIngestionPipeline(
        bundles_dir=bundles_dir,
        output_dir=processed_dir,
        taxonomy=taxonomy,
    )

    pipeline.ingest([bundle_path], actor="tester")

    bundle = load_bundle(bundle_path, taxonomy=taxonomy)
    annotation_id = str(bundle.annotation.id)
    bundle.header.operation = BundleOperation.DELETE
    bundle.header.updated_at = utcnow()
    bundle.write()

    result = pipeline.ingest([bundle_path], actor="tester")

    assert result.deleted == 1
    assert pipeline.manifest.get(annotation_id) is None
    state_path = pipeline.state_dir / f"{annotation_id}.json"
    assert not state_path.exists()
