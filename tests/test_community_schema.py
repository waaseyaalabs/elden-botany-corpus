from __future__ import annotations

from uuid import uuid4

import pytest

from corpus.community_schema import (
    AnnotationProvenance,
    AnnotationReviewState,
    CommunityAnnotation,
    CommunityAnnotationRevision,
    SubmissionChannel,
    load_motif_taxonomy,
)


def test_load_motif_taxonomy_has_required_categories() -> None:
    taxonomy = load_motif_taxonomy()
    category_slugs = {category.slug for category in taxonomy.categories}
    assert {"botanical", "elemental", "narrative"}.issubset(category_slugs)
    rot = taxonomy.get("scarlet_rot")
    assert rot is not None
    assert rot.category in category_slugs


def test_revision_validates_known_motifs() -> None:
    taxonomy = load_motif_taxonomy()
    annotation_id = uuid4()
    revision = CommunityAnnotationRevision(
        annotation_id=annotation_id,
        version=1,
        body="Scarlet rot as astral fungus",
        motif_tags=["scarlet_rot", "fungus"],
        provenance=AnnotationProvenance(
            source_type=SubmissionChannel.MANUAL,
            source_name="Lore editor",
        ),
    )
    revision.ensure_motifs_are_known(taxonomy)
    assert revision.review_state == AnnotationReviewState.PENDING


def test_revision_rejects_unknown_motif() -> None:
    taxonomy = load_motif_taxonomy()
    revision = CommunityAnnotationRevision(
        annotation_id=uuid4(),
        version=1,
        body="Unknown motif",
        motif_tags=["nonexistent"],
        provenance=AnnotationProvenance(source_type=SubmissionChannel.MANUAL),
    )
    with pytest.raises(ValueError):
        revision.ensure_motifs_are_known(taxonomy)


def test_annotation_promotes_status_after_revision() -> None:
    taxonomy = load_motif_taxonomy()
    annotation = CommunityAnnotation(
        canonical_id="sword_of_night_and_flame",
        submission_channel=SubmissionChannel.MANUAL,
        contributor_handle="LoreScribe",
    )
    revision = CommunityAnnotationRevision(
        annotation_id=annotation.id,
        version=1,
        body="Interprets duality of flame and moon",
        motif_tags=["flame", "dream"],
        provenance=AnnotationProvenance(source_type=SubmissionChannel.MANUAL),
    )
    annotation.add_revision(revision, taxonomy)
    assert annotation.status.name == "SUBMITTED"
    assert annotation.current_revision() is revision
    assert revision.is_current is True
