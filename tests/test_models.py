"""Tests for data models."""

from corpus.models import (
    Provenance,
    RawEntity,
    create_slug,
    normalize_name_for_matching,
)


def test_create_slug() -> None:
    """Test slug generation."""
    assert create_slug("Sword of Night and Flame") == "sword_of_night_and_flame"
    assert create_slug("Rennala, Queen of the Full Moon") == "rennala_queen_of_the_full_moon"
    assert create_slug("Dragon's Breath") == "dragons_breath"
    assert create_slug("  Spaces  ") == "spaces"


def test_normalize_name_for_matching() -> None:
    """Test name normalization for matching."""
    assert normalize_name_for_matching("Sword of Night") == "sword of night"
    assert normalize_name_for_matching("Rennala, Queen!") == "rennala queen"
    assert normalize_name_for_matching("  Extra   Spaces  ") == "extra spaces"


def test_raw_entity_to_slug() -> None:
    """Test RawEntity slug generation."""
    entity = RawEntity(
        entity_type="weapon",
        name="Moonlight Greatsword",
        description="A legendary weapon",
    )

    assert entity.to_slug() == "moonlight_greatsword"


def test_provenance_model() -> None:
    """Test Provenance model."""
    prov = Provenance(
        source="kaggle_base",
        dataset="robikscube/elden-ring-ultimate-dataset",
        source_file="weapons.csv",
        uri="kaggle://test/weapons.csv",
        sha256="abc123",
    )

    assert prov.source == "kaggle_base"
    assert prov.dataset == "robikscube/elden-ring-ultimate-dataset"
    assert prov.source_file == "weapons.csv"
    assert prov.uri == "kaggle://test/weapons.csv"
    assert prov.sha256 == "abc123"
    assert prov.retrieved_at is not None
