"""Tests for utility functions."""


from pathlib import Path

import polars as pl

from corpus.utils import (
    MetadataTracker,
    compute_file_hash,
    load_json,
    merge_text_fields,
    save_json,
    standardize_column_names,
)


def test_compute_file_hash(tmp_path: Path) -> None:
    """Test file hash computation."""
    test_file = tmp_path / "test.txt"
    test_file.write_text("test content")

    hash1 = compute_file_hash(test_file)
    assert len(hash1) == 64  # SHA256 hex length

    # Same content should give same hash
    hash2 = compute_file_hash(test_file)
    assert hash1 == hash2

    # Different content should give different hash
    test_file.write_text("different content")
    hash3 = compute_file_hash(test_file)
    assert hash1 != hash3


def test_save_load_json(tmp_path: Path) -> None:
    """Test JSON save and load."""
    test_data = {"key": "value", "number": 42, "list": [1, 2, 3]}
    test_file = tmp_path / "test.json"

    save_json(test_data, test_file)
    assert test_file.exists()

    loaded = load_json(test_file)
    assert loaded == test_data


def test_standardize_column_names() -> None:
    """Test column name standardization."""
    df = pl.DataFrame({
        "Item Name": ["Sword"],
        "HP Cost": [10],
        "FP": [5],
    })

    df_std = standardize_column_names(df)

    assert "item_name" in df_std.columns
    assert "hp_cost" in df_std.columns
    assert "fp" in df_std.columns


def test_merge_text_fields() -> None:
    """Test merging text fields."""
    row = {
        "description": "A powerful weapon",
        "effect": "Increases damage",
        "location": "Found in castle",
        "empty_field": None,
        "nan_field": "nan",
    }

    text = merge_text_fields(row, ["description", "effect", "location"])

    assert "Description: A powerful weapon" in text
    assert "Effect: Increases damage" in text
    assert "Location: Found in castle" in text

    # Should not include empty or nan fields
    assert "empty_field" not in text.lower()
    assert "nan_field" not in text.lower()


def test_metadata_tracker(tmp_path: Path) -> None:
    """Test metadata tracking."""
    tracker = MetadataTracker()

    tracker.add_row_count("source1", 100)
    tracker.add_row_count("source2", 200)
    tracker.add_entity_count("weapon", 50)
    tracker.add_duplicates_removed("weapon", 5)
    tracker.set_unmapped_texts(10)
    tracker.add_provenance_summary("kaggle", 150)

    # Save metadata
    output_file = tmp_path / "metadata.json"
    tracker.save(output_file)

    assert output_file.exists()

    # Load and verify
    data = load_json(output_file)
    assert data["row_counts"]["source1"] == 100
    assert data["entity_counts"]["weapon"] == 50
    assert data["unmapped_texts"] == 10
    assert data["provenance_summary"]["kaggle"] == 150
