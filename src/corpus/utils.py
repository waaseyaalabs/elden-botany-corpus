"""Utility functions for corpus processing."""

import hashlib
import json
from pathlib import Path
from typing import Any

import polars as pl
from tqdm import tqdm


def compute_file_hash(file_path: Path) -> str:
    """Compute SHA256 hash of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def load_json(file_path: Path) -> Any:
    """Load JSON file."""
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(data: Any, file_path: Path, indent: int = 2) -> None:
    """Save data to JSON file."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=indent, ensure_ascii=False)


def save_parquet(df: pl.DataFrame, file_path: Path) -> None:
    """Save Polars DataFrame to Parquet."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(file_path)


def save_csv(df: pl.DataFrame, file_path: Path) -> None:
    """Save Polars DataFrame to CSV."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(file_path)


def progress_bar(iterable: Any, desc: str = "", total: int | None = None) -> Any:
    """Create a tqdm progress bar."""
    return tqdm(iterable, desc=desc, total=total, unit="item")


def standardize_column_names(df: pl.DataFrame) -> pl.DataFrame:
    """
    Standardize DataFrame column names to snake_case.
    
    Examples:
        "Item Name" -> "item_name"
        "HP" -> "hp"
        "FP Cost" -> "fp_cost"
    """
    import re

    def to_snake_case(name: str) -> str:
        # Replace spaces and hyphens with underscores
        name = re.sub(r"[\s\-]+", "_", name)
        # Insert underscore before capital letters (for camelCase)
        name = re.sub(r"([a-z])([A-Z])", r"\1_\2", name)
        # Lowercase everything
        return name.lower()

    return df.rename({col: to_snake_case(col) for col in df.columns})


def merge_text_fields(row: dict[str, Any], text_fields: list[str]) -> str:
    """
    Merge multiple text fields into a single description.
    
    Args:
        row: Dictionary of column values
        text_fields: List of column names to merge
        
    Returns:
        Merged text with field labels
    """
    parts = []
    for field in text_fields:
        value = row.get(field)
        if value and str(value).strip() and str(value).lower() not in ("nan", "none", "null"):
            # Add field label if it's meaningful
            if len(text_fields) > 1:
                label = field.replace("_", " ").title()
                parts.append(f"{label}: {value}")
            else:
                parts.append(str(value))
    return "\n\n".join(parts)


def deduplicate_entities(
    df: pl.DataFrame, key_columns: list[str], prefer_column: str | None = None
) -> pl.DataFrame:
    """
    Deduplicate entities based on key columns.
    
    Args:
        df: Input DataFrame
        key_columns: Columns that define uniqueness
        prefer_column: If duplicates exist, prefer rows with non-null values in this column
        
    Returns:
        Deduplicated DataFrame
    """
    if prefer_column and prefer_column in df.columns:
        # Sort by prefer_column nulls last, then take first row per group
        df = df.sort(prefer_column, nulls_last=True)

    return df.unique(subset=key_columns, keep="first")


class MetadataTracker:
    """Track metadata about the curation process."""

    def __init__(self) -> None:
        self.metadata: dict[str, Any] = {
            "row_counts": {},
            "file_hashes": {},
            "entity_counts": {},
            "duplicates_removed": {},
            "unmapped_texts": 0,
            "provenance_summary": {},
        }

    def add_row_count(self, source: str, count: int) -> None:
        """Add row count for a source."""
        self.metadata["row_counts"][source] = count

    def add_file_hash(self, file_path: str, hash_value: str) -> None:
        """Add file hash."""
        self.metadata["file_hashes"][file_path] = hash_value

    def add_entity_count(self, entity_type: str, count: int) -> None:
        """Add entity count."""
        self.metadata["entity_counts"][entity_type] = count

    def add_duplicates_removed(self, entity_type: str, count: int) -> None:
        """Add count of duplicates removed."""
        self.metadata["duplicates_removed"][entity_type] = count

    def set_unmapped_texts(self, count: int) -> None:
        """Set count of unmapped DLC texts."""
        self.metadata["unmapped_texts"] = count

    def add_provenance_summary(self, source: str, count: int) -> None:
        """Add provenance summary."""
        self.metadata["provenance_summary"][source] = count

    def save(self, file_path: Path) -> None:
        """Save metadata to JSON."""
        save_json(self.metadata, file_path, indent=2)
