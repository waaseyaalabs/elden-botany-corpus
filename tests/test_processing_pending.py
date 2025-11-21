"""Focused tests for incremental processing helpers."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from pipeline.process import DataProcessor


@pytest.fixture()
def processor_setup(tmp_path: Path) -> dict[str, Path]:
    """Create a temporary processor with sample data."""
    config_dir = tmp_path / "config"
    raw_dir = tmp_path / "data" / "raw"
    processed_dir = tmp_path / "data" / "processed"

    config_dir.mkdir(parents=True)
    raw_dir.mkdir(parents=True)
    processed_dir.mkdir(parents=True)

    config_text = """
datasets:
  - name: "test-weapons"
    owner: "test"
    slug: "test-weapons"
    enabled: true

settings:
  skip_disabled: true
"""
    config_path = config_dir / "datasets.yml"
    config_path.write_text(config_text)

    dataset_dir = raw_dir / "test-weapons"
    dataset_dir.mkdir(parents=True)

    pd.DataFrame(
        {
            "ID": [1],
            "Name": ["Test Sword"],
            "Weapon Type": ["sword"],
            "Damage Physical": [100],
            "Weight": [5.0],
        }
    ).to_csv(dataset_dir / "weapons.csv", index=False)

    return {
        "config": config_path,
        "raw": raw_dir,
        "processed": processed_dir,
    }


def test_pending_detects_unprocessed_files(
    processor_setup: dict[str, Path],
) -> None:
    """Datasets with untouched raw files should appear as pending."""
    processor = DataProcessor(
        config_path=processor_setup["config"],
        raw_dir=processor_setup["raw"],
        processed_dir=processor_setup["processed"],
    )

    pending = processor.get_pending_datasets()

    assert "test-weapons" in pending
    assert pending["test-weapons"]["reason"] == "stale_files"
    assert pending["test-weapons"]["files"]


def test_pending_respects_cache_and_force(
    processor_setup: dict[str, Path],
) -> None:
    """Cache clears pending datasets unless --force is supplied."""
    processor = DataProcessor(
        config_path=processor_setup["config"],
        raw_dir=processor_setup["raw"],
        processed_dir=processor_setup["processed"],
    )

    processor.process_dataset("test-weapons")

    pending_after = processor.get_pending_datasets()
    assert pending_after == {}

    pending_force = processor.get_pending_datasets(force=True)
    assert pending_force["test-weapons"]["reason"] == "force"
    assert pending_force["test-weapons"]["files"]
