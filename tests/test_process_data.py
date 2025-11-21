"""Tests for data processing pipeline."""

import json
import os
import textwrap
import time
from pathlib import Path
from typing import TypedDict

import pandas as pd
import pytest
from pytest import MonkeyPatch

from pipeline.process import DataProcessor
from pipeline.schemas import (
    BOSSES_SCHEMA,
    ITEMS_SCHEMA,
    WEAPONS_SCHEMA,
    SchemaVersion,
    get_dataset_schema,
    list_schema_versions,
    validate_dataframe,
)
from pipeline.utils import (
    calculate_file_hash,
    normalize_categorical,
    normalize_column_names,
    read_data_file,
    write_parquet,
)


class TempDirs(TypedDict):
    config_dir: Path
    raw_dir: Path
    processed_dir: Path


class TestSchemas:
    """Test schema definitions and validation."""

    def test_get_dataset_schema_exact_match(self):
        """Test exact schema name matching."""
        schema = get_dataset_schema("weapons")
        assert schema is not None
        assert schema.schema == WEAPONS_SCHEMA
        assert schema.tag == "weapons_v1"

    def test_get_dataset_schema_fuzzy_match(self):
        """Test fuzzy schema name matching."""
        schema = get_dataset_schema("elden-ring-weapons")
        assert schema is not None
        assert schema.schema == WEAPONS_SCHEMA

    def test_get_dataset_schema_not_found(self):
        """Test schema not found returns None."""
        schema = get_dataset_schema("nonexistent")
        assert schema is None

    def test_get_dataset_schema_specific_version(self):
        """Explicit version requests should resolve identical schema."""
        schema = get_dataset_schema("weapons", version="v1")
        assert schema is not None
        assert schema.version == "v1"

    def test_list_schema_versions(self):
        """Schema registry should enumerate versions per dataset."""
        versions = list_schema_versions("weapons")
        assert isinstance(versions, list)
        assert len(versions) == 1
        first_version = versions[0]
        assert isinstance(first_version, SchemaVersion)
        assert first_version.tag == "weapons_v1"

    def test_validate_with_schema_version(self):
        """validate_dataframe accepts SchemaVersion payloads."""
        schema = get_dataset_schema("weapons")
        assert schema is not None
        df = pd.DataFrame(
            {
                "weapon_id": [1],
                "name": ["Longsword"],
                "weapon_type": ["sword"],
            }
        )
        is_valid, error, _ = validate_dataframe(df, schema)
        assert is_valid
        assert error is None

    def test_validate_weapons_schema_valid(self):
        """Test weapons schema validation with valid data."""
        df = pd.DataFrame(
            {
                "weapon_id": [1, 2],
                "name": ["Longsword", "Katana"],
                "weapon_type": ["sword", "katana"],
                "damage_physical": [110, 103],
                "weight": [3.5, 5.5],
            }
        )

        is_valid, error, _ = validate_dataframe(df, WEAPONS_SCHEMA)
        assert is_valid
        assert error is None

    def test_validate_weapons_schema_invalid_type(self):
        """Test weapons schema catches invalid weapon type."""
        df = pd.DataFrame(
            {
                "weapon_id": [1],
                "name": ["Invalid Weapon"],
                "weapon_type": ["invalid_type"],
                "damage_physical": [100],
                "weight": [5.0],
            }
        )

        is_valid, error, _ = validate_dataframe(df, WEAPONS_SCHEMA)
        assert not is_valid
        assert error is not None
        assert "weapon_type" in error

    def test_validate_items_schema_missing_required(self):
        """Test items schema catches missing required fields."""
        df = pd.DataFrame(
            {
                "item_id": [1, 2],
                # Missing 'name' and 'category' (required)
                "weight": [1.0, 2.0],
            }
        )

        is_valid, error, _ = validate_dataframe(df, ITEMS_SCHEMA)
        assert not is_valid
        assert error is not None

    def test_validate_bosses_schema_valid(self):
        """Test bosses schema with valid data."""
        df = pd.DataFrame(
            {
                "boss_id": [1, 2],
                "name": ["Margit", "Godrick"],
                "region": ["Limgrave", "Stormveil"],
                "hp": [4174, 6080],
                "optional": [False, False],
            }
        )

        is_valid, error, _ = validate_dataframe(df, BOSSES_SCHEMA)
        assert is_valid
        assert error is None


class TestUtils:
    """Test utility functions."""

    def test_normalize_column_names(self):
        """Test column name normalization."""
        df = pd.DataFrame(
            {
                "Item ID": [1, 2],
                "Item Name": ["Sword", "Shield"],
                "Weight (kg)": [3.5, 8.0],
            }
        )

        result = normalize_column_names(df)
        expected_cols = ["item_id", "item_name", "weight_kg"]
        assert list(result.columns) == expected_cols

    def test_normalize_categorical(self):
        """Test categorical value normalization."""
        df = pd.DataFrame(
            {
                "weapon_type": [
                    "Straight Sword",
                    "KATANA",
                    "great_sword",
                ]
            }
        )

        mapping = {
            "weapon_type": {
                "straight sword": "sword",
                "katana": "katana",
                "great_sword": "greatsword",
            }
        }

        result = normalize_categorical(df, mapping)
        assert result["weapon_type"].tolist() == [
            "sword",
            "katana",
            "greatsword",
        ]

    def test_calculate_file_hash(self, tmp_path: Path) -> None:
        """Test file hash calculation."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")

        hash1 = calculate_file_hash(test_file)
        hash2 = calculate_file_hash(test_file)

        # Same file should produce same hash
        assert hash1 == hash2
        assert len(hash1) == 64  # SHA256 produces 64 char hex

    def test_read_write_parquet(self, tmp_path: Path) -> None:
        """Test reading and writing Parquet files."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["A", "B", "C"],
                "value": [10.5, 20.3, 30.1],
            }
        )

        parquet_file = tmp_path / "test.parquet"
        write_parquet(df, parquet_file)

        assert parquet_file.exists()

        df_read = read_data_file(parquet_file)
        pd.testing.assert_frame_equal(df, df_read)


class TestDataProcessor:
    """Test DataProcessor class."""

    @pytest.fixture
    def temp_dirs(self, tmp_path: Path) -> TempDirs:
        """Create temporary directory structure."""
        config_dir = tmp_path / "config"
        raw_dir = tmp_path / "data" / "raw"
        processed_dir = tmp_path / "data" / "processed"

        config_dir.mkdir(parents=True)
        raw_dir.mkdir(parents=True)
        processed_dir.mkdir(parents=True)

        return {
            "config_dir": config_dir,
            "raw_dir": raw_dir,
            "processed_dir": processed_dir,
        }

    @pytest.fixture
    def config_file(self, temp_dirs: TempDirs) -> Path:
        """Create test configuration file."""
        config_path = temp_dirs["config_dir"] / "test_config.yml"
        config_content = textwrap.dedent(
            """
            datasets:
              - name: "test-weapons"
                owner: "test"
                slug: "test-weapons"
                enabled: true

              - name: "test-bosses"
                owner: "test"
                slug: "test-bosses"
                enabled: false

            settings:
              skip_disabled: true
              auto_unzip: true
              process_workers: 1
            """
        )
        config_path.write_text(config_content)
        return config_path

    @pytest.fixture
    def sample_weapons_csv(self, temp_dirs: TempDirs) -> Path:
        """Create sample weapons CSV file."""
        dataset_dir = temp_dirs["raw_dir"] / "test-weapons"
        dataset_dir.mkdir(parents=True)

        csv_path = dataset_dir / "weapons.csv"
        df = pd.DataFrame(
            {
                "ID": [1, 2, 3],
                "Name": ["Longsword", "Katana", "Greatsword"],
                "Weapon Type": ["sword", "katana", "greatsword"],
                "Damage Physical": [110, 103, 138],
                "Weight": [3.5, 5.5, 9.0],
                "Required STR": [10, 9, 31],
                "Required DEX": [10, 15, 12],
            }
        )
        df.to_csv(csv_path, index=False)
        return csv_path

    def test_processor_initialization(
        self,
        config_file: Path,
        temp_dirs: TempDirs,
    ) -> None:
        """Test DataProcessor initialization."""
        processor = DataProcessor(
            config_path=config_file,
            raw_dir=temp_dirs["raw_dir"],
            processed_dir=temp_dirs["processed_dir"],
        )

        assert processor.config is not None
        assert len(processor.config["datasets"]) == 2

    def test_process_dataset_creates_parquet(
        self,
        config_file: Path,
        temp_dirs: TempDirs,
        sample_weapons_csv: Path,
    ) -> None:
        """Test processing creates Parquet output."""
        processor = DataProcessor(
            config_path=config_file,
            raw_dir=temp_dirs["raw_dir"],
            processed_dir=temp_dirs["processed_dir"],
        )

        result = processor.process_dataset("test-weapons")

        assert result["status"] == "success"
        assert len(result["files_processed"]) == 1
        assert result["schema_version"] == "weapons_v1"
        assert result["files_processed"][0]["schema_version"] == "weapons_v1"

        # Check output file exists
        output_file: Path = (
            temp_dirs["processed_dir"] / "test-weapons" / "weapons.parquet"
        )
        assert output_file.exists()

    def test_process_dataset_validates_schema(
        self,
        config_file: Path,
        temp_dirs: TempDirs,
        sample_weapons_csv: Path,
    ) -> None:
        """Test processing validates against schema."""
        processor = DataProcessor(
            config_path=config_file,
            raw_dir=temp_dirs["raw_dir"],
            processed_dir=temp_dirs["processed_dir"],
        )

        result = processor.process_dataset("test-weapons")

        # Should succeed with valid data
        assert result["status"] == "success"
        assert result["schema_version"] == "weapons_v1"

        # Verify output data
        output_file: Path = (
            temp_dirs["processed_dir"] / "test-weapons" / "weapons.parquet"
        )
        df = pd.read_parquet(  # pyright: ignore[reportUnknownMemberType]
            output_file,
        )

        # Check transformations applied
        assert "weapon_id" in df.columns
        assert df["weapon_type"].tolist() == ["sword", "katana", "greatsword"]

    def test_process_dataset_skips_up_to_date(
        self,
        config_file: Path,
        temp_dirs: TempDirs,
        sample_weapons_csv: Path,
    ) -> None:
        """Test processing skips up-to-date files."""
        processor = DataProcessor(
            config_path=config_file,
            raw_dir=temp_dirs["raw_dir"],
            processed_dir=temp_dirs["processed_dir"],
        )

        # First processing
        result1 = processor.process_dataset("test-weapons")
        assert result1["status"] == "success"

        # Second processing (should skip)
        result2 = processor.process_dataset("test-weapons")
        # No files should be processed
        assert len(result2.get("files_processed", [])) == 0
        assert result2["schema_version"] == "weapons_v1"

    def test_process_dataset_uses_hash_cache(
        self,
        config_file: Path,
        temp_dirs: TempDirs,
        sample_weapons_csv: Path,
    ) -> None:
        """Cache prevents extra work when raw content is unchanged."""
        processor = DataProcessor(
            config_path=config_file,
            raw_dir=temp_dirs["raw_dir"],
            processed_dir=temp_dirs["processed_dir"],
        )

        # Initial processing to populate cache
        processor.process_dataset("test-weapons")

        # Touch the raw file but keep contents identical
        raw_file = sample_weapons_csv
        original_contents = raw_file.read_text()
        time.sleep(1.1)
        raw_file.write_text(original_contents)

        result = processor.process_dataset("test-weapons")
        assert result["status"] == "success"
        assert result.get("files_processed", []) == []
        assert result["schema_version"] == "weapons_v1"

    def test_process_dataset_force_reprocess(
        self,
        config_file: Path,
        temp_dirs: TempDirs,
        sample_weapons_csv: Path,
    ) -> None:
        """Test force flag reprocesses files."""
        processor = DataProcessor(
            config_path=config_file,
            raw_dir=temp_dirs["raw_dir"],
            processed_dir=temp_dirs["processed_dir"],
        )

        # First processing
        processor.process_dataset("test-weapons")

        # Force reprocessing
        result = processor.process_dataset("test-weapons", force=True)
        assert result["status"] == "success"
        assert len(result["files_processed"]) == 1
        assert result["schema_version"] == "weapons_v1"

    def test_process_dataset_dry_run(
        self,
        config_file: Path,
        temp_dirs: TempDirs,
        sample_weapons_csv: Path,
    ) -> None:
        """Test dry run doesn't write files."""
        processor = DataProcessor(
            config_path=config_file,
            raw_dir=temp_dirs["raw_dir"],
            processed_dir=temp_dirs["processed_dir"],
        )

        result = processor.process_dataset("test-weapons", dry_run=True)

        assert result["status"] == "success"
        assert result["schema_version"] == "weapons_v1"

        # Output file should NOT exist
        output_file: Path = (
            temp_dirs["processed_dir"] / "test-weapons" / "weapons.parquet"
        )
        assert not output_file.exists()

    def test_process_all_skips_disabled(
        self,
        config_file: Path,
        temp_dirs: TempDirs,
        sample_weapons_csv: Path,
    ) -> None:
        """Test process_all skips disabled datasets."""
        processor = DataProcessor(
            config_path=config_file,
            raw_dir=temp_dirs["raw_dir"],
            processed_dir=temp_dirs["processed_dir"],
        )

        results = processor.process_all()

        # test-weapons should be processed
        assert results["datasets"]["test-weapons"]["status"] == "success"
        assert (
            results["datasets"]["test-weapons"]["schema_version"]
            == "weapons_v1"
        )

        # test-bosses should be skipped
        assert results["datasets"]["test-bosses"]["status"] == "skipped"
        assert results["datasets"]["test-bosses"]["reason"] == "disabled"

        # Check summary
        assert results["summary"]["succeeded"] == 1
        assert results["summary"]["skipped"] == 1

    def test_process_all_parallel_dispatches(
        self,
        config_file: Path,
        temp_dirs: TempDirs,
        sample_weapons_csv: Path,
        monkeypatch: MonkeyPatch,
    ) -> None:
        """Ensure helper runs when multiple datasets and workers > 1."""
        processor = DataProcessor(
            config_path=config_file,
            raw_dir=temp_dirs["raw_dir"],
            processed_dir=temp_dirs["processed_dir"],
        )

        processor.config["datasets"].append(
            {
                "name": "test-extra",
                "enabled": True,
            }
        )

        captured: dict[str, object] = {}

        def fake_parallel(
            self: DataProcessor,
            dataset_names: list[str],
            force: bool,
            dry_run: bool,
            max_workers: int,
        ) -> dict[str, dict[str, object]]:
            captured["names"] = dataset_names
            captured["force"] = force
            captured["dry_run"] = dry_run
            captured["max_workers"] = max_workers
            return {
                name: {
                    "status": "success",
                    "files_processed": [],
                }
                for name in dataset_names
            }

        monkeypatch.setattr(
            DataProcessor,
            "_process_datasets_parallel",
            fake_parallel,
        )

        results = processor.process_all(workers=2)

        assert captured["names"] == ["test-weapons", "test-extra"]
        assert captured["max_workers"] == 2
        assert results["datasets"]["test-weapons"]["status"] == "success"
        assert results["datasets"]["test-extra"]["status"] == "success"

    def test_schema_version_mismatch_triggers_reprocess(
        self,
        config_file: Path,
        temp_dirs: TempDirs,
        sample_weapons_csv: Path,
    ) -> None:
        """Changing cached schema version should force reprocessing."""
        processor = DataProcessor(
            config_path=config_file,
            raw_dir=temp_dirs["raw_dir"],
            processed_dir=temp_dirs["processed_dir"],
        )

        processor.process_dataset("test-weapons")

        cache_file: Path = (
            temp_dirs["processed_dir"] / ".cache" / "test-weapons.json"
        )
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        payload = json.loads(cache_file.read_text())
        payload["schema_version"] = "weapons_v0"
        cache_file.write_text(json.dumps(payload, indent=2))

        result = processor.process_dataset("test-weapons")
        assert len(result["files_processed"]) == 1
        assert result["schema_version"] == "weapons_v1"

    def test_resolve_worker_count_auto_cpu(
        self,
        config_file: Path,
        temp_dirs: TempDirs,
    ) -> None:
        """Auto worker value should fall back to CPU count when <=0."""
        processor = DataProcessor(
            config_path=config_file,
            raw_dir=temp_dirs["raw_dir"],
            processed_dir=temp_dirs["processed_dir"],
        )

        processor.config.setdefault("settings", {})["process_workers"] = 0

        # Access the private helper via getattr to avoid pyright private-usage
        # noise while keeping the test honest about the method name.
        resolve_worker_count = getattr(processor, "_resolve_worker_count")

        assert resolve_worker_count(None) == max(
            os.cpu_count() or 1,
            1,
        )
        assert resolve_worker_count(3) == 3

    def test_transform_weapons_normalizes_types(
        self,
        config_file: Path,
        temp_dirs: TempDirs,
    ) -> None:
        """Test weapon type normalization."""
        processor = DataProcessor(
            config_path=config_file,
            raw_dir=temp_dirs["raw_dir"],
            processed_dir=temp_dirs["processed_dir"],
        )

        df = pd.DataFrame(
            {
                "id": [1, 2],
                "name": ["Sword", "Bow"],
                "weapon_type": ["straight_sword", "bows"],
            }
        )

        # See note above: getattr keeps pyright happy without suppressions.
        transform_weapons = getattr(processor, "_transform_weapons")
        result = transform_weapons(df)

        assert "weapon_id" in result.columns
        assert result["weapon_type"].tolist() == ["sword", "bow"]

    def test_transform_handles_missing_values(
        self,
        config_file: Path,
        temp_dirs: TempDirs,
    ) -> None:
        """Test missing value handling in transformations."""
        processor = DataProcessor(
            config_path=config_file,
            raw_dir=temp_dirs["raw_dir"],
            processed_dir=temp_dirs["processed_dir"],
        )

        df = pd.DataFrame(
            {
                "weapon_id": [1, 2],
                "name": ["Sword", "Bow"],
                "weapon_type": ["sword", "bow"],
                "damage_physical": [100, None],
                "scaling_str": ["B", None],
            }
        )

        transform_weapons = getattr(processor, "_transform_weapons")
        result = transform_weapons(df)

        # Missing damage should be filled with 0
        assert result["damage_physical"].tolist() == [100, 0]

        # Missing scaling should be filled with "-"
        assert result["scaling_str"].tolist() == ["B", "-"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
