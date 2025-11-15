"""Main data processor for Elden Ring corpus.

Reads raw data, applies transformations, validates schemas,
and writes processed Parquet files.
"""

import logging
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from pipeline.schemas import get_dataset_schema, validate_dataframe
from pipeline.utils import (
    get_processing_stats,
    handle_missing_values,
    needs_processing,
    normalize_categorical,
    normalize_column_names,
    read_data_file,
    update_cache,
    write_parquet,
)

ProcessingResult = dict[str, Any]

logger = logging.getLogger(__name__)


class DataProcessor:
    """Process raw Kaggle datasets into validated Parquet files."""

    def __init__(
        self,
        config_path: Path,
        raw_dir: Path,
        processed_dir: Path,
        cache_dir: Path | None = None,
    ):
        """Initialize data processor.

        Args:
            config_path: Path to kaggle_datasets.yml
            raw_dir: Directory containing raw data
            processed_dir: Directory for processed outputs
            cache_dir: Optional directory for processing cache
        """
        self.config_path = Path(config_path)
        self.raw_dir = Path(raw_dir)
        self.processed_dir = Path(processed_dir)
        self.cache_dir = cache_dir or (processed_dir / ".cache")

        self.config = self._load_config()
        self.processing_stats: dict[str, Any] = {}

    def _load_config(self) -> dict[str, Any]:
        """Load Kaggle datasets configuration."""
        with open(self.config_path) as f:
            return yaml.safe_load(f)

    def _get_dataset_files(self, dataset_name: str) -> list[Path]:
        """Find all data files for a dataset in raw directory.

        Args:
            dataset_name: Name of dataset

        Returns:
            List of Path objects for data files
        """
        dataset_dir = self.raw_dir / dataset_name
        if not dataset_dir.exists():
            return []

        # Find CSV, JSON, and Parquet files
        files: list[Path] = []
        for pattern in ["*.csv", "*.json", "*.parquet"]:
            files.extend(dataset_dir.glob(pattern))

        return sorted(files)

    def _apply_transformations(self, df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
        """Apply dataset-specific transformations.

        Args:
            df: Raw DataFrame
            dataset_name: Name of dataset

        Returns:
            Transformed DataFrame
        """
        # Normalize column names
        df = normalize_column_names(df)

        # Dataset-specific transformations
        if "weapon" in dataset_name.lower():
            df = self._transform_weapons(df)
        elif "boss" in dataset_name.lower():
            df = self._transform_bosses(df)
        elif "armor" in dataset_name.lower():
            df = self._transform_armor(df)
        elif "spell" in dataset_name.lower():
            df = self._transform_spells(df)
        elif "item" in dataset_name.lower():
            df = self._transform_items(df)

        return df

    def _transform_weapons(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform weapons dataset."""
        # Ensure ID column exists
        if "weapon_id" not in df.columns and "id" in df.columns:
            df = df.rename(columns={"id": "weapon_id"})

        # Normalize weapon types
        weapon_type_mapping = {
            "straightsword": "sword",
            "straight_sword": "sword",
            "great_sword": "greatsword",
            "curved_sword": "sword",
            "katanas": "katana",
            "thrusting_sword": "sword",
            "colossal": "colossal_sword",
            "daggers": "dagger",
            "spears": "spear",
            "halberds": "halberd",
            "axes": "axe",
            "greataxe": "axe",
            "hammers": "hammer",
            "greathammer": "hammer",
            "flails": "flail",
            "bows": "bow",
            "crossbows": "crossbow",
            "staves": "staff",
            "seals": "seal",
            "sacred_seal": "seal",
            "glintstone_staff": "staff",
            "fists": "fist",
            "claws": "claw",
            "whips": "whip",
        }

        if "weapon_type" in df.columns:
            df = normalize_categorical(df, {"weapon_type": weapon_type_mapping})

        # Handle missing damage values
        damage_cols = [
            "damage_physical",
            "damage_magic",
            "damage_fire",
            "damage_lightning",
            "damage_holy",
        ]
        strategy = {col: 0 for col in damage_cols if col in df.columns}
        df = handle_missing_values(df, strategy)

        # Normalize scaling grades
        scaling_cols = [
            "scaling_str",
            "scaling_dex",
            "scaling_int",
            "scaling_fai",
            "scaling_arc",
        ]
        for col in scaling_cols:
            if col in df.columns:
                df[col] = df[col].fillna("-")

        return df

    def _transform_bosses(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform bosses dataset."""
        # Ensure ID column exists
        if "boss_id" not in df.columns and "id" in df.columns:
            df = df.rename(columns={"id": "boss_id"})

        # Normalize boss types
        boss_type_mapping = {
            "demigods": "demigod",
            "field_boss": "field",
            "dungeon_boss": "dungeon",
            "dragons": "dragon",
            "night_boss": "night",
            "main_boss": "main",
        }

        if "boss_type" in df.columns:
            df = normalize_categorical(df, {"boss_type": boss_type_mapping})

        # Handle optional boolean
        if "optional" in df.columns:
            df["optional"] = df["optional"].fillna(False)

        return df

    def _transform_armor(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform armor dataset."""
        # Ensure ID column exists
        if "armor_id" not in df.columns and "id" in df.columns:
            df = df.rename(columns={"id": "armor_id"})

        # Normalize armor types
        armor_type_mapping = {
            "helmet": "head",
            "helm": "head",
            "gauntlet": "arms",
            "gauntlets": "arms",
            "greaves": "legs",
            "leg_armor": "legs",
            "chestpiece": "chest",
            "chest_armor": "chest",
        }

        if "armor_type" in df.columns:
            df = normalize_categorical(df, {"armor_type": armor_type_mapping})

        # Handle missing defense values
        defense_cols = [col for col in df.columns if col.startswith("defense_")]
        strategy = {col: 0.0 for col in defense_cols}
        df = handle_missing_values(df, strategy)

        return df

    def _transform_spells(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform spells dataset."""
        # Ensure ID column exists
        if "spell_id" not in df.columns and "id" in df.columns:
            df = df.rename(columns={"id": "spell_id"})

        # Normalize spell types
        spell_type_mapping = {
            "sorceries": "sorcery",
            "incantations": "incantation",
        }

        if "spell_type" in df.columns:
            df = normalize_categorical(df, {"spell_type": spell_type_mapping})

        # Handle missing costs
        if "fp_cost" in df.columns:
            df["fp_cost"] = df["fp_cost"].fillna(0)

        if "slots_required" in df.columns:
            df["slots_required"] = df["slots_required"].fillna(1)

        return df

    def _transform_items(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform items dataset."""
        # Ensure ID column exists
        if "item_id" not in df.columns and "id" in df.columns:
            df = df.rename(columns={"id": "item_id"})

        # Normalize categories
        category_mapping = {
            "weapons": "weapon",
            "armors": "armor",
            "consumables": "consumable",
            "key_items": "key_item",
            "spells": "spell",
            "ashes_of_war": "ash_of_war",
            "talismans": "talisman",
            "tools": "tool",
            "materials": "material",
            "crafting": "material",
        }

        if "category" in df.columns:
            df = normalize_categorical(df, {"category": category_mapping})

        # Normalize rarity
        rarity_mapping = {
            "legendary": "legendary",
            "rare": "rare",
            "uncommon": "uncommon",
            "common": "common",
        }

        if "rarity" in df.columns:
            df = normalize_categorical(df, {"rarity": rarity_mapping})

        return df

    def process_dataset(
        self, dataset_name: str, force: bool = False, dry_run: bool = False
    ) -> ProcessingResult:
        """Process a single dataset.

        Args:
            dataset_name: Name of dataset to process
            force: Force reprocessing even if up-to-date
            dry_run: Validate only, don't write files

        Returns:
            Processing statistics dict
        """
        logger.info(f"Processing dataset: {dataset_name}")

        raw_files = self._get_dataset_files(dataset_name)
        if not raw_files:
            logger.warning(f"No raw files found for {dataset_name}")
            return {"status": "skipped", "reason": "no_raw_files"}

        files_processed: list[dict[str, Any]] = []
        results: ProcessingResult = {
            "status": "success",
            "files_processed": files_processed,
        }

        for raw_file in raw_files:
            file_stem = raw_file.stem
            output_file = self.processed_dir / dataset_name / f"{file_stem}.parquet"
            cache_file = self.cache_dir / f"{dataset_name}.json"

            # Check if processing needed
            if not force and not needs_processing(raw_file, output_file, cache_file):
                logger.info(f"Skipping {raw_file.name} (up-to-date)")
                continue

            try:
                # Read raw data
                logger.info(f"Reading {raw_file}")
                df = read_data_file(raw_file)
                logger.info(f"Loaded {len(df)} rows")

                # Apply transformations
                df = self._apply_transformations(df, dataset_name)

                # Validate schema
                schema = get_dataset_schema(dataset_name)
                if schema:
                    logger.info("Validating schema...")
                    is_valid, error_msg, validated_df = validate_dataframe(df, schema)
                    if not is_valid:
                        logger.error(f"Schema validation failed: {error_msg}")
                        results["status"] = "failed"
                        results["error"] = error_msg
                        continue
                    # Use validated dataframe with coerced types
                    df = validated_df
                    logger.info("Schema validation passed")
                else:
                    logger.warning(f"No schema found for {dataset_name}, skipping " "validation")

                # Get stats
                stats = get_processing_stats(df)

                if not dry_run:
                    # Write output
                    logger.info(f"Writing {output_file}")
                    write_parquet(df, output_file)

                    # Update cache
                    update_cache(cache_file, raw_file)

                results["files_processed"].append(
                    {
                        "input": str(raw_file),
                        "output": str(output_file),
                        "stats": stats,
                    }
                )

            except Exception as e:
                logger.error("Error processing %s: %s", raw_file, e, exc_info=True)
                results["status"] = "failed"
                results["error"] = str(e)

        return results

    def process_all(self, force: bool = False, dry_run: bool = False) -> ProcessingResult:
        """Process all datasets defined in config.

        Args:
            force: Force reprocessing all datasets
            dry_run: Validate only, don't write files

        Returns:
            Overall processing statistics
        """
        datasets = self.config.get("datasets", [])
        skip_disabled = self.config.get("settings", {}).get("skip_disabled", True)

        results: ProcessingResult = {"datasets": {}, "summary": {}}

        for dataset in datasets:
            name = dataset.get("name")
            enabled = dataset.get("enabled", True)

            if not enabled and skip_disabled:
                logger.info(f"Skipping disabled dataset: {name}")
                results["datasets"][name] = {
                    "status": "skipped",
                    "reason": "disabled",
                }
                continue

            try:
                dataset_result = self.process_dataset(name, force, dry_run)
                results["datasets"][name] = dataset_result
            except Exception as e:
                logger.error(f"Error processing dataset {name}: {e}")
                results["datasets"][name] = {
                    "status": "failed",
                    "error": str(e),
                }

        # Generate summary
        total = len(results["datasets"])
        succeeded = sum(1 for r in results["datasets"].values() if r.get("status") == "success")
        failed = sum(1 for r in results["datasets"].values() if r.get("status") == "failed")
        skipped = total - succeeded - failed

        results["summary"] = {
            "total": total,
            "succeeded": succeeded,
            "failed": failed,
            "skipped": skipped,
        }

        return results

    def get_pending_datasets(self, force: bool = False) -> dict[str, dict[str, Any]]:
        """Return datasets that require processing.

        Args:
            force: When True, treat every dataset with raw files as pending.

        Returns:
            Mapping of dataset name to metadata describing why it needs
            processing and which raw files triggered the decision.
        """

        datasets = self.config.get("datasets", [])
        skip_disabled = self.config.get("settings", {}).get("skip_disabled", True)
        pending: dict[str, dict[str, Any]] = {}

        for dataset in datasets:
            name = dataset.get("name")
            enabled = dataset.get("enabled", True)

            if not name:
                continue

            if not enabled and skip_disabled:
                continue

            raw_files = self._get_dataset_files(name)
            if not raw_files:
                continue

            if force:
                pending[name] = {
                    "reason": "force",
                    "files": [str(path) for path in raw_files],
                }
                continue

            cache_file = self.cache_dir / f"{name}.json"
            files_needing: list[str] = []
            for raw_file in raw_files:
                output_file = self.processed_dir / name / f"{raw_file.stem}.parquet"
                if needs_processing(raw_file, output_file, cache_file):
                    files_needing.append(str(raw_file))

            if files_needing:
                pending[name] = {
                    "reason": "stale_files",
                    "files": files_needing,
                }

        return pending
