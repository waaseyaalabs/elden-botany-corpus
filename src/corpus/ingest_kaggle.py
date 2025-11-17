"""Kaggle dataset ingestion module."""

import re
import zipfile
from pathlib import Path
from typing import Any

import polars as pl
import requests
from tqdm import tqdm

from corpus.config import settings
from corpus.models import Provenance, RawEntity
from corpus.utils import (
    compute_file_hash,
    progress_bar,
    standardize_column_names,
)

# Kaggle dataset identifiers
KAGGLE_BASE_DATASET = "robikscube/elden-ring-ultimate-dataset"
KAGGLE_DLC_DATASET = "pedroaltobelli/ultimate-elden-ring-with-shadow-of-the-erdtree-dlc"

# Expected tables from base dataset
BASE_TABLES = [
    "ammos",
    "armors",
    "ashes_of_war",
    "bosses",
    "classes",
    "creatures",
    "incantations",
    "items",
    "locations",
    "npcs",
    "shields",
    "sorceries",
    "spirits",
    "talismans",
    "weapons",
]


class KaggleIngester:
    """Handle Kaggle dataset downloads and ingestion."""

    def __init__(self) -> None:
        self.base_dir = settings.raw_dir / "kaggle"
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def download_dataset(
        self,
        dataset: str,
        output_dir: Path,
        force: bool = False,
    ) -> None:
        """
        Download a Kaggle dataset using the Kaggle API.

        Args:
            dataset: Dataset identifier (owner/dataset-name)
            output_dir: Directory to extract files
            force: Force re-download even if exists
        """
        if not settings.kaggle_credentials_set:
            raise ValueError(
                "Kaggle credentials not set. Set KAGGLE_USERNAME " "and KAGGLE_KEY in .env"
            )

        # Write kaggle config
        settings.write_kaggle_config()

        output_dir.mkdir(parents=True, exist_ok=True)

        # Check if already downloaded
        existing_csv = list(output_dir.rglob("*.csv"))
        if output_dir.exists() and existing_csv and not force:
            print(f"Dataset {dataset} already downloaded to {output_dir}")
            return

        print(f"Downloading {dataset}...")

        # Use Kaggle CLI via Python
        try:
            import kaggle

            kaggle.api.authenticate()
            kaggle.api.dataset_download_files(
                dataset,
                path=output_dir,
                unzip=True,
                quiet=False,
            )
            print(f"Downloaded {dataset} to {output_dir}")

        except ImportError:
            # Fallback: use direct API calls
            self._download_via_api(dataset, output_dir)

    def _download_via_api(self, dataset: str, output_dir: Path) -> None:
        """Download dataset using Kaggle API endpoints directly."""
        # This is a simplified version - the Kaggle Python package is preferred
        url = f"https://www.kaggle.com/api/v1/datasets/download/{dataset}"

        auth = (settings.kaggle_username, settings.kaggle_key)
        response = requests.get(url, auth=auth, stream=True, timeout=60)
        response.raise_for_status()

        # Save zip file
        zip_path = output_dir / f"{dataset.split('/')[-1]}.zip"
        total_size = int(response.headers.get("content-length", 0))

        with open(zip_path, "wb") as f:
            with tqdm(
                total=total_size,
                unit="B",
                unit_scale=True,
                desc="Downloading",
            ) as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    pbar.update(len(chunk))

        # Extract
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(output_dir)

        zip_path.unlink()  # Remove zip file

    def ingest_base_game(self) -> list[RawEntity]:
        """
        Ingest base game dataset from Kaggle.

        Returns:
            List of RawEntity objects
        """
        output_dir = self.base_dir / "base"
        self.download_dataset(KAGGLE_BASE_DATASET, output_dir)

        entities: list[RawEntity] = []

        for table_name in BASE_TABLES:
            csv_path = output_dir / f"{table_name}.csv"
            if not csv_path.exists():
                print(f"Warning: {csv_path} not found, skipping")
                continue

            # Load CSV
            df = pl.read_csv(csv_path, infer_schema_length=0)
            df = standardize_column_names(df)

            # Create provenance
            provenance = Provenance(
                source="kaggle_base",
                uri=f"kaggle://{KAGGLE_BASE_DATASET}/{table_name}.csv",
                sha256=compute_file_hash(csv_path),
            )

            # Convert to RawEntity
            entity_type = self._to_entity_type(table_name)
            for row in progress_bar(
                df.iter_rows(named=True),
                desc=f"Processing {table_name}",
                total=len(df),
            ):
                name = self._extract_name(row)
                if not name:
                    continue

                description = self._extract_description(row)

                entities.append(
                    RawEntity(
                        entity_type=entity_type,
                        name=name,
                        is_dlc=False,
                        description=description,
                        raw_data=row,
                        provenance=[provenance],
                    )
                )

        return entities

    def ingest_dlc(self) -> list[RawEntity]:
        """
        Ingest DLC dataset from Kaggle.

        Returns:
            List of RawEntity objects
        """
        output_dir = self.base_dir / "dlc"
        self.download_dataset(KAGGLE_DLC_DATASET, output_dir)

        entities: list[RawEntity] = []

        # Find all CSV files
        csv_files = sorted(output_dir.rglob("*.csv"))
        if not csv_files:
            print(f"Warning: No CSV files found in {output_dir}")
            return entities

        for csv_path in csv_files:
            table_name = self._normalize_table_name(csv_path.stem)
            relative_path = csv_path.relative_to(output_dir)

            # Load CSV
            df = pl.read_csv(csv_path, infer_schema_length=0)
            df = standardize_column_names(df)

            # Check if DLC column exists
            has_dlc_column = "dlc" in df.columns

            # Create provenance
            provenance = Provenance(
                source="kaggle_dlc",
                uri=(f"kaggle://{KAGGLE_DLC_DATASET}/{relative_path.as_posix()}"),
                sha256=compute_file_hash(csv_path),
            )

            # Convert to RawEntity
            entity_type = self._to_entity_type(table_name)
            for row in progress_bar(
                df.iter_rows(named=True),
                desc=f"Processing {table_name}",
                total=len(df),
            ):
                name = self._extract_name(row)
                if not name:
                    continue

                # Determine if DLC
                if has_dlc_column:
                    is_dlc = bool(row.get("dlc"))
                else:
                    # Assume all are DLC if from DLC dataset and no column
                    is_dlc = True

                description = self._extract_description(row)

                entities.append(
                    RawEntity(
                        entity_type=entity_type,
                        name=name,
                        is_dlc=is_dlc,
                        description=description,
                        raw_data=row,
                        provenance=[provenance],
                    )
                )

        return entities

    def _extract_name(self, row: dict[str, Any]) -> str:
        """Extract name from row data."""
        for key in ["name", "item_name", "title", "boss_name", "npc_name"]:
            if key in row and row[key]:
                return str(row[key]).strip()
        return ""

    def _extract_description(self, row: dict[str, Any]) -> str:
        """Extract and merge description fields."""
        desc_fields = [
            "description",
            "effect",
            "passive",
            "skill",
            "location",
            "drops",
            "lore",
            "dialogue",
        ]

        parts = []
        for field in desc_fields:
            if field in row and row[field]:
                value = str(row[field]).strip()
                if value and value.lower() not in ("nan", "none", "null", ""):
                    parts.append(value)

        return "\n\n".join(parts)

    def _normalize_table_name(self, name: str) -> str:
        """Convert raw filenames into snake_case for downstream usage."""

        normalized = name.replace("-", "_").replace(" ", "_")
        normalized = re.sub(r"(?<!^)(?=[A-Z])", "_", normalized)
        normalized = re.sub(r"__+", "_", normalized)
        return normalized.lower().strip("_")

    def _to_entity_type(self, table_name: str) -> str:
        """Derive entity type with overrides for odd plurals."""

        normalized = table_name.lower()
        overrides = {
            "ashes_of_war": "ash",
            "spirit_ashes": "spirit",
            "great_runes": "great_rune",
            "crystal_tears": "crystal_tear",
            "key_items": "key_item",
            "upgrade_materials": "upgrade_material",
            "weapons_upgrades": "weapon_upgrade",
            "shields_upgrades": "shield_upgrade",
            "whetblades": "whetblade",
            "remembrances": "remembrance",
        }

        if normalized in overrides:
            return overrides[normalized]

        return normalized.rstrip("s")


def fetch_kaggle_data(
    include_base: bool = True,
    include_dlc: bool = True,
) -> list[RawEntity]:
    """
    Fetch Kaggle datasets.

    Args:
        include_base: Include base game dataset
        include_dlc: Include DLC dataset

    Returns:
        Combined list of RawEntity objects
    """
    ingester = KaggleIngester()
    entities: list[RawEntity] = []

    if include_base:
        print("\n=== Ingesting Base Game Data ===")
        entities.extend(ingester.ingest_base_game())

    if include_dlc:
        print("\n=== Ingesting DLC Data ===")
        entities.extend(ingester.ingest_dlc())

    print(f"\nTotal entities ingested: {len(entities)}")
    return entities
