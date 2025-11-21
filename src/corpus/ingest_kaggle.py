# pyright: reportMissingTypeStubs=false

"""Kaggle dataset ingestion module."""

import importlib
import re
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

import polars as pl
import requests
from tqdm import tqdm

from corpus.config import settings
from corpus.incremental import IncrementalManifest, build_signature
from corpus.models import Provenance, RawEntity
from corpus.utils import (
    compute_file_hash,
    progress_bar,
    standardize_column_names,
)

# Kaggle dataset identifiers
KAGGLE_BASE_DATASET = "robikscube/elden-ring-ultimate-dataset"

# fmt: off
KAGGLE_DLC_DATASET = (
    "pedroaltobelli/ultimate-elden-ring-with-"
    "shadow-of-the-erdtree-dlc"
)
# fmt: on
DATASET_KEY_BASE = "kaggle_base"
DATASET_KEY_DLC = "kaggle_dlc"

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


def _should_skip_record(
    manifest: IncrementalManifest | None,
    dataset: str,
    signature: str,
    *,
    incremental: bool,
    since: datetime | None,
) -> bool:
    if not incremental or manifest is None:
        return False
    return manifest.should_skip(dataset, signature, since=since)


def _record_signature(
    manifest: IncrementalManifest | None,
    dataset: str,
    signature: str,
    *,
    record_state: bool,
) -> None:
    if manifest is None or not record_state:
        return
    manifest.record_signature(dataset, signature)


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
            message = (
                "Kaggle credentials not set. "
                "Set KAGGLE_USERNAME and KAGGLE_KEY in .env"
            )
            raise ValueError(message)

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
            kaggle = importlib.import_module("kaggle")

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

    def ingest_base_game(
        self,
        *,
        incremental: bool = False,
        since: datetime | None = None,
        manifest: IncrementalManifest | None = None,
        record_state: bool = True,
    ) -> list[RawEntity]:
        """Ingest base game dataset from Kaggle."""

        output_dir = self.base_dir / "base"
        self.download_dataset(KAGGLE_BASE_DATASET, output_dir)

        entities: list[RawEntity] = []
        total_added = 0
        total_skipped = 0
        ingestion_mode: Literal["full", "incremental"] = (
            "incremental" if incremental else "full"
        )

        for table_name in BASE_TABLES:
            csv_path = output_dir / f"{table_name}.csv"
            if not csv_path.exists():
                print(f"Warning: {csv_path} not found, skipping")
                continue

            table_added = 0
            table_skipped = 0
            file_hash = compute_file_hash(csv_path)
            if manifest and record_state:
                manifest.update_file_hash(
                    DATASET_KEY_BASE,
                    csv_path.name,
                    file_hash,
                )

            df = pl.read_csv(csv_path, infer_schema_length=0)
            df = standardize_column_names(df)

            provenance = Provenance(
                source=DATASET_KEY_BASE,
                dataset=KAGGLE_BASE_DATASET,
                source_file=str(csv_path.relative_to(output_dir)),
                uri=f"kaggle://{KAGGLE_BASE_DATASET}/{table_name}.csv",
                sha256=file_hash,
                ingestion_mode=ingestion_mode,
            )

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
                entity = RawEntity(
                    entity_type=entity_type,
                    name=name,
                    is_dlc=False,
                    description=description,
                    raw_data=row,
                    provenance=[provenance.model_copy(deep=True)],
                )

                signature = build_signature(
                    DATASET_KEY_BASE,
                    table_name,
                    entity.to_slug(),
                    str(provenance.source_file),
                )
                if _should_skip_record(
                    manifest,
                    DATASET_KEY_BASE,
                    signature,
                    incremental=incremental,
                    since=since,
                ):
                    table_skipped += 1
                    continue

                entities.append(entity)
                table_added += 1
                _record_signature(
                    manifest,
                    DATASET_KEY_BASE,
                    signature,
                    record_state=record_state,
                )

            total_added += table_added
            total_skipped += table_skipped
            if incremental:
                label = f"{DATASET_KEY_BASE}:{table_name}"
                print(
                    f"[incremental] {label} new={table_added} "
                    f"skipped={table_skipped}"
                )

        if incremental:
            print(
                f"[incremental] {DATASET_KEY_BASE} total new={total_added} "
                f"skipped={total_skipped}"
            )

        return entities

    def ingest_dlc(
        self,
        *,
        incremental: bool = False,
        since: datetime | None = None,
        manifest: IncrementalManifest | None = None,
        record_state: bool = True,
    ) -> list[RawEntity]:
        """Ingest DLC dataset from Kaggle."""

        output_dir = self.base_dir / "dlc"
        self.download_dataset(KAGGLE_DLC_DATASET, output_dir)

        entities: list[RawEntity] = []
        csv_files = sorted(output_dir.rglob("*.csv"))
        if not csv_files:
            print(f"Warning: No CSV files found in {output_dir}")
            return entities

        total_added = 0
        total_skipped = 0
        ingestion_mode: Literal["full", "incremental"] = (
            "incremental" if incremental else "full"
        )

        for csv_path in csv_files:
            table_name = self._normalize_table_name(csv_path.stem)
            relative_path = csv_path.relative_to(output_dir)
            table_added = 0
            table_skipped = 0
            file_hash = compute_file_hash(csv_path)
            if manifest and record_state:
                manifest.update_file_hash(
                    DATASET_KEY_DLC,
                    csv_path.name,
                    file_hash,
                )

            df = pl.read_csv(csv_path, infer_schema_length=0)
            df = standardize_column_names(df)
            has_dlc_column = "dlc" in df.columns

            source_uri = (
                f"kaggle://{KAGGLE_DLC_DATASET}/{relative_path.as_posix()}"
            )
            provenance = Provenance(
                source=DATASET_KEY_DLC,
                dataset=KAGGLE_DLC_DATASET,
                source_file=relative_path.as_posix(),
                uri=source_uri,
                sha256=file_hash,
                ingestion_mode=ingestion_mode,
            )

            entity_type = self._to_entity_type(table_name)
            for row in progress_bar(
                df.iter_rows(named=True),
                desc=f"Processing {table_name}",
                total=len(df),
            ):
                name = self._extract_name(row)
                if not name:
                    continue

                if has_dlc_column:
                    is_dlc = bool(row.get("dlc"))
                else:
                    is_dlc = True

                description = self._extract_description(row)
                entity = RawEntity(
                    entity_type=entity_type,
                    name=name,
                    is_dlc=is_dlc,
                    description=description,
                    raw_data=row,
                    provenance=[provenance.model_copy(deep=True)],
                )

                signature = build_signature(
                    DATASET_KEY_DLC,
                    table_name,
                    entity.to_slug(),
                    relative_path.as_posix(),
                )
                if _should_skip_record(
                    manifest,
                    DATASET_KEY_DLC,
                    signature,
                    incremental=incremental,
                    since=since,
                ):
                    table_skipped += 1
                    continue

                entities.append(entity)
                table_added += 1
                _record_signature(
                    manifest,
                    DATASET_KEY_DLC,
                    signature,
                    record_state=record_state,
                )

            total_added += table_added
            total_skipped += table_skipped
            if incremental:
                label = f"{DATASET_KEY_DLC}:{table_name}"
                print(
                    f"[incremental] {label} new={table_added} "
                    f"skipped={table_skipped}"
                )

        if incremental:
            print(
                f"[incremental] {DATASET_KEY_DLC} total new={total_added} "
                f"skipped={total_skipped}"
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
    *,
    incremental: bool = False,
    since: datetime | None = None,
    manifest: IncrementalManifest | None = None,
    record_state: bool = True,
) -> list[RawEntity]:
    """
    Fetch Kaggle datasets.

    Args:
        include_base: Include base game dataset
        include_dlc: Include DLC dataset
        incremental: When True, consult the manifest to skip processed rows
        since: Optional cutoff timestamp for reprocessing recent rows
        manifest: Shared incremental manifest instance
        record_state: When False, do not update manifest state (fetch-only)

    Returns:
        Combined list of RawEntity objects
    """
    ingester = KaggleIngester()
    entities: list[RawEntity] = []

    if include_base:
        print("\n=== Ingesting Base Game Data ===")
        entities.extend(
            ingester.ingest_base_game(
                incremental=incremental,
                since=since,
                manifest=manifest,
                record_state=record_state,
            )
        )

    if include_dlc:
        print("\n=== Ingesting DLC Data ===")
        entities.extend(
            ingester.ingest_dlc(
                incremental=incremental,
                since=since,
                manifest=manifest,
                record_state=record_state,
            )
        )

    print(f"\nTotal entities ingested: {len(entities)}")
    return entities
