# pyright: reportGeneralTypeIssues=false, reportUnknownMemberType=false
# pyright: reportUnknownArgumentType=false, reportUnknownVariableType=false

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any, cast

import pandas as pd  # type: ignore[import]
import pandera as pa  # type: ignore[import]
from corpus.models import create_slug

from pipeline.schemas import get_dataset_schema  # type: ignore[import]
from pipelines.canonical_utils import (
    Bucket,
    SourceLoader,
    bucket_provenance,
    build_buckets,
    load_source_records,
    log_conflicts,
    log_schema_validation_failure,
    log_source_row_summary,
)
from pipelines.io.carian_fmg_loader import load_carian_armor_fmg
from pipelines.io.github_api_loader import load_github_api_armor
from pipelines.io.kaggle_base_loader import load_kaggle_base_armor
from pipelines.io.kaggle_dlc_loader import load_kaggle_dlc_armor

LOGGER = logging.getLogger(__name__)

FLOAT_COLUMNS = [
    "weight",
    "defense_physical",
    "defense_strike",
    "defense_slash",
    "defense_pierce",
    "defense_magic",
    "defense_fire",
    "defense_lightning",
    "defense_holy",
    "poise",
]
INT_COLUMN_DEFAULTS = {
    "resistance_immunity": 0,
    "resistance_robustness": 0,
    "resistance_focus": 0,
    "resistance_vitality": 0,
}
CONFLICT_COLUMNS = ["name", "armor_type", "weight"]

DEFAULT_OUTPUT = Path("data/curated/armor_canonical.parquet")

SOURCE_LOADERS: list[tuple[str, SourceLoader]] = [
    ("kaggle_dlc", load_kaggle_dlc_armor),
    ("kaggle_base", load_kaggle_base_armor),
    ("github_api", load_github_api_armor),
    ("carian_fmg", load_carian_armor_fmg),
]


def build_armor_canonical(
    *,
    raw_root: Path,
    output_path: Path = DEFAULT_OUTPUT,
    csv_output: Path | None = None,
    dry_run: bool = False,
) -> pd.DataFrame:
    """Create canonical armor table spanning base and DLC sources."""

    all_records = load_source_records(
        raw_root=raw_root,
        source_loaders=SOURCE_LOADERS,
        logger=LOGGER,
    )
    log_source_row_summary(
        stage="pre-merge",
        source_name="github_api",
        records=all_records,
        logger=LOGGER,
    )

    buckets = build_buckets(all_records)
    log_conflicts(buckets, CONFLICT_COLUMNS, LOGGER)
    merged_records = _buckets_to_records(buckets)
    log_source_row_summary(
        stage="post-merge",
        source_name="github_api",
        records=merged_records,
        logger=LOGGER,
    )
    finalized = _records_to_dataframe(merged_records)

    schema_version = get_dataset_schema("armor")
    if schema_version is None:
        raise RuntimeError("Armor schema is not registered")
    schema = cast(Any, schema_version.schema)

    try:
        validated = schema.validate(finalized, lazy=True)
    except pa.errors.SchemaErrors as exc:
        log_schema_validation_failure(
            df=finalized,
            exc=exc,
            logger=LOGGER,
        )
        raise

    finalized = validated
    LOGGER.info("Validated canonical armor: %s rows", len(finalized))

    if dry_run:
        LOGGER.info("Dry run enabled; skipping writes")
        return finalized

    output_path.parent.mkdir(parents=True, exist_ok=True)
    finalized.to_parquet(output_path, index=False)
    LOGGER.info("Wrote canonical parquet to %s", output_path)

    if csv_output:
        csv_output.parent.mkdir(parents=True, exist_ok=True)
        finalized.to_csv(csv_output, index=False)
        LOGGER.info("Wrote canonical CSV to %s", csv_output)

    return finalized


def _buckets_to_records(buckets: dict[str, Bucket]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []

    for match_key, bucket in buckets.items():
        base = dict(bucket.best)
        base["match_key"] = match_key
        base["canonical_slug"] = create_slug(base["name"])
        base["is_dlc"] = any(entry.get("is_dlc") for entry in bucket.entries)
        base["provenance"] = bucket_provenance(bucket)
        merged.append(base)

    merged.sort(key=lambda row: row["canonical_slug"])
    return merged


def _records_to_dataframe(records: list[dict[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame.from_records(records)
    frame = cast(Any, frame)

    if frame.empty:
        raise RuntimeError("No canonical records remain after merging")

    frame["source_priority"] = (
        pd.to_numeric(frame["source_priority"], errors="coerce").fillna(99).astype(int)
    )
    frame["is_dlc"] = frame["is_dlc"].fillna(False).astype(bool)
    frame["armor_type"] = (
        frame.get("armor_type", pd.Series(dtype="string")).replace("other", "chest").fillna("chest")
    )
    if "provenance" not in frame.columns:
        frame["provenance"] = "[]"

    for column in FLOAT_COLUMNS:
        if column not in frame.columns:
            frame[column] = pd.NA
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
        if column == "weight":
            frame[column] = frame[column].fillna(0.0)

    for column, default_value in INT_COLUMN_DEFAULTS.items():
        if column not in frame.columns:
            frame[column] = default_value
        series = pd.to_numeric(frame[column], errors="coerce").round(0)
        series = series.fillna(default_value)
        frame[column] = series.astype("Int64")

    frame.reset_index(drop=True, inplace=True)
    frame["armor_id"] = pd.Series(range(1, len(frame) + 1), dtype="Int64")

    column_order = [
        "armor_id",
        "canonical_slug",
        "name",
        "armor_type",
        "description",
        "weight",
        "defense_physical",
        "defense_strike",
        "defense_slash",
        "defense_pierce",
        "defense_magic",
        "defense_fire",
        "defense_lightning",
        "defense_holy",
        "resistance_immunity",
        "resistance_robustness",
        "resistance_focus",
        "resistance_vitality",
        "poise",
        "is_dlc",
        "source",
        "source_id",
        "source_priority",
        "provenance",
    ]

    extra_columns = [column for column in frame.columns if column not in column_order]
    return frame.loc[:, column_order + extra_columns]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build canonical Elden Ring armor dataset",
    )
    parser.add_argument(
        "--raw-root",
        type=Path,
        default=Path("data/raw"),
        help="Root directory containing raw source data",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Path to parquet output",
    )
    parser.add_argument(
        "--csv-output",
        type=Path,
        default=None,
        help="Optional CSV output path",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run pipeline without writing artifacts",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    return parser.parse_args()


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s %(message)s")


def main() -> None:
    args = parse_args()
    configure_logging(args.verbose)

    try:
        build_armor_canonical(
            raw_root=args.raw_root,
            output_path=args.output,
            csv_output=args.csv_output,
            dry_run=args.dry_run,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.error("Canonical armor pipeline failed: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
