from __future__ import annotations

import json
import numbers
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd  # type: ignore[import]
from corpus.models import normalize_name_for_matching

SourceRecord = dict[str, Any]
SourceLoader = Callable[[Path], list[SourceRecord]]


@dataclass
class Bucket:
    """Group of cross-source records sharing a match key."""

    best: SourceRecord
    entries: list[SourceRecord]


def load_source_records(
    *,
    raw_root: Path,
    source_loaders: Sequence[tuple[str, SourceLoader]],
    logger: Any,
) -> list[SourceRecord]:
    """Run each loader and concatenate normalized records."""

    records: list[SourceRecord] = []

    for source_name, loader in source_loaders:
        loaded = loader(raw_root)
        logger.info("Loaded %s rows from %s", len(loaded), source_name)
        records.extend(loaded)

    if not records:
        raise RuntimeError("No records were loaded from any source")

    return records


def build_buckets(records: Sequence[SourceRecord]) -> dict[str, Bucket]:
    """Deduplicate records by normalized name with source prioritization."""

    buckets: dict[str, Bucket] = {}

    for record in records:
        name = record.get("name")
        if not isinstance(name, str) or not name.strip():
            continue

        match_key = normalize_name_for_matching(name)
        if not match_key:
            match_key = str(record.get("source_id", name.strip()))

        source_priority = int(record.get("source_priority", 99))
        record["source_priority"] = source_priority
        record["name"] = name.strip()
        record["source_id"] = str(record.get("source_id", name))

        bucket = buckets.get(match_key)
        if bucket is None:
            buckets[match_key] = Bucket(best=record, entries=[record])
            continue

        bucket.entries.append(record)
        if source_priority < bucket.best.get("source_priority", 99):
            bucket.best = record

    return buckets


def bucket_provenance(bucket: Bucket) -> str:
    """Serialize provenance metadata ordered by source priority."""

    provenance: list[dict[str, Any]] = [
        {
            "source": entry.get("source"),
            "source_id": entry.get("source_id"),
            "priority": int(entry.get("source_priority", 99)),
        }
        for entry in sorted(
            bucket.entries,
            key=lambda item: int(item.get("source_priority", 99)),
        )
    ]
    return json.dumps(provenance, ensure_ascii=False)


def log_conflicts(
    buckets: dict[str, Bucket],
    columns: Sequence[str],
    logger: Any,
) -> None:
    """Log per-column conflict counts across bucketed entries."""

    summary = {column: 0 for column in columns}

    for bucket in buckets.values():
        for column in columns:
            values = {
                entry.get(column) for entry in bucket.entries if entry.get(column) not in (None, "")
            }
            if len(values) > 1:
                summary[column] += 1

    messages = [f"{column}={count}" for column, count in summary.items() if count]

    if messages:
        logger.info("Column conflicts detected: %s", ", ".join(messages))
    else:
        logger.info("No cross-source column conflicts detected")


def log_source_row_summary(
    *,
    stage: str,
    source_name: str,
    records: Sequence[SourceRecord],
    logger: Any,
) -> None:
    """Log how many rows originate from a source at a pipeline stage."""

    count = sum(1 for record in records if record.get("source") == source_name)
    logger.info("%s rows from %s: %s", stage, source_name, count)


def log_schema_validation_failure(
    *,
    df: pd.DataFrame,
    exc: Exception,
    logger: Any,
    source_name: str = "github_api",
) -> None:
    """Provide detailed context when schema validation fails."""

    failure_cases = getattr(exc, "failure_cases", None)
    if failure_cases is None or not isinstance(failure_cases, pd.DataFrame):
        logger.error("Schema validation failed: %s", exc)
        return

    logger.error(
        "Schema validation failed (%s issues)",
        len(failure_cases),
    )

    if "index" not in failure_cases or "column" not in failure_cases:
        return

    if source_name and "source" in df.columns:
        github_indices = {
            int(idx)
            for idx in df.index[df["source"] == source_name].tolist()
            if isinstance(idx, numbers.Integral)
        }

        grouped_failures: dict[int, list[dict[str, Any]]] = {}
        for _, failure_row in failure_cases.iterrows():
            try:
                idx_int = int(failure_row["index"])
            except (TypeError, ValueError):
                continue

            if idx_int not in github_indices:
                continue

            grouped_failures.setdefault(idx_int, []).append(
                {
                    "column": failure_row.get("column"),
                    "failure_case": failure_row.get("failure_case"),
                }
            )

        if not grouped_failures:
            logger.info(
                "No schema failures were attributed to %s rows",
                source_name,
            )
            return

        for idx_int, failure_entries in grouped_failures.items():
            if idx_int not in df.index:
                continue

            record = df.loc[idx_int]
            canonical = record.get("canonical_slug") or record.get("name")
            source_id = record.get("source_id")
            problem_details = ", ".join(
                f"{entry.get('column')}: {entry.get('failure_case')}" for entry in failure_entries
            )
            logger.error(
                "%s row failed schema (canonical=%s, source_id=%s): %s",
                source_name,
                canonical,
                source_id,
                problem_details,
            )
