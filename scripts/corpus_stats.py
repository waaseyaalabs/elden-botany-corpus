#!/usr/bin/env python3
"""Lightweight CLI for inspecting curated corpus statistics."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pandas as pd
from corpus.config import settings

pd = cast(Any, pd)
DataFrame = Any

DEFAULT_METADATA_PATH = settings.curated_dir / "metadata.json"
COVERAGE_CANDIDATES = (
    settings.community_processed_dir / "motif_coverage.parquet",
    settings.curated_dir / "motif_coverage.parquet",
)


def _coerce_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _coerce_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _extract_mapping(
    metadata: Mapping[str, Any],
    key: str,
) -> Mapping[Any, Any] | None:
    value = metadata.get(key)
    if isinstance(value, Mapping):
        return cast(Mapping[Any, Any], value)
    return None


def _coerce_int_dict(value: Mapping[Any, Any] | None) -> dict[str, int]:
    if value is None:
        return {}
    result: dict[str, int] = {}
    for key, raw in value.items():
        result[str(key)] = _coerce_int(raw)
    return result


def _coerce_report_dict(
    value: Mapping[Any, Any] | None,
) -> dict[str, Mapping[str, Any]]:
    if value is None:
        return {}
    result: dict[str, Mapping[str, Any]] = {}
    for key, raw in value.items():
        if isinstance(raw, Mapping):
            result[str(key)] = raw
    return result


@dataclass(slots=True)
class CoverageStats:
    """Aggregated coverage metrics derived from the motif report."""

    total_motifs: int
    zero_match: int
    under_five: int
    avg_matches: float
    avg_coverage_pct: float
    top_motifs: list[tuple[str, int]]
    sample_zero_labels: list[str]


def _read_metadata(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(
            f"Metadata file not found at {path}. Run 'poetry run corpus "
            "curate' first."
        )
    return json.loads(path.read_text(encoding="utf-8"))


def _resolve_coverage_path(explicit: Path | None) -> Path | None:
    if explicit is not None:
        return explicit
    for candidate in COVERAGE_CANDIDATES:
        if candidate.exists():
            return candidate
    return None


def _read_coverage(path: Path | None) -> DataFrame | None:
    if path is None:
        return None
    if not path.exists():
        print(
            f"Community coverage file not found at {path}. Run 'make "
            "community-report' to generate it.",
        )
        return None
    try:
        return cast(
            DataFrame,
            pd.read_parquet(path),  # type: ignore[call-overload]
        )
    except Exception as exc:  # pragma: no cover - defensive
        raise SystemExit(
            f"Unable to read coverage parquet {path}: {exc}"
        ) from exc


def summarize_coverage(frame: DataFrame) -> CoverageStats:
    if frame.empty:
        return CoverageStats(0, 0, 0, 0.0, 0.0, [], [])

    records = cast(list[dict[str, Any]], frame.to_dict(orient="records"))
    counts = [_coerce_int(record.get("match_count")) for record in records]
    coverage_values = [
        _coerce_float(record.get("coverage_pct")) for record in records
    ]
    labels = [
        str(record.get("label") or record.get("motif_slug") or "")
        for record in records
    ]

    total = len(records)
    zero_match = sum(1 for count in counts if count == 0)
    under_five = sum(1 for count in counts if count < 5)
    avg_matches = round(sum(counts) / total, 2)
    avg_cov = round(sum(coverage_values) / total, 2)

    top_indices = sorted(
        range(total),
        key=lambda idx: counts[idx],
        reverse=True,
    )[:3]
    top_motifs = [
        (labels[idx] or f"motif_{idx}", counts[idx]) for idx in top_indices
    ]
    sample_zero = [
        labels[idx] or f"motif_{idx}"
        for idx, count in enumerate(counts)
        if count == 0
    ][:5]

    return CoverageStats(
        total_motifs=total,
        zero_match=zero_match,
        under_five=under_five,
        avg_matches=avg_matches,
        avg_coverage_pct=avg_cov,
        top_motifs=top_motifs,
        sample_zero_labels=sample_zero,
    )


def list_zero_match_motifs(
    frame: DataFrame,
    limit: int = 10,
) -> list[tuple[str, str]]:
    if frame.empty:
        return []
    records = cast(list[dict[str, Any]], frame.to_dict(orient="records"))
    details: list[tuple[str, str]] = []
    for record in records:
        if _coerce_int(record.get("match_count")) != 0:
            continue
        label = str(record.get("label") or record.get("motif_slug") or "motif")
        category = str(record.get("category") or "?")
        details.append((label, category))
        if len(details) >= limit:
            break
    return details


def top_entity_counts(
    entity_counts: Mapping[str, int], limit: int = 5
) -> list[tuple[str, int]]:
    return sorted(
        ((name, int(count)) for name, count in entity_counts.items()),
        key=lambda item: item[1],
        reverse=True,
    )[:limit]


def detect_sparse_entities(
    entity_counts: Mapping[str, int], threshold: int = 10
) -> list[tuple[str, int]]:
    return sorted(
        (
            (name, int(count))
            for name, count in entity_counts.items()
            if int(count) < threshold
        ),
        key=lambda item: item[1],
    )


def collect_quality_alerts(
    metadata: Mapping[str, Any],
) -> list[tuple[str, list[Any]]]:
    reports = _coerce_report_dict(
        _extract_mapping(metadata, "quality_reports")
    )
    alerts: list[tuple[str, list[Any]]] = []
    for dataset, info in reports.items():
        dataset_alerts = info.get("alerts")
        normalized: list[Any] = []
        if isinstance(dataset_alerts, Sequence) and not isinstance(
            dataset_alerts,
            str | bytes,
        ):
            seq_alerts = cast(Sequence[Any], dataset_alerts)
            normalized = []
            for entry in seq_alerts:
                normalized.append(entry)
        if normalized:
            alerts.append((dataset, normalized))
    return alerts


def render_summary(
    metadata: Mapping[str, Any], coverage: DataFrame | None
) -> None:
    row_counts = _coerce_int_dict(_extract_mapping(metadata, "row_counts"))
    total = row_counts.get("total_entities", 0)
    base = row_counts.get("base_entities", 0)
    dlc = row_counts.get("dlc_entities", 0)
    print("=== Corpus Summary ===")
    print(f"Total entities: {total:,} (base={base:,}, dlc={dlc:,})")

    entity_counts = _coerce_int_dict(
        _extract_mapping(metadata, "entity_counts")
    )
    leaders = top_entity_counts(entity_counts)
    if leaders:
        print("Top entity types:")
        for name, count in leaders:
            print(f"  - {name}: {count:,}")

    provenance = _coerce_int_dict(
        _extract_mapping(metadata, "provenance_summary")
    )
    if provenance:
        total_sources = sum(provenance.values()) or 1
        print("Source mix:")
        for source, count in sorted(
            provenance.items(),
            key=lambda item: item[1],
            reverse=True,
        ):
            pct = (count / total_sources) * 100
            print(f"  - {source}: {count:,} ({pct:.1f}%)")

    if coverage is not None:
        stats = summarize_coverage(coverage)
        print("Motif coverage:")
        print(
            "  - Motifs tracked: {total} | avg matches={avg} | avg "
            "coverage={pct}%".format(
                total=stats.total_motifs,
                avg=stats.avg_matches,
                pct=stats.avg_coverage_pct,
            )
        )
        top_line = ", ".join(
            f"{label} ({count})" for label, count in stats.top_motifs
        )
        if top_line:
            print(f"  - Top motifs: {top_line}")
    else:
        print("Motif coverage: unavailable (generate motif_coverage.parquet).")


def render_mitigating(
    metadata: Mapping[str, Any], coverage: DataFrame | None
) -> None:
    print("=== Mitigation Stats ===")
    unmapped = _coerce_int(metadata.get("unmapped_texts"))
    print(f"Unmapped DLC texts: {unmapped:,}")

    provenance = _coerce_int_dict(
        _extract_mapping(metadata, "provenance_summary")
    )
    if provenance:
        total_sources = sum(provenance.values()) or 1
        print("Source coverage:")
        for source, count in sorted(
            provenance.items(),
            key=lambda item: item[1],
            reverse=True,
        ):
            pct = (count / total_sources) * 100
            print(f"  - {source}: {count:,} ({pct:.1f}%)")

    alerts = collect_quality_alerts(metadata)
    if alerts:
        print("Quality alerts:")
        for dataset, dataset_alerts in alerts[:5]:
            print(f"  - {dataset}: {len(dataset_alerts)} alerts")
    else:
        print("Quality alerts: none recorded")

    if coverage is not None:
        stats = summarize_coverage(coverage)
        print("Coverage gaps:")
        print(
            "  - Motifs with zero matches: {zero} | under five matches: "
            "{low}".format(zero=stats.zero_match, low=stats.under_five)
        )
        if stats.sample_zero_labels:
            sample = ", ".join(stats.sample_zero_labels)
            print(f"  - Sample zero-coverage motifs: {sample}")
    else:
        print("Coverage gaps: motif coverage parquet missing")


def render_anomalies(
    metadata: Mapping[str, Any], coverage: DataFrame | None
) -> None:
    print("=== Anomalies ===")
    entity_counts = _coerce_int_dict(
        _extract_mapping(metadata, "entity_counts")
    )
    sparse = detect_sparse_entities(entity_counts)
    if sparse:
        print("Low-volume entity types (count < 10):")
        for name, count in sparse[:10]:
            print(f"  - {name}: {count}")
    else:
        print("Low-volume entity types: none below threshold")

    if coverage is not None:
        zero_rows = list_zero_match_motifs(coverage)
        if zero_rows:
            print("Motifs without matches:")
            for label, category in zero_rows:
                print(f"  - {label} [{category}]")
        else:
            print("Motifs without matches: none")
    else:
        print("Motif anomalies: coverage parquet missing")

    alerts = collect_quality_alerts(metadata)
    if alerts:
        print("Datasets with data-quality alerts:")
        for dataset, dataset_alerts in alerts:
            print(f"  - {dataset}: {len(dataset_alerts)} alert(s)")
    else:
        print("Datasets with data-quality alerts: none")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inspect Elden Botany curated corpus stats.",
    )
    parser.add_argument(
        "--metadata",
        type=Path,
        default=None,
        help="Path to metadata.json (defaults to data/curated/metadata.json)",
    )
    parser.add_argument(
        "--coverage",
        type=Path,
        default=None,
        help=(
            "Path to motif_coverage.parquet (defaults to data/community/"
            "processed/, falling back to data/curated/)"
        ),
    )

    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("summary", help="Print overall corpus summary stats")
    subparsers.add_parser(
        "mitigating",
        help="Show mitigation-oriented diagnostics",
    )
    subparsers.add_parser(
        "anomalies",
        help="Highlight potential data anomalies",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    metadata_path = args.metadata or DEFAULT_METADATA_PATH
    metadata = _read_metadata(metadata_path)
    coverage_path = _resolve_coverage_path(args.coverage)
    coverage = _read_coverage(coverage_path)

    if args.command == "summary":
        render_summary(metadata, coverage)
    elif args.command == "mitigating":
        render_mitigating(metadata, coverage)
    elif args.command == "anomalies":
        render_anomalies(metadata, coverage)
    else:  # pragma: no cover - argparse enforces commands
        parser.error("Unknown command")


if __name__ == "__main__":  # pragma: no cover
    main()
