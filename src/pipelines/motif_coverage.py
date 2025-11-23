"""Generate motif coverage metrics from curated lore lines."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import click
import pandas as pd
from corpus.community_schema import (
    MotifEntry,
    MotifTaxonomy,
    load_motif_taxonomy,
)
from corpus.config import settings

TEXT_COLUMNS = ("text", "body", "description", "lore")
ID_COLUMNS = ("canonical_id", "slug", "id")
COVERAGE_FILENAME = "motif_coverage.parquet"
REPORT_PATH = Path("docs/motif-taxonomy.md")


@dataclass(slots=True)
class MotifCoverageRow:
    motif_slug: str
    category: str
    label: str
    match_count: int
    coverage_pct: float
    sample_ids: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "motif_slug": self.motif_slug,
            "category": self.category,
            "label": self.label,
            "match_count": self.match_count,
            "coverage_pct": self.coverage_pct,
            "sample_ids": self.sample_ids,
        }


def _resolve_text_column(frame: pd.DataFrame) -> str:
    for column in TEXT_COLUMNS:
        if column in frame.columns:
            return column
    raise ValueError(
        "Lore DataFrame must contain one of the text columns: "
        f"{', '.join(TEXT_COLUMNS)}"
    )


def _resolve_id_column(frame: pd.DataFrame) -> str:
    for column in ID_COLUMNS:
        if column in frame.columns:
            return column
    raise ValueError(
        "Lore DataFrame must contain one of the id columns: "
        f"{', '.join(ID_COLUMNS)}"
    )


def _keyword_pattern(motif: MotifEntry) -> re.Pattern[str] | None:
    keywords = (
        {motif.label} | set(motif.synonyms) | set(motif.narrative_signals)
    )
    keywords = {word.strip().lower() for word in keywords if word.strip()}
    if not keywords:
        return None
    escaped = [re.escape(word) for word in keywords]
    pattern = "|".join(escaped)
    return re.compile(pattern, re.IGNORECASE)


def compute_motif_coverage(
    frame: pd.DataFrame,
    taxonomy: MotifTaxonomy,
) -> list[MotifCoverageRow]:
    text_column = _resolve_text_column(frame)
    id_column = _resolve_id_column(frame)
    total_rows = max(len(frame), 1)

    coverage_rows: list[MotifCoverageRow] = []
    for category in taxonomy.categories:
        for motif in category.motifs:
            pattern = _keyword_pattern(motif)
            if pattern is None:
                matches = pd.Series([False] * len(frame))
            else:
                values = frame[text_column].tolist()
                normalized = [
                    str(value) if value is not None else "" for value in values
                ]
                bools = [bool(pattern.search(text)) for text in normalized]
                matches = pd.Series(bools)
            count = int(matches.sum())
            sample_ids = (
                frame.loc[matches, id_column]
                .astype(str)
                .dropna()
                .head(5)
                .tolist()
            )
            coverage_pct = round((count / total_rows) * 100, 2)
            coverage_rows.append(
                MotifCoverageRow(
                    motif_slug=motif.slug,
                    category=category.label,
                    label=motif.label,
                    match_count=count,
                    coverage_pct=coverage_pct,
                    sample_ids=sample_ids,
                )
            )
    return coverage_rows


def _load_curated_frame(
    path: Path,
    csv_fallback: Path | None = None,
) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Curated dataset not found at {path}. Run 'make curate' first."
        )
    fallback_path = csv_fallback or path.with_suffix(".csv")
    try:
        frame: pd.DataFrame = pd.read_parquet(path)  # type: ignore[misc]
        return frame
    except Exception as pandas_error:  # pragma: no cover - defensive fallback
        polars_frame: pd.DataFrame | None = None
        polars_error: Exception | None = None
        try:
            polars_frame = _polars_roundtrip(path)
        except Exception as exc:  # pragma: no cover - defensive fallback
            polars_error = exc
        if polars_frame is not None:
            click.echo(
                f"⚠️ Unable to read {path.name} via pandas ("
                f"{pandas_error}); using Polars conversion."
            )
            return polars_frame

        if not fallback_path.exists():
            detail = (
                f" and Polars conversion failed ({polars_error})"
                if polars_error
                else ""
            )
            message = (
                "Unable to read {parquet} via pandas ({pandas_error}){detail} "
                "and CSV fallback {csv} does not exist. Ensure the curated "
                "CSV artifact has been exported."
            ).format(
                parquet=path,
                pandas_error=pandas_error,
                detail=detail,
                csv=fallback_path,
            )
            if polars_error is not None:
                raise FileNotFoundError(message) from polars_error
            raise FileNotFoundError(message) from pandas_error

        click.echo(
            (
                "⚠️ Unable to read {parquet} via pandas ({pandas_error})"
                "{detail}; falling back to {csv}."
            ).format(
                parquet=path.name,
                pandas_error=pandas_error,
                detail=(
                    f" and Polars conversion failed ({polars_error})"
                    if polars_error
                    else ""
                ),
                csv=fallback_path.name,
            )
        )
        csv_frame: pd.DataFrame = pd.read_csv(  # type: ignore[call-overload]
            str(fallback_path)
        )
        return csv_frame


def _polars_roundtrip(path: Path) -> pd.DataFrame:
    import polars as pl

    polars_frame = pl.read_parquet(path)
    return polars_frame.to_pandas()


def _write_report(rows: list[MotifCoverageRow]) -> None:
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    lines = ["# Motif Taxonomy Coverage", ""]
    lines.append(
        "This report summarizes how often each motif appears in the current "
        "curated lore corpus."
    )
    lines.append("")
    lines.append("| Motif | Category | Matches | Coverage % | Sample IDs |")
    lines.append("| --- | --- | --- | --- | --- |")
    for row in sorted(rows, key=lambda item: item.category):
        sample = ", ".join(row.sample_ids) if row.sample_ids else "—"
        lines.append(
            f"| {row.label} | {row.category} | {row.match_count} | "
            f"{row.coverage_pct:.2f} | {sample} |"
        )
    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")


def _ensure_processed_dir() -> Path:
    path = settings.community_processed_dir
    path.mkdir(parents=True, exist_ok=True)
    return path


def run_motif_coverage(
    curated_path: Path | None = None,
    csv_fallback: Path | None = None,
) -> list[MotifCoverageRow]:
    taxonomy = load_motif_taxonomy()
    parquet_path = curated_path or (settings.curated_dir / "unified.parquet")
    resolved_csv_fallback: Path
    if csv_fallback is not None:
        resolved_csv_fallback = csv_fallback
    elif curated_path is not None:
        resolved_csv_fallback = curated_path.with_suffix(".csv")
    else:
        resolved_csv_fallback = settings.curated_unified_csv_path
    dataframe = _load_curated_frame(parquet_path, resolved_csv_fallback)
    rows = compute_motif_coverage(dataframe, taxonomy)
    processed_dir = _ensure_processed_dir()
    coverage_path = processed_dir / COVERAGE_FILENAME
    payload = [row.to_dict() for row in rows]
    pd.DataFrame(payload).to_parquet(coverage_path, index=False)
    _write_report(rows)
    return rows


@click.command()
@click.option(
    "--curated",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help=(
        "Optional path to a curated Parquet file "
        "(defaults to data/curated/unified.parquet)."
    ),
)
@click.option(
    "--csv-fallback",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help=(
        "Override the CSV fallback used when the curated Parquet cannot be "
        "read (defaults to data/curated/unified.csv or matches --curated if "
        "set)."
    ),
)
def main(curated: Path | None, csv_fallback: Path | None) -> None:
    """CLI entry point for generating motif coverage metrics."""

    try:
        rows = run_motif_coverage(curated, csv_fallback)
    except FileNotFoundError as exc:  # pragma: no cover - CLI convenience
        raise click.ClickException(str(exc)) from exc

    click.echo(f"✓ Generated motif coverage for {len(rows)} motifs")
    click.echo(f"  Report: {REPORT_PATH}")
    click.echo(
        f"  Parquet: {settings.community_processed_dir / COVERAGE_FILENAME}"
    )


if __name__ == "__main__":  # pragma: no cover
    main()
