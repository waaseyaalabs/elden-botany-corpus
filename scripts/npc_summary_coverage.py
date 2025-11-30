#!/usr/bin/env python3
"""Check NPC lore coverage against generated narrative summaries."""

from __future__ import annotations

import argparse
from pathlib import Path

from corpus.config import settings
from pipelines.aliasing import DEFAULT_ALIAS_TABLE
from pipelines.coverage_audit import compare_summary_coverage
from pipelines.narrative_summarizer import (
    NarrativeSummariesConfig,
    SUMMARY_PARQUET,
)

DEFAULT_LORE_PATH = settings.curated_dir / "lore_corpus.parquet"
DEFAULT_SUMMARY_PATH = (
    NarrativeSummariesConfig().output_dir / SUMMARY_PARQUET
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Ensure every curated NPC with lore lines is represented in the "
            "narrative summaries output."
        )
    )
    parser.add_argument(
        "--lore",
        type=Path,
        default=DEFAULT_LORE_PATH,
        help=(
            "Path to lore_corpus.parquet (defaults to "
            "data/curated/lore_corpus.parquet)."
        ),
    )
    parser.add_argument(
        "--summaries",
        type=Path,
        default=DEFAULT_SUMMARY_PATH,
        help=(
            "Narrative summaries file (parquet/json/jsonl). Defaults to "
            "the Phase 7 output."
        ),
    )
    parser.add_argument(
        "--alias-table",
        type=Path,
        default=DEFAULT_ALIAS_TABLE,
        help=(
            "Optional override for entity alias table (defaults to "
            "data/reference/entity_aliases.csv)."
        ),
    )
    parser.add_argument(
        "--allow-missing",
        action="store_true",
        help="Do not exit with an error code when missing NPCs are detected.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = compare_summary_coverage(
        args.lore,
        args.summaries,
        alias_table=args.alias_table,
    )

    print(f"Curated NPCs     : {result.curated_entities}")
    print(f"Summarized NPCs  : {result.summarized_entities}")

    if result.missing_ids:
        preview = ", ".join(result.missing_ids[:10])
        print(f"Missing NPC IDs  : {preview}")
    if result.extra_ids:
        preview = ", ".join(result.extra_ids[:10])
        print(f"Unexpected NPCs  : {preview}")

    if result.missing_ids and not args.allow_missing:
        print(
            "Detected curated NPCs with no summaries. Re-run analysis "
            "graph/summaries after fixing coverage."
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
