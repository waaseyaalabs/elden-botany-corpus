#!/usr/bin/env python3
"""Orchestrate fetch → curate → community motif coverage runs.

This helper bundles the three most common maintenance steps so developers can
refresh the canonical corpus and regenerate the community coverage report with
a single command (or selectively skip phases as needed).
"""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

from corpus.config import settings


def _run_step(name: str, command: Sequence[str], *, dry_run: bool) -> None:
    printable = " ".join(shlex.quote(arg) for arg in command)
    print(f"\n==> {name}: {printable}")
    if dry_run:
        return
    subprocess.run(command, check=True)


def _cli_command(*parts: str) -> list[str]:
    return [sys.executable, "-m", "corpus.cli", *parts]


def _ensure_csv_artifact(path: Path, *, dry_run: bool) -> None:
    if dry_run or path.exists():
        return
    message = (
        f"Missing curated CSV artifact at {path}. Run 'poetry run corpus "
        "curate' (or rerun without --skip-curate) before motifs-report, or "
        "pass --csv-path to point at an existing CSV."
    )
    print(message, file=sys.stderr)
    raise SystemExit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Builds the curated corpus and motif coverage report.",
    )
    parser.add_argument(
        "--skip-fetch",
        action="store_true",
        help="Do not run 'corpus fetch'.",
    )
    parser.add_argument(
        "--skip-curate",
        action="store_true",
        help="Do not run 'corpus curate'.",
    )
    parser.add_argument(
        "--skip-report",
        action="store_true",
        help="Do not run 'corpus community motifs-report'.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the orchestrated commands without executing them.",
    )
    parser.add_argument(
        "--curated-path",
        type=Path,
        default=None,
        help=(
            "Optional override for the curated Parquet used by "
            "motifs-report."
        ),
    )
    parser.add_argument(
        "--fetch-arg",
        action="append",
        default=[],
        help="Additional argument to pass to 'corpus fetch' (repeatable).",
    )
    parser.add_argument(
        "--curate-arg",
        action="append",
        default=[],
        help="Additional argument to pass to 'corpus curate' (repeatable).",
    )
    parser.add_argument(
        "--report-arg",
        action="append",
        default=[],
        help=(
            "Additional argument to pass to 'corpus community motifs-report' "
            "(repeatable)."
        ),
    )
    parser.add_argument(
        "--csv-path",
        type=Path,
        default=None,
        help=(
            "Override the CSV fallback used by motifs-report; defaults to "
            "data/curated/unified.csv"
        ),
    )

    args = parser.parse_args()
    csv_path = args.csv_path or settings.curated_unified_csv_path

    if not args.skip_fetch:
        fetch_cmd = _cli_command("fetch", "--all", *args.fetch_arg)
        _run_step("Fetching sources", fetch_cmd, dry_run=args.dry_run)

    if not args.skip_curate:
        curate_cmd = _cli_command("curate", *args.curate_arg)
        _run_step("Curating corpus", curate_cmd, dry_run=args.dry_run)

    if not args.skip_report:
        _ensure_csv_artifact(csv_path, dry_run=args.dry_run)
        report_parts: list[str] = [
            "community",
            "motifs-report",
            *args.report_arg,
        ]
        if args.curated_path is not None:
            report_parts.extend(["--curated", str(args.curated_path)])
        report_parts.extend(["--csv-fallback", str(csv_path)])
        report_cmd = _cli_command(*report_parts)
        _run_step(
            "Generating motif coverage report",
            report_cmd,
            dry_run=args.dry_run,
        )


if __name__ == "__main__":
    main()
