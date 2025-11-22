"""Click command group for community annotation tooling."""

from __future__ import annotations

import getpass
import json
import re
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import click

from corpus.community_bundle import (
    DEFAULT_BUNDLE_FILENAME,
    BundleError,
    discover_bundle_files,
    load_bundle,
    prompt_for_body,
    prompt_for_motif_tags,
    prompt_for_notes,
    scaffold_bundle,
)
from corpus.community_schema import (
    SubmissionChannel,
    load_motif_taxonomy,
)
from corpus.config import settings
from pipelines.community_ingest import CommunityIngestionPipeline
from pipelines.motif_coverage import REPORT_PATH, run_motif_coverage

CHANNEL_CHOICES = [channel.value for channel in SubmissionChannel]


@click.group(name="community")
def community() -> None:
    """Community annotation helpers (scaffolding, validation, ingestion)."""


@community.command("init")
@click.option(
    "--canonical-id",
    prompt=True,
    help="Canonical entity or lore slug.",
)
@click.option("--chunk-id", default=None, help="Optional lore chunk UUID.")
@click.option(
    "--handle",
    prompt=True,
    help="Contributor handle (GitHub-style).",
)
@click.option(
    "--channel",
    type=click.Choice(CHANNEL_CHOICES, case_sensitive=False),
    default=SubmissionChannel.MANUAL.value,
    show_default=True,
    help="Submission channel recorded on the annotation shell.",
)
@click.option(
    "--bundle-dir",
    type=click.Path(path_type=Path),
    default=None,
    help=(
        "Optional target directory for the bundle "
        "(defaults under data/community)."
    ),
)
@click.option(
    "--motifs",
    default=None,
    help="Comma-separated motif slugs. Leave empty to select interactively.",
)
@click.option(
    "--body-file",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="Optional path to a file containing the initial annotation body.",
)
@click.option(
    "--notes",
    type=str,
    default=None,
    help="Optional notes string to seed README.md (otherwise prompted).",
)
@click.option(
    "--tui",
    is_flag=True,
    help="Launch experimental TUI (falls back to prompts if unavailable).",
)
def init_bundle(
    canonical_id: str,
    chunk_id: str | None,
    handle: str,
    channel: str,
    bundle_dir: Path | None,
    motifs: str | None,
    body_file: Path | None,
    notes: str | None,
    tui: bool,
) -> None:
    """Scaffold a new annotation bundle with validated metadata."""

    taxonomy = load_motif_taxonomy()
    submission_channel = SubmissionChannel(channel.lower())
    if tui:
        click.echo("TUI mode is not available yet; using CLI prompts instead.")

    motif_tags = (
        _parse_motifs(motifs) if motifs else prompt_for_motif_tags(taxonomy)
    )
    if not motif_tags:
        click.echo("No motif tags provided; bundle can still be edited later.")

    body_text = body_file.read_text(encoding="utf-8") if body_file else None
    if not body_text:
        body_text = prompt_for_body()

    bundle_root = bundle_dir or _default_bundle_dir(handle, canonical_id)
    if notes is None:
        notes = prompt_for_notes()

    try:
        bundle = scaffold_bundle(
            root=bundle_root,
            canonical_id=canonical_id.strip(),
            contributor_handle=handle.strip(),
            submission_channel=submission_channel,
            taxonomy=taxonomy,
            chunk_id=chunk_id,
            motif_tags=motif_tags,
            body=body_text,
            notes=notes,
        )
    except BundleError as exc:  # pragma: no cover - Click surfaces message
        raise click.ClickException(str(exc)) from exc

    click.echo("✓ Bundle scaffolded")
    click.echo(f"  Path: {bundle.path}")
    click.echo(
        "  Next: edit bundle.yml, then run "
        "'poetry run corpus community validate'"
    )


@community.command("validate")
@click.argument("targets", nargs=-1, type=click.Path(path_type=Path))
@click.option(
    "--json-output",
    is_flag=True,
    help="Emit validation report as JSON.",
)
def validate_bundles(targets: Sequence[Path], json_output: bool) -> None:
    """Validate one or more bundles against the community schema."""

    taxonomy = load_motif_taxonomy()
    bundle_paths = _collect_bundle_paths(targets)
    if not bundle_paths:
        raise click.ClickException("No bundle files found to validate.")

    results: list[dict[str, object]] = []
    success = True
    for bundle_path in bundle_paths:
        try:
            bundle = load_bundle(bundle_path, taxonomy=taxonomy)
        except BundleError as exc:
            success = False
            results.append(
                {
                    "path": str(bundle_path),
                    "ok": False,
                    "error": str(exc),
                }
            )
            continue
        summary = bundle.to_summary()
        summary.update({"path": str(bundle_path), "ok": True})
        results.append(summary)

    if json_output:
        click.echo(json.dumps(results, indent=2))
    else:
        for entry in results:
            status = "OK" if entry["ok"] else "FAILED"
            click.echo(f"[{status}] {entry['path']}")
            if not entry["ok"]:
                click.echo(f"    ↳ {entry['error']}")

    if not success:
        raise click.ClickException("One or more bundles failed validation.")


@community.command("ingest")
@click.argument("targets", nargs=-1, type=click.Path(path_type=Path))
@click.option(
    "--all",
    "ingest_all",
    is_flag=True,
    help="Ingest every bundle under data/community/bundles.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Plan operations without writing outputs.",
)
@click.option(
    "--force",
    is_flag=True,
    help="Override timestamp checks (last-write-wins).",
)
@click.option(
    "--allow-conflicts",
    is_flag=True,
    help="Keep running even if conflicts are detected.",
)
@click.option(
    "--actor",
    default=None,
    help=(
        "Override actor recorded in provenance logs "
        "(defaults to system user)."
    ),
)
def ingest_bundles(
    targets: Sequence[Path],
    ingest_all: bool,
    dry_run: bool,
    force: bool,
    allow_conflicts: bool,
    actor: str | None,
) -> None:
    """Ingest bundles into processed Parquet tables under data/community."""

    taxonomy = load_motif_taxonomy()
    pipeline = CommunityIngestionPipeline(
        bundles_dir=settings.community_bundles_dir,
        output_dir=settings.community_processed_dir,
        taxonomy=taxonomy,
    )

    bundle_paths: Sequence[Path] | None
    if ingest_all:
        bundle_paths = None
    else:
        bundle_paths = _collect_bundle_paths(targets)
        if not bundle_paths:
            raise click.ClickException("No bundle files found to ingest.")

    actor_name = actor or getpass.getuser()
    result = pipeline.ingest(
        bundle_paths,
        dry_run=dry_run,
        force=force,
        allow_conflicts=allow_conflicts,
        actor=actor_name,
    )

    click.echo("=== Ingestion Summary ===")
    click.echo(f"Created: {result.created}")
    click.echo(f"Updated: {result.updated}")
    click.echo(f"Deleted: {result.deleted}")
    click.echo(f"Skipped: {result.skipped}")
    if result.conflicts:
        click.echo(f"Conflicts: {len(result.conflicts)}")
        for conflict in result.conflicts:
            message = (
                "  - annotation={aid} bundle={bid} " "({reason}) -> {path}"
            ).format(
                aid=conflict.annotation_id,
                bid=conflict.bundle_id,
                reason=conflict.reason,
                path=conflict.conflict_path,
            )
            click.echo(message)
    else:
        click.echo("Conflicts: 0")

    if result.conflicts and not allow_conflicts:
        raise click.ClickException(
            "Conflicts detected; resolve before re-running."
        )


@community.command("list")
@click.argument("targets", nargs=-1, type=click.Path(path_type=Path))
@click.option(
    "--json-output",
    is_flag=True,
    help="Return bundle summaries as JSON.",
)
def list_bundles(targets: Sequence[Path], json_output: bool) -> None:
    """List bundles on disk with basic metadata."""

    taxonomy = load_motif_taxonomy()
    bundle_paths = _collect_bundle_paths(targets)
    summaries: list[dict[str, object]] = []
    for bundle_path in bundle_paths:
        summary: dict[str, object]
        try:
            bundle = load_bundle(bundle_path, taxonomy=taxonomy)
            summary = cast(dict[str, object], bundle.to_summary())
            summary.update({"path": str(bundle_path), "ok": True})
        except BundleError as exc:
            summary = cast(
                dict[str, object],
                {
                    "path": str(bundle_path),
                    "ok": False,
                    "error": str(exc),
                },
            )
        summaries.append(summary)

    if json_output:
        click.echo(json.dumps(summaries, indent=2))
        return

    if not summaries:
        click.echo("No bundles found.")
        return

    for summary in summaries:
        status = "OK" if summary.get("ok") else "FAILED"
        click.echo(f"[{status}] {summary['path']}")
        if summary.get("ok"):
            click.echo(
                "    canonical={canonical_id} handle={contributor_handle} "
                "operation={operation} updated={updated_at}".format(**summary)
            )
        else:
            click.echo(f"    ↳ {summary['error']}")


@community.command("motifs-report")
@click.option(
    "--curated",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help=(
        "Override path to curated Parquet input "
        "(defaults to data/curated/unified.parquet)."
    ),
)
@click.option(
    "--csv-fallback",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help=(
        "Override CSV fallback when the curated Parquet cannot be read "
        "(defaults to data/curated/unified.csv or mirrors --curated)."
    ),
)
def motifs_report(curated: Path | None, csv_fallback: Path | None) -> None:
    """Generate motif coverage metrics + Markdown report."""

    try:
        rows = run_motif_coverage(curated, csv_fallback)
    except FileNotFoundError as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(f"✓ Wrote motif coverage for {len(rows)} motifs")
    click.echo(f"  Report: {REPORT_PATH}")
    parquet_path = settings.community_processed_dir / "motif_coverage.parquet"
    click.echo(f"  Parquet: {parquet_path}")


def _parse_motifs(raw: str) -> list[str]:
    return [slug.strip().lower() for slug in raw.split(",") if slug.strip()]


def _default_bundle_dir(handle: str, canonical_id: str) -> Path:
    slug = _slugify(canonical_id)
    timestamp = datetime.now(tz=UTC).strftime("%Y%m%d%H%M%S")
    return (
        settings.community_bundles_dir / handle.lower() / f"{timestamp}_{slug}"
    )


def _slugify(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return normalized or "bundle"


def _collect_bundle_paths(targets: Sequence[Path]) -> list[Path]:
    if not targets:
        targets = [settings.community_bundles_dir]
    discovered: list[Path] = []
    for target in targets:
        if target.is_dir():
            discovered.extend(discover_bundle_files(target))
        elif target.is_file():
            discovered.append(target)
        else:
            bundle_file = target / DEFAULT_BUNDLE_FILENAME
            if bundle_file.exists():
                discovered.append(bundle_file)
    deduped: dict[str, Path] = {str(path): path for path in discovered}
    return list(deduped.values())
