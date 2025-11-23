"""Analysis-oriented CLI commands (Phase 7 pipelines)."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

import click

from pipelines.embedding_backends import ProviderLiteral
from pipelines.llm.base import resolve_llm_config
from pipelines.llm.batch_job import OpenAIBatchJob
from pipelines.motif_clustering import (
    MotifClusteringConfig,
    MotifClusteringPipeline,
)
from pipelines.narrative_summarizer import (
    NarrativeSummariesConfig,
    NarrativeSummariesPipeline,
)
from pipelines.npc_motif_graph import (
    NPCMotifGraphConfig,
    NPCMotifGraphPipeline,
)


@click.group()
def analysis() -> None:
    """Run analytical pipelines over the curated corpus."""


@analysis.command("clusters")
@click.option(
    "--export/--no-export",
    default=False,
    help="Persist motif clustering artifacts to disk.",
)
@click.option(
    "--curated",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Optional override for lore_corpus.parquet.",
)
@click.option(
    "--coverage",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Optional override for motif_coverage.parquet.",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=None,
    help=(
        "Directory for motif clustering artifacts "
        "(defaults to data/analysis/motif_clustering)."
    ),
)
@click.option("--model", type=str, default=None)
@click.option(
    "--provider",
    type=click.Choice(["local", "openai"]),
    default=None,
)
@click.option("--max-rows", type=int, default=None)
@click.option("--min-cluster", type=int, default=None)
@click.option("--min-samples", type=int, default=None)
@click.option("--seed", type=int, default=None)
@click.option("--neighbors", type=int, default=None)
@click.option("--min-dist", type=float, default=None)
@click.option("--components", type=int, default=None)
@click.option("--exemplars", type=int, default=None)
def clusters(
    export: bool,
    curated: Path | None,
    coverage: Path | None,
    output_dir: Path | None,
    model: str | None,
    provider: str | None,
    max_rows: int | None,
    min_cluster: int | None,
    min_samples: int | None,
    seed: int | None,
    neighbors: int | None,
    min_dist: float | None,
    components: int | None,
    exemplars: int | None,
) -> None:
    """Generate motif clusters via embeddings + HDBSCAN."""

    if not export:
        raise click.ClickException(
            "Add --export to persist motif clustering artifacts."
        )

    defaults = MotifClusteringConfig()
    provider_value = cast(
        ProviderLiteral, provider or defaults.embedding_provider
    )
    config = MotifClusteringConfig(
        curated_path=curated,
        coverage_path=coverage,
        output_dir=output_dir or defaults.output_dir,
        embedding_model=model or defaults.embedding_model,
        embedding_provider=provider_value,
        max_rows=max_rows or defaults.max_rows,
        min_cluster_size=min_cluster or defaults.min_cluster_size,
        min_samples=min_samples or defaults.min_samples,
        random_seed=seed or defaults.random_seed,
        umap_neighbors=neighbors or defaults.umap_neighbors,
        umap_min_dist=min_dist or defaults.umap_min_dist,
        umap_components=components or defaults.umap_components,
        exemplars_per_cluster=exemplars or defaults.exemplars_per_cluster,
    )

    pipeline = MotifClusteringPipeline(config)
    try:
        artifacts = pipeline.run()
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo("✓ Motif clustering complete")
    click.echo(f"  Clusters : {artifacts.cluster_parquet}")
    click.echo(f"  Density  : {artifacts.motif_density_parquet}")
    click.echo(f"  Samples  : {artifacts.samples_json}")
    click.echo(f"  Plot     : {artifacts.umap_plot}")


@analysis.command("graph")
@click.option(
    "--curated",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Optional override for lore_corpus.parquet.",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=None,
    help=(
        "Directory for graph artifacts "
        "(defaults to data/analysis/npc_motif_graph)."
    ),
)
@click.option(
    "--category",
    "categories",
    multiple=True,
    help="Limit lore categories (repeat flag; default npc only).",
)
def graph(
    curated: Path | None,
    output_dir: Path | None,
    categories: tuple[str, ...],
) -> None:
    """Build the NPC motif interaction graph."""

    defaults = NPCMotifGraphConfig()
    config = NPCMotifGraphConfig(
        curated_path=curated,
        output_dir=output_dir or defaults.output_dir,
        categories=categories or defaults.categories,
    )
    pipeline = NPCMotifGraphPipeline(config)
    try:
        artifacts = pipeline.run()
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo("✓ NPC motif graph generated")
    click.echo(f"  Graph     : {artifacts.graph_path}")
    click.echo(f"  GraphML   : {artifacts.graphml_path}")
    click.echo(f"  Summary   : {artifacts.report_path}")


@analysis.command("summaries")
@click.option(
    "--graph-dir",
    type=click.Path(path_type=Path),
    default=None,
    help=(
        "Directory containing NPC motif graph artifacts "
        "(defaults to data/analysis/npc_motif_graph)."
    ),
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=None,
    help=(
        "Directory for narrative summaries "
        "(defaults to data/analysis/narrative_summaries)."
    ),
)
@click.option("--max-motifs", type=int, default=None)
@click.option("--max-quotes", type=int, default=None)
@click.option("--llm-provider", type=str, default=None)
@click.option("--llm-model", type=str, default=None)
@click.option("--llm-reasoning", type=str, default=None)
@click.option(
    "--dry-run-llm",
    is_flag=True,
    help="Skip LLM calls and emit heuristic summaries instead.",
)
def summaries(
    graph_dir: Path | None,
    output_dir: Path | None,
    max_motifs: int | None,
    max_quotes: int | None,
    llm_provider: str | None,
    llm_model: str | None,
    llm_reasoning: str | None,
    dry_run_llm: bool,
) -> None:
    """Generate narrative summaries from the motif graph."""

    defaults = NarrativeSummariesConfig()
    resolved_provider: str | None = None
    resolved_model: str | None = None
    resolved_reasoning: str | None = None
    resolved_max_output: int | None = None
    if not dry_run_llm:
        llm_config = resolve_llm_config(
            provider_override=llm_provider,
            model_override=llm_model,
            reasoning_override=llm_reasoning,
        )
        resolved_provider = llm_config.provider
        resolved_model = llm_config.model
        resolved_reasoning = llm_config.reasoning_effort
        resolved_max_output = llm_config.max_output_tokens

    config = NarrativeSummariesConfig(
        graph_dir=graph_dir or defaults.graph_dir,
        output_dir=output_dir or defaults.output_dir,
        max_motifs=(
            max_motifs if max_motifs is not None else defaults.max_motifs
        ),
        max_quotes=(
            max_quotes if max_quotes is not None else defaults.max_quotes
        ),
        use_llm=not dry_run_llm,
        llm_provider=resolved_provider or llm_provider,
        llm_model=resolved_model or llm_model,
        llm_reasoning=resolved_reasoning or llm_reasoning,
        llm_max_output_tokens=resolved_max_output,
    )

    pipeline = NarrativeSummariesPipeline(config)
    try:
        artifacts = pipeline.run()
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo("✓ Narrative summaries refreshed")
    click.echo(f"  JSON    : {artifacts.summaries_json}")
    click.echo(f"  Parquet : {artifacts.summaries_parquet}")
    click.echo(f"  Markdown: {artifacts.markdown_path}")


@analysis.command("summaries-batch")
@click.option(
    "--graph-dir",
    type=click.Path(path_type=Path),
    default=None,
    help=(
        "Directory containing NPC motif graph artifacts "
        "(defaults to data/analysis/npc_motif_graph)."
    ),
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=None,
    help=(
        "Directory for narrative summaries "
        "(defaults to data/analysis/narrative_summaries)."
    ),
)
@click.option("--batch-input", type=click.Path(path_type=Path), default=None)
@click.option("--batch-output", type=click.Path(path_type=Path), default=None)
@click.option("--max-motifs", type=int, default=None)
@click.option("--max-quotes", type=int, default=None)
@click.option("--llm-provider", type=str, default=None)
@click.option("--llm-model", type=str, default=None)
@click.option("--llm-reasoning", type=str, default=None)
@click.option(
    "--completion-window",
    type=str,
    default="24h",
    help="OpenAI batch completion window (e.g., 24h).",
)
@click.option(
    "--skip-build",
    is_flag=True,
    help="Assume batch input already exists; skip rebuilding JSONL payload.",
)
@click.option(
    "--submit",
    is_flag=True,
    help="Submit the batch to OpenAI after building the payload.",
)
@click.option(
    "--wait/--no-wait",
    default=False,
    help="Poll until the OpenAI batch reaches a terminal state.",
)
@click.option(
    "--poll-interval",
    type=float,
    default=10.0,
    help="Polling interval (seconds) when waiting on OpenAI batches.",
)
@click.option(
    "--download-output",
    is_flag=True,
    help="Download the completed batch output JSONL to --batch-output.",
)
@click.option(
    "--batch-id",
    type=str,
    default=None,
    help="Existing OpenAI batch ID to poll or download.",
)
def summaries_batch(
    graph_dir: Path | None,
    output_dir: Path | None,
    batch_input: Path | None,
    batch_output: Path | None,
    max_motifs: int | None,
    max_quotes: int | None,
    llm_provider: str | None,
    llm_model: str | None,
    llm_reasoning: str | None,
    completion_window: str,
    skip_build: bool,
    submit: bool,
    wait: bool,
    poll_interval: float,
    download_output: bool,
    batch_id: str | None,
) -> None:
    """Generate and optionally submit OpenAI batch jobs for summaries."""

    defaults = NarrativeSummariesConfig()
    config = NarrativeSummariesConfig(
        graph_dir=graph_dir or defaults.graph_dir,
        output_dir=output_dir or defaults.output_dir,
        batch_input_path=batch_input,
        batch_output_path=batch_output,
        max_motifs=(
            max_motifs if max_motifs is not None else defaults.max_motifs
        ),
        max_quotes=(
            max_quotes if max_quotes is not None else defaults.max_quotes
        ),
        use_llm=True,
        llm_provider=llm_provider,
        llm_model=llm_model,
        llm_reasoning=llm_reasoning,
    )

    pipeline = NarrativeSummariesPipeline(config)
    batch_payload = pipeline.batch_input_path

    if not skip_build:
        llm_config = resolve_llm_config(
            provider_override=llm_provider,
            model_override=llm_model,
            reasoning_override=llm_reasoning,
        )
        built_path = pipeline.build_batch_file(
            destination=batch_payload,
            llm_config=llm_config,
        )
        click.echo(f"✓ Batch payload written to {built_path}")
    else:
        click.echo(f"Skipping build; using existing payload {batch_payload}")

    job: OpenAIBatchJob | None = None
    effective_batch_id = batch_id
    submitted_batch = None

    if submit:
        job = OpenAIBatchJob()
        metadata = {
            "pipeline": "narrative_summaries",
            "graph_dir": str(config.graph_dir),
        }
        submitted_batch = job.submit(
            batch_payload,
            completion_window=completion_window,
            metadata={k: v for k, v in metadata.items() if v},
        )
        effective_batch_id = _extract_batch_id(submitted_batch)
        click.echo(f"✓ Batch submitted to OpenAI (id={effective_batch_id})")

    managed_batch_id: str | None = effective_batch_id
    if (wait or download_output) and managed_batch_id is None:
        raise click.ClickException(
            "Provide --batch-id when using --wait/--download-output without"
            " submission."
        )

    batch_result = None
    if wait:
        assert managed_batch_id is not None
        job = job or OpenAIBatchJob()
        batch_result = job.poll(managed_batch_id, interval=poll_interval)
        status = getattr(batch_result, "status", None) or batch_result.get(
            "status"
        )
        click.echo(
            f"✓ Batch {managed_batch_id} reached terminal state: {status}"
        )

    if download_output:
        assert managed_batch_id is not None
        job = job or OpenAIBatchJob()
        batch_result = batch_result or (job.retrieve(managed_batch_id))
        destination = pipeline.batch_output_path
        if batch_result is None:
            raise click.ClickException(
                "Unable to download batch output; batch metadata missing."
            )
        output_path = job.download_output(batch_result, destination)
        click.echo(f"✓ Batch output downloaded to {output_path}")

    if not submit and not wait and not download_output:
        click.echo("ℹ Batch payload ready for manual submission.")


def _extract_batch_id(batch: Any) -> str:
    batch_id_value: Any | None = getattr(batch, "id", None)
    if batch_id_value is None and isinstance(batch, Mapping):
        mapping = cast(Mapping[str, Any], batch)
        batch_id_value = mapping.get("id")
    if batch_id_value is None:
        raise click.ClickException("OpenAI batch missing identifier")
    return str(batch_id_value)
