"""Analysis-oriented CLI commands (Phase 7 pipelines)."""

from __future__ import annotations

from pathlib import Path
from typing import cast

import click

from pipelines.embedding_backends import ProviderLiteral
from pipelines.llm.base import create_llm_client_from_env
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
    config = NarrativeSummariesConfig(
        graph_dir=graph_dir or defaults.graph_dir,
        output_dir=output_dir or defaults.output_dir,
        max_motifs=max_motifs or defaults.max_motifs,
        max_quotes=max_quotes or defaults.max_quotes,
        use_llm=not dry_run_llm,
    )
    llm_client = None
    if config.use_llm:
        try:
            llm_client = create_llm_client_from_env(
                provider_override=llm_provider,
                model_override=llm_model,
                reasoning_override=llm_reasoning,
            )
        except RuntimeError as exc:
            raise click.ClickException(str(exc)) from exc

    pipeline = NarrativeSummariesPipeline(config, llm_client=llm_client)
    try:
        artifacts = pipeline.run()
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo("✓ Narrative summaries refreshed")
    click.echo(f"  JSON    : {artifacts.summaries_json}")
    click.echo(f"  Parquet : {artifacts.summaries_parquet}")
    click.echo(f"  Markdown: {artifacts.markdown_path}")
