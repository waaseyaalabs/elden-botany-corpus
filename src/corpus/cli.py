"""Command-line interface for corpus management."""

import sys
from pathlib import Path
from typing import Literal, cast

import click

from corpus.config import settings
from corpus.curate import curate_corpus
from corpus.ingest_carian_fmg import fetch_carian_fmg_files
from corpus.ingest_github_json import fetch_github_api_data
from corpus.ingest_impalers import fetch_impalers_data
from corpus.ingest_kaggle import fetch_kaggle_data
from corpus.pgvector_loader import load_to_postgres
from corpus.reconcile import reconcile_all_sources


@click.group()
@click.version_option()
def main() -> None:
    """Elden Ring Botany Corpus - Data ingestion and curation."""
    pass


@main.command()
@click.option("--base/--no-base", default=True, help="Include base game data")
@click.option("--dlc/--no-dlc", default=True, help="Include DLC data")
@click.option(
    "--github/--no-github",
    default=True,
    help="Include GitHub API fallback",
)
@click.option(
    "--impalers/--no-impalers",
    default=True,
    help="Include Impalers text dump",
)
@click.option(
    "--carian/--no-carian",
    default=True,
    help="Include Carian Archive FMG XMLs",
)
@click.option(
    "--all",
    "fetch_all",
    is_flag=True,
    help="Fetch all sources (override individual flags)",
)
def fetch(
    base: bool,
    dlc: bool,
    github: bool,
    impalers: bool,
    carian: bool,
    fetch_all: bool,
) -> None:
    """Fetch data from all configured sources."""
    if not settings.kaggle_credentials_set:
        warning = (
            "Warning: Kaggle credentials not set. "
            "Kaggle datasets will be skipped."
        )
        click.echo(warning, err=True)

    if fetch_all:
        base = dlc = github = impalers = carian = True

    try:
        # Fetch Kaggle data
        kaggle_base = []
        kaggle_dlc = []
        if base or dlc:
            kaggle_entities = fetch_kaggle_data(
                include_base=base,
                include_dlc=dlc,
            )
            # Split by is_dlc
            kaggle_base = [e for e in kaggle_entities if not e.is_dlc]
            kaggle_dlc = [e for e in kaggle_entities if e.is_dlc]

        # Fetch GitHub API
        github_api = []
        if github:
            github_api = fetch_github_api_data()

        # Fetch Impalers
        dlc_texts = []
        if impalers:
            dlc_texts = fetch_impalers_data()

        carian_fmg = []
        if carian:
            carian_fmg = fetch_carian_fmg_files()

        # Save counts
        click.echo("\n=== Fetch Summary ===")
        click.echo(f"Kaggle base: {len(kaggle_base)}")
        click.echo(f"Kaggle DLC: {len(kaggle_dlc)}")
        click.echo(f"GitHub API: {len(github_api)}")
        click.echo(f"Impalers text: {len(dlc_texts)}")
        click.echo(f"Carian FMG XMLs: {len(carian_fmg)}")

    except Exception as e:
        click.echo(f"Error during fetch: {e}", err=True)
        sys.exit(1)


@main.command()
@click.option(
    "--quality/--no-quality",
    default=True,
    help="Generate HTML/JSON quality reports for curated datasets",
)
def curate(quality: bool) -> None:
    """Reconcile and curate corpus data."""
    try:
        # Re-fetch (should use cached data)
        click.echo("Loading cached data...")

        kaggle_entities = fetch_kaggle_data(
            include_base=True,
            include_dlc=True,
        )
        kaggle_base = [e for e in kaggle_entities if not e.is_dlc]
        kaggle_dlc = [e for e in kaggle_entities if e.is_dlc]

        github_api = fetch_github_api_data()
        dlc_texts = fetch_impalers_data()

        # Reconcile
        entities, unmapped = reconcile_all_sources(
            kaggle_base=kaggle_base,
            kaggle_dlc=kaggle_dlc,
            github_api=github_api,
            dlc_texts=dlc_texts,
        )

        # Curate and export
        df = curate_corpus(
            entities,
            unmapped,
            enable_quality_reports=quality,
        )

        click.echo(f"\n✓ Curated {len(df)} entities")
        click.echo(f"  Output: {settings.curated_dir / 'unified.parquet'}")

    except Exception as e:
        click.echo(f"Error during curation: {e}", err=True)
        sys.exit(1)


@main.command()
@click.option(
    "--dsn",
    default=None,
    help="PostgreSQL DSN (default from env)",
)
@click.option(
    "--parquet",
    default=None,
    help="Path to unified.parquet (default: curated/unified.parquet)",
)
@click.option(
    "--create-schema/--no-create-schema",
    default=True,
    help="Create schema before loading",
)
@click.option(
    "--embed",
    type=click.Choice(["openai", "local", "none"]),
    default="none",
    help="Generate embeddings",
)
def load(
    dsn: str | None,
    parquet: str | None,
    create_schema: bool,
    embed: str,  # Type narrowed by click.Choice validation
) -> None:
    """Load curated data into PostgreSQL."""
    try:
        dsn = dsn or settings.postgres_dsn
        parquet_path = (
            Path(parquet)
            if parquet
            else settings.curated_dir / "unified.parquet"
        )

        if not parquet_path.exists():
            click.echo(
                f"Error: {parquet_path} not found. Run 'corpus curate' first.",
                err=True,
            )
            sys.exit(1)

        # Set embed provider if requested
        if embed != "none":
            # Temporarily override settings
            original_provider = settings.embed_provider
            # Cast is safe because click.Choice validates the input
            settings.embed_provider = cast(
                Literal["openai", "local", "none"],
                embed,
            )

        load_to_postgres(
            dsn=dsn,
            parquet_path=parquet_path,
            create=create_schema,
            embed=(embed != "none"),
        )

        if embed != "none":
            settings.embed_provider = original_provider

        click.echo("✓ Data loaded successfully")

    except Exception as e:
        click.echo(f"Error during load: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
