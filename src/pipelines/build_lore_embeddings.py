# pyright: reportMissingImports=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false

"""Generate lore embeddings and persist them for downstream RAG indexing."""

from __future__ import annotations

import argparse
import logging
import os
from collections import Counter
from pathlib import Path
from typing import cast

import pandas as pd  # type: ignore[import]

from corpus.config import settings
from pipelines.embedding_backends import (
    EmbeddingEncoder,
    EncoderConfig,
    ProviderLiteral,
    create_encoder,
)

LOGGER = logging.getLogger(__name__)

DEFAULT_LORE_CORPUS = Path("data/curated/lore_corpus.parquet")
DEFAULT_OUTPUT = Path("data/embeddings/lore_embeddings.parquet")

REQUIRED_COLUMNS = (
    "lore_id",
    "canonical_id",
    "category",
    "text_type",
    "source",
    "text",
)


class EmbeddingGenerationError(RuntimeError):
    """Raised when embedding generation fails."""


def build_lore_embeddings(
    *,
    lore_path: Path = DEFAULT_LORE_CORPUS,
    output_path: Path = DEFAULT_OUTPUT,
    provider: ProviderLiteral | None = None,
    model_name: str | None = None,
    batch_size: int | None = None,
    encoder: EmbeddingEncoder | None = None,
    dry_run: bool = False,
) -> pd.DataFrame:
    """Load the lore corpus and materialize embeddings per lore row."""

    frame = _load_lore_corpus(lore_path)
    if frame.empty:
        raise EmbeddingGenerationError(f"Lore corpus at {lore_path} is empty")

    configured_provider = provider or settings.embed_provider
    if configured_provider not in {"local", "openai"}:
        raise EmbeddingGenerationError("Embedding provider is not configured")
    resolved_provider = cast(ProviderLiteral, configured_provider)

    resolved_model = model_name or settings.embed_model
    resolved_batch = batch_size or settings.embed_batch_size

    resolved_encoder = encoder or _build_encoder(
        provider=resolved_provider,
        model_name=resolved_model,
        batch_size=resolved_batch,
    )

    prepared = _prepare_rows(frame)
    if prepared.empty:
        raise EmbeddingGenerationError("No lore rows with text to embed")

    texts = prepared["text"].tolist()
    vectors = resolved_encoder.encode(texts)
    if not vectors:
        raise EmbeddingGenerationError("Embedding backend returned no vectors")
    if len(vectors) != len(prepared):
        raise EmbeddingGenerationError(
            "Embedding count mismatch with lore rows"
        )

    dimension = len(vectors[0])
    if dimension == 0:
        raise EmbeddingGenerationError("Embedding vectors have zero dimension")

    prepared = prepared.copy()
    prepared["embedding"] = vectors
    prepared["embedding_model"] = resolved_model
    prepared["embedding_provider"] = resolved_provider
    prepared["embedding_dim"] = dimension

    _log_summary(prepared, dimension)

    if dry_run:
        LOGGER.info("Dry run enabled; skipping parquet write")
        return prepared

    output_path.parent.mkdir(parents=True, exist_ok=True)
    prepared.to_parquet(output_path, index=False)
    LOGGER.info("Wrote lore embeddings to %s", output_path)

    return prepared


def _build_encoder(
    *,
    provider: ProviderLiteral,
    model_name: str,
    batch_size: int,
) -> EmbeddingEncoder:
    api_key = settings.openai_api_key or os.getenv("OPENAI_API_KEY")
    config = EncoderConfig(
        provider=provider,
        model_name=model_name,
        batch_size=batch_size,
        openai_api_key=api_key,
    )
    return create_encoder(config)


def _load_lore_corpus(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Lore corpus parquet not found at {path}")

    frame = pd.read_parquet(path)
    missing = [
        column for column in REQUIRED_COLUMNS if column not in frame.columns
    ]
    if missing:
        columns = ", ".join(missing)
        raise EmbeddingGenerationError(
            f"Lore corpus missing columns: {columns}"
        )

    return frame


def _prepare_rows(frame: pd.DataFrame) -> pd.DataFrame:
    subset = frame.loc[:, REQUIRED_COLUMNS].copy()
    subset = cast(
        pd.DataFrame,
        subset.sort_values("lore_id"),  # type: ignore[call-arg]
    )
    subset.reset_index(drop=True, inplace=True)
    subset["text"] = subset["text"].astype(str).str.strip()
    subset = cast(
        pd.DataFrame,
        subset.loc[subset["text"].astype(bool)],
    )
    subset.reset_index(drop=True, inplace=True)
    return subset


def _log_summary(frame: pd.DataFrame, dimension: int) -> None:
    LOGGER.info("Generated %s embeddings (dim=%s)", len(frame), dimension)
    for label in ("category", "source", "text_type"):
        counts = Counter(frame[label])
        formatted = ", ".join(
            f"{key}={value}" for key, value in counts.items()
        )
        LOGGER.info("Distribution by %s: %s", label, formatted)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build lore embeddings parquet"
    )
    parser.add_argument(
        "--lore-path",
        type=Path,
        default=DEFAULT_LORE_CORPUS,
        help="Path to lore_corpus.parquet",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Embedding parquet destination",
    )
    parser.add_argument(
        "--provider",
        choices=["local", "openai"],
        default=None,
        help="Embedding provider to use",
    )
    parser.add_argument(
        "--model-name",
        type=str,
        default=None,
        help="Override embedding model name",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Override batch size for embedding backend",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip writing artifacts",
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
        build_lore_embeddings(
            lore_path=args.lore_path,
            output_path=args.output,
            provider=args.provider,
            model_name=args.model_name,
            batch_size=args.batch_size,
            dry_run=args.dry_run,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.error("Lore embedding pipeline failed: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":  # pragma: no cover
    main()
