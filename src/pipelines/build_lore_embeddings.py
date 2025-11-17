# pyright: reportMissingImports=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false
# pyright: reportGeneralTypeIssues=false

"""Generate lore text embeddings for downstream RAG indexing."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence
from pathlib import Path
from typing import Literal, Protocol

import pandas as pd  # type: ignore[import]
from corpus.config import settings

from pipelines.embedding_backends import EncoderConfig, create_encoder

ProviderName = Literal["local", "openai"]


class EncoderProtocol(Protocol):
    """Local protocol for embedding encoders."""

    def encode(self, texts: Sequence[str]) -> list[list[float]]:
        """Encode input texts into vectors."""

        pass
        raise NotImplementedError


LOGGER = logging.getLogger(__name__)

DEFAULT_LORE_PATH = Path("data/curated/lore_corpus.parquet")
DEFAULT_OUTPUT_PATH = Path("data/embeddings/lore_embeddings.parquet")
TEXT_COLUMN = "text"
REQUIRED_COLUMNS = (
    "lore_id",
    "canonical_id",
    "category",
    "text_type",
    "source",
    TEXT_COLUMN,
)


class EmbeddingGenerationError(RuntimeError):
    """Raised when lore embedding generation fails."""


class LoreEmbeddingError(EmbeddingGenerationError):
    """Backward-compatible alias for embedding pipeline failures."""


__all__ = [
    "build_lore_embeddings",
    "EmbeddingGenerationError",
    "LoreEmbeddingError",
    "main",
]


def build_lore_embeddings(
    *,
    lore_path: Path = DEFAULT_LORE_PATH,
    output_path: Path = DEFAULT_OUTPUT_PATH,
    provider: ProviderName | None = None,
    model_name: str | None = None,
    batch_size: int | None = None,
    encoder: EncoderProtocol | None = None,
    limit: int | None = None,
    dry_run: bool = False,
) -> pd.DataFrame:
    """Embed lore corpus rows and write them to a parquet file."""

    frame = _load_lore_frame(lore_path)
    frame = _sanitize_lore_frame(frame)
    if limit is not None:
        frame = frame.head(limit)
    if frame.empty:
        raise EmbeddingGenerationError(
            "Lore corpus is empty; nothing to embed",
        )

    resolved_provider = _resolve_provider(provider)
    resolved_model = model_name or settings.embed_model
    resolved_batch = batch_size or settings.embed_batch_size
    resolved_encoder = encoder or _build_encoder(
        provider=resolved_provider,
        model_name=resolved_model,
        batch_size=resolved_batch,
    )

    texts = frame[TEXT_COLUMN].astype(str).tolist()
    vectors = _encode_texts(texts, resolved_encoder, resolved_batch)

    if len(vectors) != len(frame):
        raise LoreEmbeddingError(
            "Encoder returned mismatched vector count; "
            f"expected {len(frame)} got {len(vectors)}",
        )

    enriched = frame.copy()
    enriched["embedding"] = vectors
    enriched["embedding_provider"] = resolved_provider
    enriched["embedding_model"] = resolved_model

    if not dry_run:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        enriched.to_parquet(output_path, index=False)
        LOGGER.info(
            "Wrote %s embeddings (provider=%s, model=%s) to %s",
            len(enriched),
            resolved_provider,
            resolved_model,
            output_path,
        )
    return enriched


def _load_lore_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Lore corpus parquet not found: {path}")

    frame = pd.read_parquet(path)
    missing = [
        column
        for column in REQUIRED_COLUMNS
        if column not in frame.columns
    ]
    if missing:
        missing_str = ", ".join(missing)
        raise EmbeddingGenerationError(
            f"Lore corpus is missing required columns: {missing_str}",
        )
    return frame.reset_index(drop=True)


def _sanitize_lore_frame(frame: pd.DataFrame) -> pd.DataFrame:
    sanitized = frame.copy()
    sanitized[TEXT_COLUMN] = sanitized[TEXT_COLUMN].astype(str).str.strip()
    sanitized = sanitized[sanitized[TEXT_COLUMN].notna()]
    sanitized = sanitized[sanitized[TEXT_COLUMN] != ""]
    return sanitized.reset_index(drop=True)


def _resolve_provider(provider: ProviderName | None) -> ProviderName:
    resolved = provider or settings.embed_provider
    if resolved == "none":
        msg = (
            "Embedding provider is 'none'. Set EMBED_PROVIDER or pass "
            "--provider when running the pipeline."
        )
        raise LoreEmbeddingError(msg)
    return resolved


def _build_encoder(
    *,
    provider: ProviderName,
    model_name: str,
    batch_size: int,
) -> EncoderProtocol:
    config = EncoderConfig(
        provider=provider,
        model_name=model_name,
        batch_size=batch_size,
        openai_api_key=settings.openai_api_key,
    )
    return create_encoder(config)


def _encode_texts(
    texts: Sequence[str],
    encoder: EncoderProtocol,
    batch_size: int,
) -> list[list[float]]:
    vectors: list[list[float]] = []
    for start in range(0, len(texts), batch_size):
        batch = list(texts[start:start + batch_size])
        if not batch:
            continue
        encoded = encoder.encode(batch)
        if len(encoded) != len(batch):
            raise LoreEmbeddingError(
                "Embedding backend returned mismatched vector count within a "
                "batch",
            )
        vectors.extend(encoded)
    return vectors


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build lore text embeddings")
    parser.add_argument(
        "--lore-path",
        type=Path,
        default=DEFAULT_LORE_PATH,
        help="Path to lore_corpus.parquet",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Output parquet path for embeddings",
    )
    parser.add_argument(
        "--provider",
        choices=["local", "openai"],
        help="Embedding provider to use (overrides settings)",
    )
    parser.add_argument(
        "--model-name",
        help="Embedding model identifier",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        help="Batch size for embedding requests",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional maximum number of rows to embed",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run embeddings without writing parquet output",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
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
            limit=args.limit,
            dry_run=args.dry_run,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.error("Lore embedding pipeline failed: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":  # pragma: no cover
    main()
