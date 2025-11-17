# pyright: reportMissingImports=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false
# pyright: reportGeneralTypeIssues=false

"""Build and query a FAISS index for the lore embeddings."""

from __future__ import annotations

import argparse
import json
import logging
import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, cast

import faiss  # type: ignore[import]
import numpy as np  # type: ignore[import]
import pandas as pd  # type: ignore[import]
from corpus.config import settings

from pipelines.embedding_backends import (
    EmbeddingEncoder,
    EncoderConfig,
    ProviderLiteral,
    create_encoder,
)

FAISSIndex = Any
VectorMatrix = Any

LOGGER = logging.getLogger(__name__)

DEFAULT_EMBEDDINGS = Path("data/embeddings/lore_embeddings.parquet")
DEFAULT_INDEX = Path("data/embeddings/faiss_index.bin")
LEGACY_INDEX = Path("data/embeddings/lore_index.faiss")
DEFAULT_METADATA = Path("data/embeddings/rag_metadata.parquet")
DEFAULT_INFO = Path("data/embeddings/rag_index_meta.json")

META_COLUMNS = (
    "lore_id",
    "canonical_id",
    "category",
    "text_type",
    "source",
    "text",
)


class RAGIndexError(RuntimeError):
    """Raised when index construction or queries fail."""


def build_rag_index(
    *,
    embeddings_path: Path = DEFAULT_EMBEDDINGS,
    index_path: Path = DEFAULT_INDEX,
    metadata_path: Path = DEFAULT_METADATA,
    info_path: Path = DEFAULT_INFO,
    normalize: bool = True,
    dry_run: bool = False,
) -> pd.DataFrame:
    """Construct a FAISS index from the lore embeddings parquet."""

    frame = _load_embedding_frame(embeddings_path)
    if frame.empty:
        raise RAGIndexError("Embedding parquet is empty")

    matrix = _vectors_to_matrix(frame)
    if normalize:
        faiss.normalize_L2(matrix)

    dimension = matrix.shape[1]
    index = faiss.IndexFlatIP(dimension)
    index.add(matrix)

    metadata = frame.drop(columns=["embedding"]).reset_index(drop=True)
    _log_index_summary(metadata, dimension)

    if dry_run:
        LOGGER.info("Dry run enabled; skipping artifact writes")
        return metadata

    index_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    info_path.parent.mkdir(parents=True, exist_ok=True)

    faiss.write_index(index, str(index_path))
    metadata.to_parquet(metadata_path, index=False)
    _write_info(info_path, metadata, dimension, normalize)

    LOGGER.info(
        "Wrote FAISS index (%s vectors, dim=%s) to %s",
        index.ntotal,
        dimension,
        index_path,
    )
    LOGGER.info("Wrote metadata parquet to %s", metadata_path)

    return metadata


def load_query_helper(
    *,
    index_path: Path = DEFAULT_INDEX,
    metadata_path: Path = DEFAULT_METADATA,
    info_path: Path = DEFAULT_INFO,
    provider: ProviderLiteral | None = None,
    model_name: str | None = None,
    batch_size: int | None = None,
    encoder: EmbeddingEncoder | None = None,
) -> RAGQueryHelper:
    """Load persisted artifacts and return a helper ready for querying."""

    resolved_index_path = _resolve_index_path(index_path)
    if not resolved_index_path.exists():
        raise FileNotFoundError(f"Index file not found: {index_path}")
    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadata parquet not found: {metadata_path}")

    metadata = pd.read_parquet(metadata_path)
    info = _read_info(info_path) if info_path.exists() else {}
    resolved_provider = provider or info.get("embedding_provider")
    if resolved_provider is None:
        resolved_provider = _get_constant_value(metadata, "embedding_provider")
    if resolved_provider not in {"local", "openai"}:
        raise RAGIndexError("Cannot determine embedding provider; rebuild embeddings")
    resolved_provider = cast(ProviderLiteral, resolved_provider)

    resolved_model = model_name or info.get("embedding_model")
    if resolved_model is None:
        resolved_model = _get_constant_value(metadata, "embedding_model")
    if resolved_model is None:
        resolved_model = settings.embed_model
    resolved_model = str(resolved_model)

    resolved_batch = batch_size or settings.embed_batch_size
    resolved_encoder = encoder or _build_encoder(
        provider=resolved_provider,
        model_name=resolved_model,
        batch_size=resolved_batch,
    )

    normalize = bool(info.get("normalized", True))
    index = faiss.read_index(str(resolved_index_path))
    helper = RAGQueryHelper(
        index=index,
        metadata=metadata.reset_index(drop=True),
        encoder=resolved_encoder,
        normalize=normalize,
    )
    return helper


def _resolve_index_path(target: Path) -> Path:
    if target.exists():
        return target
    if target == DEFAULT_INDEX and LEGACY_INDEX.exists():
        LOGGER.warning(
            ("Legacy lore_index.faiss artifact detected; rebuild the RAG " "index to generate %s."),
            DEFAULT_INDEX,
        )
        return LEGACY_INDEX
    return target


def query_index(
    query_text: str,
    *,
    top_k: int = 5,
    filter_by: Mapping[str, str | Sequence[str]] | None = None,
    index_path: Path = DEFAULT_INDEX,
    metadata_path: Path = DEFAULT_METADATA,
    info_path: Path = DEFAULT_INFO,
    provider: ProviderLiteral | None = None,
    model_name: str | None = None,
    batch_size: int | None = None,
    encoder: EmbeddingEncoder | None = None,
) -> pd.DataFrame:
    """Convenience wrapper that loads the helper and executes a query."""

    helper = load_query_helper(
        index_path=index_path,
        metadata_path=metadata_path,
        info_path=info_path,
        provider=provider,
        model_name=model_name,
        batch_size=batch_size,
        encoder=encoder,
    )
    return helper.query(query_text, top_k=top_k, filter_by=filter_by)


def _load_embedding_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Embedding parquet not found at {path}")

    frame = pd.read_parquet(path)
    if "embedding" not in frame.columns:
        raise RAGIndexError("Embedding parquet is missing 'embedding' column")
    return frame


def _vectors_to_matrix(frame: pd.DataFrame) -> VectorMatrix:
    vectors = frame["embedding"].tolist()
    if not vectors:
        raise RAGIndexError("No embeddings found in parquet")

    matrix = np.asarray(vectors, dtype=np.float32)
    if matrix.ndim != 2:
        raise RAGIndexError("Embedding matrix must be 2-dimensional")
    return matrix


def _write_info(
    info_path: Path,
    metadata: pd.DataFrame,
    dimension: int,
    normalize: bool,
) -> None:
    provider = _get_constant_value(metadata, "embedding_provider")
    model_name = _get_constant_value(metadata, "embedding_model")
    payload = {
        "vector_count": len(metadata),
        "dimension": dimension,
        "normalized": normalize,
        "embedding_provider": provider,
        "embedding_model": model_name,
    }
    info_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _read_info(path: Path) -> dict[str, object]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        raise RAGIndexError(f"Failed to parse index metadata JSON: {exc}") from exc


def _get_constant_value(frame: pd.DataFrame, column: str) -> str | None:
    if column not in frame.columns:
        return None
    values = frame[column].dropna().unique()
    if len(values) != 1:
        return None
    return str(values[0])


class RAGQueryHelper:
    """Helper for executing FAISS similarity searches."""

    def __init__(
        self,
        *,
        index: FAISSIndex,
        metadata: pd.DataFrame,
        encoder: EmbeddingEncoder,
        normalize: bool,
    ) -> None:
        self._index = index
        self._metadata = metadata.reset_index(drop=True)
        self._encoder = encoder
        self._normalize = normalize

    def query(
        self,
        query_text: str,
        *,
        top_k: int = 5,
        filter_by: Mapping[str, str | Sequence[str]] | None = None,
    ) -> pd.DataFrame:
        vectors = self._encoder.encode([query_text])
        if not vectors:
            raise RAGIndexError("Embedding backend returned no vector for query")

        query_vec = np.asarray(vectors, dtype=np.float32)
        if query_vec.ndim == 1:
            query_vec = query_vec.reshape(1, -1)
        if self._normalize:
            faiss.normalize_L2(query_vec)

        return _search_index(
            index=self._index,
            metadata=self._metadata,
            query_vec=query_vec,
            top_k=top_k,
            filter_by=filter_by,
        )


def _search_index(
    *,
    index: FAISSIndex,
    metadata: pd.DataFrame,
    query_vec: VectorMatrix,
    top_k: int,
    filter_by: Mapping[str, str | Sequence[str]] | None,
) -> pd.DataFrame:
    if metadata.empty:
        return metadata.head(0).copy()

    limit = min(len(metadata), max(top_k * 5, 10))
    while True:
        distances, indices = index.search(query_vec, limit)
        valid = indices[0] >= 0
        candidate_idx = indices[0][valid]
        candidate_scores = distances[0][valid]
        candidate_df = metadata.iloc[candidate_idx].copy()
        candidate_df["score"] = candidate_scores[: len(candidate_df)]

        if filter_by:
            candidate_df = _apply_filters(candidate_df, filter_by)

        candidate_df.sort_values("score", ascending=False, inplace=True)
        if len(candidate_df) >= top_k or limit == len(metadata):
            return candidate_df.head(top_k)

        limit = min(len(metadata), limit * 2)


def _apply_filters(
    frame: pd.DataFrame,
    filters: Mapping[str, str | Sequence[str]],
) -> pd.DataFrame:
    filtered = frame
    for column, value in filters.items():
        if column not in filtered.columns:
            continue
        if isinstance(value, str):
            filtered = cast(pd.DataFrame, filtered[filtered[column] == value])
        else:
            filtered = cast(
                pd.DataFrame,
                filtered[filtered[column].isin(list(value))],
            )
    return filtered


def _log_index_summary(frame: pd.DataFrame, dimension: int) -> None:
    LOGGER.info("Indexing %s embeddings (dim=%s)", len(frame), dimension)
    for label in ("category", "source", "text_type"):
        counts = frame[label].value_counts()
        formatted = ", ".join(f"{key}={value}" for key, value in counts.items())
        LOGGER.info("Distribution by %s: %s", label, formatted)


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build FAISS index for lore embeddings",
    )
    parser.add_argument(
        "--embeddings",
        type=Path,
        default=DEFAULT_EMBEDDINGS,
        help="Path to lore_embeddings.parquet",
    )
    parser.add_argument(
        "--index",
        type=Path,
        default=DEFAULT_INDEX,
        help="Output path for FAISS index",
    )
    parser.add_argument(
        "--metadata",
        type=Path,
        default=DEFAULT_METADATA,
        help="Path to write accompanying metadata parquet",
    )
    parser.add_argument(
        "--info",
        type=Path,
        default=DEFAULT_INFO,
        help="Path to write JSON metadata for the index",
    )
    parser.add_argument(
        "--no-normalize",
        action="store_true",
        help="Disable L2 normalization before indexing",
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
        build_rag_index(
            embeddings_path=args.embeddings,
            index_path=args.index,
            metadata_path=args.metadata,
            info_path=args.info,
            normalize=not args.no_normalize,
            dry_run=args.dry_run,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.error("RAG index pipeline failed: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":  # pragma: no cover
    main()
