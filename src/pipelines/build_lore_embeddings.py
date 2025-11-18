# pyright: reportMissingImports=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false
# pyright: reportGeneralTypeIssues=false

"""Generate lore text embeddings for downstream RAG indexing."""

from __future__ import annotations

import argparse
import logging
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Protocol

import pandas as pd  # type: ignore[import]
import yaml
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
DEFAULT_WEIGHT_CONFIG = Path("config/text_type_weights.yml")
TEXT_COLUMN = "text"
EMBEDDING_INPUT_COLUMN = "embedding_text"
EMBEDDING_STRATEGY = "weighted_text_types_v1"
INLINE_WEIGHT_SENTINEL = "<inline-text-type-weights>"
REQUIRED_COLUMNS = (
    "lore_id",
    "canonical_id",
    "category",
    "text_type",
    "source",
    TEXT_COLUMN,
)
DEFAULT_TEXT_TYPE_WEIGHTS: dict[str, float] = {
    "description": 1.4,
    "impalers_excerpt": 1.7,
    "quote": 1.3,
    "lore": 1.2,
    "dialogue": 0.7,
    "effect": 0.7,
    "obtained_from": 0.8,
    "drops": 0.8,
}


@dataclass(slots=True)
class WeightingResult:
    """Container for information about a weighted text block."""

    text_type: str
    weight: float
    original_length: int
    weighted_length: int
    text: str


@dataclass(slots=True)
class WeightingStats:
    """Track summary statistics for weighting operations."""

    total_blocks: int = 0
    truncated_blocks: int = 0
    expanded_blocks: int = 0
    empty_blocks: int = 0
    canonical_skipped: int = 0
    before_lengths: list[int] = field(default_factory=list)
    after_lengths: list[int] = field(default_factory=list)


class EmbeddingGenerationError(RuntimeError):
    """Raised when lore embedding generation fails."""


class LoreEmbeddingError(EmbeddingGenerationError):
    """Backward-compatible alias for embedding pipeline failures."""


__all__ = [
    "build_lore_embeddings",
    "EmbeddingGenerationError",
    "LoreEmbeddingError",
    "main",
    "apply_text_type_weighting",
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
    weights_path: Path | None = None,
    text_type_weights: Mapping[str, float] | None = None,
) -> pd.DataFrame:
    """Embed lore corpus rows and write them to a parquet file."""

    frame = _load_lore_frame(lore_path)
    frame = _sanitize_lore_frame(frame)
    weights = text_type_weights or _load_text_type_weights(weights_path)
    frame = _apply_weighted_concatenation(frame, weights)
    if EMBEDDING_INPUT_COLUMN not in frame.columns:
        msg = "Weighted lore frame is missing the embedding text column"
        raise LoreEmbeddingError(msg)
    if limit is not None:
        frame = frame.head(limit)
    if frame.empty:
        raise EmbeddingGenerationError(
            "Lore corpus is empty; nothing to embed",
        )

    embedding_inputs = frame[EMBEDDING_INPUT_COLUMN].astype(str).tolist()
    frame = frame.drop(columns=[EMBEDDING_INPUT_COLUMN])

    resolved_provider = _resolve_provider(provider)
    resolved_model = model_name or settings.embed_model
    resolved_batch = batch_size or settings.embed_batch_size
    resolved_encoder = encoder or _build_encoder(
        provider=resolved_provider,
        model_name=resolved_model,
        batch_size=resolved_batch,
    )

    texts = embedding_inputs
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
    enriched["embedding_strategy"] = EMBEDDING_STRATEGY
    weight_source = (
        INLINE_WEIGHT_SENTINEL
        if text_type_weights is not None
        else str(weights_path or DEFAULT_WEIGHT_CONFIG)
    )
    enriched["weight_config_path"] = weight_source

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
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
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


def _load_text_type_weights(path: Path | None) -> dict[str, float]:
    resolved = path or DEFAULT_WEIGHT_CONFIG
    if resolved.exists():
        with resolved.open("r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
        if not isinstance(payload, Mapping):  # type: ignore[arg-type]
            msg = "Text-type weight config must be a mapping of type -> weight"
            raise LoreEmbeddingError(msg)
        parsed: dict[str, float] = {}
        for key, value in payload.items():
            try:
                parsed[str(key).strip().lower()] = float(value)
            except (TypeError, ValueError) as exc:  # pragma: no cover - config
                msg = f"Invalid weight for text_type '{key}': {value}"
                raise LoreEmbeddingError(msg) from exc
        weights = DEFAULT_TEXT_TYPE_WEIGHTS | parsed
        LOGGER.info("Loaded text-type weights from %s", resolved)
        return weights

    LOGGER.warning(
        "Weight config %s not found; falling back to built-in defaults",
        resolved,
    )
    return DEFAULT_TEXT_TYPE_WEIGHTS.copy()


def _apply_weighted_concatenation(
    frame: pd.DataFrame,
    weights: Mapping[str, float],
) -> pd.DataFrame:
    stats = WeightingStats()
    records: list[dict[str, object]] = []
    for _, group in frame.groupby("canonical_id", sort=False):
        record = _compose_weighted_record(group, weights, stats)
        if record is not None:
            records.append(record)

    _log_weighting_summary(stats)
    if not records:
        return frame.head(0)
    return pd.DataFrame(records)


def _compose_weighted_record(
    group: pd.DataFrame,
    weights: Mapping[str, float],
    stats: WeightingStats,
) -> dict[str, object] | None:
    canonical_id = str(group["canonical_id"].iloc[0])
    block_pairs: list[tuple[WeightingResult, str]] = []
    for text_type, subset in group.groupby("text_type", sort=False):
        combined_text = _merge_text_block(subset)
        if not combined_text:
            continue
        result = apply_text_type_weighting(
            {"text_type": text_type, TEXT_COLUMN: combined_text},
            weights,
        )
        _update_weighting_stats(stats, result)
        block_pairs.append((result, combined_text))

    if not block_pairs:
        stats.canonical_skipped += 1
        return None

    ordered = sorted(
        block_pairs,
        key=lambda item: (item[0].weight, item[0].weighted_length),
        reverse=True,
    )
    components = [result.text_type for result, _ in ordered]
    display_sections = [_format_section(result.text_type, original) for result, original in ordered]
    embedding_sections = [_format_section(result.text_type, result.text) for result, _ in ordered]

    display_text = "\n\n".join(section for section in display_sections if section)
    embedding_text = "\n\n".join(section for section in embedding_sections if section)
    if not display_text or not embedding_text:
        stats.canonical_skipped += 1
        return None

    record: dict[str, object] = {
        "lore_id": f"{canonical_id}::{EMBEDDING_STRATEGY}",
        "canonical_id": canonical_id,
        "category": _first_value(group, "category") or "unknown",
        "text_type": _select_primary_text_type(components, weights),
        "source": _joined_unique(group, "source"),
        TEXT_COLUMN: display_text,
        "text_type_components": "|".join(components),
        "component_count": len(components),
        EMBEDDING_INPUT_COLUMN: embedding_text,
    }
    language = _first_value(group, "language")
    if language:
        record["language"] = language
    provenance = _joined_unique(group, "provenance")
    if provenance:
        record["provenance"] = provenance
    return record


def _merge_text_block(subset: pd.DataFrame) -> str:
    texts = subset[TEXT_COLUMN].astype(str).map(str.strip).tolist()
    filtered = [value for value in texts if value]
    return "\n\n".join(filtered).strip()


def _format_section(text_type: str, content: str) -> str:
    clean = content.strip()
    if not clean:
        return ""
    label = text_type.replace("_", " ").title() or "Text"
    return f"{label}:\n{clean}"


def apply_text_type_weighting(
    row: Mapping[str, object],
    weights: Mapping[str, float],
) -> WeightingResult:
    raw_type = str(row.get("text_type", "")).strip()
    normalized_type = raw_type.lower() or "text"
    text = str(row.get(TEXT_COLUMN, "")).strip()
    weight = float(weights.get(normalized_type, weights.get(raw_type, 1.0)))
    weighted_text = _scale_text_block(text, weight)
    return WeightingResult(
        text_type=raw_type or normalized_type,
        weight=weight,
        original_length=len(text),
        weighted_length=len(weighted_text),
        text=weighted_text,
    )


def _scale_text_block(text: str, weight: float) -> str:
    sanitized = text.strip()
    normalized = max(weight, 0.0)
    if not sanitized or normalized == 0.0:
        return ""

    tokens = sanitized.split()
    segments: list[str] = []
    if normalized >= 1.0:
        segments.append(sanitized)
        normalized -= 1.0
        while normalized >= 1.0:
            segments.append(sanitized)
            normalized -= 1.0
        if normalized > 0.0:
            keep = max(1, int(len(tokens) * normalized))
            segments.append(" ".join(tokens[:keep]))
    else:
        keep = max(1, int(len(tokens) * normalized))
        segments.append(" ".join(tokens[:keep]))
    return "\n\n".join(segment for segment in segments if segment).strip()


def _select_primary_text_type(
    components: Sequence[str],
    weights: Mapping[str, float],
) -> str:
    if not components:
        return "text"

    best_type = components[0]
    best_weight = weights.get(best_type.lower(), weights.get(best_type, 1.0))
    for candidate in components[1:]:
        candidate_weight = weights.get(
            candidate.lower(),
            weights.get(candidate, 1.0),
        )
        if candidate_weight > best_weight:
            best_type = candidate
            best_weight = candidate_weight
    return best_type


def _first_value(frame: pd.DataFrame, column: str) -> str | None:
    if column not in frame.columns:
        return None
    values = [
        str(value) for value in frame[column].tolist() if value is not None and str(value).strip()
    ]
    return values[0] if values else None


def _joined_unique(frame: pd.DataFrame, column: str) -> str:
    if column not in frame.columns:
        return ""
    values = [
        str(value).strip()
        for value in frame[column].tolist()
        if value is not None and str(value).strip()
    ]
    if not values:
        return ""
    deduped: list[str] = []
    for value in values:
        if value not in deduped:
            deduped.append(value)
    return "|".join(deduped)


def _update_weighting_stats(
    stats: WeightingStats,
    result: WeightingResult,
) -> None:
    stats.total_blocks += 1
    stats.before_lengths.append(result.original_length)
    stats.after_lengths.append(result.weighted_length)
    if result.weighted_length == 0:
        stats.empty_blocks += 1
    elif result.weighted_length < result.original_length:
        stats.truncated_blocks += 1
    elif result.weighted_length > result.original_length:
        stats.expanded_blocks += 1


def _log_weighting_summary(stats: WeightingStats) -> None:
    if not stats.before_lengths:
        LOGGER.warning("No lore rows available for weighting")
        return

    median_before = statistics.median(stats.before_lengths)
    median_after = statistics.median(stats.after_lengths)
    LOGGER.info(
        "Applied text-type weighting to %s blocks (median chars %.1f -> %.1f)",
        stats.total_blocks,
        median_before,
        median_after,
    )
    LOGGER.debug(
        "Truncated=%s expanded=%s zeroed=%s skipped_canonicals=%s",
        stats.truncated_blocks,
        stats.expanded_blocks,
        stats.empty_blocks,
        stats.canonical_skipped,
    )


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
        batch = list(texts[start : start + batch_size])
        if not batch:
            continue
        encoded = encoder.encode(batch)
        if len(encoded) != len(batch):
            raise LoreEmbeddingError(
                "Embedding backend returned mismatched vector count within " "a batch",
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
        "--text-type-weights",
        dest="text_type_weights",
        type=Path,
        help=(
            "Path to a YAML file that maps text_type -> weighting factor. "
            "Defaults to config/text_type_weights.yml"
        ),
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
            weights_path=args.text_type_weights,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.error("Lore embedding pipeline failed: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":  # pragma: no cover
    main()
