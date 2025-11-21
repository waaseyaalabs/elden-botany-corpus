# pyright: reportMissingImports=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false

"""High-level helpers to query the lore RAG index."""

from __future__ import annotations

import argparse
import logging
from collections import defaultdict, deque
from collections.abc import Mapping, MutableMapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal, Protocol

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from pipelines.build_rag_index import (
    DEFAULT_INDEX,
    DEFAULT_INFO,
    DEFAULT_METADATA,
    FilterClause,
    RAGIndexError,
    load_query_helper,
)
from rag.reranker import (
    RerankerProtocol,
    load_reranker,
)

FilterValue = str | Sequence[str]
FilterMapping = Mapping[str, FilterValue]
FilterInput = FilterMapping | Sequence["FilterExpression"]
_DEDUP_PADDING_FACTOR = 3
_SEMANTIC_DUPLICATE_THRESHOLD = 0.97
_SEMANTIC_TEXT_TYPES = {"dialogue", "quote"}
_BALANCED_MAX_PER_TYPE = 2
_BALANCED_PRIORITY = ("description", "lore", "impalers_excerpt", "dialogue")
BalancedMode = Literal["balanced", "raw"]


class EncoderProtocol(Protocol):
    """Minimal protocol for embedding encoders."""

    def encode(self, texts: Sequence[str]) -> list[list[float]]:
        """Encode input texts into vector representations."""

        pass
        raise NotImplementedError


LOGGER = logging.getLogger(__name__)
_ALLOW_FILTERS = {"category", "text_type", "source"}


@dataclass(slots=True, frozen=True)
class FilterExpression:
    """Structured representation of include/exclude filter clauses."""

    column: str
    values: tuple[str, ...]
    operator: Literal["include", "exclude"] = "include"


@dataclass(slots=True)
class LoreMatch:
    """Structured response returned by ``query_lore``."""

    lore_id: str
    text: str
    score: float
    canonical_id: str | None
    category: str | None
    text_type: str | None
    source: str | None
    reranker_score: float | None = None
    ordering_notes: str | None = None

    def to_dict(self) -> dict[str, object]:
        """Convert the match into a serialisable dictionary."""

        return asdict(self)


def query_lore(
    query_text: str,
    *,
    top_k: int = 10,
    filters: FilterInput | None = None,
    index_path: Path = DEFAULT_INDEX,
    metadata_path: Path = DEFAULT_METADATA,
    info_path: Path = DEFAULT_INFO,
    encoder: EncoderProtocol | None = None,
    reranker: RerankerProtocol | None = None,
    mode: BalancedMode = "balanced",
) -> list[LoreMatch]:
    """Query the persisted FAISS index and return matches with metadata."""

    helper = load_query_helper(
        index_path=index_path,
        metadata_path=metadata_path,
        info_path=info_path,
        encoder=encoder,
    )
    normalized_filters = _prepare_filters(filters)
    active_reranker = reranker or load_reranker(None)
    padded_top_k = _resolve_candidate_window(top_k, active_reranker)
    frame = helper.query(
        query_text,
        top_k=padded_top_k,
        filter_by=normalized_filters,
        include_vectors=True,
    )
    frame = _deduplicate_frame(frame)
    matches = _frame_to_matches(frame)
    reranked = active_reranker.rerank(query_text, matches)
    return _apply_mode(reranked, top_k, mode=mode)


def _prepare_filters(
    filters: FilterInput | None,
) -> MutableMapping[str, FilterClause] | None:
    if not filters:
        return None

    normalized: MutableMapping[str, FilterClause] = {}
    if isinstance(filters, Mapping):
        for key, value in filters.items():
            _add_filter_values(normalized, key, value, operator="include")
    else:
        for expression in filters:
            _add_filter_values(
                normalized,
                expression.column,
                expression.values,
                operator=expression.operator,
            )
    return normalized or None


def _add_filter_values(
    clauses: MutableMapping[str, FilterClause],
    column: str,
    values: FilterValue | Sequence[str],
    *,
    operator: Literal["include", "exclude"] = "include",
) -> None:
    if column not in _ALLOW_FILTERS:
        LOGGER.warning("Ignoring unsupported filter column: %s", column)
        return

    resolved_values = _coerce_filter_values(values)
    if not resolved_values:
        LOGGER.warning("Ignoring empty filter for column: %s", column)
        return

    clause = clauses.setdefault(column, FilterClause())
    target = clause.include if operator == "include" else clause.exclude
    target.update(resolved_values)


def _coerce_filter_values(values: FilterValue | Sequence[str]) -> list[str]:
    if isinstance(values, str):
        raw_values = [values]
    else:
        raw_values = list(values)

    normalized = [candidate.strip() for candidate in raw_values if candidate]
    return [candidate for candidate in normalized if candidate]


def _frame_to_matches(frame: pd.DataFrame) -> list[LoreMatch]:
    matches: list[LoreMatch] = []
    for _, row in frame.iterrows():
        matches.append(
            LoreMatch(
                lore_id=row.get("lore_id", ""),
                text=row.get("text", ""),
                score=float(row.get("score", 0.0)),
                canonical_id=row.get("canonical_id"),
                category=row.get("category"),
                text_type=row.get("text_type"),
                source=row.get("source"),
            )
        )
    return matches


def _resolve_candidate_window(
    top_k: int,
    reranker: RerankerProtocol | None,
) -> int:
    window = max(1, top_k) * _DEDUP_PADDING_FACTOR
    pool_size = getattr(reranker, "candidate_pool_size", None)
    if isinstance(pool_size, int) and pool_size > 0:
        window = max(window, pool_size)
    return window


def _apply_mode(
    matches: Sequence[LoreMatch],
    top_k: int,
    *,
    mode: BalancedMode,
) -> list[LoreMatch]:
    if top_k <= 0:
        return []
    if mode == "raw":
        return list(matches[:top_k])
    return _balanced_interleave(matches, top_k)


def _balanced_interleave(
    matches: Sequence[LoreMatch],
    top_k: int,
) -> list[LoreMatch]:
    if not matches:
        return []

    buckets = _build_type_buckets(matches)
    if not buckets:
        return list(matches[:top_k])

    priority = [key for key in _BALANCED_PRIORITY if key in buckets]
    remaining = [key for key in buckets if key not in priority]
    ordered_keys = priority + remaining
    counts: defaultdict[str, int] = defaultdict(int)
    results: list[LoreMatch] = []
    overflow: list[LoreMatch] = []

    while len(results) < top_k and ordered_keys:
        progress = False
        for key in list(ordered_keys):
            queue = buckets.get(key)
            if not queue:
                ordered_keys.remove(key)
                buckets.pop(key, None)
                continue
            maxed_out = counts[key] >= _BALANCED_MAX_PER_TYPE
            if maxed_out and _has_diversity_options(buckets, counts):
                overflow.append(queue.popleft())
                if not queue:
                    ordered_keys.remove(key)
                    buckets.pop(key, None)
                continue
            candidate = queue.popleft()
            counts[key] += 1
            _append_ordering_note(
                candidate,
                f"balanced-slot:{key}:{counts[key]}/{_BALANCED_MAX_PER_TYPE}",
            )
            results.append(candidate)
            progress = True
            if not queue:
                ordered_keys.remove(key)
                buckets.pop(key, None)
            if len(results) >= top_k:
                break
        if not progress:
            break

    if len(results) < top_k:
        remainder: list[LoreMatch] = []
        for queue in buckets.values():
            remainder.extend(list(queue))
        remainder = overflow + remainder
        for candidate in remainder:
            _append_ordering_note(candidate, "balanced-fallback")
            results.append(candidate)
            if len(results) >= top_k:
                break
    return results[:top_k]


def _build_type_buckets(
    matches: Sequence[LoreMatch],
) -> dict[str, deque[LoreMatch]]:
    buckets: dict[str, deque[LoreMatch]] = {}
    for match in matches:
        key = (match.text_type or "unknown").lower()
        buckets.setdefault(key, deque()).append(match)
    return buckets


def _has_diversity_options(
    buckets: Mapping[str, deque[LoreMatch]],
    counts: Mapping[str, int],
) -> bool:
    for key, queue in buckets.items():
        if queue and counts.get(key, 0) < _BALANCED_MAX_PER_TYPE:
            return True
    return False


def _append_ordering_note(match: LoreMatch, note: str) -> None:
    if not note:
        return
    if match.ordering_notes:
        match.ordering_notes = f"{match.ordering_notes}; {note}"
    else:
        match.ordering_notes = note


def _deduplicate_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "text" not in frame.columns:
        return frame

    seen_texts: set[str] = set()
    kept_vectors: list[NDArray[np.float32]] = []
    columns = [column for column in frame.columns if column != "_vector"]
    unique_rows: list[dict[str, object]] = []

    for _, row in frame.iterrows():
        normalized = _normalize_text(row.get("text"))
        if normalized and normalized in seen_texts:
            continue

        text_type = str(row.get("text_type", "")).strip().lower()
        vector = _coerce_vector(row.get("_vector"))

        if (
            vector is not None
            and text_type in _SEMANTIC_TEXT_TYPES
            and _is_semantic_duplicate(vector, kept_vectors)
        ):
            continue

        if normalized:
            seen_texts.add(normalized)
        if vector is not None and text_type in _SEMANTIC_TEXT_TYPES:
            kept_vectors.append(vector)

        record = row.to_dict()
        record.pop("_vector", None)
        unique_rows.append(record)

    if not unique_rows:
        return frame.head(0)

    return pd.DataFrame.from_records(unique_rows, columns=columns)


def _normalize_text(value: object) -> str:
    if value is None:
        return ""
    normalized = str(value).strip()
    normalized = (
        normalized.replace("\n", " ")
        .replace("\r", " ")
        .replace("“", '"')
        .replace("”", '"')
        .replace("’", "'")
        .replace("–", "-")
        .replace("—", "-")
    )
    collapsed = " ".join(normalized.split())
    return collapsed.lower()


def _coerce_vector(value: object) -> NDArray[np.float32] | None:
    if value is None:
        return None
    try:
        array = np.asarray(value, dtype=np.float32)
    except (TypeError, ValueError):  # pragma: no cover - defensive
        return None
    if array.ndim != 1:
        return None
    return array


def _is_semantic_duplicate(
    vector: NDArray[np.float32],
    existing: Sequence[NDArray[np.float32]],
    *,
    threshold: float = _SEMANTIC_DUPLICATE_THRESHOLD,
) -> bool:
    if not existing:
        return False
    vector_norm = float(np.linalg.norm(vector))
    if vector_norm == 0.0:
        return False
    for candidate in existing:
        candidate_norm = float(np.linalg.norm(candidate))
        if candidate_norm == 0.0:
            continue
        numerator = np.dot(candidate, vector)
        denominator = candidate_norm * vector_norm
        similarity = float(numerator / denominator)
        if similarity >= threshold:
            return True
    return False


def _format_match(match: LoreMatch, counter: int) -> str:
    text_label = match.text_type or "text"
    category_label = match.category or "unknown"
    score_label = f"emb={match.score:.3f}"
    if match.reranker_score is not None:
        score_label = f"{score_label} rerank={match.reranker_score:.3f}"
    header = f"{counter}. [{score_label}] {text_label} | {category_label}"
    provenance = (
        "    lore_id="
        f"{match.lore_id} canonical_id={match.canonical_id or '—'} "
        f"source={match.source or '—'}"
    )
    body = f"    {match.text}"
    lines = [header, provenance, body]
    if match.ordering_notes:
        lines.append(f"    note={match.ordering_notes}")
    return "\n".join(lines)


def _parse_cli_filters(
    args: argparse.Namespace,
) -> Sequence[FilterExpression] | None:
    expressions: list[FilterExpression] = []

    def _append(column: str, values: Sequence[str] | None) -> None:
        if not values:
            return
        expressions.append(
            FilterExpression(
                column=column,
                values=tuple(values),
                operator="include",
            )
        )

    _append("category", args.category)
    _append("text_type", args.text_type)
    _append("source", args.source)

    if getattr(args, "filters", None):
        for raw_expression in args.filters:
            expression = _parse_filter_expression(raw_expression)
            if expression:
                expressions.append(expression)

    return expressions or None


def _parse_filter_expression(raw_expression: str) -> FilterExpression | None:
    if not raw_expression:
        return None

    operator: Literal["include", "exclude"]
    delimiter: str
    if "!=" in raw_expression:
        operator = "exclude"
        delimiter = "!="
    elif "=" in raw_expression:
        operator = "include"
        delimiter = "="
    else:
        LOGGER.warning(
            "Ignoring malformed filter expression: %s",
            raw_expression,
        )
        return None

    column, raw_values = raw_expression.split(delimiter, 1)
    column = column.strip()
    if not column:
        LOGGER.warning(
            "Ignoring filter with missing column: %s",
            raw_expression,
        )
        return None

    values = [
        token.strip()
        for token in raw_values.split(",")
        if token.strip()
    ]
    if not values:
        LOGGER.warning(
            "Ignoring filter with missing values: %s",
            raw_expression,
        )
        return None

    return FilterExpression(
        column=column,
        values=tuple(values),
        operator=operator,
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query the lore RAG index")
    parser.add_argument(
        "query",
        help="Free-form text describing the concept to search for",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=10,
        help="Number of results to return",
    )
    parser.add_argument(
        "--category",
        action="append",
        help="Filter by category (repeatable)",
    )
    parser.add_argument(
        "--text-type",
        dest="text_type",
        action="append",
        help="Filter by text_type (repeatable)",
    )
    parser.add_argument(
        "--source",
        action="append",
        help="Filter by source (repeatable)",
    )
    parser.add_argument(
        "--filter",
        dest="filters",
        action="append",
        help=(
            "Advanced filter expression (e.g. text_type=description or "
            "text_type!=dialogue). Repeatable."
        ),
    )
    parser.add_argument(
        "--index",
        type=Path,
        default=DEFAULT_INDEX,
        help="Path to the FAISS index file",
    )
    parser.add_argument(
        "--metadata",
        type=Path,
        default=DEFAULT_METADATA,
        help="Path to the metadata parquet",
    )
    parser.add_argument(
        "--info",
        type=Path,
        default=DEFAULT_INFO,
        help="Path to the index metadata JSON",
    )
    parser.add_argument(
        "--reranker",
        default="identity",
        help=(
            "Name of the reranker to apply (identity, none). Additional names "
            "can be registered via rag.reranker.load_reranker."
        ),
    )
    parser.add_argument(
        "--mode",
        choices=("balanced", "raw"),
        default="balanced",
        help=(
            "Retrieval ordering strategy: balanced interleaves text types, "
            "raw preserves FAISS or reranker order."
        ),
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    return parser.parse_args(argv)


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s %(message)s")


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    configure_logging(args.verbose)

    try:
        filters = _parse_cli_filters(args)
        reranker = load_reranker(args.reranker)
        matches = query_lore(
            args.query,
            top_k=args.top_k,
            filters=filters,
            index_path=args.index,
            metadata_path=args.metadata,
            info_path=args.info,
            reranker=reranker,
            mode=args.mode,
        )
    except (FileNotFoundError, RAGIndexError, ValueError) as exc:
        LOGGER.error("Query failed: %s", exc)
        raise SystemExit(1) from exc

    if not matches:
        LOGGER.info("No matches found")
        return

    for idx, match in enumerate(matches, start=1):
        print(_format_match(match, idx))


if __name__ == "__main__":  # pragma: no cover
    main()
