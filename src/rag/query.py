# pyright: reportMissingImports=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false

"""High-level helpers to query the lore RAG index."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Mapping, MutableMapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal, Protocol

import pandas as pd  # type: ignore[import]

from pipelines.build_rag_index import (
    DEFAULT_INDEX,
    DEFAULT_INFO,
    DEFAULT_METADATA,
    FilterClause,
    RAGIndexError,
    load_query_helper,
)
from rag.reranker import (  # type: ignore[import-not-found]
    RerankerProtocol,
    load_reranker,
)

FilterValue = str | Sequence[str]
FilterMapping = Mapping[str, FilterValue]
FilterInput = FilterMapping | Sequence["FilterExpression"]


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
) -> list[LoreMatch]:
    """Query the persisted FAISS index and return matches with metadata."""

    helper = load_query_helper(
        index_path=index_path,
        metadata_path=metadata_path,
        info_path=info_path,
        encoder=encoder,
    )
    normalized_filters = _prepare_filters(filters)
    frame = helper.query(query_text, top_k=top_k, filter_by=normalized_filters)
    frame = _deduplicate_frame(frame)
    matches = _frame_to_matches(frame)
    active_reranker = reranker or load_reranker(None)
    return active_reranker.rerank(matches)


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


def _deduplicate_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "text" not in frame.columns:
        return frame

    seen: set[str] = set()
    unique_rows: list[dict[str, object]] = []
    duplicates: list[dict[str, object]] = []

    for _, row in frame.iterrows():
        text_value = _normalize_text(row.get("text"))
        is_duplicate = bool(text_value and text_value in seen)
        target = duplicates if is_duplicate else unique_rows
        if text_value and not is_duplicate:
            seen.add(text_value)
        target.append(row.to_dict())

    if not duplicates:
        return frame

    combined = unique_rows + duplicates
    return pd.DataFrame(combined, columns=frame.columns)


def _normalize_text(value: object) -> str:
    if value is None:
        return ""
    normalized = str(value).strip()
    return " ".join(normalized.split()).lower()


def _format_match(match: LoreMatch, counter: int) -> str:
    text_label = match.text_type or "text"
    category_label = match.category or "unknown"
    header = f"{counter}. [{match.score:.3f}] {text_label} | {category_label}"
    provenance = (
        "    lore_id="
        f"{match.lore_id} canonical_id={match.canonical_id or '—'} "
        f"source={match.source or '—'}"
    )
    body = f"    {match.text}"
    return "\n".join([header, provenance, body])


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
