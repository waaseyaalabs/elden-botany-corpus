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
from typing import Protocol

import pandas as pd  # type: ignore[import]

from pipelines.build_rag_index import (
    DEFAULT_INDEX,
    DEFAULT_INFO,
    DEFAULT_METADATA,
    RAGIndexError,
    load_query_helper,
)

FilterValue = str | Sequence[str]
FilterMapping = Mapping[str, FilterValue]


class EncoderProtocol(Protocol):
    """Minimal protocol for embedding encoders."""

    def encode(self, texts: Sequence[str]) -> list[list[float]]:
        """Encode input texts into vector representations."""

        ...


LOGGER = logging.getLogger(__name__)
_ALLOW_FILTERS = {"category", "text_type", "source"}


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
    top_k: int = 5,
    filters: FilterMapping | None = None,
    index_path: Path = DEFAULT_INDEX,
    metadata_path: Path = DEFAULT_METADATA,
    info_path: Path = DEFAULT_INFO,
    encoder: EncoderProtocol | None = None,
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
    return _frame_to_matches(frame)


def _prepare_filters(
    filters: FilterMapping | None,
) -> MutableMapping[str, FilterValue] | None:
    if not filters:
        return None

    normalized: MutableMapping[str, FilterValue] = {}
    for key, value in filters.items():
        if key not in _ALLOW_FILTERS:
            LOGGER.warning("Ignoring unsupported filter column: %s", key)
            continue
        normalized[key] = value
    return normalized or None


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


def _parse_cli_filters(args: argparse.Namespace) -> FilterMapping | None:
    filters: dict[str, FilterValue] = {}
    if args.category:
        values = args.category
        filters["category"] = values if len(values) > 1 else values[0]
    if args.text_type:
        values = args.text_type
        filters["text_type"] = values if len(values) > 1 else values[0]
    if args.source:
        values = args.source
        filters["source"] = values if len(values) > 1 else values[0]
    return filters or None


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query the lore RAG index")
    parser.add_argument(
        "query",
        help="Free-form text describing the concept to search for",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=5,
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
        matches = query_lore(
            args.query,
            top_k=args.top_k,
            filters=filters,
            index_path=args.index,
            metadata_path=args.metadata,
            info_path=args.info,
        )
    except (FileNotFoundError, RAGIndexError) as exc:
        LOGGER.error("Query failed: %s", exc)
        raise SystemExit(1) from exc

    if not matches:
        LOGGER.info("No matches found")
        return

    for idx, match in enumerate(matches, start=1):
        print(_format_match(match, idx))


if __name__ == "__main__":  # pragma: no cover
    main()
