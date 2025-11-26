"""Helpers for mapping speaker or alias identifiers to canonical IDs."""
# pyright: reportUnknownMemberType=false

from __future__ import annotations

import logging
from collections.abc import Mapping
from pathlib import Path

import pandas as pd
from pandas import DataFrame

LOGGER = logging.getLogger(__name__)

_ALIAS_CANDIDATES = (
    "alias_id",
    "alias",
    "speaker_id",
    "speaker_slug",
    "raw_id",
)
_CANONICAL_CANDIDATES = (
    "canonical_id",
    "target_canonical_id",
    "canonical",
)


def load_alias_map(path: Path | None) -> Mapping[str, str]:
    """Load an alias→canonical mapping from parquet or CSV files."""

    if path is None:
        return {}
    if not path.exists():
        LOGGER.warning(
            "Alias table %s missing; continuing without overrides",
            path,
        )
        return {}

    suffix = path.suffix.lower()
    frame: DataFrame
    if suffix in {".parquet", ".pq"}:
        frame = pd.read_parquet(path)
    elif suffix in {".json", ".jsonl"}:
        frame = pd.read_json(path, lines=(suffix == ".jsonl"))
    else:
        frame = pd.read_csv(path)

    alias_column: str | None = None
    for candidate in _ALIAS_CANDIDATES:
        if candidate in frame.columns:
            alias_column = candidate
            break
    if alias_column is None:
        raise ValueError(
            "Alias table must include one of the columns: "
            f"{', '.join(_ALIAS_CANDIDATES)}"
        )

    canonical_column: str | None = None
    for candidate in _CANONICAL_CANDIDATES:
        if candidate in frame.columns:
            canonical_column = candidate
            break
    if canonical_column is None:
        raise ValueError(
            "Alias table must include one of the columns: "
            f"{', '.join(_CANONICAL_CANDIDATES)}"
        )

    alias_map: dict[str, str] = {}
    for alias_value, canonical_value in zip(
        frame[alias_column],
        frame[canonical_column],
        strict=False,
    ):
        if alias_value is None or canonical_value is None:
            continue
        alias_map[str(alias_value)] = str(canonical_value)
    LOGGER.info(
        "Loaded %s alias overrides from %s",
        len(alias_map),
        path,
    )
    return alias_map


__all__ = ["load_alias_map"]
