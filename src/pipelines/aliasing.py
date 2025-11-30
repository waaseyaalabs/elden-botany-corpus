"""Helpers for mapping speaker or alias identifiers to canonical IDs."""
# pyright: reportUnknownMemberType=false

from __future__ import annotations

import fnmatch
import logging
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator

import pandas as pd
from pandas import DataFrame

LOGGER = logging.getLogger(__name__)

DEFAULT_ALIAS_TABLE = Path("data/reference/entity_aliases.csv")

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


@dataclass(slots=True)
class _PatternAlias:
    raw_pattern: str
    regex: re.Pattern[str]
    canonical_id: str


class AliasResolver(Mapping[str, str]):
    """Mapping-compatible helper that resolves wildcard alias patterns."""

    def __init__(
        self,
        exact_map: dict[str, str] | None = None,
        patterns: Iterable[_PatternAlias] | None = None,
    ) -> None:
        self._exact: dict[str, str] = exact_map or {}
        self._patterns: tuple[_PatternAlias, ...] = tuple(patterns or ())
        pattern_keys = [item.raw_pattern for item in self._patterns]
        self._keys: tuple[str, ...] = tuple(
            [*self._exact.keys(), *pattern_keys]
        )
        self._cache: dict[str, str] = {}

    def __getitem__(self, key: str) -> str:
        if key in self._exact:
            return self._exact[key]
        if key in self._cache:
            return self._cache[key]
        for pattern in self._patterns:
            if pattern.regex.match(key):
                self._cache[key] = pattern.canonical_id
                return pattern.canonical_id
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        return iter(self._keys)

    def __len__(self) -> int:  # pragma: no cover - trivial
        return len(self._keys)

    def resolve(self, value: str) -> str:
        """Return the canonical value for ``value`` (identity when missing)."""

        try:
            return self[value]
        except KeyError:
            return value


def _read_alias_frame(path: Path) -> DataFrame:
    suffix = path.suffix.lower()
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    if suffix in {".json", ".jsonl"}:
        return pd.read_json(path, lines=(suffix == ".jsonl"))
    return pd.read_csv(path)


def load_alias_map(path: Path | None) -> AliasResolver:
    """Load wildcard-aware alias overrides from tabular files."""

    if path is None:
        return AliasResolver()
    if not path.exists():
        LOGGER.warning(
            "Alias table %s missing; continuing without overrides",
            path,
        )
        return AliasResolver()

    frame = _read_alias_frame(path)

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

    exact_map: dict[str, str] = {}
    patterns: list[_PatternAlias] = []
    for alias_value, canonical_value in zip(
        frame[alias_column],
        frame[canonical_column],
        strict=False,
    ):
        if alias_value is None or canonical_value is None:
            continue
        alias_text = str(alias_value).strip()
        canonical_text = str(canonical_value).strip()
        if not alias_text or not canonical_text:
            continue
        if any(char in alias_text for char in "*?["):
            regex = re.compile(fnmatch.translate(alias_text))
            patterns.append(
                _PatternAlias(
                    raw_pattern=alias_text,
                    regex=regex,
                    canonical_id=canonical_text,
                )
            )
        else:
            exact_map[alias_text] = canonical_text

    resolver = AliasResolver(exact_map=exact_map, patterns=patterns)
    LOGGER.info(
        "Loaded %s alias overrides (%s patterns) from %s",
        len(exact_map),
        len(patterns),
        path,
    )
    return resolver


__all__ = ["AliasResolver", "DEFAULT_ALIAS_TABLE", "load_alias_map"]
