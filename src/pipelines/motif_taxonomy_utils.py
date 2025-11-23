"""Shared helpers for working with the motif taxonomy."""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass

import pandas as pd
from corpus.community_schema import MotifEntry, MotifTaxonomy

PatternMap = dict[str, re.Pattern[str]]


@dataclass(slots=True)
class MotifMetadata:
    """Lightweight metadata projection for motif lookups."""

    slug: str
    label: str
    category: str
    description: str


def compile_motif_patterns(taxonomy: MotifTaxonomy) -> PatternMap:
    """Compile regex patterns that capture all motif keywords."""

    patterns: PatternMap = {}
    for category in taxonomy.categories:
        for motif in category.motifs:
            pattern = _keyword_pattern(motif)
            if pattern is not None:
                patterns[motif.slug] = pattern
    return patterns


def detect_motif_hits(
    texts: pd.Series,
    patterns: PatternMap,
) -> pd.DataFrame:
    """Return a boolean DataFrame indexed by lore rows for motif matches."""

    if not patterns:
        return pd.DataFrame(index=texts.index)

    normalized = texts.astype(str).fillna("")
    hits: dict[str, list[bool]] = {}
    for slug, pattern in patterns.items():
        hits[slug] = [bool(pattern.search(value)) for value in normalized]
    return pd.DataFrame(hits, index=texts.index)


def motif_lookup(taxonomy: MotifTaxonomy) -> dict[str, MotifMetadata]:
    """Build a lookup dictionary keyed by motif slug."""

    lookup: dict[str, MotifMetadata] = {}
    for category in taxonomy.categories:
        for motif in category.motifs:
            lookup[motif.slug] = MotifMetadata(
                slug=motif.slug,
                label=motif.label,
                category=category.slug,
                description=motif.description,
            )
    return lookup


def ensure_known_motifs(
    motifs: Iterable[str],
    taxonomy: MotifTaxonomy,
) -> None:
    """Validate motif slugs exist inside the taxonomy."""

    taxonomy.ensure(list(motifs))


def _keyword_pattern(motif: MotifEntry) -> re.Pattern[str] | None:
    keywords = (
        {motif.label}
        | set(motif.synonyms)
        | set(motif.narrative_signals)
        | set(motif.canonical_examples)
    )
    normalized = {word.strip().lower() for word in keywords if word.strip()}
    if not normalized:
        return None

    escaped = [re.escape(word) for word in normalized]
    pattern = "|".join(sorted(escaped))
    return re.compile(pattern, re.IGNORECASE)
