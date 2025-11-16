"""Entity reconciliation and deduplication logic."""

from datetime import datetime
from typing import Any

import Levenshtein
import polars as pl

from corpus.config import settings
from corpus.models import (
    Provenance,
    RawEntity,
    create_slug,
    normalize_name_for_matching,
)
from corpus.utils import progress_bar


class EntityReconciler:
    """Reconcile entities from multiple sources with priority ordering."""

    def __init__(self, threshold: float | None = None) -> None:
        """
        Initialize reconciler.

        Args:
            threshold: Similarity threshold for matching (default from config)
        """
        self.threshold = threshold or settings.match_threshold
        self.entity_map: dict[str, dict[str, Any]] = {}

    def add_entities(self, entities: list[RawEntity], priority: int) -> None:
        """
        Add entities with a priority level (lower = higher priority).

        Args:
            entities: List of raw entities
            priority: Priority level (1 = highest)
        """
        for entity in progress_bar(
            entities, desc=f"Adding entities (priority {priority})"
        ):
            slug = entity.to_slug()
            key = f"{entity.entity_type}:{slug}"

            if key not in self.entity_map:
                self.entity_map[key] = {
                    "entity": entity.model_copy(deep=True),
                    "priority": priority,
                    "sources": self._unique_sources(entity.provenance),
                }
                continue

            entry = self.entity_map[key]
            existing_entity: RawEntity = entry["entity"]

            if priority < entry["priority"]:
                # Preserve heritage before replacing the entity reference
                previous_provenance = existing_entity.provenance
                entry["entity"] = entity.model_copy(deep=True)
                entry["priority"] = priority
                existing_entity = entry["entity"]
                self._merge_provenance(entry, previous_provenance)

            self._merge_provenance(entry, entity.provenance)

    def get_reconciled_entities(self) -> list[RawEntity]:
        """
        Get final reconciled entities.

        Returns:
            List of deduplicated RawEntity objects
        """
        entities = [item["entity"] for item in self.entity_map.values()]
        print(f"\nReconciled to {len(entities)} unique entities")
        return entities

    def match_text_to_entities(
        self, text_snippets: list[RawEntity]
    ) -> tuple[dict[str, RawEntity], list[RawEntity]]:
        """
        Match text snippets to entities using fuzzy matching.

        Args:
            text_snippets: List of RawEntity objects with type 'text_snippet'

        Returns:
            Tuple of (matched dict, unmapped list)
        """
        print(f"\nMatching {len(text_snippets)} text snippets to entities...")

        # Build normalized name index
        entity_index: dict[str, list[tuple[str, RawEntity]]] = {}
        for item in self.entity_map.values():
            entity = item["entity"]
            normalized = normalize_name_for_matching(entity.name)
            if normalized not in entity_index:
                entity_index[normalized] = []
            entity_index[normalized].append((entity.name, entity))

        matched: dict[str, RawEntity] = {}
        unmapped: list[RawEntity] = []

        for snippet in progress_bar(text_snippets, desc="Matching text"):
            best_match = self._find_best_match(snippet.name, entity_index)

            if best_match:
                match_key, matched_entity = best_match
                # Merge the text into matched entity
                if snippet.description:
                    if matched_entity.description:
                        matched_entity.description += (
                            "\n\n" + snippet.description
                        )
                    else:
                        matched_entity.description = snippet.description

                # Merge provenance
                for prov in snippet.provenance:
                    if prov not in matched_entity.provenance:
                        matched_entity.provenance.append(prov)

                matched[snippet.name] = matched_entity
            else:
                unmapped.append(snippet)

        print(f"Matched {len(matched)} snippets, {len(unmapped)} unmapped")
        return matched, unmapped

    def _find_best_match(
        self,
        name: str,
        entity_index: dict[str, list[tuple[str, RawEntity]]],
    ) -> tuple[str, RawEntity] | None:
        """
        Find best matching entity for a name.

        Args:
            name: Name to match
            entity_index: Index of normalized names to entities

        Returns:
            Tuple of (match_key, entity) or None
        """
        normalized = normalize_name_for_matching(name)

        # Exact match
        if normalized in entity_index:
            return normalized, entity_index[normalized][0][1]

        # Fuzzy match
        best_score = 0.0
        best_match = None

        for norm_name, entities in entity_index.items():
            score = Levenshtein.ratio(normalized, norm_name)
            if score > best_score and score >= self.threshold:
                best_score = score
                best_match = (norm_name, entities[0][1])

        return best_match

    @staticmethod
    def _unique_sources(provenance: list[Provenance]) -> list[str]:
        """Return provenance sources without duplicates while keeping order."""
        seen: set[str] = set()
        ordered_sources: list[str] = []
        for prov in provenance:
            if prov.source not in seen:
                seen.add(prov.source)
                ordered_sources.append(prov.source)
        return ordered_sources

    def _merge_provenance(
        self, entry: dict[str, Any], provenance: list[Provenance]
    ) -> None:
        """Merge provenance records and source summary into an entry."""
        if not provenance:
            return

        existing_sources = set(entry["sources"])
        existing_prov_keys = {
            self._provenance_key(prov) for prov in entry["entity"].provenance
        }

        for prov in provenance.copy():
            prov_key = self._provenance_key(prov)
            if prov.source not in existing_sources:
                entry["sources"].append(prov.source)
                existing_sources.add(prov.source)
            if prov_key not in existing_prov_keys:
                entry["entity"].provenance.append(prov.model_copy(deep=True))
                existing_prov_keys.add(prov_key)

    @staticmethod
    def _provenance_key(
        prov: Provenance,
    ) -> tuple[str, str, str | None, datetime]:
        """Generate a tuple that uniquely identifies a provenance record."""
        return prov.source, prov.uri, prov.sha256, prov.retrieved_at


def reconcile_all_sources(
    kaggle_base: list[RawEntity],
    kaggle_dlc: list[RawEntity],
    github_api: list[RawEntity],
    dlc_texts: list[RawEntity],
) -> tuple[list[RawEntity], list[RawEntity]]:
    """
    Reconcile entities from all sources with priority ordering.

    Priority order:
    1. Kaggle DLC (most complete for DLC)
    2. Kaggle base (most complete for base game)
    3. GitHub API (fallback)

    Args:
        kaggle_base: Base game entities from Kaggle
        kaggle_dlc: DLC entities from Kaggle
        github_api: Entities from GitHub API
        dlc_texts: DLC text snippets from Impalers

    Returns:
        Tuple of (reconciled entities, unmapped texts)
    """
    reconciler = EntityReconciler()

    # Add sources in priority order (1 = highest priority)
    print("\n=== Reconciling Entities ===")

    reconciler.add_entities(kaggle_dlc, priority=1)
    reconciler.add_entities(kaggle_base, priority=2)
    reconciler.add_entities(github_api, priority=3)

    # Get reconciled entities
    entities = reconciler.get_reconciled_entities()

    # Match DLC text snippets
    matched, unmapped = reconciler.match_text_to_entities(dlc_texts)

    return entities, unmapped


def entities_to_dataframe(entities: list[RawEntity]) -> pl.DataFrame:
    """
    Convert RawEntity list to Polars DataFrame.

    Args:
        entities: List of RawEntity objects

    Returns:
        Polars DataFrame
    """
    rows = []

    for entity in entities:
        slug = create_slug(entity.name)

        # Extract metadata from raw_data
        meta = {
            k: v
            for k, v in entity.raw_data.items()
            if k not in ["name", "description"]
        }

        # Collect provenance sources
        sources = [prov.source for prov in entity.provenance]

        rows.append(
            {
                "entity_type": entity.entity_type,
                "slug": slug,
                "name": entity.name,
                "is_dlc": entity.is_dlc,
                "description": entity.description or "",
                "meta_json": meta,
                "sources": sources,
            }
        )

    return pl.DataFrame(rows)
