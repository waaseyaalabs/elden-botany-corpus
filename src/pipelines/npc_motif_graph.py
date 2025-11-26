"""NPC motif graph construction for the Phase 7 analysis layer."""

from __future__ import annotations

import json
import logging
import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import networkx as nx
import numpy as np
import pandas as pd
from corpus.community_schema import MotifTaxonomy, load_motif_taxonomy
from corpus.config import settings

from pipelines.aliasing import load_alias_map
from pipelines.motif_taxonomy_utils import (
    MotifMetadata,
    PatternMap,
    compile_motif_patterns,
    detect_motif_hits,
    motif_lookup,
)

LOGGER = logging.getLogger(__name__)

GRAPH_FILENAME = "npc_motif_graph.gpickle"
GRAPHML_FILENAME = "npc_motif_graph.graphml"
ENTITY_SUMMARY_FILENAME = "entity_summary.parquet"
ENTITY_MOTIF_FILENAME = "entity_motif_stats.parquet"
LORE_HITS_FILENAME = "lore_motif_hits.parquet"
REPORT_FILENAME = "graph_report.json"


@dataclass(slots=True)
class NPCMotifGraphConfig:
    """Runtime configuration for the motif graph pipeline."""

    curated_path: Path | None = None
    output_dir: Path = Path("data/analysis/npc_motif_graph")
    taxonomy_path: Path | None = None
    categories: tuple[str, ...] = ("npc",)
    alias_table_path: Path | None = None


@dataclass(slots=True)
class NPCMotifGraphArtifacts:
    """Collection of artifact paths emitted by the pipeline."""

    graph_path: Path
    graphml_path: Path
    entity_summary: Path
    entity_motif_stats: Path
    lore_hits: Path
    report_path: Path


class NPCMotifGraphPipeline:
    """Build a NetworkX view connecting NPCs, lore lines, and motifs."""

    def __init__(
        self,
        config: NPCMotifGraphConfig | None = None,
        taxonomy: MotifTaxonomy | None = None,
    ) -> None:
        self.config = config or NPCMotifGraphConfig()
        self._taxonomy = taxonomy or load_motif_taxonomy(
            self.config.taxonomy_path
        )
        self._patterns: PatternMap = compile_motif_patterns(self._taxonomy)
        self._motif_lookup = motif_lookup(self._taxonomy)
        self._alias_map = load_alias_map(self.config.alias_table_path)

    def run(self) -> NPCMotifGraphArtifacts:
        """Execute the pipeline and persist graph artifacts."""

        lore_frame = self._load_lore()
        motif_hits = detect_motif_hits(lore_frame["text"], self._patterns)
        hit_rows = self._expand_hits(lore_frame, motif_hits)
        if hit_rows.empty:
            raise RuntimeError(
                "No motif matches detected for the selected lore subset"
            )

        graph = self._build_graph(lore_frame, hit_rows)
        artifacts = self._write_artifacts(lore_frame, hit_rows, graph)
        return artifacts

    def _load_lore(self) -> pd.DataFrame:
        path = self.config.curated_path or (
            settings.curated_dir / "lore_corpus.parquet"
        )
        if not path.exists():
            message = (
                "Lore corpus parquet is missing. Run 'make build-corpus' or "
                "pass --curated."
            )
            raise FileNotFoundError(message)

        frame = pd.read_parquet(path)
        required = {
            "lore_id",
            "canonical_id",
            "category",
            "text_type",
            "text",
        }
        missing = required - set(frame.columns)
        if missing:
            formatted = ", ".join(sorted(missing))
            raise ValueError(f"Lore corpus missing columns: {formatted}")

        trimmed = frame.loc[frame["text"].notna()].copy()
        trimmed["text"] = trimmed["text"].astype(str).str.strip()
        trimmed = trimmed.loc[trimmed["text"] != ""]
        if self.config.categories:
            trimmed = trimmed.loc[
                trimmed["category"].isin(self.config.categories)
            ]
        trimmed.reset_index(drop=True, inplace=True)
        if trimmed.empty:
            categories = ", ".join(self.config.categories)
            raise RuntimeError(
                f"Lore corpus contains no rows for categories: {categories}"
            )
        return self._apply_alias_map(trimmed)

    def _apply_alias_map(self, frame: pd.DataFrame) -> pd.DataFrame:
        if not self._alias_map:
            return frame
        updated = frame.copy()
        resolved = [
            self._alias_map.get(str(value), str(value))
            for value in updated["canonical_id"].astype(str)
        ]
        updated["canonical_id"] = resolved
        changed = updated["canonical_id"].ne(frame["canonical_id"])
        if changed.any():
            LOGGER.info(
                "Applied alias overrides to %s lore rows",
                int(changed.sum()),
            )
        return updated

    def _expand_hits(
        self,
        frame: pd.DataFrame,
        motif_hits: pd.DataFrame,
    ) -> pd.DataFrame:
        if motif_hits.empty:
            return pd.DataFrame(
                columns=[
                    "lore_id",
                    "canonical_id",
                    "motif_slug",
                    "text",
                    "text_type",
                ]
            )

        boolean_hits = motif_hits.fillna(False).astype(bool)
        matrix = boolean_hits.to_numpy(dtype=bool, copy=True)
        row_indices, col_indices = np.nonzero(matrix)
        columns = list(boolean_hits.columns)

        records: list[dict[str, str]] = []
        for row_idx, col_idx in zip(row_indices, col_indices, strict=False):
            lore_row = frame.iloc[int(row_idx)]
            slug = columns[int(col_idx)]
            records.append(
                {
                    "lore_id": lore_row["lore_id"],
                    "canonical_id": lore_row["canonical_id"],
                    "motif_slug": slug,
                    "text": lore_row["text"],
                    "text_type": lore_row["text_type"],
                }
            )
        return pd.DataFrame(records)

    def _build_graph(
        self,
        lore_frame: pd.DataFrame,
        hits: pd.DataFrame,
    ) -> nx.DiGraph[Any]:
        graph: nx.DiGraph[Any] = nx.DiGraph()

        for canonical_id, group in lore_frame.groupby("canonical_id"):
            graph.add_node(
                canonical_id,
                type="entity",
                category=str(group["category"].iloc[0]),
                lore_count=int(group["lore_id"].nunique()),
            )

        for _, row in lore_frame.iterrows():
            graph.add_node(
                row["lore_id"],
                type="lore",
                canonical_id=row["canonical_id"],
                text_type=row["text_type"],
                text=row["text"],
            )
            graph.add_edge(
                row["canonical_id"],
                row["lore_id"],
                kind="speaks",
            )

        for slug, metadata in self._motif_lookup.items():
            graph.add_node(
                slug,
                type="motif",
                label=metadata.label,
                category=metadata.category,
                description=metadata.description,
            )

        for _, row in hits.iterrows():
            graph.add_edge(
                row["lore_id"],
                row["motif_slug"],
                kind="evokes",
            )

        grouped = (
            hits.groupby(["canonical_id", "motif_slug"])
            .size()
            .reset_index(name="hit_count")
        )
        for _, row in grouped.iterrows():
            graph.add_edge(
                row["canonical_id"],
                row["motif_slug"],
                kind="embodies",
                weight=int(row["hit_count"]),
            )

        return graph

    def _write_artifacts(
        self,
        lore_frame: pd.DataFrame,
        hits: pd.DataFrame,
        graph: nx.DiGraph[Any],
    ) -> NPCMotifGraphArtifacts:
        output_dir = self.config.output_dir
        output_dir.mkdir(parents=True, exist_ok=True)

        entity_summary = self._entity_summary(lore_frame, hits)
        entity_motif = self._entity_motif_stats(hits)
        lore_hits = self._annotated_hits(hits)

        entity_summary_path = output_dir / ENTITY_SUMMARY_FILENAME
        entity_motif_path = output_dir / ENTITY_MOTIF_FILENAME
        lore_hits_path = output_dir / LORE_HITS_FILENAME
        graph_path = output_dir / GRAPH_FILENAME
        graphml_path = output_dir / GRAPHML_FILENAME
        report_path = output_dir / REPORT_FILENAME

        entity_summary.to_parquet(entity_summary_path, index=False)
        entity_motif.to_parquet(entity_motif_path, index=False)
        lore_hits.to_parquet(lore_hits_path, index=False)

        with graph_path.open("wb") as handle:
            pickle.dump(graph, handle)
        nx.write_graphml(graph, graphml_path)

        report = {
            "summary": {
                "entities": int(entity_summary["canonical_id"].nunique()),
                "lore_nodes": int(lore_frame["lore_id"].nunique()),
                "motif_edges": int(len(entity_motif)),
                "taxonomy_version": self._taxonomy.version,
            },
            "sample_queries": [
                {
                    "description": "List motifs for an entity",
                    "networkx": (
                        "graph[canonical_id] to find lore neighbors, then "
                        "motif successors"
                    ),
                },
                {
                    "description": "Find NPCs sharing a motif",
                    "networkx": (
                        "{n for n in graph.predecessors(motif_slug) if "
                        "graph.nodes[n]['type'] == 'entity'}"
                    ),
                },
            ],
            "artifacts": {
                "graph": str(graph_path),
                "graphml": str(graphml_path),
                "entity_summary": str(entity_summary_path),
                "entity_motif_stats": str(entity_motif_path),
                "lore_hits": str(lore_hits_path),
            },
        }
        report_path.write_text(
            json.dumps(report, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        LOGGER.info("NPC motif graph written to %s", output_dir)
        return NPCMotifGraphArtifacts(
            graph_path=graph_path,
            graphml_path=graphml_path,
            entity_summary=entity_summary_path,
            entity_motif_stats=entity_motif_path,
            lore_hits=lore_hits_path,
            report_path=report_path,
        )

    def _entity_summary(
        self,
        lore_frame: pd.DataFrame,
        hits: pd.DataFrame,
    ) -> pd.DataFrame:
        lore_counts = (
            lore_frame.groupby(["canonical_id", "category"])
            .agg(lore_count=("lore_id", "nunique"))
            .reset_index()
        )
        motif_counts = (
            hits.groupby("canonical_id")
            .agg(
                motif_mentions=("motif_slug", "count"),
                unique_motifs=("motif_slug", "nunique"),
            )
            .reset_index()
        )
        merged = lore_counts.merge(
            motif_counts,
            on="canonical_id",
            how="left",
        )
        merged.fillna({"motif_mentions": 0, "unique_motifs": 0}, inplace=True)
        merged["motif_mentions"] = merged["motif_mentions"].astype(int)
        merged["unique_motifs"] = merged["unique_motifs"].astype(int)
        return merged

    def _entity_motif_stats(self, hits: pd.DataFrame) -> pd.DataFrame:
        grouped = (
            hits.groupby(["canonical_id", "motif_slug"])
            .agg(
                hit_count=("motif_slug", "size"),
                unique_lore=("lore_id", "nunique"),
            )
            .reset_index()
        )
        grouped["hit_count"] = grouped["hit_count"].astype(int)
        grouped["unique_lore"] = grouped["unique_lore"].astype(int)
        grouped["motif_label"] = grouped["motif_slug"].map(
            lambda slug: self._motif_meta(slug).label
        )
        grouped["motif_category"] = grouped["motif_slug"].map(
            lambda slug: self._motif_meta(slug).category
        )
        return grouped

    def _annotated_hits(self, hits: pd.DataFrame) -> pd.DataFrame:
        enriched = hits.copy()
        enriched["motif_label"] = enriched["motif_slug"].map(
            lambda slug: self._motif_meta(slug).label
        )
        enriched["motif_category"] = enriched["motif_slug"].map(
            lambda slug: self._motif_meta(slug).category
        )
        return enriched

    def _motif_meta(self, slug: str) -> MotifMetadata:
        return self._motif_lookup.get(
            slug,
            MotifMetadata(
                slug=slug,
                label=slug,
                category="unknown",
                description="",
            ),
        )


def load_graph(graph_path: Path) -> nx.DiGraph[Any]:
    """Convenience helper for tests and downstream notebooks."""

    with graph_path.open("rb") as handle:
        graph = cast(nx.DiGraph[Any], pickle.load(handle))
    return graph
