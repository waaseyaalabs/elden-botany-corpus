"""Narrative summarizer for distilling NPC motif graphs into briefs."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from corpus.community_schema import MotifTaxonomy, load_motif_taxonomy

from pipelines.npc_motif_graph import (
    ENTITY_MOTIF_FILENAME,
    ENTITY_SUMMARY_FILENAME,
    LORE_HITS_FILENAME,
)

LOGGER = logging.getLogger(__name__)

SUMMARY_JSON = "npc_narrative_summaries.json"
SUMMARY_PARQUET = "npc_narrative_summaries.parquet"
SUMMARY_MARKDOWN = "npc_narrative_summaries.md"


@dataclass(slots=True)
class NarrativeSummariesConfig:
    """Configuration for transforming graph artifacts into summaries."""

    graph_dir: Path = Path("data/analysis/npc_motif_graph")
    output_dir: Path = Path("data/analysis/narrative_summaries")
    taxonomy_path: Path | None = None
    max_motifs: int = 4
    max_quotes: int = 3


@dataclass(slots=True)
class NarrativeSummaryArtifacts:
    """Paths to generated summary artifacts."""

    summaries_json: Path
    summaries_parquet: Path
    markdown_path: Path


class NarrativeSummariesPipeline:
    """Compose lightweight natural language briefs per NPC."""

    def __init__(
        self,
        config: NarrativeSummariesConfig | None = None,
        taxonomy: MotifTaxonomy | None = None,
    ) -> None:
        self.config = config or NarrativeSummariesConfig()
        self._taxonomy = taxonomy or load_motif_taxonomy(
            self.config.taxonomy_path
        )
        self._motif_order = self._build_motif_order()

    def run(self) -> NarrativeSummaryArtifacts:
        entity_summary = self._read_parquet(
            ENTITY_SUMMARY_FILENAME,
            "entity summary",
        )
        entity_motif = self._read_parquet(
            ENTITY_MOTIF_FILENAME,
            "entity motif stats",
        )
        lore_hits = self._read_parquet(
            LORE_HITS_FILENAME,
            "lore motif hits",
        )

        summaries = self._build_summaries(
            entity_summary,
            entity_motif,
            lore_hits,
        )
        if not summaries:
            raise RuntimeError("No narrative summaries were produced")

        artifacts = self._write_artifacts(summaries)
        LOGGER.info(
            "Narrative summaries written to %s", self.config.output_dir
        )
        return artifacts

    def _read_parquet(self, name: str, label: str) -> pd.DataFrame:
        path = self.config.graph_dir / name
        if not path.exists():
            raise FileNotFoundError(
                f"Missing {label} artifact at {path}. "
                "Run 'corpus analysis graph' first."
            )
        return pd.read_parquet(path)

    def _build_summaries(
        self,
        entity_summary: pd.DataFrame,
        entity_motif: pd.DataFrame,
        lore_hits: pd.DataFrame,
    ) -> list[dict[str, object]]:
        summaries: list[dict[str, object]] = []
        grouped_hits = lore_hits.groupby("canonical_id")
        grouped_motifs = entity_motif.groupby("canonical_id")

        for _, row in entity_summary.iterrows():
            canonical_id = str(row["canonical_id"])
            if canonical_id not in grouped_motifs.groups:
                continue
            motif_rows = grouped_motifs.get_group(canonical_id)
            if motif_rows.empty:
                continue

            top_motifs = self._top_motifs(motif_rows)
            quotes = self._quotes_for_entity(
                canonical_id,
                grouped_hits,
            )
            summary_text = self._compose_summary_text(
                canonical_id,
                row,
                top_motifs,
            )
            summaries.append(
                {
                    "canonical_id": canonical_id,
                    "category": row.get("category", "npc"),
                    "lore_count": int(row.get("lore_count", 0)),
                    "motif_mentions": int(row.get("motif_mentions", 0)),
                    "unique_motifs": int(row.get("unique_motifs", 0)),
                    "top_motifs": top_motifs,
                    "quotes": quotes,
                    "summary_text": summary_text,
                }
            )

        return summaries

    def _top_motifs(self, motif_rows: pd.DataFrame) -> list[dict[str, object]]:
        ranked = motif_rows.copy()
        ranked["__rank"] = (
            ranked["motif_slug"]
            .map(self._motif_order)
            .fillna(len(self._motif_order))
        )
        ordered = ranked.sort_values(
            ["hit_count", "__rank"],
            ascending=[False, True],
        ).head(self.config.max_motifs)
        top_motifs: list[dict[str, object]] = []
        for _, row in ordered.iterrows():
            top_motifs.append(
                {
                    "slug": row["motif_slug"],
                    "label": row.get("motif_label", row["motif_slug"]),
                    "category": row.get("motif_category", "unknown"),
                    "hit_count": int(row["hit_count"]),
                    "unique_lore": int(row["unique_lore"]),
                }
            )
        return top_motifs

    def _quotes_for_entity(
        self,
        canonical_id: str,
        grouped_hits: Any,
    ) -> list[dict[str, object]]:
        if canonical_id not in grouped_hits.groups:
            return []
        subset = grouped_hits.get_group(canonical_id)
        quotes: list[dict[str, object]] = []
        for lore_id, group in subset.groupby("lore_id"):
            motifs = sorted(group["motif_label"].unique())
            quotes.append(
                {
                    "lore_id": lore_id,
                    "text": group["text"].iloc[0],
                    "motifs": motifs,
                }
            )
            if len(quotes) >= self.config.max_quotes:
                break
        return quotes

    def _compose_summary_text(
        self,
        canonical_id: str,
        summary_row: pd.Series,
        top_motifs: list[dict[str, object]],
    ) -> str:
        motif_labels = [item["label"] for item in top_motifs]
        motif_phrase = self._format_list(motif_labels)
        lore_count = int(summary_row.get("lore_count", 0))
        if not motif_phrase:
            return (
                f"{canonical_id} surfaces {lore_count} lore lines but has "
                "no registered motifs yet."
            )
        return (
            f"{canonical_id} leans on {motif_phrase} across {lore_count} "
            "lore lines."
        )

    def _format_list(self, items: list[str]) -> str:
        cleaned = [item for item in items if item]
        if not cleaned:
            return ""
        if len(cleaned) == 1:
            return cleaned[0]
        return ", ".join(cleaned[:-1]) + f" and {cleaned[-1]}"

    def _write_artifacts(
        self,
        summaries: list[dict[str, object]],
    ) -> NarrativeSummaryArtifacts:
        output_dir = self.config.output_dir
        output_dir.mkdir(parents=True, exist_ok=True)

        json_path = output_dir / SUMMARY_JSON
        parquet_path = output_dir / SUMMARY_PARQUET
        markdown_path = output_dir / SUMMARY_MARKDOWN

        payload = {
            "summary": {
                "entities": len(summaries),
                "graph_dir": str(self.config.graph_dir),
                "taxonomy_version": self._taxonomy.version,
            },
            "summaries": summaries,
        }
        json_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        parquet_rows = [
            {
                "canonical_id": item["canonical_id"],
                "category": item["category"],
                "lore_count": item["lore_count"],
                "motif_mentions": item["motif_mentions"],
                "unique_motifs": item["unique_motifs"],
                "top_motifs": json.dumps(
                    item["top_motifs"], ensure_ascii=False
                ),
                "quotes": json.dumps(item["quotes"], ensure_ascii=False),
                "summary_text": item["summary_text"],
            }
            for item in summaries
        ]
        pd.DataFrame(parquet_rows).to_parquet(parquet_path, index=False)

        markdown_path.write_text(
            self._markdown_body(summaries),
            encoding="utf-8",
        )

        return NarrativeSummaryArtifacts(
            summaries_json=json_path,
            summaries_parquet=parquet_path,
            markdown_path=markdown_path,
        )

    def _build_motif_order(self) -> dict[str, int]:
        order: dict[str, int] = {}
        index = 0
        for category in self._taxonomy.categories:
            for motif in category.motifs:
                order[motif.slug] = index
                index += 1
        return order

    def _markdown_body(self, summaries: list[dict[str, object]]) -> str:
        lines = ["# NPC Narrative Summaries", ""]
        lines.append(f"Total entities: {len(summaries)}")
        lines.append(f"Source graph: {self.config.graph_dir}")
        lines.append("")
        for entry in summaries:
            lines.append(f"## {entry['canonical_id']}")
            lines.append(entry["summary_text"])
            lines.append("")
            motif_labels = ", ".join(
                [
                    f"{item['label']} ({item['hit_count']})"
                    for item in entry["top_motifs"]
                ]
            )
            lines.append(f"Top motifs: {motif_labels}")
            if entry["quotes"]:
                lines.append("Quotes:")
                for quote in entry["quotes"]:
                    motif_list = ", ".join(quote["motifs"])
                    lines.append(f"- {quote['text']} ({motif_list})")
            lines.append("")
        return "\n".join(lines)
