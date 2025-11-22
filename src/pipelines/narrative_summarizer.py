"""Narrative summarizer for distilling NPC motif graphs into briefs."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, cast

import pandas as pd
from corpus.community_schema import MotifTaxonomy, load_motif_taxonomy

from pipelines.llm.base import (
    LLMClient,
    LLMResponseError,
    create_llm_client_from_env,
)
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
    use_llm: bool = True


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
        llm_client: LLMClient | None = None,
    ) -> None:
        self.config = config or NarrativeSummariesConfig()
        self._taxonomy = taxonomy or load_motif_taxonomy(
            self.config.taxonomy_path
        )
        self._motif_order = self._build_motif_order()
        self._llm_client = self._resolve_llm_client(llm_client)

    def _resolve_llm_client(
        self,
        llm_client: LLMClient | None,
    ) -> LLMClient | None:
        if llm_client is not None:
            return llm_client
        if not self.config.use_llm:
            return None
        return create_llm_client_from_env()

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
    ) -> list[dict[str, Any]]:
        summaries: list[dict[str, Any]] = []
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
            summaries.append(
                self._summarize_entity(
                    canonical_id,
                    row,
                    top_motifs,
                    quotes,
                )
            )

        return summaries

    def _summarize_entity(
        self,
        canonical_id: str,
        summary_row: pd.Series,
        top_motifs: list[dict[str, Any]],
        quotes: list[dict[str, Any]],
    ) -> dict[str, Any]:
        category = summary_row.get("category", "npc")
        entry: dict[str, Any] = {
            "canonical_id": canonical_id,
            "category": category,
            "lore_count": int(summary_row.get("lore_count", 0)),
            "motif_mentions": int(summary_row.get("motif_mentions", 0)),
            "unique_motifs": int(summary_row.get("unique_motifs", 0)),
            "top_motifs": top_motifs,
            "quotes": quotes,
        }

        summary_text = self._compose_summary_text(
            canonical_id,
            summary_row,
            top_motifs,
        )
        summary_motif_slugs = [
            str(item.get("slug"))
            for item in top_motifs
            if item.get("slug")
        ]
        supporting_quotes = [
            str(quote.get("lore_id"))
            for quote in quotes
            if quote.get("lore_id")
        ]
        llm_used = False
        llm_provider: str | None = None
        llm_model: str | None = None

        if self.config.use_llm and self._llm_client:
            llm_provider = self._llm_client.config.provider
            llm_model = self._llm_client.config.model
            try:
                llm_result = self._invoke_llm(
                    canonical_id,
                    summary_row,
                    top_motifs,
                    quotes,
                )
            except LLMResponseError as exc:
                LOGGER.warning(
                    "LLM summarization failed for %s: %s",
                    canonical_id,
                    exc,
                )
            else:
                summary_text = llm_result["summary_text"]
                if llm_result["motif_slugs"]:
                    summary_motif_slugs = llm_result["motif_slugs"]
                supporting_quotes = llm_result["supporting_quotes"]
                llm_used = True

        entry.update(
            {
                "summary_text": summary_text,
                "summary_motif_slugs": summary_motif_slugs,
                "supporting_quotes": supporting_quotes,
                "llm_provider": llm_provider,
                "llm_model": llm_model,
                "llm_used": llm_used,
            }
        )
        return entry

    def _invoke_llm(
        self,
        canonical_id: str,
        summary_row: pd.Series,
        top_motifs: list[dict[str, Any]],
        quotes: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if not self._llm_client:
            raise LLMResponseError("LLM client is not configured")
        payload = self._build_llm_payload(
            canonical_id,
            summary_row,
            top_motifs,
            quotes,
        )
        response = self._llm_client.summarize_entity(payload)
        return self._normalize_llm_response(
            canonical_id,
            response,
            quotes,
        )

    def _normalize_llm_response(
        self,
        canonical_id: str,
        response: Mapping[str, Any],
        quotes: list[dict[str, Any]],
    ) -> dict[str, Any]:
        response_id = str(response.get("canonical_id", "")).strip()
        if response_id and response_id != canonical_id:
            raise LLMResponseError(
                f"LLM response canonical_id mismatch for {canonical_id}"
            )

        summary_text = str(response.get("summary_text", "")).strip()
        if not summary_text:
            raise LLMResponseError("LLM response missing summary_text")

        motif_slugs = self._ensure_str_list(
            response.get("motif_slugs"),
            "motif_slugs",
        )
        supporting_quotes = self._ensure_str_list(
            response.get("supporting_quotes"),
            "supporting_quotes",
        )
        supporting_quotes = self._validate_supporting_quotes(
            canonical_id,
            supporting_quotes,
            quotes,
        )

        return {
            "summary_text": summary_text,
            "motif_slugs": motif_slugs,
            "supporting_quotes": supporting_quotes,
        }

    def _ensure_str_list(
        self,
        value: Any,
        field_name: str,
    ) -> list[str]:
        if value is None:
            return []
        if not isinstance(value, list):
            raise LLMResponseError(
                f"LLM field '{field_name}' must be a list"
            )
        typed_list = cast(list[Any], value)
        result: list[str] = []
        for item in typed_list:
            if item is None:
                continue
            result.append(str(item))
        return result

    def _validate_supporting_quotes(
        self,
        canonical_id: str,
        supporting_quotes: list[str],
        quotes: list[dict[str, Any]],
    ) -> list[str]:
        available = {
            str(quote.get("lore_id"))
            for quote in quotes
            if quote.get("lore_id")
        }
        if not available and supporting_quotes:
            raise LLMResponseError(
                f"LLM referenced quotes for {canonical_id} but none exist"
            )
        missing = [
            quote_id
            for quote_id in supporting_quotes
            if quote_id not in available
        ]
        if missing:
            raise LLMResponseError(
                "LLM referenced unknown quote IDs "
                f"{missing} for {canonical_id}"
            )
        return supporting_quotes

    def _build_llm_payload(
        self,
        canonical_id: str,
        summary_row: pd.Series,
        top_motifs: list[dict[str, Any]],
        quotes: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return {
            "canonical_id": canonical_id,
            "category": summary_row.get("category", "npc"),
            "lore_count": int(summary_row.get("lore_count", 0)),
            "motif_mentions": int(summary_row.get("motif_mentions", 0)),
            "unique_motifs": int(summary_row.get("unique_motifs", 0)),
            "top_motifs": top_motifs[: self.config.max_motifs],
            "quotes": quotes[: self.config.max_quotes],
        }

    def _top_motifs(self, motif_rows: pd.DataFrame) -> list[dict[str, Any]]:
        ranked = motif_rows.copy()
        order_series = ranked["motif_slug"].map(self._motif_order)
        filled = order_series.fillna(  # type: ignore[arg-type]
            float(len(self._motif_order))
        )
        ranked.loc[:, "__rank"] = filled
        ordered = ranked.sort_values(  # type: ignore[arg-type]
            ["hit_count", "__rank"],
            ascending=[False, True],
        ).head(self.config.max_motifs)
        top_motifs: list[dict[str, Any]] = []
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
    ) -> list[dict[str, Any]]:
        if canonical_id not in grouped_hits.groups:
            return []
        subset = grouped_hits.get_group(canonical_id)
        quotes: list[dict[str, Any]] = []
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
        top_motifs: list[dict[str, Any]],
    ) -> str:
        motif_labels = [
            str(item.get("label", ""))
            for item in top_motifs
            if item.get("label")
        ]
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

        payload: dict[str, Any] = {
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

        parquet_rows: list[dict[str, Any]] = [
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
                "summary_motif_slugs": json.dumps(
                    item["summary_motif_slugs"],
                    ensure_ascii=False,
                ),
                "supporting_quotes": json.dumps(
                    item["supporting_quotes"],
                    ensure_ascii=False,
                ),
                "llm_provider": item["llm_provider"],
                "llm_model": item["llm_model"],
                "llm_used": item["llm_used"],
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

    def _markdown_body(self, summaries: list[dict[str, Any]]) -> str:
        lines = ["# NPC Narrative Summaries", ""]
        lines.append(f"Total entities: {len(summaries)}")
        lines.append(f"Source graph: {self.config.graph_dir}")
        lines.append("")
        for entry in summaries:
            lines.append(f"## {entry['canonical_id']}")
            lines.append(str(entry["summary_text"]))
            lines.append("")
            motif_items = cast(list[dict[str, Any]], entry["top_motifs"])
            motif_labels = ", ".join(
                [
                    f"{item['label']} ({item['hit_count']})"
                    for item in motif_items
                ]
            )
            lines.append(f"Top motifs: {motif_labels}")
            if entry["quotes"]:
                lines.append("Quotes:")
                quote_items = cast(list[dict[str, Any]], entry["quotes"])
                for quote in quote_items:
                    motif_list = ", ".join(
                        cast(list[str], quote["motifs"])
                    )
                    lines.append(f"- {quote['text']} ({motif_list})")
            lines.append("")
        return "\n".join(lines)
