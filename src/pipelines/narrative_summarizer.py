"""Narrative summarizer for distilling NPC motif graphs into briefs."""

from __future__ import annotations

import json
import logging
import re
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pandas as pd
from corpus.community_schema import MotifTaxonomy, load_motif_taxonomy

from pipelines.llm.base import (
    LLMConfig,
    LLMResponseError,
    resolve_llm_config,
)
from pipelines.llm.openai_client import build_summary_request_body
from pipelines.npc_motif_graph import (
    ENTITY_MOTIF_FILENAME,
    ENTITY_SUMMARY_FILENAME,
    LORE_HITS_FILENAME,
)

LOGGER = logging.getLogger(__name__)

SUMMARY_JSON = "npc_narrative_summaries.json"
SUMMARY_PARQUET = "npc_narrative_summaries.parquet"
SUMMARY_MARKDOWN = "npc_narrative_summaries.md"


_QUOTE_ID_PATTERN = re.compile(r"lore_id\s*(?:=|:)\s*([A-Za-z0-9_:-]+)")


@dataclass(slots=True)
class NarrativeSummariesConfig:
    """Configuration for transforming graph artifacts into summaries."""

    graph_dir: Path = Path("data/analysis/npc_motif_graph")
    output_dir: Path = Path("data/analysis/narrative_summaries")
    taxonomy_path: Path | None = None
    max_motifs: int = 4
    max_quotes: int = 3
    use_llm: bool = True
    batch_input_path: Path | None = None
    batch_output_path: Path | None = None
    llm_provider: str | None = None
    llm_model: str | None = None
    llm_reasoning: str | None = None
    llm_max_output_tokens: int | None = None


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
        self._llm_config: LLMConfig | None = None
        if self.config.use_llm:
            self._llm_config = resolve_llm_config(
                provider_override=self.config.llm_provider,
                model_override=self.config.llm_model,
                reasoning_override=self.config.llm_reasoning,
                max_output_override=self.config.llm_max_output_tokens,
            )

    @property
    def batch_input_path(self) -> Path:
        if self.config.batch_input_path is not None:
            return self.config.batch_input_path
        return self.config.output_dir / "batch_input.jsonl"

    @property
    def batch_output_path(self) -> Path:
        if self.config.batch_output_path is not None:
            return self.config.batch_output_path
        return self.config.output_dir / "batch_output.jsonl"

    def _require_llm_config(self) -> LLMConfig:
        if self._llm_config is None:
            self._llm_config = resolve_llm_config(
                provider_override=self.config.llm_provider,
                model_override=self.config.llm_model,
                reasoning_override=self.config.llm_reasoning,
                max_output_override=self.config.llm_max_output_tokens,
            )
        return self._llm_config

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

        batch_results: dict[str, Mapping[str, Any]] | None = None
        if self.config.use_llm:
            self._require_llm_config()
            batch_results = self._load_batch_results()

        summaries = self._build_summaries(
            entity_summary,
            entity_motif,
            lore_hits,
            batch_results=batch_results,
        )
        if not summaries:
            raise RuntimeError("No narrative summaries were produced")

        artifacts = self._write_artifacts(summaries)
        LOGGER.info(
            "Narrative summaries written to %s", self.config.output_dir
        )
        return artifacts

    def build_batch_file(
        self,
        *,
        destination: Path | None = None,
        llm_config: LLMConfig | None = None,
    ) -> Path:
        """Render the JSONL payload for the OpenAI batch API."""

        if not self.config.use_llm:
            raise RuntimeError(
                "LLM mode is disabled; enable --use-llm to build a batch "
                "payload"
            )

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
        llm_config = llm_config or self._require_llm_config()
        batch_path = destination or self.batch_input_path
        batch_path.parent.mkdir(parents=True, exist_ok=True)

        with batch_path.open("w", encoding="utf-8") as handle:
            for (
                canonical_id,
                summary_row,
                top_motifs,
                quotes,
            ) in self._iter_entity_contexts(
                entity_summary,
                entity_motif,
                lore_hits,
            ):
                payload = self._build_llm_payload(
                    canonical_id,
                    summary_row,
                    top_motifs,
                    quotes,
                )
                request_body = build_summary_request_body(
                    llm_config,
                    payload,
                )
                record = {
                    "custom_id": canonical_id,
                    "method": "POST",
                    "url": "/v1/responses",
                    "body": request_body,
                }
                handle.write(json.dumps(record, ensure_ascii=False))
                handle.write("\n")

        LOGGER.info("Batch payload written to %s", batch_path)
        return batch_path

    def _read_parquet(self, name: str, label: str) -> pd.DataFrame:
        path = self.config.graph_dir / name
        if not path.exists():
            raise FileNotFoundError(
                f"Missing {label} artifact at {path}. "
                "Run 'corpus analysis graph' first."
            )
        return pd.read_parquet(path)

    def _iter_entity_contexts(
        self,
        entity_summary: pd.DataFrame,
        entity_motif: pd.DataFrame,
        lore_hits: pd.DataFrame,
    ) -> list[
        tuple[
            str,
            pd.Series,
            list[dict[str, Any]],
            list[dict[str, Any]],
        ]
    ]:
        grouped_hits = lore_hits.groupby("canonical_id")
        grouped_motifs = entity_motif.groupby("canonical_id")

        contexts: list[
            tuple[str, pd.Series, list[dict[str, Any]], list[dict[str, Any]]]
        ] = []
        for _, row in entity_summary.iterrows():
            canonical_id = str(row["canonical_id"])
            if canonical_id not in grouped_motifs.groups:
                continue
            motif_rows = grouped_motifs.get_group(canonical_id)
            if motif_rows.empty:
                continue
            top_motifs = self._top_motifs(motif_rows)
            quotes = self._quotes_for_entity(canonical_id, grouped_hits)
            contexts.append((canonical_id, row, top_motifs, quotes))
        return contexts

    def _load_batch_results(self) -> dict[str, Mapping[str, Any]]:
        path = self.batch_output_path
        if not path.exists():
            raise FileNotFoundError(
                f"Batch output missing at {path}. "
                "Run 'corpus analysis summaries-batch' to generate it."
            )

        results: dict[str, Mapping[str, Any]] = {}
        total_records = 0
        failures = 0
        failure_messages: Counter[str] = Counter()
        with path.open("r", encoding="utf-8") as handle:
            for line_no, raw_line in enumerate(handle, 1):
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    LOGGER.warning(
                        "Skipping invalid batch line %s (%s)",
                        line_no,
                        exc,
                    )
                    continue
                total_records += 1
                custom_id = str(record.get("custom_id") or "").strip()
                if not custom_id:
                    LOGGER.warning(
                        "Batch line %s missing custom_id; skipping",
                        line_no,
                    )
                    continue
                response_payload = record.get("response")
                error_detail = record.get("error")
                status_code = self._extract_batch_status_code(response_payload)
                if error_detail or (
                    status_code is not None and status_code >= 400
                ):
                    message = self._format_batch_error(
                        error_detail,
                        status_code,
                    )
                    failure_messages[message] += 1
                    failures += 1
                    LOGGER.warning(
                        "Batch entry for %s failed: %s",
                        custom_id,
                        message,
                    )
                    continue
                text_payload = self._extract_batch_response_text(
                    response_payload
                )
                if not text_payload:
                    LOGGER.warning(
                        "Batch entry for %s did not include response text",
                        custom_id,
                    )
                    continue
                try:
                    parsed = json.loads(text_payload)
                except json.JSONDecodeError as exc:
                    LOGGER.warning(
                        "Batch entry for %s produced invalid JSON: %s",
                        custom_id,
                        exc,
                    )
                    continue
                results[custom_id] = parsed

        if total_records:
            LOGGER.info(
                "Batch output %s processed %s records "
                "(%s successes, %s failures)",
                path,
                total_records,
                total_records - failures,
                failures,
            )
        if failures:
            top_errors = ", ".join(
                f"{msg} (x{count})"
                for msg, count in failure_messages.most_common(2)
            )
            LOGGER.warning(
                "Batch output %s had %s failures: %s",
                path,
                failures,
                top_errors,
            )
        if total_records and failures == total_records:
            sample_error = (
                next(iter(failure_messages))
                if failure_messages
                else "unknown error"
            )
            LOGGER.error(
                "Batch output %s failed completely (%s/%s errors). Example "
                "error: %s",
                path,
                failures,
                total_records,
                sample_error,
            )
        if not results:
            LOGGER.warning(
                "Batch output %s contained no usable summaries", path
            )
        return results

    def _extract_batch_response_text(
        self,
        response: Mapping[str, Any] | None,
    ) -> str | None:
        if response is None:
            return None
        output = response.get("output") or []
        chunks: list[str] = []
        for item in output:
            content = item.get("content") or []
            for piece in content:
                if isinstance(piece, dict):
                    text = piece.get("text")
                    if isinstance(text, str):
                        chunks.append(text)
        if chunks:
            return "".join(chunks)
        body = response.get("body")
        if isinstance(body, dict):
            choices = body.get("choices") or []
            for choice in choices:
                message = choice.get("message") or {}
                content = message.get("content") or []
                for piece in content:
                    if isinstance(piece, dict):
                        text = piece.get("text")
                        if isinstance(text, str):
                            chunks.append(text)
            if chunks:
                return "".join(chunks)
        return None

    def _extract_batch_status_code(
        self,
        response: Mapping[str, Any] | None,
    ) -> int | None:
        if not isinstance(response, Mapping):
            return None
        status_code = response.get("status_code")
        if status_code is None:
            return None
        try:
            return int(status_code)
        except (TypeError, ValueError):
            return None

    def _format_batch_error(
        self,
        error_detail: Any,
        status_code: int | None,
    ) -> str:
        if error_detail is not None:
            if isinstance(error_detail, Mapping):
                message = error_detail.get("message")
                if isinstance(message, str):
                    return message
                return json.dumps(
                    error_detail,
                    ensure_ascii=False,
                    sort_keys=True,
                )
            if isinstance(error_detail, list | tuple):
                return json.dumps(error_detail, ensure_ascii=False)
            return str(error_detail)
        if status_code is not None:
            return f"status_code={status_code}"
        return "unknown error"

    def _build_summaries(
        self,
        entity_summary: pd.DataFrame,
        entity_motif: pd.DataFrame,
        lore_hits: pd.DataFrame,
        batch_results: dict[str, Mapping[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        summaries: list[dict[str, Any]] = []
        missing_batch: list[str] = []
        contexts = self._iter_entity_contexts(
            entity_summary,
            entity_motif,
            lore_hits,
        )

        for canonical_id, row, top_motifs, quotes in contexts:
            entry = None
            if batch_results is not None:
                entry = batch_results.get(canonical_id)
                if entry is None:
                    missing_batch.append(canonical_id)
            summaries.append(
                self._summarize_entity(
                    canonical_id,
                    row,
                    top_motifs,
                    quotes,
                    batch_entry=entry,
                )
            )

        if missing_batch:
            preview = ", ".join(missing_batch[:5])
            if len(missing_batch) > 5:
                preview += ", …"
            LOGGER.warning(
                "Missing %s batch summaries; falling back to heuristics for "
                "%s",
                len(missing_batch),
                preview,
            )
        return summaries

    def _summarize_entity(
        self,
        canonical_id: str,
        summary_row: pd.Series,
        top_motifs: list[dict[str, Any]],
        quotes: list[dict[str, Any]],
        batch_entry: Mapping[str, Any] | None = None,
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
            str(item.get("slug")) for item in top_motifs if item.get("slug")
        ]
        supporting_quotes = [
            str(quote.get("lore_id"))
            for quote in quotes
            if quote.get("lore_id")
        ]
        llm_used = False
        llm_provider: str | None = None
        llm_model: str | None = None
        if self.config.use_llm:
            llm_config = self._require_llm_config()
            llm_provider = self.config.llm_provider or llm_config.provider
            llm_model = self.config.llm_model or llm_config.model
            if batch_entry:
                try:
                    llm_result = self._normalize_llm_response(
                        canonical_id,
                        batch_entry,
                        quotes,
                    )
                except LLMResponseError as exc:
                    LOGGER.warning(
                        "Batch summary invalid for %s: %s",
                        canonical_id,
                        exc,
                    )
                else:
                    summary_text = llm_result["summary_text"]
                    if llm_result["motif_slugs"]:
                        summary_motif_slugs = llm_result["motif_slugs"]
                    supporting_quotes = llm_result["supporting_quotes"]
                    llm_used = True
            else:
                LOGGER.debug(
                    "No batch summary found for %s; using heuristic output",
                    canonical_id,
                )

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
            raise LLMResponseError(f"LLM field '{field_name}' must be a list")
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
        text_index: dict[str, str] = {}
        for quote in quotes:
            lore_id = quote.get("lore_id")
            text = quote.get("text")
            if not lore_id or not text:
                continue
            normalized_text = self._normalize_quote_text(str(text))
            text_index.setdefault(normalized_text, str(lore_id))
        if not available and supporting_quotes:
            raise LLMResponseError(
                f"LLM referenced quotes for {canonical_id} but none exist"
            )
        normalized: list[str] = []
        missing: list[str] = []
        for quote_id in supporting_quotes:
            resolved = self._normalize_quote_reference(
                quote_id,
                available,
                text_index,
            )
            if resolved in available:
                normalized.append(resolved)
            else:
                missing.append(quote_id)
        if missing:
            if normalized:
                LOGGER.warning(
                    "LLM referenced unknown quote IDs %s for %s; "
                    "dropping unmatched references",
                    missing,
                    canonical_id,
                )
                return normalized
            raise LLMResponseError(
                "LLM referenced unknown quote IDs "
                f"{missing} for {canonical_id}"
            )
        return normalized

    def _normalize_quote_reference(
        self,
        value: str,
        available: set[str],
        text_index: dict[str, str],
    ) -> str:
        trimmed = value.strip()
        if trimmed in available:
            return trimmed
        match = _QUOTE_ID_PATTERN.search(trimmed)
        if match:
            candidate = match.group(1)
            if candidate in available:
                return candidate
        normalized_text = self._normalize_quote_text(trimmed)
        resolved = text_index.get(normalized_text)
        if resolved:
            return resolved
        for text_value, quote_id in text_index.items():
            if text_value and (
                text_value in normalized_text or normalized_text in text_value
            ):
                return quote_id
        return trimmed

    def _normalize_quote_text(self, value: str) -> str:
        lowered = value.lower()
        collapsed = " ".join(lowered.split())
        return collapsed.strip()

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
                    motif_list = ", ".join(cast(list[str], quote["motifs"]))
                    lines.append(f"- {quote['text']} ({motif_list})")
            lines.append("")
        return "\n".join(lines)
