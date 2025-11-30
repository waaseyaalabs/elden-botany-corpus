"""Narrative summarizer for distilling NPC motif graphs into briefs."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, cast

import pandas as pd
import yaml
from corpus.community_schema import MotifTaxonomy, load_motif_taxonomy

from pipelines.aliasing import (
    DEFAULT_ALIAS_TABLE,
    AliasResolver,
    load_alias_map,
)
from pipelines.llm.base import (
    LLMConfig,
    LLMResponseError,
    resolve_llm_config,
)
from pipelines.llm.batch_utils import (
    BatchDiagnostics,
    extract_response_text,
    extract_status_code,
    format_batch_error,
)
from pipelines.llm.openai_client import (
    OpenAILLMClient,
    build_summary_request_body,
)
from pipelines.motif_taxonomy_utils import MotifMetadata, motif_lookup
from pipelines.npc_motif_graph import (
    ENTITY_MOTIF_FILENAME,
    ENTITY_SUMMARY_FILENAME,
    LORE_ENTRIES_FILENAME,
    LORE_HITS_FILENAME,
)
from pipelines.speech_motifs import SPEECH_MOTIF_HITS_FILENAME

LOGGER = logging.getLogger(__name__)

SUMMARY_JSON = "npc_narrative_summaries.json"
SUMMARY_PARQUET = "npc_narrative_summaries.parquet"
SUMMARY_MARKDOWN = "npc_narrative_summaries.md"
DEFAULT_MOTIF_OVERRIDE_PATH = Path("data/reference/npc_motif_overrides.yaml")


_QUOTE_ID_PATTERN = re.compile(r"lore_id\s*(?:=|:)\s*([A-Za-z0-9_:-]+)")
LLMMode = Literal["batch", "per-entity", "heuristic"]
_METADATA_LEAK_TOKENS = ("lore_count=", "motif_mentions=", "unique_motifs=")
_DISALLOWED_TEXT_TYPES = frozenset({"ambient", "system"})


@dataclass(slots=True)
class NarrativeSummariesConfig:
    """Configuration for transforming graph artifacts into summaries."""

    graph_dir: Path = Path("data/analysis/npc_motif_graph")
    output_dir: Path = Path("data/analysis/narrative_summaries")
    taxonomy_path: Path | None = None
    max_motifs: int = 4
    max_quotes: int = 3
    llm_mode: LLMMode = "batch"
    batch_input_path: Path | None = None
    batch_output_path: Path | None = None
    llm_provider: str | None = None
    llm_model: str | None = None
    llm_reasoning: str | None = None
    llm_max_output_tokens: int | None = None
    alias_table_path: Path | None = DEFAULT_ALIAS_TABLE
    min_motif_unique_lore: int = 2
    motif_override_path: Path | None = DEFAULT_MOTIF_OVERRIDE_PATH
    codex_mode: bool = False
    speech_motif_dir: Path | None = Path("data/analysis/llm_motifs")
    speech_motif_hits_path: Path | None = None
    use_speech_motifs: bool = True


@dataclass(slots=True)
class NarrativeSummaryArtifacts:
    """Paths to generated summary artifacts."""

    summaries_json: Path
    summaries_parquet: Path
    markdown_path: Path


@dataclass(slots=True)
class MotifOverride:
    """Per-entity motif override instructions."""

    block: frozenset[str] = field(default_factory=frozenset)


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
        self._motif_lookup = motif_lookup(self._taxonomy)
        self._alias_map: AliasResolver = load_alias_map(
            self.config.alias_table_path
        )
        self._motif_overrides = self._load_motif_overrides(
            self.config.motif_override_path
        )
        self._llm_config: LLMConfig | None = None
        self._llm_client: OpenAILLMClient | None = None
        self._batch_diagnostics: BatchDiagnostics | None = None
        if self.config.llm_mode != "heuristic":
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

    @property
    def batch_diagnostics(self) -> BatchDiagnostics | None:
        return self._batch_diagnostics

    def _require_llm_config(self) -> LLMConfig:
        if self._llm_config is None:
            self._llm_config = resolve_llm_config(
                provider_override=self.config.llm_provider,
                model_override=self.config.llm_model,
                reasoning_override=self.config.llm_reasoning,
                max_output_override=self.config.llm_max_output_tokens,
            )
        return self._llm_config

    def _require_llm_client(self) -> OpenAILLMClient:
        if self._llm_client is None:
            config = self._require_llm_config()
            self._llm_client = OpenAILLMClient(
                config=config,
                codex_mode=self.config.codex_mode,
            )
        return self._llm_client

    def _canonicalize_id(self, value: str) -> str:
        if not value:
            return value
        return self._alias_map.resolve(value)

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
        lore_entries = self._read_parquet(
            LORE_ENTRIES_FILENAME,
            "entity lore entries",
        )

        speech_hits = self._load_speech_hits()
        if speech_hits is not None and not speech_hits.empty:
            LOGGER.info(
                "Using speech-level motif hits (%s rows)",
                len(speech_hits),
            )
            lore_hits = speech_hits
            entity_motif = self._speech_entity_motif_stats(speech_hits)
            entity_summary = self._speech_summary_overrides(
                entity_summary,
                speech_hits,
            )

        batch_results: dict[str, Mapping[str, Any]] | None = None
        if self.config.llm_mode == "batch":
            self._require_llm_config()
            try:
                batch_results = self._load_batch_results()
            except FileNotFoundError:
                LOGGER.warning(
                    "Batch output missing at %s; falling back to heuristic "
                    "summaries",
                    self.batch_output_path,
                )
        elif self.config.llm_mode == "per-entity":
            self._require_llm_config()

        summaries = self._build_summaries(
            entity_summary,
            entity_motif,
            lore_hits,
            lore_entries,
            batch_results=batch_results,
        )
        if not summaries:
            raise RuntimeError("No narrative summaries were produced")
        self._validate_coverage(entity_summary, summaries)

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
        only_ids: set[str] | None = None,
    ) -> Path:
        """Render the JSONL payload for the OpenAI batch API."""

        if self.config.llm_mode == "heuristic":
            raise RuntimeError(
                "LLM mode is disabled; rerun with --llm-mode batch or "
                "per-entity to build a payload"
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
        lore_entries = self._read_parquet(
            LORE_ENTRIES_FILENAME,
            "entity lore entries",
        )
        motif_text_types = self._motif_text_types(lore_hits)
        llm_config = llm_config or self._require_llm_config()
        batch_path = destination or self.batch_input_path
        batch_path.parent.mkdir(parents=True, exist_ok=True)

        allowlist = {str(item) for item in only_ids} if only_ids else None
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
                lore_entries,
                motif_text_types,
            ):
                if allowlist is not None and canonical_id not in allowlist:
                    continue
                payload = self._build_llm_payload(
                    canonical_id,
                    summary_row,
                    top_motifs,
                    quotes,
                )
                request_body = build_summary_request_body(
                    llm_config,
                    payload,
                    codex_mode=self.config.codex_mode,
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
        lore_entries: pd.DataFrame,
        motif_text_types: Mapping[tuple[str, str], set[str]],
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
        grouped_lore_entries = lore_entries.groupby("canonical_id")
        empty_motif_rows = entity_motif.head(0).copy()

        contexts: list[
            tuple[str, pd.Series, list[dict[str, Any]], list[dict[str, Any]]]
        ] = []
        for _, row in entity_summary.iterrows():
            canonical_id = str(row["canonical_id"])
            motif_rows = (
                grouped_motifs.get_group(canonical_id)
                if canonical_id in grouped_motifs.groups
                else empty_motif_rows
            )
            top_motifs = self._top_motifs(
                canonical_id,
                motif_rows,
                motif_text_types,
            )
            quotes = self._quotes_for_entity(
                canonical_id,
                grouped_hits,
                grouped_lore_entries,
            )
            contexts.append((canonical_id, row, top_motifs, quotes))
        return contexts

    def _load_batch_results(
        self,
        path: Path | None = None,
    ) -> dict[str, Mapping[str, Any]]:
        target_path = path or self.batch_output_path
        if not target_path.exists():
            raise FileNotFoundError(
                f"Batch output missing at {target_path}. "
                "Run 'corpus analysis summaries-batch' to generate it."
            )

        diagnostics = BatchDiagnostics(path=target_path)
        results: dict[str, Mapping[str, Any]] = {}
        with target_path.open("r", encoding="utf-8") as handle:
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
                diagnostics.total_records += 1
                custom_id = str(record.get("custom_id") or "").strip()
                if not custom_id:
                    LOGGER.warning(
                        "Batch line %s missing custom_id; skipping",
                        line_no,
                    )
                    continue
                response_payload = record.get("response")
                error_detail = record.get("error")
                status_code = extract_status_code(response_payload)
                if error_detail or (
                    status_code is not None and status_code >= 400
                ):
                    message = format_batch_error(error_detail, status_code)
                    diagnostics.failure_messages[message] += 1
                    diagnostics.failed_ids.append(custom_id)
                    LOGGER.warning(
                        "Batch entry for %s failed: %s",
                        custom_id,
                        message,
                    )
                    continue
                text_payload = extract_response_text(response_payload)
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

        diagnostics.successes = len(results)
        diagnostics.failures = (
            diagnostics.total_records - diagnostics.successes
        )
        self._batch_diagnostics = diagnostics

        if diagnostics.total_records:
            LOGGER.info(
                "Batch output %s processed %s records "
                "(%s successes, %s failures)",
                target_path,
                diagnostics.total_records,
                diagnostics.successes,
                diagnostics.failures,
            )
        if diagnostics.failures:
            top_errors = ", ".join(
                f"{msg} (x{count})"
                for msg, count in diagnostics.failure_messages.most_common(2)
            )
            LOGGER.warning(
                "Batch output %s had %s failures: %s",
                target_path,
                diagnostics.failures,
                top_errors,
            )
        if (
            diagnostics.total_records
            and diagnostics.failures == diagnostics.total_records
        ):
            sample_error = (
                next(iter(diagnostics.failure_messages))
                if diagnostics.failure_messages
                else "unknown error"
            )
            LOGGER.error(
                "Batch output %s failed completely (%s/%s errors). Example "
                "error: %s",
                target_path,
                diagnostics.failures,
                diagnostics.total_records,
                sample_error,
            )
        if not results:
            LOGGER.warning(
                "Batch output %s contained no usable summaries", target_path
            )
        return results

    def _load_motif_overrides(
        self,
        path: Path | None,
    ) -> dict[str, MotifOverride]:
        if path is None:
            return {}
        if not path.exists():
            LOGGER.info(
                "Motif override table %s missing; continuing without"
                " overrides",
                path,
            )
            return {}
        try:
            with path.open("r", encoding="utf-8") as handle:
                payload = yaml.safe_load(handle) or {}
        except yaml.YAMLError as exc:  # pragma: no cover - unlikely
            raise ValueError(
                f"Failed to parse motif override table at {path}: {exc}"
            ) from exc
        if not isinstance(payload, Mapping):
            raise ValueError(
                "Motif override table must map canonical_id values to rules"
            )
        overrides: dict[str, MotifOverride] = {}
        for raw_canonical_id, raw_spec in payload.items():
            canonical_id = str(raw_canonical_id or "").strip()
            if not canonical_id:
                continue
            block_values: list[str] = []
            if isinstance(raw_spec, Mapping):
                candidates = raw_spec.get("block", [])
                if isinstance(candidates, list):
                    block_values = [
                        str(value).strip()
                        for value in candidates
                        if str(value or "").strip()
                    ]
            overrides[canonical_id] = MotifOverride(
                block=frozenset(block_values)
            )
        if overrides:
            LOGGER.info(
                "Loaded %s motif override entries from %s",
                len(overrides),
                path,
            )
        return overrides

    def _blocked_motifs(self, canonical_id: str) -> frozenset[str]:
        override = self._motif_overrides.get(canonical_id)
        if override is None:
            return frozenset()
        return override.block

    def _build_summaries(
        self,
        entity_summary: pd.DataFrame,
        entity_motif: pd.DataFrame,
        lore_hits: pd.DataFrame,
        lore_entries: pd.DataFrame,
        batch_results: dict[str, Mapping[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        summaries: list[dict[str, Any]] = []
        missing_batch: list[str] = []
        motif_text_types = self._motif_text_types(lore_hits)
        contexts = self._iter_entity_contexts(
            entity_summary,
            entity_motif,
            lore_hits,
            lore_entries,
            motif_text_types,
        )

        for canonical_id, row, top_motifs, quotes in contexts:
            entry = None
            if self.config.llm_mode == "batch" and batch_results is not None:
                entry = batch_results.get(canonical_id)
                if entry is None:
                    missing_batch.append(canonical_id)
            elif self.config.llm_mode == "per-entity":
                entry = self._summarize_with_llm_client(
                    canonical_id,
                    row,
                    top_motifs,
                    quotes,
                )
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
            quotes,
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
        if self.config.llm_mode != "heuristic":
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
            elif self.config.llm_mode == "batch":
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

    def _summarize_with_llm_client(
        self,
        canonical_id: str,
        summary_row: pd.Series,
        top_motifs: list[dict[str, Any]],
        quotes: list[dict[str, Any]],
    ) -> Mapping[str, Any] | None:
        payload = self._build_llm_payload(
            canonical_id,
            summary_row,
            top_motifs,
            quotes,
        )
        client = self._require_llm_client()
        try:
            return client.summarize_entity(payload)
        except LLMResponseError as exc:
            LOGGER.warning(
                "Per-entity LLM call failed for %s: %s",
                canonical_id,
                exc,
            )
            return None

    def _normalize_llm_response(
        self,
        canonical_id: str,
        response: Mapping[str, Any],
        quotes: list[dict[str, Any]],
    ) -> dict[str, Any]:
        response_id = self._canonicalize_id(
            str(response.get("canonical_id", "")).strip()
        )
        expected_id = self._canonicalize_id(canonical_id)
        if response_id and response_id != expected_id:
            raise LLMResponseError(
                f"LLM response canonical_id mismatch for {canonical_id}"
            )

        summary_text = str(response.get("summary_text", "")).strip()
        if not summary_text:
            raise LLMResponseError("LLM response missing summary_text")
        if self._looks_truncated(summary_text):
            raise LLMResponseError(
                "LLM response summary_text appears truncated ('...')"
            )
        if self._contains_metadata_leak(summary_text):
            raise LLMResponseError(
                "LLM response summary_text leaked prompt metadata"
            )

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

    def _looks_truncated(self, summary_text: str) -> bool:
        """Return True when the LLM summary looks truncated."""

        return "..." in summary_text

    def _contains_metadata_leak(self, summary_text: str) -> bool:
        lowered = summary_text.lower()
        return any(token in lowered for token in _METADATA_LEAK_TOKENS)

    def _ensure_str_list(
        self,
        value: Any,
        field_name: str,
    ) -> list[str]:
        if value is None:
            return []
        if not isinstance(value, list):
            raise LLMResponseError(f"LLM field '{field_name}' must be a list")
        typed_list = value
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

    def _top_motifs(
        self,
        canonical_id: str,
        motif_rows: pd.DataFrame,
        motif_text_types: Mapping[tuple[str, str], set[str]],
    ) -> list[dict[str, Any]]:
        ranked = motif_rows.copy()
        ranked["unique_lore"] = ranked["unique_lore"].astype(int)
        min_unique = max(1, int(self.config.min_motif_unique_lore))
        ranked = ranked.loc[ranked["unique_lore"] >= min_unique]
        blocked = self._blocked_motifs(canonical_id)
        if blocked:
            ranked = ranked.loc[~ranked["motif_slug"].isin(blocked)]
        if ranked.empty:
            return []
        allowed_mask = ranked["motif_slug"].astype(str).apply(
            lambda slug: self._motif_has_allowed_text_type(
                canonical_id,
                slug,
                motif_text_types,
            )
        )
        ranked = ranked.loc[allowed_mask]
        if ranked.empty:
            return []
        order_series = ranked["motif_slug"].map(self._motif_order)
        filled = order_series.fillna(float(len(self._motif_order)))
        ranked.loc[:, "__rank"] = filled
        ordered = ranked.sort_values(
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
        grouped_lore_entries: Any,
    ) -> list[dict[str, Any]]:
        quotes: list[dict[str, Any]] = []
        if canonical_id in grouped_hits.groups:
            subset = grouped_hits.get_group(canonical_id)
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
                    return quotes
        if quotes:
            return quotes
        if canonical_id not in grouped_lore_entries.groups:
            return []
        subset = grouped_lore_entries.get_group(canonical_id)
        deduped = subset.drop_duplicates("lore_id")
        for _, row in deduped.head(self.config.max_quotes).iterrows():
            quotes.append(
                {
                    "lore_id": row.get("lore_id"),
                    "text": row.get("text"),
                    "motifs": [],
                }
            )
        return quotes

    def _load_speech_hits(self) -> pd.DataFrame | None:
        if not self.config.use_speech_motifs:
            return None
        target = self.config.speech_motif_hits_path
        if target is None:
            base = self.config.speech_motif_dir
            if base is None:
                return None
            target = base / SPEECH_MOTIF_HITS_FILENAME
        if target is None or not target.exists():
            return None
        frame = pd.read_parquet(target)
        required = {
            "canonical_id",
            "motif_slug",
            "lore_id",
            "text",
            "text_type",
            "motif_label",
        }
        missing = required - set(frame.columns)
        if missing:
            LOGGER.warning(
                "Speech motif hits at %s missing columns %s; skipping",
                target,
                ", ".join(sorted(missing)),
            )
            return None
        normalized = frame.copy()
        normalized["canonical_id"] = normalized["canonical_id"].astype(str)
        normalized["motif_slug"] = normalized["motif_slug"].astype(str)
        normalized["text_type"] = normalized["text_type"].astype(str)
        normalized["motif_label"] = normalized["motif_label"].astype(str)
        return normalized

    def _speech_entity_motif_stats(
        self,
        hits: pd.DataFrame,
    ) -> pd.DataFrame:
        if hits.empty:
            return pd.DataFrame(
                columns=[
                    "canonical_id",
                    "motif_slug",
                    "hit_count",
                    "unique_lore",
                    "motif_label",
                    "motif_category",
                ]
            )
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
            lambda slug: self._motif_metadata(slug).label
        )
        grouped["motif_category"] = grouped["motif_slug"].map(
            lambda slug: self._motif_metadata(slug).category
        )
        return grouped

    def _speech_summary_overrides(
        self,
        entity_summary: pd.DataFrame,
        hits: pd.DataFrame,
    ) -> pd.DataFrame:
        if hits.empty:
            return entity_summary
        overrides = (
            hits.groupby("canonical_id")
            .agg(
                motif_mentions=("motif_slug", "count"),
                unique_motifs=("motif_slug", "nunique"),
            )
            .reset_index()
        )
        merged = entity_summary.merge(
            overrides,
            on="canonical_id",
            how="left",
            suffixes=("", "_speech"),
        )
        for column in ("motif_mentions", "unique_motifs"):
            speech_col = f"{column}_speech"
            merged[column] = (
                merged[speech_col]
                .fillna(merged[column])
                .fillna(0)
                .astype(int)
            )
            merged.drop(columns=[speech_col], inplace=True)
        return merged

    def _motif_text_types(
        self,
        lore_hits: pd.DataFrame,
    ) -> dict[tuple[str, str], set[str]]:
        index: dict[tuple[str, str], set[str]] = {}
        if lore_hits.empty:
            return index
        for _, row in lore_hits.iterrows():
            canonical_id = str(row.get("canonical_id", "")).strip()
            motif_slug = str(row.get("motif_slug", "")).strip()
            if not canonical_id or not motif_slug:
                continue
            text_type = str(row.get("text_type", "")).strip().lower()
            if not text_type:
                continue
            key = (canonical_id, motif_slug)
            bucket = index.setdefault(key, set())
            bucket.add(text_type)
        return index

    def _motif_has_allowed_text_type(
        self,
        canonical_id: str,
        motif_slug: str,
        motif_text_types: Mapping[tuple[str, str], set[str]],
    ) -> bool:
        text_types = motif_text_types.get((canonical_id, motif_slug))
        if not text_types:
            return True
        return any(
            text_type not in _DISALLOWED_TEXT_TYPES for text_type in text_types
        )

    def _compose_summary_text(
        self,
        canonical_id: str,
        summary_row: pd.Series,
        top_motifs: list[dict[str, Any]],
        quotes: list[dict[str, Any]],
    ) -> str:
        motif_labels = [
            str(item.get("label", ""))
            for item in top_motifs
            if item.get("label")
        ]
        motif_phrase = self._format_list(motif_labels)
        lore_count = int(summary_row.get("lore_count", 0))
        if self.config.codex_mode:
            return self._compose_codex_summary(
                canonical_id,
                lore_count,
                motif_phrase,
                quotes,
            )
        if not motif_phrase:
            return self._compose_quote_summary(
                canonical_id,
                lore_count,
                quotes,
            )
        return (
            f"{canonical_id} leans on {motif_phrase} across {lore_count} "
            "lore lines."
        )

    def _compose_quote_summary(
        self,
        canonical_id: str,
        lore_count: int,
        quotes: list[dict[str, Any]],
    ) -> str:
        excerpts = [
            self._quote_excerpt(str(quote.get("text", "")))
            for quote in quotes
            if quote.get("text")
        ]
        excerpts = [item for item in excerpts if item]
        if not excerpts:
            return (
                f"{canonical_id} surfaces {lore_count} lore lines but has "
                "no registered motifs yet."
            )
        sample = excerpts[:2]
        joined = "; ".join(f'"{item}"' for item in sample)
        if len(excerpts) > len(sample):
            joined += " …"
        return f"{canonical_id} speaks directly: {joined}"

    def _compose_codex_summary(
        self,
        canonical_id: str,
        lore_count: int,
        motif_phrase: str,
        quotes: list[dict[str, Any]],
    ) -> str:
        name = self._codex_name(canonical_id)
        clauses: list[str] = []
        if motif_phrase:
            clauses.append(
                f"{name} is etched into the codex amid {motif_phrase}"
            )
        else:
            clauses.append(
                f"{name} is etched into the codex by testimony alone"
            )
        if lore_count > 0:
            clauses.append(
                f"Their voice threads through {lore_count} sworn accounts"
            )
        excerpt = self._first_quote_excerpt(quotes)
        if excerpt:
            clauses.append(f'"{excerpt}"')
        sentence = ". ".join(clauses).rstrip(".")
        return sentence + "."

    def _codex_name(self, canonical_id: str) -> str:
        name = canonical_id.split(":", 1)[-1]
        cleaned = name.replace("_", " ").strip()
        return cleaned.title() if cleaned else canonical_id

    def _first_quote_excerpt(self, quotes: list[dict[str, Any]]) -> str:
        for quote in quotes:
            snippet = self._quote_excerpt(str(quote.get("text", "")))
            if snippet:
                return snippet
        return ""

    def _quote_excerpt(self, text: str) -> str:
        collapsed = " ".join(text.split()).strip()
        if not collapsed:
            return ""
        if len(collapsed) <= 160:
            return collapsed
        return collapsed[:157].rstrip() + "…"

    def _format_list(self, items: list[str]) -> str:
        cleaned = [item for item in items if item]
        if not cleaned:
            return ""
        if len(cleaned) == 1:
            return cleaned[0]
        return ", ".join(cleaned[:-1]) + f" and {cleaned[-1]}"

    def _validate_coverage(
        self,
        entity_summary: pd.DataFrame,
        summaries: list[dict[str, Any]],
    ) -> None:
        expected: set[str] = set()
        for _, row in entity_summary.iterrows():
            canonical_id = self._canonicalize_id(
                str(row.get("canonical_id", "")).strip()
            )
            if not canonical_id:
                continue
            category = str(row.get("category", "")).strip().lower()
            if category and category != "npc":
                continue
            try:
                lore_count = int(row.get("lore_count", 0))
            except (TypeError, ValueError):  # pragma: no cover - defensive
                lore_count = 0
            if lore_count <= 0:
                continue
            expected.add(canonical_id)

        observed: set[str] = set()
        for entry in summaries:
            raw_id = str(entry.get("canonical_id", "")).strip()
            if not raw_id:
                continue
            observed.add(self._canonicalize_id(raw_id))

        missing = sorted(expected - observed)
        if missing:
            preview = ", ".join(missing[:10])
            message = (
                "Missing narrative summaries for NPCs: "
                f"{preview}"
            )
            if len(missing) > 10:
                message += f" (total {len(missing)})"
            raise RuntimeError(message)

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

    def _motif_metadata(self, slug: str) -> MotifMetadata:
        metadata = self._motif_lookup.get(slug)
        if metadata is not None:
            return metadata
        return MotifMetadata(
            slug=slug,
            label=slug,
            category="unknown",
            description="",
        )

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
