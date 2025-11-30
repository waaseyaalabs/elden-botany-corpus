"""Speech-level motif detection powered by OpenAI or regex fallbacks."""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Literal, TextIO

import pandas as pd
from corpus.community_schema import MotifTaxonomy, load_motif_taxonomy
from corpus.config import settings

from pipelines.llm.base import LLMConfig, LLMResponseError, resolve_llm_config
from pipelines.llm.batch_utils import (
    BatchDiagnostics,
    extract_response_text,
    extract_status_code,
    format_batch_error,
)
from pipelines.llm.openai_client import OpenAILLMClient
from pipelines.motif_taxonomy_utils import (
    MotifMetadata,
    compile_motif_patterns,
    motif_lookup,
)

LOGGER = logging.getLogger(__name__)

SPEECHES_FILENAME = "speeches.parquet"
SPEECH_MOTIFS_FILENAME = "speech_motifs.parquet"
SPEECH_MOTIF_HITS_FILENAME = "speech_motif_hits.parquet"
PAYLOAD_CACHE_FILENAME = "speech_payloads.jsonl"
_DEFAULT_TEXT_TYPES = ("dialogue", "quote", "impalers_excerpt")
_SYSTEM_PROMPT = (
    "You are an Elden Ring archivist analysing NPC speeches. "
    "A literary motif is a repeated image, symbol, idea or phrase that supports "
    "a theme. You will be given a speech and a motif taxonomy. Do not invent "
    "motifs or characters. Pick at most three motifs from the taxonomy that clearly "
    "recur in the speech. A motif should be selected only when the speech repeats "
    "or emphasises concepts described in the motif's label or synonyms. Return JSON "
    "with the provided schema. If no motif applies, return an empty list."
)
_SPEECH_SCHEMA = {
    "type": "object",
    "properties": {
        "speech_id": {"type": "string"},
        "canonical_id": {"type": "string"},
        "motifs": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "slug": {"type": "string"},
                    "support_indices": {
                        "type": "array",
                        "items": {"type": "integer", "minimum": 0},
                    },
                },
                "required": ["slug", "support_indices"],
                "additionalProperties": False,
            },
            "additionalItems": False,
        },
    },
    "required": ["speech_id", "canonical_id", "motifs"],
    "additionalProperties": False,
}

SpeechLLMMode = Literal["sync", "batch"]


@dataclass(slots=True)
class SpeechMotifConfig:
    """Configuration for the speech-level motif detector."""
    curated_path: Path | None = None
    taxonomy_path: Path | None = None
    output_dir: Path = Path("data/analysis/llm_motifs")
    include_text_types: tuple[str, ...] = _DEFAULT_TEXT_TYPES
    speech_level: bool = True
    max_motifs: int = 3
    llm_provider: str | None = None
    llm_model: str | None = None
    llm_reasoning: str | None = None
    llm_max_output_tokens: int | None = None
    llm_mode: SpeechLLMMode = "sync"
    batch_input_path: Path | None = None
    batch_output_path: Path | None = None
    dry_run_llm: bool = False
    store_payloads: bool = True


@dataclass(slots=True)
class SpeechMotifArtifacts:
    """Artifact pointers emitted by the pipeline."""

    speeches_parquet: Path
    speech_motifs_parquet: Path
    speech_motif_hits_parquet: Path
    payload_cache: Path | None


@dataclass(slots=True)
class SpeechRecord:
    """In-memory representation of a grouped speech."""

    speech_id: str
    canonical_id: str
    speech_text: str
    lines: list[dict[str, Any]]
    context: dict[str, Any]


class SpeechMotifPipeline:
    """Group lore lines into speeches and classify motifs via LLM."""

    def __init__(
        self,
        config: SpeechMotifConfig | None = None,
        taxonomy: MotifTaxonomy | None = None,
    ) -> None:
        self.config = config or SpeechMotifConfig()
        self._taxonomy = taxonomy or load_motif_taxonomy(
            self.config.taxonomy_path
        )
        self._patterns = compile_motif_patterns(self._taxonomy)
        self._motif_lookup = motif_lookup(self._taxonomy)
        self._taxonomy_rows = [
            {
                "slug": metadata.slug,
                "label": metadata.label,
                "description": metadata.description,
            }
            for metadata in self._motif_lookup.values()
        ]
        self._llm_config: LLMConfig | None = None
        self._llm_client: OpenAILLMClient | None = None
        self._run_id = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%S")
        self._batch_diagnostics: BatchDiagnostics | None = None

    @property
    def batch_input_path(self) -> Path:
        if self.config.batch_input_path is not None:
            return self.config.batch_input_path
        return self.config.output_dir / "speech_batch_input.jsonl"

    @property
    def batch_output_path(self) -> Path:
        if self.config.batch_output_path is not None:
            return self.config.batch_output_path
        return self.config.output_dir / "speech_batch_output.jsonl"

    @property
    def batch_diagnostics(self) -> BatchDiagnostics | None:
        return self._batch_diagnostics

    def run(self) -> SpeechMotifArtifacts:
        speeches = self._build_speeches()
        if not speeches:
            raise RuntimeError("No speeches were assembled from the lore corpus")

        speeches_path = self._write_speeches(speeches)
        use_llm = self._llm_enabled()
        llm_mode = self.config.llm_mode
        use_sync_llm = use_llm and llm_mode == "sync"
        if use_sync_llm:
            try:
                self._require_llm_client()
            except RuntimeError as exc:
                LOGGER.warning("LLM disabled due to configuration error: %s", exc)
                use_sync_llm = False
        elif use_llm and llm_mode == "batch":
            try:
                self._require_llm_config()
            except RuntimeError as exc:
                LOGGER.warning("LLM disabled due to configuration error: %s", exc)
                use_llm = False

        if use_llm and llm_mode == "batch":
            assignments = self._assignments_from_batch(speeches)
        else:
            assignments = self._classify_speeches(speeches, use_sync_llm)
        motifs_path, hits_path = self._write_results(speeches, assignments)
        payload_path = self._payload_cache_path(use_sync_llm)

        return SpeechMotifArtifacts(
            speeches_parquet=speeches_path,
            speech_motifs_parquet=motifs_path,
            speech_motif_hits_parquet=hits_path,
            payload_cache=payload_path,
        )

    def _build_speeches(self) -> list[SpeechRecord]:
        frame = self._load_lore_frame()
        if self.config.include_text_types:
            frame = frame.loc[frame["text_type"].isin(self.config.include_text_types)]
        if not self.config.speech_level:
            return [self._solo_speech(row) for _, row in frame.iterrows()]
        grouped = self._group_into_speeches(frame)
        return [self._speech_from_group(key, rows) for key, rows in grouped.items()]

    def _load_lore_frame(self) -> pd.DataFrame:
        path = self.config.curated_path or (
            settings.curated_dir / "lore_corpus.parquet"
        )
        if not path.exists():
            raise FileNotFoundError(
                "Lore corpus parquet is missing. Run 'make build-corpus' or pass --curated."
            )
        frame = pd.read_parquet(path)
        required = {
            "lore_id",
            "canonical_id",
            "category",
            "text_type",
            "text",
            "source",
            "provenance",
        }
        missing = required - set(frame.columns)
        if missing:
            formatted = ", ".join(sorted(missing))
            raise ValueError(f"Lore corpus missing columns: {formatted}")
        filtered = frame.loc[frame["category"] == "npc"].copy()
        filtered["text"] = filtered["text"].astype(str).str.strip()
        filtered = filtered.loc[filtered["text"] != ""]
        filtered.sort_values(
            ["canonical_id", "source", "text_type", "lore_id"], inplace=True
        )
        filtered.reset_index(drop=True, inplace=True)
        return filtered

    def _solo_speech(self, row: pd.Series) -> SpeechRecord:
        line = self._line_payload(row, 0)
        context = self._context_payload(row, line["metadata"])
        lore_id = line["lore_id"].encode("utf-8")
        speech_id = (
            f"{row['canonical_id']}:"
            f"{hashlib.sha1(lore_id).hexdigest()[:10]}"
        )
        line.pop("metadata", None)
        return SpeechRecord(
            speech_id=speech_id,
            canonical_id=str(row["canonical_id"]),
            speech_text=str(row["text"]),
            lines=[line],
            context=context,
        )

    def _group_into_speeches(
        self,
        frame: pd.DataFrame,
    ) -> dict[tuple[str, str], list[pd.Series]]:
        groups: dict[tuple[str, str], list[pd.Series]] = {}
        for _, row in frame.iterrows():
            canonical_id = str(row["canonical_id"])
            provenance = self._provenance_dict(row.get("provenance"))
            signature = self._context_signature(canonical_id, row, provenance)
            key = (canonical_id, signature)
            bucket = groups.setdefault(key, [])
            bucket.append(row)
        return groups

    def _speech_from_group(
        self,
        key: tuple[str, str],
        rows: list[pd.Series],
    ) -> SpeechRecord:
        canonical_id, signature = key
        lines: list[dict[str, Any]] = []
        for index, row in enumerate(rows):
            lines.append(self._line_payload(row, index))
        text = "\n".join(line["text"] for line in lines if line["text"])
        context = self._context_payload(rows[0], lines[0]["metadata"])
        context["signature"] = signature
        for line in lines:
            line.pop("metadata", None)
        speech_id = f"{canonical_id}:{signature}:{len(lines):02d}"
        return SpeechRecord(
            speech_id=speech_id,
            canonical_id=canonical_id,
            speech_text=text,
            lines=lines,
            context=context,
        )

    def _line_payload(self, row: pd.Series, index: int) -> dict[str, Any]:
        provenance = self._provenance_dict(row.get("provenance"))
        return {
            "index": index,
            "lore_id": str(row.get("lore_id")),
            "text": str(row.get("text", "")),
            "text_type": str(row.get("text_type", "")),
            "metadata": provenance,
        }

    def _context_payload(
        self,
        row: pd.Series,
        provenance: dict[str, Any],
    ) -> dict[str, Any]:
        context = {
            "source": str(row.get("source", "")),
            "text_type": str(row.get("text_type", "")),
        }
        for key in ("talk_id", "location", "phase", "trigger", "speaker_slug"):
            value = provenance.get(key)
            if value not in (None, "", []):
                context[key] = value
        return context

    def _context_signature(
        self,
        canonical_id: str,
        row: pd.Series,
        provenance: dict[str, Any],
    ) -> str:
        tokens = [
            canonical_id,
            str(provenance.get("talk_id", "")),
            str(provenance.get("conversation_id", "")),
            str(provenance.get("location", "")),
            str(provenance.get("phase", "")),
            str(provenance.get("trigger", "")),
            str(row.get("source", "")),
        ]
        joined = "|".join(tokens)
        return hashlib.sha1(joined.encode("utf-8")).hexdigest()[:12]

    def _write_speeches(self, speeches: list[SpeechRecord]) -> Path:
        output_dir = self._ensure_output_dir()
        rows = [
            {
                "speech_id": speech.speech_id,
                "canonical_id": speech.canonical_id,
                "speech_text": speech.speech_text,
                "line_count": len(speech.lines),
                "context": json.dumps(speech.context, ensure_ascii=False),
                "lines": json.dumps(
                    [
                        {
                            "index": line["index"],
                            "lore_id": line["lore_id"],
                            "text": line["text"],
                            "text_type": line["text_type"],
                        }
                        for line in speech.lines
                    ],
                    ensure_ascii=False,
                ),
            }
            for speech in speeches
        ]
        path = output_dir / SPEECHES_FILENAME
        pd.DataFrame(rows).to_parquet(path, index=False)
        return path

    def build_batch_file(
        self,
        *,
        destination: Path | None = None,
        llm_config: LLMConfig | None = None,
        speeches: list[SpeechRecord] | None = None,
    ) -> Path:
        """Render the OpenAI batch JSONL payload for the speech classifier."""

        if speeches is None:
            speeches = self._build_speeches()
        llm_config = llm_config or self._require_llm_config()
        batch_path = destination or self.batch_input_path
        batch_path.parent.mkdir(parents=True, exist_ok=True)

        previous_config = self._llm_config
        self._llm_config = llm_config
        try:
            with batch_path.open("w", encoding="utf-8") as handle:
                for speech in speeches:
                    payload = self._speech_payload(speech)
                    request_body = self._build_llm_request(payload)
                    request_body["model"] = llm_config.model
                    record = {
                        "custom_id": speech.speech_id,
                        "method": "POST",
                        "url": "/v1/responses",
                        "body": request_body,
                    }
                    handle.write(json.dumps(record, ensure_ascii=False))
                    handle.write("\n")
        finally:
            self._llm_config = previous_config

        LOGGER.info(
            "Batch payload written to %s (%s speeches)",
            batch_path,
            len(speeches),
        )
        return batch_path

    def _classify_speeches(
        self,
        speeches: list[SpeechRecord],
        use_llm: bool,
    ) -> list[dict[str, Any]]:
        assignments: list[dict[str, Any]] = []
        payload_handle = self._open_payload_cache(use_llm)
        for speech in speeches:
            if use_llm:
                payload = self._speech_payload(speech)
                if payload_handle is not None:
                    payload_handle.write(json.dumps(payload, ensure_ascii=False))
                    payload_handle.write("\n")
                try:
                    assignment = self._llm_assignment(speech, payload)
                except LLMResponseError as exc:
                    LOGGER.warning(
                        "LLM classification failed for %s (%s); falling back",
                        speech.speech_id,
                        exc,
                    )
                    assignment = self._regex_assignment(
                        speech,
                        strategy="regex_fallback",
                    )
            else:
                assignment = self._regex_assignment(speech)
            assignments.append(assignment)
        if payload_handle is not None:
            payload_handle.close()
        return assignments

    def _assignments_from_batch(
        self,
        speeches: list[SpeechRecord],
    ) -> list[dict[str, Any]]:
        results = self._load_batch_results()
        assignments: list[dict[str, Any]] = []
        successes = 0
        missing_ids: list[str] = []
        for speech in speeches:
            payload = results.get(speech.speech_id)
            if payload is None:
                missing_ids.append(speech.speech_id)
                assignment = self._regex_assignment(
                    speech,
                    strategy="batch_regex_fallback",
                )
                assignments.append(assignment)
                continue
            motifs = self._normalize_motifs(payload, len(speech.lines))
            assignments.append(
                self._assignment_dict(
                    speech,
                    motifs,
                    strategy="llm_batch",
                    raw_response=json.dumps(payload, ensure_ascii=False),
                )
            )
            successes += 1
        total = len(speeches)
        if total:
            if successes == 0:
                LOGGER.warning(
                    "Batch output lacked usable entries; emitted regex-only"
                    " motif hits",
                )
            elif successes < total:
                LOGGER.warning(
                    "Batch output covered %s/%s speeches; %s fell back to"
                    " regex",
                    successes,
                    total,
                    total - successes,
                )
                if len(missing_ids) <= 5:
                    LOGGER.debug(
                        "Missing speech IDs: %s",
                        ", ".join(missing_ids),
                    )
        return assignments

    def _load_batch_results(
        self,
        path: Path | None = None,
    ) -> dict[str, Any]:
        target_path = path or self.batch_output_path
        if not target_path.exists():
            raise FileNotFoundError(
                f"Batch output missing at {target_path}. Run 'corpus analysis "
                "llm-motifs-batch' to submit and download results."
            )

        diagnostics = BatchDiagnostics(path=target_path)
        results: dict[str, Any] = {}
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
                "Batch output %s processed %s records (%s successes, %s"
                " failures)",
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
                "Batch output %s failed completely (%s/%s errors). Example"
                " error: %s",
                target_path,
                diagnostics.failures,
                diagnostics.total_records,
                sample_error,
            )
        if not results:
            LOGGER.warning(
                "Batch output %s contained no usable motif assignments",
                target_path,
            )
        return results

    def _speech_payload(self, speech: SpeechRecord) -> dict[str, Any]:
        return {
            "speech_id": speech.speech_id,
            "canonical_id": speech.canonical_id,
            "context": speech.context,
            "lines": [
                {
                    "index": idx,
                    "lore_id": line["lore_id"],
                    "text": line["text"],
                }
                for idx, line in enumerate(speech.lines)
            ],
            "taxonomy": self._taxonomy_rows,
            "max_motifs": self.config.max_motifs,
        }

    def _llm_assignment(
        self,
        speech: SpeechRecord,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        request = self._build_llm_request(payload)
        client = self._require_llm_client()
        response = client.invoke_json(request)
        motifs = self._normalize_motifs(response, len(speech.lines))
        return self._assignment_dict(
            speech,
            motifs,
            strategy="llm",
            raw_response=json.dumps(response, ensure_ascii=False),
        )

    def _regex_assignment(
        self,
        speech: SpeechRecord,
        *,
        strategy: str = "regex",
    ) -> dict[str, Any]:
        min_support = 1 if len(speech.lines) == 1 else 2
        candidates: list[tuple[str, list[int]]] = []
        texts = [line["text"] for line in speech.lines]
        for slug, pattern in self._patterns.items():
            support = [
                idx
                for idx, text in enumerate(texts)
                if pattern.search(text)
            ]
            if len(support) >= min_support:
                candidates.append((slug, support))
        candidates.sort(key=lambda item: len(item[1]), reverse=True)
        motifs = [
            {
                "slug": slug,
                "support_indices": support[: len(speech.lines)],
            }
            for slug, support in candidates[: self.config.max_motifs]
        ]
        return self._assignment_dict(speech, motifs, strategy=strategy)

    def _assignment_dict(
        self,
        speech: SpeechRecord,
        motifs: list[dict[str, Any]],
        *,
        strategy: str,
        raw_response: str | None = None,
    ) -> dict[str, Any]:
        llm_config = self._llm_config
        return {
            "speech_id": speech.speech_id,
            "canonical_id": speech.canonical_id,
            "motifs": motifs,
            "strategy": strategy,
            "llm_provider": llm_config.provider if llm_config else None,
            "llm_model": llm_config.model if llm_config else None,
            "run_id": self._run_id,
            "taxonomy_version": self._taxonomy.version,
            "raw_response": raw_response,
        }

    def _build_llm_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        schema = json.loads(json.dumps(_SPEECH_SCHEMA))
        schema["properties"]["motifs"]["maxItems"] = self.config.max_motifs
        request = {
            "model": self._require_llm_config().model,
            "input": [
                {
                    "role": "system",
                    "content": [{"type": "input_text", "text": _SYSTEM_PROMPT}],
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": json.dumps(payload, ensure_ascii=False),
                        }
                    ],
                },
            ],
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "speech_motifs",
                    "schema": schema,
                }
            },
        }
        max_output = self._require_llm_config().max_output_tokens
        if max_output is not None:
            request["max_output_tokens"] = max_output
        return request

    def _normalize_motifs(
        self,
        response: Any,
        line_count: int,
    ) -> list[dict[str, Any]]:
        motifs = response.get("motifs", []) if isinstance(response, dict) else []
        normalized: list[dict[str, Any]] = []
        for entry in motifs:
            slug = str(entry.get("slug", "")).strip().lower()
            if not slug or slug not in self._motif_lookup:
                continue
            indices_raw = entry.get("support_indices") or []
            indices = self._sanitize_indices(indices_raw, line_count)
            if not indices:
                continue
            normalized.append({"slug": slug, "support_indices": indices})
            if len(normalized) >= self.config.max_motifs:
                break
        return normalized

    def _sanitize_indices(
        self,
        values: Iterable[Any],
        line_count: int,
    ) -> list[int]:
        indices: list[int] = []
        for value in values:
            try:
                idx = int(value)
            except (TypeError, ValueError):
                continue
            if 0 <= idx < line_count and idx not in indices:
                indices.append(idx)
        indices.sort()
        return indices

    def _write_results(
        self,
        speeches: list[SpeechRecord],
        assignments: list[dict[str, Any]],
    ) -> tuple[Path, Path]:
        output_dir = self._ensure_output_dir()
        speech_index = {speech.speech_id: speech for speech in speeches}
        motifs_rows = [
            {
                "speech_id": item["speech_id"],
                "canonical_id": item["canonical_id"],
                "strategy": item["strategy"],
                "motifs": json.dumps(item["motifs"], ensure_ascii=False),
                "llm_provider": item.get("llm_provider"),
                "llm_model": item.get("llm_model"),
                "run_id": item.get("run_id"),
                "taxonomy_version": item.get("taxonomy_version"),
                "raw_response": item.get("raw_response"),
            }
            for item in assignments
        ]
        motifs_path = output_dir / SPEECH_MOTIFS_FILENAME
        pd.DataFrame(motifs_rows).to_parquet(motifs_path, index=False)

        hits_rows: list[dict[str, Any]] = []
        for item in assignments:
            speech = speech_index[item["speech_id"]]
            for motif in item["motifs"]:
                slug = motif["slug"]
                metadata: MotifMetadata | None = self._motif_lookup.get(slug)
                for idx in motif["support_indices"]:
                    if 0 <= idx < len(speech.lines):
                        line = speech.lines[idx]
                        hits_rows.append(
                            {
                                "speech_id": speech.speech_id,
                                "canonical_id": speech.canonical_id,
                                "motif_slug": slug,
                                "motif_label": (
                                    metadata.label if metadata else slug
                                ),
                                "motif_category": (
                                    metadata.category if metadata else "unknown"
                                ),
                                "lore_id": line["lore_id"],
                                "line_index": idx,
                                "text": line["text"],
                                "text_type": line["text_type"],
                                "strategy": item["strategy"],
                            }
                        )
        hits_path = output_dir / SPEECH_MOTIF_HITS_FILENAME
        pd.DataFrame(hits_rows).to_parquet(hits_path, index=False)
        return motifs_path, hits_path

    def _ensure_output_dir(self) -> Path:
        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        return self.config.output_dir

    def _llm_enabled(self) -> bool:
        if self.config.dry_run_llm:
            return False
        provider = (self.config.llm_provider or "openai").lower()
        return provider == "openai"

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
            if config.provider != "openai":
                raise RuntimeError(
                    "Speech motif pipeline currently supports the OpenAI provider only."
                )
            self._llm_client = OpenAILLMClient(config=config)
        return self._llm_client

    def _open_payload_cache(self, use_llm: bool) -> TextIO | None:
        if not (self.config.store_payloads and use_llm):
            return None
        path = self.config.output_dir / PAYLOAD_CACHE_FILENAME
        path.parent.mkdir(parents=True, exist_ok=True)
        return path.open("w", encoding="utf-8")

    def _payload_cache_path(self, use_llm: bool) -> Path | None:
        if not (self.config.store_payloads and use_llm):
            return None
        return self.config.output_dir / PAYLOAD_CACHE_FILENAME

    def _provenance_dict(self, value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str) and value:
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return {"raw": value}
        return {}


__all__ = [
    "SpeechLLMMode",
    "SpeechMotifArtifacts",
    "SpeechMotifConfig",
    "SpeechMotifPipeline",
    "SPEECHES_FILENAME",
    "SPEECH_MOTIFS_FILENAME",
    "SPEECH_MOTIF_HITS_FILENAME",
]
