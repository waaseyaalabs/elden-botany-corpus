"""OpenAI-backed implementation of the LLM connector."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from typing import Any, cast

from pipelines.llm.base import LLMConfig, LLMResponseError

try:  # pragma: no cover - import guard for optional dependency
    from openai import OpenAI
except ModuleNotFoundError as exc:  # pragma: no cover - handled at runtime
    message = (
        "The 'openai' extra is required: "
        "poetry install --with embeddings-openai"
    )
    raise RuntimeError(message) from exc


_SYSTEM_PROMPT_BRIEF = (
    "You author concise Elden Ring lore briefs from Elden Ring motif "
    "analysis. Use only the JSON context supplied in the user message. "
    "Mention motifs and cite lore_id values that justify each point. "
    "Do not mention prompt metadata, counts, token budgets, or field names. "
    "Respond in JSON using the provided schema."
)

_SYSTEM_PROMPT_CODEX = (
    "You speak as an archivist of the Elden Codex. Weave mythic but clear "
    "summaries drawn only from the provided JSON context. Blend motifs, "
    "quotes, and lore_id citations into an in-universe retelling without "
    "revealing metadata, counts, or prompt mechanics. Respond strictly as "
    "JSON that conforms to the supplied schema."
)

_JSON_SCHEMA_NAME = "narrative_summary"


def _system_prompt(codex_mode: bool) -> str:
    return _SYSTEM_PROMPT_CODEX if codex_mode else _SYSTEM_PROMPT_BRIEF


_SUMMARY_JSON_SCHEMA = {
    "type": "object",
    "properties": {
        "canonical_id": {"type": "string"},
        "summary_text": {"type": "string"},
        "motif_slugs": {
            "type": "array",
            "items": {"type": "string"},
        },
        "supporting_quotes": {
            "type": "array",
            "items": {"type": "string"},
        },
    },
    "required": [
        "canonical_id",
        "summary_text",
        "motif_slugs",
        "supporting_quotes",
    ],
    "additionalProperties": False,
}


def _format_payload(payload: Mapping[str, Any]) -> str:
    def _int_value(value: Any) -> int | None:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    motif_rows = []
    for motif in payload.get("top_motifs", []) or []:
        motif_rows.append(
            {
                "slug": motif.get("slug"),
                "label": motif.get("label"),
                "category": motif.get("motif_category")
                or motif.get("category"),
                "hits": _int_value(motif.get("hit_count")),
                "unique_lore": _int_value(motif.get("unique_lore")),
            }
        )

    quote_rows = []
    for quote in payload.get("quotes", []) or []:
        quote_rows.append(
            {
                "lore_id": quote.get("lore_id"),
                "text": quote.get("text"),
                "motifs": quote.get("motifs", []),
            }
        )

    context = {
        "canonical_id": payload.get("canonical_id"),
        "category": payload.get("category"),
        "motifs": motif_rows,
        "quotes": quote_rows,
    }
    return json.dumps(context, ensure_ascii=False, separators=(",", ":"))


def build_summary_request_body(
    config: LLMConfig,
    payload: Mapping[str, Any],
    *,
    codex_mode: bool = False,
) -> dict[str, Any]:
    """Render the Responses API request body for the narrative summarizer."""

    schema_body = json.loads(json.dumps(_SUMMARY_JSON_SCHEMA))
    text_payload = {
        "format": {
            "type": "json_schema",
            "name": _JSON_SCHEMA_NAME,
            "schema": schema_body,
        }
    }

    user_prompt = _format_payload(payload)
    request: dict[str, Any] = {
        "model": config.model,
        "input": [
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": _system_prompt(codex_mode),
                    }
                ],
            },
            {
                "role": "user",
                "content": [{"type": "input_text", "text": user_prompt}],
            },
        ],
        "text": text_payload,
    }
    if config.reasoning_effort:
        request["reasoning"] = {
            "effort": config.reasoning_effort,
        }
    if config.max_output_tokens is not None:
        request["max_output_tokens"] = config.max_output_tokens
    return request


class OpenAILLMClient:
    """LLMClient backed by OpenAI's Responses API."""

    def __init__(
        self,
        config: LLMConfig | None = None,
        *,
        api_key: str | None = None,
        client: Any | None = None,
        codex_mode: bool = False,
    ) -> None:
        self.config = config or LLMConfig(
            provider="openai",
            model="gpt-5.1",
        )
        self._codex_mode = codex_mode
        key = api_key or os.environ.get("OPENAI_API_KEY")
        if not key:
            raise RuntimeError(
                "OPENAI_API_KEY is required to use the OpenAI LLM connector"
            )
        self._client: Any = client or OpenAI(api_key=key)

    def summarize_entity(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        request = self._build_request(payload)
        try:
            response = self._client.responses.create(**request)
        except Exception as exc:  # pragma: no cover - network/SDK failure
            msg = "OpenAI request failed"
            raise LLMResponseError(msg) from exc

        raw = self._response_text(response)
        parsed = self._parse_summary(raw)
        return parsed

    def _build_request(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        return build_summary_request_body(
            self.config,
            payload,
            codex_mode=self._codex_mode,
        )

    def _response_text(self, response: Any) -> str:
        chunks: list[str] = []
        output = getattr(response, "output", None)
        if output:
            for item in output:
                content = getattr(item, "content", None)
                if content:
                    chunks.extend(self._extract_content_text(content))
        if not chunks and hasattr(response, "model_dump"):
            data = response.model_dump()
            for item in data.get("output", []):
                content = item.get("content", [])
                chunks.extend(self._extract_content_text(content))
        if not chunks:
            raise LLMResponseError("OpenAI response contained no text content")
        return "".join(chunks)

    def _extract_content_text(self, content: Any) -> list[str]:
        chunks: list[str] = []
        for piece in content:
            text = getattr(piece, "text", None)
            if isinstance(text, str):
                chunks.append(text)
                continue
            if isinstance(piece, dict):
                piece_dict = cast(dict[str, Any], piece)
                value = cast(str | None, piece_dict.get("text"))
                if isinstance(value, str):
                    chunks.append(value)
        return chunks

    def _parse_summary(self, raw: str) -> dict[str, Any]:
        parsed: dict[str, Any]
        try:
            parsed = cast(dict[str, Any], json.loads(raw))
        except json.JSONDecodeError:
            repaired = self._repair_json(raw)
            try:
                parsed = cast(dict[str, Any], json.loads(repaired))
            except json.JSONDecodeError as exc:
                msg = "OpenAI response was not valid JSON"
                raise LLMResponseError(msg) from exc
        self._validate_payload(parsed)
        return parsed

    def _repair_json(self, raw: str) -> str:
        start = raw.find("{")
        end = raw.rfind("}")
        if start == -1 or end == -1:
            return raw
        return raw[start:end + 1]

    def _validate_payload(self, parsed: Mapping[str, Any]) -> None:
        for field in (
            "canonical_id",
            "summary_text",
            "motif_slugs",
            "supporting_quotes",
        ):
            if field not in parsed:
                msg = f"OpenAI response missing '{field}'"
                raise LLMResponseError(msg)
        if not isinstance(parsed["summary_text"], str):
            msg = "summary_text must be a string"
            raise LLMResponseError(msg)
        if not isinstance(parsed["motif_slugs"], list):
            msg = "motif_slugs must be a list"
            raise LLMResponseError(msg)
        if not isinstance(parsed["supporting_quotes"], list):
            msg = "supporting_quotes must be a list"
            raise LLMResponseError(msg)


__all__ = ["OpenAILLMClient", "build_summary_request_body"]
