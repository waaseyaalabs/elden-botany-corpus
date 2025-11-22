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


_SYSTEM_PROMPT = (
    "You author concise Elden Ring lore briefs. Only use provided data. "
    "Never invent characters, motifs, or quotes. Cite lore_id values for "
    "each claim. Respond in JSON using the provided schema."
)


class OpenAILLMClient:
    """LLMClient backed by OpenAI's Responses API."""

    def __init__(
        self,
        config: LLMConfig | None = None,
        *,
        api_key: str | None = None,
        client: Any | None = None,
    ) -> None:
        self.config = config or LLMConfig(
            provider="openai",
            model="gpt-5.1",
        )
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
        schema: dict[str, Any] = {
            "name": "narrative_summary",
            "schema": {
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
            },
        }

        user_prompt = self._format_payload(payload)
        request: dict[str, Any] = {
            "model": self.config.model,
            "input": [
                {
                    "role": "system",
                    "content": [{"type": "text", "text": _SYSTEM_PROMPT}],
                },
                {
                    "role": "user",
                    "content": [{"type": "text", "text": user_prompt}],
                },
            ],
            "response_format": {
                "type": "json_schema",
                "json_schema": schema,
            },
        }
        if self.config.reasoning_effort:
            request["reasoning"] = {
                "effort": self.config.reasoning_effort,
            }
        if self.config.max_output_tokens is not None:
            request["max_output_tokens"] = self.config.max_output_tokens
        return request

    def _format_payload(self, payload: Mapping[str, Any]) -> str:
        motifs = payload.get("top_motifs", [])
        quotes = payload.get("quotes", [])
        lines = [
            f"Canonical ID: {payload.get('canonical_id')}",
            f"Category: {payload.get('category')}",
            (
                "Entity stats: "
                f"lore_count={payload.get('lore_count')} "
                f"motif_mentions={payload.get('motif_mentions')} "
                f"unique_motifs={payload.get('unique_motifs')}"
            ),
            "Top motifs:",
        ]
        for motif in motifs:
            lines.append(
                f"- {motif.get('slug')}: {motif.get('label')} "
                f"(hits={motif.get('hit_count')}, "
                f"unique_lore={motif.get('unique_lore')})"
            )
        if not motifs:
            lines.append("- None available")

        lines.append("Quotes:")
        for quote in quotes:
            motifs_joined = ", ".join(quote.get("motifs", []))
            lines.append(
                f"- lore_id={quote.get('lore_id')} motifs={motifs_joined}\n"
                f"  text={quote.get('text')}"
            )
        if not quotes:
            lines.append("- No lore quotes provided")

        return "\n".join(lines)

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
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            repaired = self._repair_json(raw)
            try:
                parsed = json.loads(repaired)
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
        return raw[start : end + 1]

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


__all__ = ["OpenAILLMClient"]
