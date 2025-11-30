"""Shared helpers for parsing OpenAI batch job payloads."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping


@dataclass(slots=True)
class BatchDiagnostics:
    """Diagnostics captured while processing an OpenAI batch output."""

    path: Path
    total_records: int = 0
    successes: int = 0
    failures: int = 0
    failed_ids: list[str] = field(default_factory=list)
    failure_messages: Counter[str] = field(default_factory=Counter)

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": str(self.path),
            "total_records": self.total_records,
            "successes": self.successes,
            "failures": self.failures,
            "failed_ids": list(self.failed_ids),
            "failure_messages": dict(self.failure_messages),
        }


def extract_response_text(response: Mapping[str, Any] | None) -> str | None:
    """Extract the concatenated text payload from an OpenAI batch response."""

    if response is None:
        return None

    def _collect_output_text(payload: Any) -> str | None:
        if not isinstance(payload, list):
            return None
        chunks: list[str] = []
        for item in payload:
            if not isinstance(item, Mapping):
                continue
            content = item.get("content") or []
            if not isinstance(content, list):
                continue
            for piece in content:
                if not isinstance(piece, Mapping):
                    continue
                text = piece.get("text")
                if isinstance(text, str):
                    chunks.append(text)
        if chunks:
            return "".join(chunks)
        return None

    text_payload = _collect_output_text(response.get("output"))
    if text_payload:
        return text_payload

    body = response.get("body")
    if isinstance(body, Mapping):
        text_payload = _collect_output_text(body.get("output"))
        if text_payload:
            return text_payload
        choices = body.get("choices") or []
        chunks: list[str] = []
        for choice in choices:
            if not isinstance(choice, Mapping):
                continue
            message = choice.get("message") or {}
            if not isinstance(message, Mapping):
                continue
            content = message.get("content") or []
            if not isinstance(content, list):
                continue
            for piece in content:
                if not isinstance(piece, Mapping):
                    continue
                text = piece.get("text")
                if isinstance(text, str):
                    chunks.append(text)
        if chunks:
            return "".join(chunks)
    return None


def extract_status_code(response: Mapping[str, Any] | None) -> int | None:
    """Return the HTTP status code (if any) from a batch entry."""

    if not isinstance(response, Mapping):
        return None
    status_code = response.get("status_code")
    if status_code is None:
        return None
    try:
        return int(status_code)
    except (TypeError, ValueError):
        return None


def format_batch_error(error_detail: Any, status_code: int | None) -> str:
    """Render a terse error string for logging diagnostics."""

    if error_detail is not None:
        if isinstance(error_detail, Mapping):
            message = error_detail.get("message")
            if isinstance(message, str):
                return message
            return json.dumps(error_detail, ensure_ascii=False, sort_keys=True)
        if isinstance(error_detail, (list, tuple)):
            return json.dumps(error_detail, ensure_ascii=False)
        return str(error_detail)
    if status_code is not None:
        return f"status_code={status_code}"
    return "unknown error"


__all__ = [
    "BatchDiagnostics",
    "extract_response_text",
    "extract_status_code",
    "format_batch_error",
]
