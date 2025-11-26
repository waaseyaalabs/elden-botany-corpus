"""Unit tests for the OpenAI-backed LLM connector."""

from __future__ import annotations

import json
from typing import Any, cast

import pytest

from pipelines.llm.base import LLMConfig, LLMResponseError
from pipelines.llm.openai_client import OpenAILLMClient


class _FakeContentPiece:
    def __init__(self, text: str) -> None:
        self.text = text


class _FakeOutputChunk:
    def __init__(self, text: str) -> None:
        self.content = [_FakeContentPiece(text)]


class _FakeResponse:
    def __init__(self, text: str) -> None:
        self.output = [_FakeOutputChunk(text)]

    def model_dump(self) -> dict[str, Any]:
        return {"output": []}


class _FakeResponsesAPI:
    def __init__(
        self,
        response: _FakeResponse | None = None,
        exc: Exception | None = None,
    ) -> None:
        self._response = response
        self._exc = exc
        self.called_with: dict[str, Any] | None = None

    def create(self, **kwargs: Any) -> Any:
        self.called_with = kwargs
        if self._exc:
            raise self._exc
        assert self._response is not None, "fake response must be injected"
        return self._response


class _FakeClient:
    def __init__(
        self,
        response: _FakeResponse | None = None,
        exc: Exception | None = None,
    ) -> None:
        self.responses = _FakeResponsesAPI(response=response, exc=exc)


class _FakeTextOnlyResponsesAPI:
    def __init__(self, response: _FakeResponse) -> None:
        self._response = response
        self.called_with: dict[str, Any] | None = None

    def create(
        self,
        *,
        model: str,
        input: list[Any],
        text: Any | None = None,
        reasoning: Any | None = None,
        max_output_tokens: Any | None = None,
    ) -> Any:
        self.called_with = {
            "model": model,
            "input": input,
            "text": text,
            "reasoning": reasoning,
            "max_output_tokens": max_output_tokens,
        }
        return self._response


class _FakeTextOnlyClient:
    def __init__(self, response: _FakeResponse) -> None:
        self.responses = _FakeTextOnlyResponsesAPI(response=response)


def _sample_payload() -> dict[str, Any]:
    return {
        "canonical_id": "npc:melina",
        "category": "npc",
        "lore_count": 3,
        "motif_mentions": 5,
        "unique_motifs": 2,
        "top_motifs": [
            {
                "slug": "scarlet_rot",
                "label": "Scarlet Rot",
                "hit_count": 2,
                "unique_lore": 2,
            }
        ],
        "quotes": [
            {
                "lore_id": "l-1",
                "text": "Rot blooms eternal",
                "motifs": ["scarlet_rot"],
            }
        ],
    }


def test_openai_client_summarize_entity_happy_path() -> None:
    expected: dict[str, Any] = {
        "canonical_id": "npc:melina",
        "summary_text": "Brief",
        "motif_slugs": ["scarlet_rot"],
        "supporting_quotes": ["l-1"],
    }
    response = _FakeResponse(json.dumps(expected))
    fake_client = _FakeClient(response=response)
    client = OpenAILLMClient(
        config=LLMConfig(provider="openai", model="dummy-model"),
        api_key="token",
        client=fake_client,
    )

    result = client.summarize_entity(_sample_payload())

    assert result == expected
    assert fake_client.responses.called_with is not None
    assert fake_client.responses.called_with["model"] == "dummy-model"


def test_openai_client_repairs_partial_json() -> None:
    expected: dict[str, Any] = {
        "canonical_id": "npc:ranni",
        "summary_text": "Moon shade",
        "motif_slugs": ["moon"],
        "supporting_quotes": ["l-2"],
    }
    raw = f"NOTE: {json.dumps(expected)} -- end"
    response = _FakeResponse(raw)
    fake_client = _FakeClient(response=response)
    client = OpenAILLMClient(api_key="token", client=fake_client)

    result = client.summarize_entity(_sample_payload())

    assert result == expected


def test_openai_client_surfaces_api_errors() -> None:
    exploding_client = _FakeClient(exc=ValueError("boom"))
    client = OpenAILLMClient(api_key="token", client=exploding_client)

    with pytest.raises(LLMResponseError):
        client.summarize_entity(_sample_payload())


def test_openai_client_requires_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    with pytest.raises(RuntimeError):
        OpenAILLMClient(client=_FakeClient(response=_FakeResponse("{}")))


def test_openai_client_falls_back_to_text_json_schema() -> None:
    expected: dict[str, Any] = {
        "canonical_id": "npc:millicent",
        "summary_text": "Scarlet descent",
        "motif_slugs": ["scarlet_rot"],
        "supporting_quotes": ["l-99"],
    }
    response = _FakeResponse(json.dumps(expected))
    text_client = _FakeTextOnlyClient(response=response)
    client = OpenAILLMClient(api_key="token", client=text_client)

    client.summarize_entity(_sample_payload())

    assert text_client.responses.called_with is not None
    text_config = text_client.responses.called_with["text"]
    assert isinstance(text_config, dict)
    format_config = cast(dict[str, Any], text_config["format"])
    assert isinstance(format_config, dict)
    assert format_config["type"] == "json_schema"
    assert format_config["name"] == "narrative_summary"


def test_openai_client_request_body_shape_uses_text_format() -> None:
    expected: dict[str, Any] = {
        "canonical_id": "npc:melina",
        "summary_text": "Brief",
        "motif_slugs": ["scarlet_rot"],
        "supporting_quotes": ["l-1"],
    }
    response = _FakeResponse(json.dumps(expected))
    fake_client = _FakeClient(response=response)
    client = OpenAILLMClient(api_key="token", client=fake_client)

    client.summarize_entity(_sample_payload())

    assert fake_client.responses.called_with is not None
    called = fake_client.responses.called_with
    assert "response_format" not in called
    text_config = called.get("text")
    assert isinstance(text_config, dict)
    format_config = cast(dict[str, Any], text_config["format"])
    assert format_config["type"] == "json_schema"
    assert format_config["name"] == "narrative_summary"
