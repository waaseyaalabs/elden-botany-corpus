"""Tests for the narrative summarizer pipeline."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from corpus.community_schema import MotifTaxonomy
from pipelines.llm.base import LLMConfig, LLMResponseError
from pipelines.narrative_summarizer import (
    NarrativeSummariesConfig,
    NarrativeSummariesPipeline,
)
from pipelines.npc_motif_graph import (
    NPCMotifGraphConfig,
    NPCMotifGraphPipeline,
)
from tests.helpers import sample_taxonomy


def _write_lore_fixture(path: Path) -> None:
    frame = pd.DataFrame(
        [
            {
                "lore_id": "lore-010",
                "canonical_id": "npc:melina",
                "category": "npc",
                "text_type": "dialogue",
                "text": "The scarlet rot takes root in every oath I keep.",
                "source": "test",
            },
            {
                "lore_id": "lore-011",
                "canonical_id": "npc:melina",
                "category": "npc",
                "text_type": "dialogue",
                "text": "In dreams I walk the mirrored paths again.",
                "source": "test",
            },
        ]
    )
    frame.to_parquet(path, index=False)


def _prepare_graph(tmp_path: Path) -> tuple[Path, MotifTaxonomy]:
    taxonomy = sample_taxonomy()
    curated_path = tmp_path / "lore.parquet"
    _write_lore_fixture(curated_path)

    graph_dir = tmp_path / "graph"
    NPCMotifGraphPipeline(
        config=NPCMotifGraphConfig(
            curated_path=curated_path,
            output_dir=graph_dir,
            categories=("npc",),
        ),
        taxonomy=taxonomy,
    ).run()
    return graph_dir, taxonomy


class FakeLLMClient:
    """In-memory LLM client used for deterministic tests."""

    def __init__(self) -> None:
        self.calls: list[Mapping[str, Any]] = []
        self.config = LLMConfig(provider="fake", model="fake-model")

    def summarize_entity(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        self.calls.append(dict(payload))
        quotes = payload.get("quotes", [])
        first_quote = quotes[0]["lore_id"] if quotes else "none"
        return {
            "canonical_id": payload["canonical_id"],
            "summary_text": "FAKE SUMMARY",
            "motif_slugs": ["scarlet_rot"],
            "supporting_quotes": [first_quote],
        }


class FailingLLMClient(FakeLLMClient):
    """LLM client that surfaces response errors for fallback coverage."""

    def summarize_entity(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        raise LLMResponseError("boom")


def test_narrative_summaries_pipeline(tmp_path: Path) -> None:
    graph_dir, taxonomy = _prepare_graph(tmp_path)

    pipeline = NarrativeSummariesPipeline(
        config=NarrativeSummariesConfig(
            graph_dir=graph_dir,
            output_dir=tmp_path / "summaries",
            max_motifs=2,
            max_quotes=1,
            use_llm=False,
        ),
        taxonomy=taxonomy,
    )
    artifacts = pipeline.run()

    payload = json.loads(artifacts.summaries_json.read_text())
    assert payload["summaries"], "Expected at least one summary"
    entry = payload["summaries"][0]
    assert entry["top_motifs"][0]["slug"] == "scarlet_rot"
    assert entry["llm_used"] is False
    assert artifacts.markdown_path.exists()


def test_narrative_summaries_pipeline_llm_client_invoked(
    tmp_path: Path,
) -> None:
    graph_dir, taxonomy = _prepare_graph(tmp_path)
    fake_llm = FakeLLMClient()

    pipeline = NarrativeSummariesPipeline(
        config=NarrativeSummariesConfig(
            graph_dir=graph_dir,
            output_dir=tmp_path / "summaries_llm",
            max_motifs=2,
            max_quotes=1,
        ),
        taxonomy=taxonomy,
        llm_client=fake_llm,
    )
    artifacts = pipeline.run()

    assert fake_llm.calls, "LLM client was never invoked"
    payload = json.loads(artifacts.summaries_json.read_text())
    entry = payload["summaries"][0]
    assert entry["summary_text"] == "FAKE SUMMARY"
    assert entry["llm_used"] is True
    assert entry["supporting_quotes"], "Expected citations from fake LLM"


def test_narrative_summaries_pipeline_llm_fallback(tmp_path: Path) -> None:
    graph_dir, taxonomy = _prepare_graph(tmp_path)
    failing_llm = FailingLLMClient()

    pipeline = NarrativeSummariesPipeline(
        config=NarrativeSummariesConfig(
            graph_dir=graph_dir,
            output_dir=tmp_path / "summaries_fallback",
            max_motifs=2,
            max_quotes=1,
        ),
        taxonomy=taxonomy,
        llm_client=failing_llm,
    )
    artifacts = pipeline.run()

    payload = json.loads(artifacts.summaries_json.read_text())
    entry = payload["summaries"][0]
    assert entry["llm_used"] is False
    assert entry["llm_provider"] == "fake"
    assert "npc:melina" in entry["summary_text"]
