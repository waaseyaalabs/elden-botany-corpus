"""Tests for the narrative summarizer pipeline."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from corpus.community_schema import MotifTaxonomy

from pipelines.llm.base import LLMConfig
from pipelines.narrative_summarizer import (
    NarrativeSummariesConfig,
    NarrativeSummariesPipeline,
)
from pipelines.npc_motif_graph import (
    NPCMotifGraphConfig,
    NPCMotifGraphPipeline,
)
from tests.helpers import sample_taxonomy

LORE_TEXT = "The scarlet rot takes root in every oath I keep."


def _write_lore_fixture(path: Path) -> None:
    frame = pd.DataFrame(
        [
            {
                "lore_id": "lore-010",
                "canonical_id": "npc:melina",
                "category": "npc",
                "text_type": "dialogue",
                "text": LORE_TEXT,
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


def _batch_response(custom_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "custom_id": custom_id,
        "response": {
            "output": [
                {
                    "content": [
                        {
                            "type": "output_text",
                            "text": json.dumps(payload),
                        }
                    ]
                }
            ]
        },
    }


def _write_batch_file(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record))
            handle.write("\n")


def _summary_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "canonical_id": "npc:melina",
        "summary_text": "FAKE SUMMARY",
        "motif_slugs": ["scarlet_rot"],
        "supporting_quotes": ["lore-010"],
    }
    payload.update(overrides)
    return payload


@pytest.fixture(autouse=True)
def _stub_llm_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "pipelines.narrative_summarizer.resolve_llm_config",
        lambda **_: LLMConfig(provider="fake", model="fake-model"),
    )


def test_narrative_summaries_pipeline(tmp_path: Path) -> None:
    graph_dir, taxonomy = _prepare_graph(tmp_path)

    pipeline = NarrativeSummariesPipeline(
        config=NarrativeSummariesConfig(
            graph_dir=graph_dir,
            output_dir=tmp_path / "summaries",
            max_motifs=2,
            max_quotes=1,
            llm_mode="heuristic",
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


def test_narrative_summaries_pipeline_consumes_batch_output(
    tmp_path: Path,
) -> None:
    graph_dir, taxonomy = _prepare_graph(tmp_path)
    batch_path = tmp_path / "batch_output.jsonl"
    _write_batch_file(
        batch_path,
        [_batch_response("npc:melina", _summary_payload())],
    )

    pipeline = NarrativeSummariesPipeline(
        config=NarrativeSummariesConfig(
            graph_dir=graph_dir,
            output_dir=tmp_path / "summaries_llm",
            batch_output_path=batch_path,
            max_motifs=2,
            max_quotes=1,
        ),
        taxonomy=taxonomy,
    )
    artifacts = pipeline.run()

    payload = json.loads(artifacts.summaries_json.read_text())
    entry = payload["summaries"][0]
    assert entry["summary_text"] == "FAKE SUMMARY"
    assert entry["llm_used"] is True
    assert entry["supporting_quotes"] == ["lore-010"]


def test_narrative_summaries_pipeline_llm_fallback(tmp_path: Path) -> None:
    graph_dir, taxonomy = _prepare_graph(tmp_path)
    batch_path = tmp_path / "batch_error.jsonl"
    _write_batch_file(
        batch_path,
        [
            {
                "custom_id": "npc:melina",
                "error": {"message": "boom"},
            }
        ],
    )

    pipeline = NarrativeSummariesPipeline(
        config=NarrativeSummariesConfig(
            graph_dir=graph_dir,
            output_dir=tmp_path / "summaries_fallback",
            batch_output_path=batch_path,
            max_motifs=2,
            max_quotes=1,
        ),
        taxonomy=taxonomy,
    )
    artifacts = pipeline.run()

    payload = json.loads(artifacts.summaries_json.read_text())
    entry = payload["summaries"][0]
    assert entry["llm_used"] is False
    assert entry["llm_provider"] == "fake"
    assert "npc:melina" in entry["summary_text"]


def test_pipeline_falls_back_when_batch_missing(tmp_path: Path) -> None:
    graph_dir, taxonomy = _prepare_graph(tmp_path)

    pipeline = NarrativeSummariesPipeline(
        config=NarrativeSummariesConfig(
            graph_dir=graph_dir,
            output_dir=tmp_path / "summaries_missing",
            llm_mode="batch",
        ),
        taxonomy=taxonomy,
    )
    artifacts = pipeline.run()

    payload = json.loads(artifacts.summaries_json.read_text())
    entry = payload["summaries"][0]
    assert entry["llm_used"] is False
    assert pipeline.batch_diagnostics is None


def test_pipeline_records_batch_diagnostics(tmp_path: Path) -> None:
    graph_dir, taxonomy = _prepare_graph(tmp_path)
    batch_path = tmp_path / "batch_diag.jsonl"
    _write_batch_file(
        batch_path,
        [
            _batch_response("npc:melina", _summary_payload()),
            {"custom_id": "npc:unknown", "error": {"message": "boom"}},
        ],
    )

    pipeline = NarrativeSummariesPipeline(
        config=NarrativeSummariesConfig(
            graph_dir=graph_dir,
            output_dir=tmp_path / "summaries_diag",
            batch_output_path=batch_path,
        ),
        taxonomy=taxonomy,
    )
    pipeline.run()

    diagnostics = pipeline.batch_diagnostics
    assert diagnostics is not None
    assert diagnostics.total_records == 2
    assert diagnostics.successes == 1
    assert diagnostics.failures == 1
    assert diagnostics.failed_ids == ["npc:unknown"]


def test_pipeline_accepts_alias_responses(tmp_path: Path) -> None:
    graph_dir, taxonomy = _prepare_graph(tmp_path)
    batch_path = tmp_path / "batch_alias.jsonl"
    alias_path = tmp_path / "aliases.csv"
    pd.DataFrame(
        [
            {
                "alias_id": "npc:melina-alt",
                "canonical_id": "npc:melina",
            }
        ]
    ).to_csv(alias_path, index=False)

    _write_batch_file(
        batch_path,
        [
            _batch_response(
                "npc:melina",
                _summary_payload(canonical_id="npc:melina-alt"),
            )
        ],
    )

    pipeline = NarrativeSummariesPipeline(
        config=NarrativeSummariesConfig(
            graph_dir=graph_dir,
            output_dir=tmp_path / "summaries_alias",
            batch_output_path=batch_path,
            alias_table_path=alias_path,
        ),
        taxonomy=taxonomy,
    )
    artifacts = pipeline.run()

    payload = json.loads(artifacts.summaries_json.read_text())
    entry = payload["summaries"][0]
    assert entry["llm_used"] is True
    assert entry["summary_text"] == "FAKE SUMMARY"


def test_narrative_summaries_pipeline_rejects_mismatched_responses(
    tmp_path: Path,
) -> None:
    graph_dir, taxonomy = _prepare_graph(tmp_path)
    batch_path = tmp_path / "batch_mismatch.jsonl"
    _write_batch_file(
        batch_path,
        [
            _batch_response(
                "npc:melina",
                _summary_payload(canonical_id="npc:radahn"),
            )
        ],
    )

    pipeline = NarrativeSummariesPipeline(
        config=NarrativeSummariesConfig(
            graph_dir=graph_dir,
            output_dir=tmp_path / "summaries_mismatch",
            batch_output_path=batch_path,
            max_motifs=2,
            max_quotes=1,
        ),
        taxonomy=taxonomy,
    )
    artifacts = pipeline.run()

    payload = json.loads(artifacts.summaries_json.read_text())
    entry = payload["summaries"][0]
    assert entry["llm_used"] is False
    assert entry["summary_text"] != "FAKE SUMMARY"
    assert "npc:melina" in entry["summary_text"]


def test_narrative_summaries_pipeline_rejects_hallucinated_quotes(
    tmp_path: Path,
) -> None:
    graph_dir, taxonomy = _prepare_graph(tmp_path)
    batch_path = tmp_path / "batch_hallucinated.jsonl"
    _write_batch_file(
        batch_path,
        [
            _batch_response(
                "npc:melina",
                _summary_payload(supporting_quotes=["invented-quote"]),
            )
        ],
    )

    pipeline = NarrativeSummariesPipeline(
        config=NarrativeSummariesConfig(
            graph_dir=graph_dir,
            output_dir=tmp_path / "summaries_hallucinated",
            batch_output_path=batch_path,
            max_motifs=2,
            max_quotes=1,
        ),
        taxonomy=taxonomy,
    )
    artifacts = pipeline.run()

    payload = json.loads(artifacts.summaries_json.read_text())
    entry = payload["summaries"][0]
    assert entry["llm_used"] is False
    assert entry["supporting_quotes"] != ["invented-quote"]
    assert entry["supporting_quotes"], "Expected fallback quotes"


def test_narrative_summaries_pipeline_ignores_partial_quote_hallucinations(
    tmp_path: Path,
) -> None:
    graph_dir, taxonomy = _prepare_graph(tmp_path)
    batch_path = tmp_path / "batch_partial_hallucinated.jsonl"
    _write_batch_file(
        batch_path,
        [
            _batch_response(
                "npc:melina",
                _summary_payload(
                    supporting_quotes=["lore-010", "unknown-quote"],
                ),
            )
        ],
    )

    pipeline = NarrativeSummariesPipeline(
        config=NarrativeSummariesConfig(
            graph_dir=graph_dir,
            output_dir=tmp_path / "summaries_partial_hallucinated",
            batch_output_path=batch_path,
            max_motifs=2,
            max_quotes=1,
        ),
        taxonomy=taxonomy,
    )
    artifacts = pipeline.run()

    payload = json.loads(artifacts.summaries_json.read_text())
    entry = payload["summaries"][0]
    assert entry["llm_used"] is True
    assert entry["supporting_quotes"] == ["lore-010"]


def test_narrative_summaries_pipeline_normalizes_verbose_quote_ids(
    tmp_path: Path,
) -> None:
    graph_dir, taxonomy = _prepare_graph(tmp_path)
    batch_path = tmp_path / "batch_verbose.jsonl"
    _write_batch_file(
        batch_path,
        [
            _batch_response(
                "npc:melina",
                _summary_payload(
                    supporting_quotes=[
                        f"lore_id=lore-010 text={LORE_TEXT}",
                    ],
                ),
            )
        ],
    )

    pipeline = NarrativeSummariesPipeline(
        config=NarrativeSummariesConfig(
            graph_dir=graph_dir,
            output_dir=tmp_path / "summaries_verbose",
            batch_output_path=batch_path,
            max_motifs=2,
            max_quotes=1,
        ),
        taxonomy=taxonomy,
    )
    artifacts = pipeline.run()

    payload = json.loads(artifacts.summaries_json.read_text())
    entry = payload["summaries"][0]
    quote_id = entry["supporting_quotes"][0]
    assert entry["llm_used"] is True
    assert quote_id == "lore-010"


def test_narrative_summaries_pipeline_maps_text_only_quote_references(
    tmp_path: Path,
) -> None:
    graph_dir, taxonomy = _prepare_graph(tmp_path)
    batch_path = tmp_path / "batch_text_only.jsonl"
    _write_batch_file(
        batch_path,
        [
            _batch_response(
                "npc:melina",
                _summary_payload(supporting_quotes=[LORE_TEXT]),
            )
        ],
    )

    pipeline = NarrativeSummariesPipeline(
        config=NarrativeSummariesConfig(
            graph_dir=graph_dir,
            output_dir=tmp_path / "summaries_text_only",
            batch_output_path=batch_path,
            max_motifs=2,
            max_quotes=1,
        ),
        taxonomy=taxonomy,
    )
    artifacts = pipeline.run()

    payload = json.loads(artifacts.summaries_json.read_text())
    entry = payload["summaries"][0]
    quote_id = entry["supporting_quotes"][0]
    assert entry["llm_used"] is True
    assert quote_id == "lore-010"
