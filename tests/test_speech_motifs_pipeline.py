"""Tests for the speech-level motif detection pipeline."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from pipelines.llm.base import LLMConfig
from pipelines.speech_motifs import SpeechMotifConfig, SpeechMotifPipeline
from tests.helpers import sample_taxonomy


def _write_dialogue_fixture(tmp_path: Path) -> Path:
    curated_dir = tmp_path / "data" / "curated"
    curated_dir.mkdir(parents=True, exist_ok=True)
    path = curated_dir / "lore_corpus.parquet"
    rows = [
        {
            "lore_id": "npc-line-001",
            "canonical_id": "npc:melina",
            "category": "npc",
            "text_type": "dialogue",
            "text": "Rot returns with every oath.",
            "source": "test",
            "provenance": json.dumps({"talk_id": "intro"}),
        },
        {
            "lore_id": "npc-line-002",
            "canonical_id": "npc:melina",
            "category": "npc",
            "text_type": "dialogue",
            "text": "Let the scarlet rot bloom once more.",
            "source": "test",
            "provenance": json.dumps({"talk_id": "intro"}),
        },
    ]
    frame = pd.DataFrame(rows)
    frame.to_parquet(path, index=False)
    return path


def test_speech_motif_pipeline_regex_fallback(tmp_path: Path) -> None:
    curated_path = _write_dialogue_fixture(tmp_path)
    output_dir = tmp_path / "artifacts"

    pipeline = SpeechMotifPipeline(
        config=SpeechMotifConfig(
            curated_path=curated_path,
            output_dir=output_dir,
            dry_run_llm=True,
        ),
        taxonomy=sample_taxonomy(),
    )
    artifacts = pipeline.run()

    hits = pd.read_parquet(artifacts.speech_motif_hits_parquet)
    assert not hits.empty
    assert set(hits["motif_slug"]) == {"scarlet_rot"}
    speeches = pd.read_parquet(artifacts.speeches_parquet)
    assert int(speeches.iloc[0]["line_count"]) == 2


def test_speech_motif_pipeline_uses_llm(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    curated_path = _write_dialogue_fixture(tmp_path)
    output_dir = tmp_path / "llm"

    class _FakeLLM:
        def __init__(self, config: object, **_: object) -> None:
            self.config = config

        def invoke_json(self, request: dict[str, object]) -> dict[str, object]:
            user_block = request["input"][1]["content"][0]["text"]  # type: ignore[index]
            payload = json.loads(user_block)
            return {
                "speech_id": payload["speech_id"],
                "canonical_id": payload["canonical_id"],
                "motifs": [
                    {
                        "slug": "dream_cycle",
                        "support_indices": [0],
                    }
                ],
            }

    monkeypatch.setattr("pipelines.speech_motifs.OpenAILLMClient", _FakeLLM)

    pipeline = SpeechMotifPipeline(
        config=SpeechMotifConfig(
            curated_path=curated_path,
            output_dir=output_dir,
            store_payloads=False,
        ),
        taxonomy=sample_taxonomy(),
    )
    artifacts = pipeline.run()

    motifs = pd.read_parquet(artifacts.speech_motifs_parquet)
    assert set(motifs["strategy"]) == {"llm"}
    assert "dream_cycle" in pd.read_parquet(artifacts.speech_motif_hits_parquet)["motif_slug"].tolist()


def test_speech_motif_batch_payload_builder(tmp_path: Path) -> None:
    curated_path = _write_dialogue_fixture(tmp_path)
    output_dir = tmp_path / "batch"
    pipeline = SpeechMotifPipeline(
        config=SpeechMotifConfig(
            curated_path=curated_path,
            output_dir=output_dir,
        ),
        taxonomy=sample_taxonomy(),
    )

    payload_path = pipeline.build_batch_file(
        destination=output_dir / "speech_batch.jsonl",
        llm_config=LLMConfig(provider="openai", model="gpt-5-mini"),
    )

    lines = payload_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["method"] == "POST"
    assert record["url"] == "/v1/responses"
    assert record["body"]["model"] == "gpt-5-mini"


def test_speech_motif_pipeline_consumes_batch_output(tmp_path: Path) -> None:
    curated_path = _write_dialogue_fixture(tmp_path)
    bootstrap_dir = tmp_path / "bootstrap"
    bootstrap_pipeline = SpeechMotifPipeline(
        config=SpeechMotifConfig(
            curated_path=curated_path,
            output_dir=bootstrap_dir,
            dry_run_llm=True,
        ),
        taxonomy=sample_taxonomy(),
    )
    bootstrap_artifacts = bootstrap_pipeline.run()
    speech_frame = pd.read_parquet(bootstrap_artifacts.speeches_parquet)
    speech_id = str(speech_frame.iloc[0]["speech_id"])
    canonical_id = str(speech_frame.iloc[0]["canonical_id"])

    payload = {
        "speech_id": speech_id,
        "canonical_id": canonical_id,
        "motifs": [
            {
                "slug": "scarlet_rot",
                "support_indices": [0],
            }
        ],
    }
    batch_entry = {
        "custom_id": speech_id,
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
            ],
            "status_code": 200,
        },
    }
    batch_output = tmp_path / "batch_out" / "speech_batch_output.jsonl"
    batch_output.parent.mkdir(parents=True, exist_ok=True)
    batch_output.write_text(json.dumps(batch_entry) + "\n", encoding="utf-8")

    pipeline = SpeechMotifPipeline(
        config=SpeechMotifConfig(
            curated_path=curated_path,
            output_dir=tmp_path / "final",
            llm_mode="batch",
            batch_output_path=batch_output,
        ),
        taxonomy=sample_taxonomy(),
    )
    artifacts = pipeline.run()

    motifs = pd.read_parquet(artifacts.speech_motifs_parquet)
    assert set(motifs["strategy"]) == {"llm_batch"}
    hits = pd.read_parquet(artifacts.speech_motif_hits_parquet)
    assert "scarlet_rot" in hits["motif_slug"].tolist()
