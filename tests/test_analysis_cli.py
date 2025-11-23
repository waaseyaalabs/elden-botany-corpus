"""Regression tests for the analysis CLI helpers."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from corpus import analysis_cli


def test_summaries_cli_respects_zero_overrides(monkeypatch, tmp_path):
    captured: dict[str, object] = {}

    class DummyPipeline:
        def __init__(self, config):
            captured["config"] = config
            self._artifacts = SimpleNamespace(
                summaries_json=tmp_path / "summaries.json",
                summaries_parquet=tmp_path / "summaries.parquet",
                markdown_path=tmp_path / "summaries.md",
            )

        def run(self):
            return self._artifacts

    monkeypatch.setattr(
        analysis_cli,
        "NarrativeSummariesPipeline",
        DummyPipeline,
    )

    analysis_cli.summaries.callback(
        graph_dir=tmp_path / "graph",
        output_dir=tmp_path / "out",
        max_motifs=0,
        max_quotes=0,
        llm_provider=None,
        llm_model=None,
        llm_reasoning=None,
        dry_run_llm=True,
    )

    config = captured["config"]
    assert config.max_motifs == 0
    assert config.max_quotes == 0
    assert config.use_llm is False


def test_summaries_batch_build_submit_and_download(monkeypatch, tmp_path):
    captured: dict[str, object] = {}
    batch_input = tmp_path / "batch_input.jsonl"
    batch_output = tmp_path / "batch_output.jsonl"

    class DummyPipeline:
        def __init__(self, config):
            captured["config"] = config
            self.batch_input_path = batch_input
            self.batch_output_path = batch_output

        def build_batch_file(self, destination, llm_config):
            captured["build_destination"] = destination
            captured["llm_config"] = llm_config
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text("{}")
            return destination

    class DummyJob:
        def __init__(self):
            captured.setdefault("job_inits", 0)
            captured["job_inits"] += 1

        def submit(self, batch_file, completion_window, metadata):
            captured["submitted"] = {
                "batch_file": Path(batch_file),
                "window": completion_window,
                "metadata": metadata,
            }
            return SimpleNamespace(id="batch-123")

        def poll(self, batch_id, interval):
            captured["polled"] = {
                "batch_id": batch_id,
                "interval": interval,
            }
            return SimpleNamespace(
                status="completed",
                output_file_id="file-xyz",
            )

        def retrieve(self, batch_id):
            captured["retrieved"] = batch_id
            return SimpleNamespace(
                status="completed",
                output_file_id="file-xyz",
            )

        def download_output(self, batch, destination):
            destination.write_text("done")
            captured["download_destination"] = destination
            captured["download_batch"] = batch
            return destination

    fake_llm = SimpleNamespace(
        provider="openai",
        model="gpt-4o-mini",
        reasoning_effort="medium",
        max_output_tokens=2048,
    )

    monkeypatch.setattr(
        analysis_cli,
        "resolve_llm_config",
        lambda **_: fake_llm,
    )
    monkeypatch.setattr(
        analysis_cli,
        "NarrativeSummariesPipeline",
        DummyPipeline,
    )
    monkeypatch.setattr(
        analysis_cli,
        "OpenAIBatchJob",
        DummyJob,
    )

    analysis_cli.summaries_batch.callback(
        graph_dir=tmp_path / "graph",
        output_dir=tmp_path / "out",
        batch_input=batch_input,
        batch_output=batch_output,
        max_motifs=None,
        max_quotes=None,
        llm_provider="openai",
        llm_model="gpt-4o-mini",
        llm_reasoning="medium",
        completion_window="24h",
        skip_build=False,
        submit=True,
        wait=True,
        poll_interval=5.0,
        download_output=True,
        batch_id=None,
    )

    assert Path(captured["build_destination"]) == batch_input
    assert captured["submitted"]["batch_file"] == batch_input
    assert captured["polled"] == {"batch_id": "batch-123", "interval": 5.0}
    assert Path(captured["download_destination"]) == batch_output
