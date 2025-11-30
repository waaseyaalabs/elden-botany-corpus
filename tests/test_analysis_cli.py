"""Regression tests for the analysis CLI helpers."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from click import ClickException
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
        llm_mode="batch",
        dry_run_llm=True,
        codex_mode=False,
        alias_table=None,
    )

    config = captured["config"]
    assert config.max_motifs == 0
    assert config.max_quotes == 0
    assert config.llm_mode == "heuristic"
    assert config.alias_table_path is not None


def test_summaries_cli_allows_llm_mode_override(monkeypatch, tmp_path):
    captured: dict[str, object] = {}

    class DummyPipeline:
        def __init__(self, config):
            captured["config"] = config

        def run(self):
            return SimpleNamespace(
                summaries_json=tmp_path / "summaries.json",
                summaries_parquet=tmp_path / "summaries.parquet",
                markdown_path=tmp_path / "summaries.md",
            )

    monkeypatch.setattr(
        analysis_cli,
        "NarrativeSummariesPipeline",
        DummyPipeline,
    )

    analysis_cli.summaries.callback(
        graph_dir=tmp_path / "graph",
        output_dir=tmp_path / "out",
        max_motifs=None,
        max_quotes=None,
        llm_provider="openai",
        llm_model="gpt-4o-mini",
        llm_reasoning="medium",
        llm_mode="per-entity",
        dry_run_llm=False,
        codex_mode=False,
        alias_table=tmp_path / "aliases.csv",
    )

    config = captured["config"]
    assert config.llm_mode == "per-entity"
    assert config.alias_table_path == tmp_path / "aliases.csv"


def test_summaries_cli_rejects_conflicting_dry_run(monkeypatch, tmp_path):
    with pytest.raises(ClickException):
        analysis_cli.summaries.callback(
            graph_dir=tmp_path / "graph",
            output_dir=tmp_path / "out",
            max_motifs=None,
            max_quotes=None,
            llm_provider=None,
            llm_model=None,
            llm_reasoning=None,
            llm_mode="per-entity",
            dry_run_llm=True,
            codex_mode=False,
            alias_table=None,
        )


def test_summaries_cli_enables_codex_mode(monkeypatch, tmp_path):
    captured: dict[str, object] = {}

    class DummyPipeline:
        def __init__(self, config):
            captured["config"] = config

        def run(self):
            return SimpleNamespace(
                summaries_json=tmp_path / "summaries.json",
                summaries_parquet=tmp_path / "summaries.parquet",
                markdown_path=tmp_path / "summaries.md",
            )

    monkeypatch.setattr(
        analysis_cli,
        "NarrativeSummariesPipeline",
        DummyPipeline,
    )

    analysis_cli.summaries.callback(
        graph_dir=tmp_path / "graph",
        output_dir=tmp_path / "out",
        max_motifs=None,
        max_quotes=None,
        llm_provider=None,
        llm_model=None,
        llm_reasoning=None,
        llm_mode="heuristic",
        dry_run_llm=False,
        codex_mode=True,
        alias_table=None,
    )

    config = captured["config"]
    assert config.codex_mode is True


def test_llm_motifs_cli_supports_batch_mode(monkeypatch, tmp_path):
    captured: dict[str, object] = {}
    batch_out = tmp_path / "batch_output.jsonl"

    class DummyPipeline:
        def __init__(self, config):
            captured["config"] = config
            self._artifacts = SimpleNamespace(
                speeches_parquet=tmp_path / "speeches.parquet",
                speech_motifs_parquet=tmp_path / "motifs.parquet",
                speech_motif_hits_parquet=tmp_path / "hits.parquet",
                payload_cache=None,
            )

        def run(self):
            return self._artifacts

    monkeypatch.setattr(analysis_cli, "SpeechMotifPipeline", DummyPipeline)

    analysis_cli.llm_motifs.callback(
        curated=None,
        taxonomy=None,
        output_dir=tmp_path / "out",
        max_motifs=None,
        llm_provider=None,
        llm_model=None,
        llm_reasoning=None,
        llm_max_output=None,
        llm_mode="batch",
        speech_level=True,
        dry_run_llm=False,
        no_payload_cache=False,
        batch_output=batch_out,
    )

    config = captured["config"]
    assert config.llm_mode == "batch"
    assert config.batch_output_path == batch_out


def test_llm_motifs_cli_rejects_batch_dry_run(tmp_path):
    with pytest.raises(ClickException):
        analysis_cli.llm_motifs.callback(
            curated=None,
            taxonomy=None,
            output_dir=tmp_path / "out",
            max_motifs=None,
            llm_provider=None,
            llm_model=None,
            llm_reasoning=None,
            llm_max_output=None,
            llm_mode="batch",
            speech_level=True,
            dry_run_llm=True,
            no_payload_cache=False,
            batch_output=tmp_path / "batch.jsonl",
        )


def test_llm_motifs_batch_build_submit(monkeypatch, tmp_path):
    captured: dict[str, object] = {}
    batch_input = tmp_path / "speech_batch_input.jsonl"
    batch_output = tmp_path / "speech_batch_output.jsonl"

    class DummyPipeline:
        def __init__(self, config):
            captured["config"] = config
            self.batch_input_path = batch_input
            self.batch_output_path = batch_output

        def build_batch_file(self, destination, llm_config):
            captured["build_destination"] = destination
            captured["llm_config"] = llm_config
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text("{}", encoding="utf-8")
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
            return SimpleNamespace(id="speech-batch-42")

        def poll(self, batch_id, interval):
            captured["polled"] = {"batch_id": batch_id, "interval": interval}
            return SimpleNamespace(status="completed", output_file_id="file-1")

        def retrieve(self, batch_id):
            captured["retrieved"] = batch_id
            return SimpleNamespace(status="completed", output_file_id="file-1")

        def download_output(self, batch, destination):
            destination.write_text("done", encoding="utf-8")
            captured["download_destination"] = destination
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
        "SpeechMotifPipeline",
        DummyPipeline,
    )
    monkeypatch.setattr(analysis_cli, "OpenAIBatchJob", DummyJob)

    analysis_cli.llm_motifs_batch.callback(
        curated=None,
        taxonomy=None,
        output_dir=tmp_path / "artifacts",
        batch_input=batch_input,
        batch_output=batch_output,
        max_motifs=None,
        llm_provider="openai",
        llm_model="gpt-4o-mini",
        llm_reasoning="medium",
        llm_max_output=None,
        speech_level=True,
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
    assert captured["polled"] == {"batch_id": "speech-batch-42", "interval": 5.0}
    assert Path(captured["download_destination"]) == batch_output
    assert captured["config"].llm_mode == "batch"


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
        alias_table=None,
        codex_mode=True,
    )

    assert Path(captured["build_destination"]) == batch_input
    assert captured["submitted"]["batch_file"] == batch_input
    assert captured["polled"] == {"batch_id": "batch-123", "interval": 5.0}
    assert Path(captured["download_destination"]) == batch_output
    assert captured["config"].llm_mode == "batch"
    assert captured["config"].codex_mode is True


def test_graph_cli_passes_alias_override(monkeypatch, tmp_path):
    captured: dict[str, object] = {}

    class DummyPipeline:
        def __init__(self, config):
            captured["config"] = config

        def run(self):
            return SimpleNamespace(
                graph_path=tmp_path / "graph.gpickle",
                graphml_path=tmp_path / "graph.graphml",
                entity_summary=tmp_path / "entity.parquet",
                entity_motif_stats=tmp_path / "motif.parquet",
                lore_hits=tmp_path / "hits.parquet",
                report_path=tmp_path / "report.json",
            )

    monkeypatch.setattr(
        analysis_cli,
        "NPCMotifGraphPipeline",
        DummyPipeline,
    )

    alias_override = tmp_path / "alias.csv"
    analysis_cli.graph.callback(
        curated=tmp_path / "lore.parquet",
        output_dir=tmp_path / "out",
        categories=("npc",),
        alias_table=alias_override,
    )

    config = captured["config"]
    assert config.alias_table_path == alias_override
