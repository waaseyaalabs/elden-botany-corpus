from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from corpus import cli


class ManifestSpy:
    """Capture manifest interactions during CLI runs."""

    def __init__(self) -> None:
        self.saved_calls = 0

    def save(self) -> None:
        self.saved_calls += 1


@pytest.fixture()
def temp_data_dirs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> SimpleNamespace:
    data_dir = tmp_path / "data"
    raw_dir = data_dir / "raw"
    curated_dir = data_dir / "curated"
    for path in (data_dir, raw_dir, curated_dir):
        path.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(cli.settings, "data_dir", data_dir)
    monkeypatch.setattr(cli.settings, "raw_dir", raw_dir)
    monkeypatch.setattr(cli.settings, "curated_dir", curated_dir)
    return SimpleNamespace(
        data_dir=data_dir,
        raw_dir=raw_dir,
        curated_dir=curated_dir,
    )


@pytest.fixture()
def manifest_spy(monkeypatch: pytest.MonkeyPatch) -> ManifestSpy:
    spy = ManifestSpy()

    def fake_load_manifest(enabled: bool):
        return spy if enabled else None

    monkeypatch.setattr(cli, "_load_manifest", fake_load_manifest)
    return spy


def test_fetch_since_treats_manifest_as_read_only(
    monkeypatch: pytest.MonkeyPatch,
    temp_data_dirs: SimpleNamespace,
    manifest_spy: ManifestSpy,
) -> None:
    runner = CliRunner()

    record_state_flags: list[bool] = []

    def fake_fetch_kaggle_data(
        *,
        include_base: bool,
        include_dlc: bool,
        incremental: bool,
        since: object,
        manifest: ManifestSpy | None,
        record_state: bool,
    ) -> list[SimpleNamespace]:
        assert include_base or include_dlc
        assert incremental is True
        assert manifest is manifest_spy
        record_state_flags.append(record_state)
        return [SimpleNamespace(is_dlc=False), SimpleNamespace(is_dlc=True)]

    def fail_if_called(
        *args: object,
        **kwargs: object,
    ) -> None:  # pragma: no cover - defensive
        raise AssertionError("unexpected call in fetch test")

    monkeypatch.setattr(cli, "fetch_kaggle_data", fake_fetch_kaggle_data)
    monkeypatch.setattr(cli, "fetch_impalers_data", fail_if_called)
    monkeypatch.setattr(cli, "fetch_carian_fmg_files", fail_if_called)

    result = runner.invoke(
        cli.main,
        [
            "fetch",
            "--since",
            "2024-01-01T00:00:00Z",
            "--no-github",
            "--no-impalers",
            "--no-carian",
        ],
    )

    assert result.exit_code == 0, result.output
    assert record_state_flags == [False]
    assert manifest_spy.saved_calls == 0


def test_curate_incremental_persists_manifest(
    monkeypatch: pytest.MonkeyPatch,
    temp_data_dirs: SimpleNamespace,
    manifest_spy: ManifestSpy,
) -> None:
    runner = CliRunner()

    kaggle_record_states: list[bool] = []
    impalers_record_states: list[bool] = []

    def fake_fetch_kaggle_data(
        *,
        include_base: bool,
        include_dlc: bool,
        incremental: bool,
        since: object,
        manifest: ManifestSpy | None,
        record_state: bool,
    ) -> list[SimpleNamespace]:
        assert include_base and include_dlc
        assert incremental is True
        assert manifest is manifest_spy
        kaggle_record_states.append(record_state)
        return [SimpleNamespace(is_dlc=False), SimpleNamespace(is_dlc=True)]

    def fake_fetch_impalers_data(
        *,
        incremental: bool,
        since: object,
        manifest: ManifestSpy | None,
        record_state: bool,
    ) -> list[SimpleNamespace]:
        assert incremental is True
        assert manifest is manifest_spy
        impalers_record_states.append(record_state)
        return []

    def fake_fetch_github_api_data() -> list[SimpleNamespace]:
        return []

    def fake_load_reconciled_state(_path: Path) -> list[SimpleNamespace]:
        return [SimpleNamespace(entity_id="baseline", is_dlc=False)]

    def fake_reconcile_all_sources(
        **kwargs: object,
    ) -> tuple[list[SimpleNamespace], list[SimpleNamespace]]:
        return [SimpleNamespace(entity_id="new", is_dlc=False)], []

    def fake_build_entity_map(
        baseline: list[SimpleNamespace],
    ) -> dict[str, list[SimpleNamespace]]:
        assert baseline
        return {"baseline": baseline}

    def fake_diff_entities(
        entities: list[SimpleNamespace],
        baseline_map: dict[str, list[SimpleNamespace]],
    ) -> list[SimpleNamespace]:
        assert baseline_map
        return [SimpleNamespace(entity_id="new")]

    def fake_curate_corpus(*args: object, **kwargs: object) -> list[int]:
        return [1, 2, 3]

    monkeypatch.setattr(cli, "fetch_kaggle_data", fake_fetch_kaggle_data)
    monkeypatch.setattr(cli, "fetch_impalers_data", fake_fetch_impalers_data)
    monkeypatch.setattr(
        cli,
        "fetch_github_api_data",
        fake_fetch_github_api_data,
    )
    monkeypatch.setattr(
        cli,
        "load_reconciled_state",
        fake_load_reconciled_state,
    )
    monkeypatch.setattr(
        cli,
        "reconcile_all_sources",
        fake_reconcile_all_sources,
    )
    monkeypatch.setattr(cli, "build_entity_map", fake_build_entity_map)
    monkeypatch.setattr(cli, "diff_entities", fake_diff_entities)
    monkeypatch.setattr(cli, "curate_corpus", fake_curate_corpus)

    result = runner.invoke(
        cli.main,
        ["curate", "--incremental", "--no-quality"],
    )

    assert result.exit_code == 0, result.output
    assert kaggle_record_states == [True]
    assert impalers_record_states == [True]
    assert manifest_spy.saved_calls == 1
