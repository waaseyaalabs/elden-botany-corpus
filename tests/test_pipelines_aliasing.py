"""Tests for the pipelines.aliasing helpers."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from pipelines.aliasing import load_alias_map


def test_load_alias_map_returns_empty_when_missing(tmp_path: Path, caplog):
    caplog.set_level("WARNING")
    missing = tmp_path / "entity_aliases.csv"
    result = load_alias_map(missing)
    assert result == {}
    assert "missing" in caplog.text


def test_load_alias_map_supports_parquet(tmp_path: Path) -> None:
    alias_path = tmp_path / "aliases.parquet"
    frame = pd.DataFrame(
        [
            {"alias_id": "npc:test", "canonical_id": "npc:canonical"},
            {"alias_id": "npc:other", "canonical_id": "npc:canonical"},
        ]
    )
    frame.to_parquet(alias_path, index=False)

    alias_map = load_alias_map(alias_path)
    assert alias_map == {
        "npc:test": "npc:canonical",
        "npc:other": "npc:canonical",
    }


def test_load_alias_map_validates_columns(tmp_path: Path) -> None:
    alias_path = tmp_path / "bad_aliases.csv"
    pd.DataFrame([{"foo": "bar"}]).to_csv(alias_path, index=False)

    with pytest.raises(ValueError):
        load_alias_map(alias_path)
