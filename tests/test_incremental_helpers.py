"""Tests for incremental manifest and curator state helpers."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from corpus.curate import (
    build_entity_map,
    diff_entities,
    load_reconciled_state,
    save_reconciled_state,
)
from corpus.incremental import (
    IncrementalManifest,
    build_signature,
    parse_since,
)
from corpus.models import RawEntity


def test_manifest_should_skip_respects_since(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest = IncrementalManifest(manifest_path)
    signature = build_signature("dataset", "table", "entity")

    assert not manifest.should_skip("dataset", signature)

    recorded_at = datetime(2025, 11, 15, tzinfo=UTC)
    manifest.record_signature("dataset", signature, timestamp=recorded_at)
    assert manifest.should_skip("dataset", signature)

    cutoff_recent = recorded_at - timedelta(days=1)
    cutoff_future = recorded_at + timedelta(days=1)

    assert not manifest.should_skip("dataset", signature, since=cutoff_recent)
    assert manifest.should_skip("dataset", signature, since=cutoff_future)


def test_reconciled_state_round_trip(tmp_path: Path) -> None:
    output_dir = tmp_path / "curated"
    entities = [
        RawEntity(
            entity_type="weapon",
            name="Moonblade",
            description="A blade of moonlight.",
            raw_data={"name": "Moonblade"},
        )
    ]

    save_reconciled_state(entities, output_dir=output_dir)
    loaded = load_reconciled_state(output_dir=output_dir)

    assert len(loaded) == 1
    assert loaded[0].name == "Moonblade"


def test_diff_entities_detects_changes() -> None:
    baseline = RawEntity(
        entity_type="armor",
        name="Bloom Robe",
        description="A robe.",
        raw_data={},
    )
    updated = RawEntity(
        entity_type="armor",
        name="Bloom Robe",
        description="A robe with thorns.",
        raw_data={},
    )

    baseline_map = build_entity_map([baseline])
    delta = diff_entities([updated], baseline_map)

    assert delta == [updated]


def test_load_reconciled_state_missing_file(tmp_path: Path) -> None:
    assert load_reconciled_state(output_dir=tmp_path) == []


def test_manifest_persists_to_disk(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest = IncrementalManifest(manifest_path)
    signature = build_signature("dataset", "table", "entity")
    manifest.record_signature("dataset", signature)
    manifest.save()

    reloaded = IncrementalManifest(manifest_path)
    assert reloaded.should_skip("dataset", signature)


def test_parse_since_normalizes_timezone() -> None:
    parsed = parse_since("2025-11-15T12:00:00Z")
    assert parsed is not None
    assert parsed.tzinfo is UTC
    assert parsed.isoformat() == "2025-11-15T12:00:00+00:00"


def test_parse_since_handles_empty_values() -> None:
    assert parse_since(None) is None
    assert parse_since("   ") is None
