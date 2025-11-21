# pyright: reportMissingImports=false
# pyright: reportMissingModuleSource=false
# pyright: reportMissingTypeStubs=false
# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false

from __future__ import annotations

import hashlib
from importlib import import_module
from pathlib import Path

rag_guard = import_module("pipelines.rag_guard")
build_guard_state = rag_guard.build_guard_state
describe_status = rag_guard.describe_status
load_guard_state = rag_guard.load_guard_state
needs_rebuild = rag_guard.needs_rebuild
write_guard_state = rag_guard.write_guard_state


def _hash_bytes(payload: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(payload)
    return digest.hexdigest()


def test_build_guard_state_uses_file_hashes(tmp_path: Path) -> None:
    lore_path = tmp_path / "lore.parquet"
    lore_path.write_bytes(b"lore-text")
    weights_path = tmp_path / "weights.yml"
    weights_path.write_text("dialogue: 0.7\n", encoding="utf-8")

    state = build_guard_state(
        lore_path=lore_path,
        weight_path=weights_path,
        embed_provider="local",
        embed_model="all-MiniLM-L6-v2",
        reranker_name="identity",
        reranker_model="none",
    )

    assert state["files"]["lore_corpus"] == _hash_bytes(b"lore-text")
    assert state["weights"]["path"] == str(weights_path)
    assert state["config"]["embed_provider"] == "local"
    assert "fingerprint" in state


def test_build_guard_state_supports_inline_weights(tmp_path: Path) -> None:
    lore_path = tmp_path / "lore.parquet"
    lore_path.write_bytes(b"abc")

    state = build_guard_state(
        lore_path=lore_path,
        inline_weights={"dialogue": 0.7, "lore": 1.2},
    )

    assert state["weights"]["inline"] is True
    assert state["weights"]["values"]["dialogue"] == 0.7
    assert "text_type_weights" not in state["files"]


def test_guard_state_persistence_and_status(tmp_path: Path) -> None:
    lore_path = tmp_path / "lore.parquet"
    lore_path.write_bytes(b"initial")
    weights_path = tmp_path / "weights.yml"
    weights_path.write_text("dialogue: 0.7\n", encoding="utf-8")
    guard_path = tmp_path / "guard.json"

    current = build_guard_state(
        lore_path=lore_path,
        weight_path=weights_path,
    )
    write_guard_state(current, state_path=guard_path)

    stored = load_guard_state(guard_path)
    assert stored is not None
    assert stored["fingerprint"] == current["fingerprint"]
    assert needs_rebuild(current, stored) is False

    status = describe_status(
        current_state=current,
        stored_state=stored,
        state_path=guard_path,
    )
    assert "aligned" in status

    lore_path.write_bytes(b"modified")
    updated = build_guard_state(
        lore_path=lore_path,
        weight_path=weights_path,
    )
    assert needs_rebuild(updated, stored) is True
    missing_status = describe_status(
        current_state=current,
        stored_state=None,
        state_path=guard_path,
    )
    assert "No RAG rebuild state" in missing_status
