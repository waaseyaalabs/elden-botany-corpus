"""Checksum guard for lore embeddings and RAG index inputs."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from corpus.config import settings

DEFAULT_LORE_PATH = settings.curated_dir / "lore_corpus.parquet"
DEFAULT_WEIGHT_PATH = Path("config/text_type_weights.yml")
DEFAULT_STATE_PATH = (
    settings.data_dir / "embeddings" / "rag_rebuild_state.json"
)
_HASH_BUFFER = 1024 * 1024


def build_guard_state(
    *,
    lore_path: Path = DEFAULT_LORE_PATH,
    weight_path: Path | None = DEFAULT_WEIGHT_PATH,
    inline_weights: Mapping[str, float] | None = None,
    embed_provider: str | None = None,
    embed_model: str | None = None,
    reranker_name: str | None = None,
    reranker_model: str | None = None,
) -> dict[str, Any]:
    """Return the fingerprint payload for the current lore + config inputs."""

    provider = embed_provider or settings.embed_provider
    model = embed_model or settings.embed_model
    rerank_name = reranker_name or settings.reranker_name
    rerank_model = reranker_model or settings.reranker_model

    files: dict[str, str] = {
        "lore_corpus": _hash_file(lore_path),
    }

    if inline_weights is None:
        resolved_weights = weight_path or DEFAULT_WEIGHT_PATH
        files["text_type_weights"] = _hash_file(resolved_weights)
        weight_descriptor: dict[str, Any] = {"path": str(resolved_weights)}
    else:
        weight_descriptor = {
            "inline": True,
            "values": _canonicalize_weights(inline_weights),
        }

    config = {
        "embed_provider": provider,
        "embed_model": model,
        "reranker_name": rerank_name,
        "reranker_model": rerank_model,
    }

    payload: dict[str, Any] = {
        "files": files,
        "weights": weight_descriptor,
        "config": config,
    }
    fingerprint = _fingerprint(payload)
    payload["fingerprint"] = fingerprint
    return payload


def write_guard_state(
    state: Mapping[str, Any],
    *,
    state_path: Path = DEFAULT_STATE_PATH,
) -> None:
    """Persist the guard fingerprint to disk."""

    payload: dict[str, Any] = {
        **state,
        "generated_at": datetime.now(UTC).isoformat(),
    }
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_guard_state(
    state_path: Path = DEFAULT_STATE_PATH,
) -> dict[str, Any] | None:
    """Return the stored guard fingerprint, if it exists."""

    if not state_path.exists():
        return None
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):  # pragma: no cover - defensive
            message = f"Guard state at {state_path} must be a JSON object"
            raise RuntimeError(message)
        return cast(dict[str, Any], payload)
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        message = f"Failed to parse guard state at {state_path}: {exc}"
        raise RuntimeError(message) from exc


def needs_rebuild(
    current_state: Mapping[str, Any],
    stored_state: Mapping[str, Any] | None,
) -> bool:
    """Return True when artifacts must be rebuilt."""

    if not stored_state:
        return True
    return current_state.get("fingerprint") != stored_state.get("fingerprint")


def describe_status(
    *,
    current_state: Mapping[str, Any],
    stored_state: Mapping[str, Any] | None,
    state_path: Path = DEFAULT_STATE_PATH,
) -> str:
    """Return a human-readable status message."""

    if stored_state is None:
        try:
            relative_path = state_path.relative_to(settings.data_dir.parent)
        except ValueError:
            relative_path = state_path
        return (
            "No RAG rebuild state recorded. "
            "Run 'make rag-embeddings && make rag-index' "
            f"to generate artifacts and {relative_path}."
        )
    if needs_rebuild(current_state, stored_state):
        return (
            "Lore corpus or config inputs changed since the last RAG build. "
            "Re-run 'make rag-embeddings && make rag-index' "
            "to refresh artifacts."
        )
    return (
        "RAG embeddings and index are aligned with the current lore corpus "
        f"and configs. Guard file: {state_path}"
    )


def _hash_file(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Required file not found: {path}")
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(_HASH_BUFFER)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _canonicalize_weights(weights: Mapping[str, float]) -> dict[str, float]:
    return {
        str(key).lower(): float(weights[key])
        for key in sorted(weights.keys(), key=lambda item: str(item).lower())
    }


def _fingerprint(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Check whether lore embeddings/RAG index require a rebuild"
        ),
    )
    parser.add_argument(
        "command",
        choices=("status", "print"),
        nargs="?",
        default="status",
        help="Action to perform",
    )
    parser.add_argument(
        "--state",
        dest="state_path",
        type=Path,
        default=DEFAULT_STATE_PATH,
        help="Location of the recorded guard state JSON",
    )
    parser.add_argument(
        "--lore",
        dest="lore_path",
        type=Path,
        default=DEFAULT_LORE_PATH,
        help="Path to lore_corpus.parquet",
    )
    parser.add_argument(
        "--weights",
        dest="weights_path",
        type=Path,
        default=DEFAULT_WEIGHT_PATH,
        help="Path to text_type_weights.yml",
    )
    parser.add_argument(
        "--provider",
        dest="embed_provider",
        choices=("local", "openai", "none"),
        help="Embedding provider to assume when comparing state",
    )
    parser.add_argument(
        "--model",
        dest="embed_model",
        help="Embedding model identifier",
    )
    parser.add_argument(
        "--reranker-name",
        dest="reranker_name",
        help="Reranker name used when querying",
    )
    parser.add_argument(
        "--reranker-model",
        dest="reranker_model",
        help="Reranker model identifier",
    )
    return parser.parse_args()


def _run_cli() -> None:
    args = _parse_args()
    current = build_guard_state(
        lore_path=args.lore_path,
        weight_path=args.weights_path,
        embed_provider=args.embed_provider,
        embed_model=args.embed_model,
        reranker_name=args.reranker_name,
        reranker_model=args.reranker_model,
    )
    stored = load_guard_state(args.state_path)

    if args.command == "print":
        print(json.dumps({"current": current, "stored": stored}, indent=2))
        return

    message = describe_status(
        current_state=current,
        stored_state=stored,
        state_path=args.state_path,
    )
    print(message)
    if needs_rebuild(current, stored):
        raise SystemExit(1)


if __name__ == "__main__":  # pragma: no cover - CLI utility
    _run_cli()
