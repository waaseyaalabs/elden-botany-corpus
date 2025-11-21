"""Incremental ingestion manifest utilities."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class IncrementalSignature:
    """Helper for building stable record signatures."""

    dataset: str
    parts: tuple[str, ...]

    def to_hex(self) -> str:
        payload = "|".join((self.dataset, *self.parts))
        return sha256(payload.encode("utf-8")).hexdigest()


class IncrementalManifest:
    """Persisted record hashes used to skip redundant ingestion."""

    def __init__(self, path: Path) -> None:
        self.path = path
        if path.exists():
            try:
                self._data = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:  # pragma: no cover - corrupted
                message = f"Failed to parse manifest at {path}: {exc}"
                raise RuntimeError(message) from exc
        else:
            self._data: dict[str, Any] = {
                "version": 1,
                "datasets": {},
                "updated_at": datetime.now(UTC).isoformat(),
            }

    def should_skip(
        self,
        dataset: str,
        signature: str,
        *,
        since: datetime | None = None,
    ) -> bool:
        """Return True when a signature was processed prior to the cutoff.

        Args:
            dataset: Dataset key such as ``kaggle_base``.
            signature: Stable content signature returned by
                :func:`build_signature`.
            since: Optional timestamp; when provided only records older than
                this instant are considered processed.

        Returns:
            ``True`` when the manifest already contains ``signature`` for the
            dataset and, if supplied, the recorded timestamp predates
            ``since``.
        """

        dataset_bucket = self._data["datasets"].get(dataset)
        if not dataset_bucket:
            return False

        records: dict[str, str] = dataset_bucket.get("records", {})
        recorded_at = records.get(signature)
        if recorded_at is None:
            return False

        if since is None:
            return True

        recorded_dt = _parse_iso(recorded_at)
        return recorded_dt < since

    def record_signature(
        self,
        dataset: str,
        signature: str,
        *,
        timestamp: datetime | None = None,
    ) -> None:
        """Persist ``signature`` for ``dataset`` with an optional timestamp."""
        bucket = self._data["datasets"].setdefault(
            dataset,
            {"records": {}, "file_hashes": {}},
        )
        records: dict[str, str] = bucket.setdefault("records", {})
        records[signature] = (timestamp or datetime.now(UTC)).isoformat()
        bucket["last_recorded"] = records[signature]
        self._data["updated_at"] = records[signature]

    def update_file_hash(self, dataset: str, file_name: str, sha: str) -> None:
        """Record the latest hash for a supporting file such as a CSV."""
        bucket = self._data["datasets"].setdefault(
            dataset,
            {"records": {}, "file_hashes": {}},
        )
        hashes: dict[str, str] = bucket.setdefault("file_hashes", {})
        hashes[file_name] = sha
        bucket["last_file_hash"] = sha
        self._data["updated_at"] = datetime.now(UTC).isoformat()

    def save(self) -> None:
        """Write the manifest and ensure parent directories exist."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._data["updated_at"] = datetime.now(UTC).isoformat()
        payload = json.dumps(self._data, indent=2, sort_keys=True)
        self.path.write_text(payload, encoding="utf-8")


def build_signature(dataset: str, *parts: str) -> str:
    """Return a stable SHA256 signature for a dataset record.

    Args:
        dataset: Dataset key that anchors the signature namespace.
        parts: Salient pieces of the record (table name, slug, etc.).

    Returns:
        Hex-encoded SHA256 digest suitable for manifest lookups.
    """

    return IncrementalSignature(dataset=dataset, parts=tuple(parts)).to_hex()


def parse_since(value: str | None) -> datetime | None:
    """Parse an ISO-8601 timestamp into a timezone-aware datetime.

    The parser accepts timestamps with or without ``Z`` suffixes and normalizes
    them to UTC. Whitespace-only strings return ``None``.
    """

    if not value:
        return None
    normalized = value.strip()
    if not normalized:
        return None

    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError as exc:  # pragma: no cover - invalid user input
        raise ValueError(f"Invalid --since timestamp: {value}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _parse_iso(raw: str) -> datetime:
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)
