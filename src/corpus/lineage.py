"""Utilities for building lineage manifests."""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from corpus.models import Provenance, RawEntity
from corpus.utils import save_json


class LineageManifestBuilder:
    """Create JSON lineage manifests from curated entities."""

    def __init__(
        self,
        output_root: Path,
        relative_root: Path | None = None,
    ) -> None:
        self.output_root = output_root
        self.relative_root = relative_root or output_root

    def build(self, entities: list[RawEntity]) -> dict[str, Any]:
        """Emit lineage manifests grouped by entity type."""
        grouped: dict[str, list[RawEntity]] = defaultdict(list)
        for entity in entities:
            grouped[entity.entity_type].append(entity)

        self.output_root.mkdir(parents=True, exist_ok=True)

        summary: dict[str, Any] = {
            "root": self._relative_path(self.output_root),
            "datasets": {},
            "total_records": 0,
        }

        total_records = 0
        for entity_type, bucket in grouped.items():
            records = [self._serialize_entity(item) for item in bucket]
            records.sort(key=lambda item: item["slug"])

            file_path = self.output_root / f"{entity_type}.json"
            save_json(records, file_path)

            summary["datasets"][entity_type] = {
                "path": self._relative_path(file_path),
                "records": len(records),
            }
            total_records += len(records)

        index_payload: dict[str, Any] = {
            "generated_at": datetime.now(UTC).isoformat(),
            "total_records": total_records,
            "datasets": {
                entity_type: meta["path"] for entity_type, meta in summary["datasets"].items()
            },
        }

        index_path = self.output_root / "index.json"
        save_json(index_payload, index_path)

        summary["index"] = self._relative_path(index_path)
        summary["total_records"] = total_records
        return summary

    def _serialize_entity(self, entity: RawEntity) -> dict[str, Any]:
        """Convert a RawEntity into a lineage record."""
        return {
            "slug": entity.to_slug(),
            "entity_type": entity.entity_type,
            "name": entity.name,
            "sources": [self._serialize_provenance(p) for p in entity.provenance],
        }

    @staticmethod
    def _serialize_provenance(prov: Provenance) -> dict[str, Any]:
        """Convert provenance metadata into JSON-serializable payload."""
        return {
            "source": prov.source,
            "dataset": prov.dataset,
            "source_file": prov.source_file,
            "uri": prov.uri,
            "sha256": prov.sha256,
            "retrieved_at": prov.retrieved_at.isoformat(),
        }

    def _relative_path(self, path: Path) -> str:
        """Return the relative path from the configured root."""
        return path.relative_to(self.relative_root).as_posix()
