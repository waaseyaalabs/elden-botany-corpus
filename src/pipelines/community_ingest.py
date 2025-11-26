"""Community bundle ingestion pipeline for processed artifacts."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import pandas as pd
from corpus.community_bundle import (
    BundleOperation,
    CommunityBundle,
    load_bundle,
)
from corpus.community_schema import (
    CommunityAnnotation,
    CommunityAnnotationRevision,
    MotifTaxonomy,
    load_motif_taxonomy,
    utcnow,
)

ANNOTATIONS_FILENAME = "community_annotations.parquet"
REVISIONS_FILENAME = "community_revisions.parquet"
REFERENCES_FILENAME = "community_references.parquet"
SYMBOLISM_FILENAME = "community_symbolism.parquet"
MANIFEST_FILENAME = "community_manifest.json"
STATE_DIRNAME = "state"
CONFLICT_DIRNAME = "conflicts"
PROVENANCE_LOG = "provenance.log"

ANNOTATION_COLUMNS = [
    "annotation_id",
    "canonical_id",
    "chunk_id",
    "contributor_handle",
    "submission_channel",
    "status",
    "created_at",
    "updated_at",
    "bundle_id",
    "bundle_operation",
    "bundle_updated_at",
    "bundle_checksum",
    "revision_count",
    "latest_revision_id",
]

REVISION_COLUMNS = [
    "revision_id",
    "annotation_id",
    "version",
    "body",
    "motif_tags",
    "symbolism",
    "provenance_source_type",
    "provenance_source_name",
    "provenance_source_uri",
    "provenance_captured_at",
    "references_count",
    "confidence",
    "is_current",
    "review_state",
    "reviewer_handle",
    "review_notes",
    "submitted_at",
    "ingested_at",
    "bundle_id",
]

REFERENCE_COLUMNS = [
    "reference_id",
    "revision_id",
    "annotation_id",
    "reference_type",
    "title",
    "uri",
    "author",
    "notes",
    "published_at",
]

SYMBOLISM_COLUMNS = [
    "revision_id",
    "annotation_id",
    "field",
    "value",
]


@dataclass(slots=True)
class ConflictRecord:
    """Represents a bundle that could not be applied cleanly."""

    annotation_id: str
    bundle_id: str
    reason: str
    conflict_path: Path


@dataclass(slots=True)
class CommunityManifestEntry:
    """Single manifest entry tracking last applied bundle metadata."""

    annotation_id: str
    canonical_id: str
    contributor_handle: str
    bundle_id: str
    bundle_operation: str
    bundle_updated_at: datetime
    checksum: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "annotation_id": self.annotation_id,
            "canonical_id": self.canonical_id,
            "contributor_handle": self.contributor_handle,
            "bundle_id": self.bundle_id,
            "bundle_operation": self.bundle_operation,
            "bundle_updated_at": self.bundle_updated_at.isoformat(),
            "checksum": self.checksum,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> CommunityManifestEntry:
        return cls(
            annotation_id=payload["annotation_id"],
            canonical_id=payload["canonical_id"],
            contributor_handle=payload["contributor_handle"],
            bundle_id=payload["bundle_id"],
            bundle_operation=payload["bundle_operation"],
            bundle_updated_at=datetime.fromisoformat(
                payload["bundle_updated_at"]
            ),
            checksum=payload["checksum"],
        )


class CommunityManifest:
    """Persistent manifest stored alongside processed tables."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.entries: dict[str, CommunityManifestEntry] = {}
        if path.exists():
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                raw_entries = payload.get("entries", {})
                if isinstance(raw_entries, dict):
                    typed_entries = cast(dict[str, Any], raw_entries)
                    for key, value in typed_entries.items():
                        if not isinstance(value, dict):
                            continue
                        entry = CommunityManifestEntry.from_dict(
                            cast(dict[str, Any], value)
                        )
                        self.entries[str(key)] = entry

    def get(self, annotation_id: str) -> CommunityManifestEntry | None:
        return self.entries.get(annotation_id)

    def set(self, entry: CommunityManifestEntry) -> None:
        self.entries[entry.annotation_id] = entry

    def remove(self, annotation_id: str) -> None:
        self.entries.pop(annotation_id, None)

    def save(self) -> None:
        data = {
            "entries": {
                key: entry.to_dict() for key, entry in self.entries.items()
            }
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(data, indent=2), encoding="utf-8")


@dataclass(slots=True)
class CommunityIngestionResult:
    """Aggregate stats reported back to the CLI."""

    created: int = 0
    updated: int = 0
    deleted: int = 0
    skipped: int = 0
    conflicts: list[ConflictRecord] | None = None
    dry_run: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "created": self.created,
            "updated": self.updated,
            "deleted": self.deleted,
            "skipped": self.skipped,
            "conflicts": [
                {
                    "annotation_id": conflict.annotation_id,
                    "bundle_id": conflict.bundle_id,
                    "reason": conflict.reason,
                    "conflict_path": str(conflict.conflict_path),
                }
                for conflict in self.conflicts or []
            ],
            "dry_run": self.dry_run,
        }


class CommunityIngestionPipeline:
    """Orchestrates ingestion of bundles into processed artifacts."""

    def __init__(
        self,
        *,
        bundles_dir: Path,
        output_dir: Path,
        taxonomy: MotifTaxonomy | None = None,
    ) -> None:
        self.bundles_dir = bundles_dir
        self.output_dir = output_dir
        self.taxonomy = taxonomy or load_motif_taxonomy()
        self.manifest = CommunityManifest(output_dir / MANIFEST_FILENAME)
        self.state_dir = output_dir / STATE_DIRNAME
        self.conflict_dir = output_dir / CONFLICT_DIRNAME
        self.provenance_log = output_dir / PROVENANCE_LOG
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.conflict_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def ingest(
        self,
        bundles: Sequence[Path] | None = None,
        *,
        dry_run: bool = False,
        force: bool = False,
        allow_conflicts: bool = False,
        actor: str | None = None,
    ) -> CommunityIngestionResult:
        bundle_paths = (
            list(bundles) if bundles else self._discover_bundle_paths()
        )
        if not bundle_paths:
            return CommunityIngestionResult(dry_run=dry_run)

        loaded_bundles = [
            load_bundle(path, taxonomy=self.taxonomy) for path in bundle_paths
        ]

        result = CommunityIngestionResult(
            conflicts=[],
            dry_run=dry_run,
        )

        annotations_df = self._read_dataframe(
            self.annotations_path,
            ANNOTATION_COLUMNS,
        )
        revisions_df = self._read_dataframe(
            self.revisions_path,
            REVISION_COLUMNS,
        )
        references_df = self._read_dataframe(
            self.references_path,
            REFERENCE_COLUMNS,
        )
        symbolism_df = self._read_dataframe(
            self.symbolism_path,
            SYMBOLISM_COLUMNS,
        )

        for bundle in loaded_bundles:
            annotation_id = str(bundle.annotation.id)
            checksum = bundle.checksum()
            manifest_entry = self.manifest.get(annotation_id)
            if bundle.header.operation is BundleOperation.DELETE:
                if manifest_entry is None:
                    result.skipped += 1
                    continue
                if dry_run:
                    result.deleted += 1
                    continue
                annotations_df = self._drop_rows(
                    annotations_df,
                    "annotation_id",
                    annotation_id,
                )
                revisions_df = self._drop_rows(
                    revisions_df,
                    "annotation_id",
                    annotation_id,
                )
                references_df = self._drop_rows(
                    references_df,
                    "annotation_id",
                    annotation_id,
                )
                symbolism_df = self._drop_rows(
                    symbolism_df,
                    "annotation_id",
                    annotation_id,
                )
                self.manifest.remove(annotation_id)
                self._delete_state(annotation_id)
                self._append_log(
                    action="delete",
                    bundle=bundle,
                    actor=actor,
                )
                result.deleted += 1
                continue

            if manifest_entry and manifest_entry.checksum == checksum:
                result.skipped += 1
                continue

            if (
                manifest_entry
                and bundle.header.updated_at
                <= manifest_entry.bundle_updated_at
                and not force
            ):
                conflict = self._record_conflict(
                    bundle=bundle,
                    manifest_entry=manifest_entry,
                    reason="stale_bundle",
                )
                if result.conflicts is None:
                    result.conflicts = []
                result.conflicts.append(conflict)
                if not allow_conflicts:
                    continue

            if dry_run:
                if manifest_entry:
                    result.updated += 1
                else:
                    result.created += 1
                continue

            annotations_df = self._replace_annotation(
                annotations_df,
                bundle,
                checksum,
            )
            revisions_df = self._replace_revisions(revisions_df, bundle)
            references_df = self._replace_references(references_df, bundle)
            symbolism_df = self._replace_symbolism(symbolism_df, bundle)

            manifest_payload = CommunityManifestEntry(
                annotation_id=annotation_id,
                canonical_id=bundle.annotation.canonical_id,
                contributor_handle=bundle.annotation.contributor_handle,
                bundle_id=str(bundle.header.bundle_id),
                bundle_operation=bundle.header.operation.value,
                bundle_updated_at=bundle.header.updated_at,
                checksum=checksum,
            )
            self.manifest.set(manifest_payload)
            self._write_state(bundle.annotation)
            self._append_log(
                action="upsert",
                bundle=bundle,
                actor=actor,
            )
            if manifest_entry:
                result.updated += 1
            else:
                result.created += 1

        if not dry_run:
            annotations_df.to_parquet(self.annotations_path, index=False)
            revisions_df.to_parquet(self.revisions_path, index=False)
            references_df.to_parquet(self.references_path, index=False)
            symbolism_df.to_parquet(self.symbolism_path, index=False)
            self.manifest.save()

        return result

    @property
    def annotations_path(self) -> Path:
        return self.output_dir / ANNOTATIONS_FILENAME

    @property
    def revisions_path(self) -> Path:
        return self.output_dir / REVISIONS_FILENAME

    @property
    def references_path(self) -> Path:
        return self.output_dir / REFERENCES_FILENAME

    @property
    def symbolism_path(self) -> Path:
        return self.output_dir / SYMBOLISM_FILENAME

    def _discover_bundle_paths(self) -> list[Path]:
        if not self.bundles_dir.exists():
            return []
        return sorted(self.bundles_dir.glob("**/bundle.yml"))

    def _replace_annotation(
        self,
        frame: pd.DataFrame,
        bundle: CommunityBundle,
        checksum: str,
    ) -> pd.DataFrame:
        annotation_id = str(bundle.annotation.id)
        filtered = self._drop_rows(frame, "annotation_id", annotation_id)
        row = self._annotation_row(bundle, checksum)
        columns = (
            list(filtered.columns)
            if not filtered.empty
            else list(ANNOTATION_COLUMNS)
        )
        row_frame = pd.DataFrame([row], columns=columns)
        return pd.concat([filtered, row_frame], ignore_index=True)

    def _replace_revisions(
        self,
        frame: pd.DataFrame,
        bundle: CommunityBundle,
    ) -> pd.DataFrame:
        annotation_id = str(bundle.annotation.id)
        filtered = self._drop_rows(frame, "annotation_id", annotation_id)
        rows = [
            self._revision_row(rev, bundle)
            for rev in bundle.annotation.revisions
        ]
        return pd.concat([filtered, pd.DataFrame(rows)], ignore_index=True)

    def _replace_references(
        self,
        frame: pd.DataFrame,
        bundle: CommunityBundle,
    ) -> pd.DataFrame:
        annotation_id = str(bundle.annotation.id)
        filtered = self._drop_rows(frame, "annotation_id", annotation_id)
        rows: list[dict[str, Any]] = []
        for revision in bundle.annotation.revisions:
            rows.extend(self._reference_rows(revision))
        if not rows:
            return filtered
        return pd.concat([filtered, pd.DataFrame(rows)], ignore_index=True)

    def _replace_symbolism(
        self,
        frame: pd.DataFrame,
        bundle: CommunityBundle,
    ) -> pd.DataFrame:
        annotation_id = str(bundle.annotation.id)
        filtered = self._drop_rows(frame, "annotation_id", annotation_id)
        rows: list[dict[str, Any]] = []
        for revision in bundle.annotation.revisions:
            rows.extend(self._symbolism_rows(revision))
        if not rows:
            return filtered
        return pd.concat([filtered, pd.DataFrame(rows)], ignore_index=True)

    def _annotation_row(
        self,
        bundle: CommunityBundle,
        checksum: str,
    ) -> dict[str, Any]:
        current = bundle.annotation.current_revision()
        return {
            "annotation_id": str(bundle.annotation.id),
            "canonical_id": bundle.annotation.canonical_id,
            "chunk_id": (
                str(bundle.annotation.chunk_id)
                if bundle.annotation.chunk_id is not None
                else None
            ),
            "contributor_handle": bundle.annotation.contributor_handle,
            "submission_channel": bundle.annotation.submission_channel.value,
            "status": bundle.annotation.status.value,
            "created_at": bundle.annotation.created_at,
            "updated_at": bundle.annotation.updated_at,
            "bundle_id": str(bundle.header.bundle_id),
            "bundle_operation": bundle.header.operation.value,
            "bundle_updated_at": bundle.header.updated_at,
            "bundle_checksum": checksum,
            "revision_count": len(bundle.annotation.revisions),
            "latest_revision_id": str(current.id) if current else None,
        }

    def _revision_row(
        self,
        revision: CommunityAnnotationRevision,
        bundle: CommunityBundle,
    ) -> dict[str, Any]:
        return {
            "revision_id": str(revision.id),
            "annotation_id": str(revision.annotation_id),
            "version": revision.version,
            "body": revision.body,
            "motif_tags": revision.motif_tags,
            "symbolism": revision.symbolism.model_dump(mode="json"),
            "provenance_source_type": revision.provenance.source_type.value,
            "provenance_source_name": revision.provenance.source_name,
            "provenance_source_uri": (
                str(revision.provenance.source_uri)
                if revision.provenance.source_uri
                else None
            ),
            "provenance_captured_at": revision.provenance.captured_at,
            "references_count": len(revision.references),
            "confidence": (
                float(revision.confidence) if revision.confidence else None
            ),
            "is_current": revision.is_current,
            "review_state": revision.review_state.value,
            "reviewer_handle": revision.reviewer_handle,
            "review_notes": revision.review_notes,
            "submitted_at": revision.submitted_at,
            "ingested_at": utcnow(),
            "bundle_id": str(bundle.header.bundle_id),
        }

    def _reference_rows(
        self,
        revision: CommunityAnnotationRevision,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for reference in revision.references:
            rows.append(
                {
                    "reference_id": str(reference.id),
                    "revision_id": str(revision.id),
                    "annotation_id": str(revision.annotation_id),
                    "reference_type": reference.reference_type.value,
                    "title": reference.title,
                    "uri": str(reference.uri),
                    "author": reference.author,
                    "notes": reference.notes,
                    "published_at": reference.published_at,
                }
            )
        return rows

    def _symbolism_rows(
        self,
        revision: CommunityAnnotationRevision,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        symbolism_payload = revision.symbolism.model_dump(mode="json")
        for symbolism_field, values in symbolism_payload.items():
            for value in values:
                rows.append(
                    {
                        "revision_id": str(revision.id),
                        "annotation_id": str(revision.annotation_id),
                        "field": symbolism_field,
                        "value": value,
                    }
                )
        return rows

    def _read_dataframe(self, path: Path, columns: list[str]) -> pd.DataFrame:
        if path.exists():
            frame: pd.DataFrame = pd.read_parquet(path)
            return frame
        return pd.DataFrame(columns=columns)

    def _drop_rows(
        self,
        frame: pd.DataFrame,
        column: str,
        value: str,
    ) -> pd.DataFrame:
        if frame.empty:
            return frame
        mask = frame[column] != value
        return frame.loc[mask].reset_index(drop=True)

    def _write_state(self, annotation: CommunityAnnotation) -> None:
        payload = annotation.model_dump(mode="json")
        state_path = self.state_dir / f"{annotation.id}.json"
        state_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _delete_state(self, annotation_id: str) -> None:
        state_path = self.state_dir / f"{annotation_id}.json"
        if state_path.exists():
            state_path.unlink()

    def _record_conflict(
        self,
        *,
        bundle: CommunityBundle,
        manifest_entry: CommunityManifestEntry,
        reason: str,
    ) -> ConflictRecord:
        conflict_payload: dict[str, Any] = {
            "reason": reason,
            "bundle": bundle.to_summary(),
            "manifest_entry": manifest_entry.to_dict(),
        }
        conflict_path = self.conflict_dir / (
            f"{bundle.annotation.id}_{bundle.header.bundle_id}.json"
        )
        conflict_path.write_text(
            json.dumps(conflict_payload, indent=2),
            encoding="utf-8",
        )
        return ConflictRecord(
            annotation_id=str(bundle.annotation.id),
            bundle_id=str(bundle.header.bundle_id),
            reason=reason,
            conflict_path=conflict_path,
        )

    def _append_log(
        self,
        *,
        action: str,
        bundle: CommunityBundle,
        actor: str | None,
    ) -> None:
        actor_name = actor or "unknown"
        line = (
            f"[{utcnow().isoformat()}] action={action} "
            f"annotation_id={bundle.annotation.id} "
            f"bundle_id={bundle.header.bundle_id} actor={actor_name}\n"
        )
        with self.provenance_log.open("a", encoding="utf-8") as handle:
            handle.write(line)
