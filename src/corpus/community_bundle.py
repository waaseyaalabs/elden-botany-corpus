"""Helpers for loading, validating, and scaffolding community bundles."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, cast
from uuid import UUID, uuid4

import click
import yaml

from corpus.community_schema import (
    AnnotationDiff,
    AnnotationProvenance,
    CommunityAnnotation,
    CommunityAnnotationRevision,
    MotifTaxonomy,
    SubmissionChannel,
    load_motif_taxonomy,
    utcnow,
)

DEFAULT_BUNDLE_FILENAME = "bundle.yml"
DOCUMENT_VERSION = 1


def _json_default(value: Any) -> Any:
    """Helper to JSON-encode common types for hashing."""

    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, UUID):
        return str(value)
    return value


class BundleError(RuntimeError):
    """Raised when a bundle file cannot be parsed or validated."""


class BundleOperation(str, Enum):
    """Operations requested by a bundle."""

    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"


@dataclass(slots=True)
class CommunityBundleHeader:
    """Front-matter metadata stored alongside the annotation payload."""

    bundle_id: UUID
    created_at: datetime
    updated_at: datetime
    operation: BundleOperation
    document_version: int = DOCUMENT_VERSION


@dataclass(slots=True)
class CommunityBundle:
    """Wrapper around a bundle document on disk."""

    root: Path
    path: Path
    header: CommunityBundleHeader
    annotation: CommunityAnnotation
    notes: str | None = None

    def checksum(self) -> str:
        """Return a deterministic checksum for the bundle contents."""

        payload = self._to_document(include_notes=True)
        serialized = json.dumps(payload, default=_json_default, sort_keys=True)
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    def to_summary(self) -> dict[str, Any]:
        """Return a summary dictionary for status displays."""

        revisions = self.annotation.revisions
        latest = self.annotation.current_revision()
        return {
            "bundle_id": str(self.header.bundle_id),
            "annotation_id": str(self.annotation.id),
            "canonical_id": self.annotation.canonical_id,
            "contributor_handle": self.annotation.contributor_handle,
            "operation": self.header.operation.value,
            "version_count": len(revisions),
            "latest_version": latest.version if latest else None,
            "updated_at": self.header.updated_at.isoformat(),
            "status": self.annotation.status.value,
        }

    def diff_against(
        self,
        other: CommunityAnnotation | None,
    ) -> AnnotationDiff | None:
        """Return an AnnotationDiff compared to a previous annotation."""

        current_revision = self.annotation.current_revision()
        if current_revision is None:
            return None
        if other is None:
            return AnnotationDiff.between(None, current_revision)
        previous_revision = other.current_revision()
        if previous_revision is None:
            return None
        return AnnotationDiff.between(previous_revision, current_revision)

    def validate(self, taxonomy: MotifTaxonomy | None = None) -> None:
        """Validate motif tags using the taxonomy, if provided."""

        if taxonomy is None:
            return
        for revision in self.annotation.revisions:
            revision.ensure_motifs_are_known(taxonomy)

    def write(self) -> None:
        """Persist the bundle back to its YAML file."""

        document = self._to_document(include_notes=True)
        yaml.safe_dump(
            document,
            self.path.open("w", encoding="utf-8"),
            sort_keys=False,
            indent=2,
            default_flow_style=False,
        )

    def _to_document(self, *, include_notes: bool) -> dict[str, Any]:
        """Return a serialisable dictionary representing the bundle."""

        annotation_payload: dict[str, Any] = self.annotation.model_dump(
            mode="json"
        )
        revisions: list[Any] = list(annotation_payload.pop("revisions", []))
        document: dict[str, Any] = {
            "bundle": {
                "id": str(self.header.bundle_id),
                "created_at": self.header.created_at.isoformat(),
                "updated_at": self.header.updated_at.isoformat(),
                "operation": self.header.operation.value,
                "document_version": self.header.document_version,
            },
            "annotation": annotation_payload,
            "revisions": revisions,
        }
        if include_notes and self.notes is not None:
            document["notes"] = self.notes
        return document


def _coerce_path(path: Path | str | None) -> Path:
    if path is None:
        raise BundleError("Bundle path cannot be None")
    resolved = Path(path)
    if resolved.is_dir():
        resolved = resolved / DEFAULT_BUNDLE_FILENAME
    return resolved


def load_bundle(
    path: Path | str,
    *,
    taxonomy: MotifTaxonomy | None = None,
) -> CommunityBundle:
    """Load a bundle YAML file into typed models."""

    bundle_path = _coerce_path(path)
    if not bundle_path.exists():
        raise BundleError(f"Bundle file not found: {bundle_path}")

    with bundle_path.open("r", encoding="utf-8") as handle:
        raw_payload = yaml.safe_load(handle)

    if not isinstance(raw_payload, dict):
        raise BundleError("Bundle YAML must be a mapping")
    payload: dict[str, Any] = cast(dict[str, Any], raw_payload)

    bundle_section = payload.get("bundle")
    if not bundle_section:
        raise BundleError("Missing 'bundle' section in bundle YAML")
    if not isinstance(bundle_section, dict):
        raise BundleError("'bundle' section must be a mapping")
    bundle_map: dict[str, Any] = cast(dict[str, Any], bundle_section)

    header = CommunityBundleHeader(
        bundle_id=UUID(str(bundle_map.get("id", uuid4()))),
        created_at=_parse_datetime(bundle_map.get("created_at")),
        updated_at=_parse_datetime(bundle_map.get("updated_at")),
        operation=BundleOperation(bundle_map.get("operation", "create")),
        document_version=int(
            bundle_map.get("document_version", DOCUMENT_VERSION)
        ),
    )

    annotation_section = payload.get("annotation")
    if annotation_section is None:
        annotation_map: dict[str, Any] = {}
    else:
        if not isinstance(annotation_section, dict):
            raise BundleError("'annotation' section must be a mapping")
        annotation_map = cast(dict[str, Any], annotation_section)

    revisions_section = payload.get("revisions")
    if revisions_section is None:
        revisions_payload: list[dict[str, Any]] = []
    else:
        if not isinstance(revisions_section, list):
            raise BundleError("'revisions' section must be a list")
        revisions_payload = []
        entries = cast(list[Any], revisions_section)
        for entry in entries:
            if not isinstance(entry, dict):
                raise BundleError("Revision entries must be mappings")
            revisions_payload.append(cast(dict[str, Any], entry))

    # Backwards compatibility: allow revisions embedded inside annotation
    embedded_revisions = annotation_map.pop("revisions", [])
    if embedded_revisions and not revisions_payload:
        revisions_payload = [
            cast(dict[str, Any], rev) for rev in embedded_revisions
        ]

    annotation = CommunityAnnotation(**annotation_map)
    revisions: list[CommunityAnnotationRevision] = []
    for revision_payload in revisions_payload:
        revisions.append(CommunityAnnotationRevision(**revision_payload))
    if revisions:
        annotation.revisions = revisions

    bundle = CommunityBundle(
        root=bundle_path.parent,
        path=bundle_path,
        header=header,
        annotation=annotation,
        notes=payload.get("notes"),
    )

    bundle.validate(taxonomy)
    return bundle


def scaffold_bundle(
    *,
    root: Path,
    canonical_id: str,
    contributor_handle: str,
    submission_channel: SubmissionChannel,
    taxonomy: MotifTaxonomy | None = None,
    chunk_id: str | None = None,
    motif_tags: Iterable[str] | None = None,
    body: str | None = None,
    notes: str | None = None,
) -> CommunityBundle:
    """Create a new bundle directory with a starter annotation payload."""

    taxonomy = taxonomy or load_motif_taxonomy()
    motif_tags_list = [tag.strip().lower() for tag in motif_tags or [] if tag]
    if motif_tags_list:
        taxonomy.ensure(motif_tags_list)

    if not canonical_id:
        raise BundleError("Canonical ID is required to scaffold a bundle")
    if not contributor_handle:
        raise BundleError("Contributor handle is required")

    root.mkdir(parents=True, exist_ok=True)
    _ensure_references_dir(root)
    bundle_path = root / DEFAULT_BUNDLE_FILENAME

    annotation = CommunityAnnotation(
        canonical_id=canonical_id,
        chunk_id=UUID(chunk_id) if chunk_id else None,
        contributor_handle=contributor_handle,
        submission_channel=submission_channel,
    )
    provenance = AnnotationProvenance(
        source_type=submission_channel,
        source_name=contributor_handle,
    )
    revision = CommunityAnnotationRevision(
        annotation_id=annotation.id,
        version=1,
        body=body or "TODO: Add annotation body",
        motif_tags=motif_tags_list,
        provenance=provenance,
    )
    annotation.add_revision(revision, taxonomy)

    header = CommunityBundleHeader(
        bundle_id=uuid4(),
        created_at=utcnow(),
        updated_at=utcnow(),
        operation=BundleOperation.CREATE,
    )

    bundle = CommunityBundle(
        root=root,
        path=bundle_path,
        header=header,
        annotation=annotation,
        notes=notes,
    )
    bundle.write()
    _write_readme(root)
    _write_notes(root, notes)
    return bundle


def discover_bundle_files(root: Path) -> list[Path]:
    """Return all bundle files under the provided root directory."""

    if not root.exists():
        return []
    bundle_files = sorted(root.glob(f"**/{DEFAULT_BUNDLE_FILENAME}"))
    return bundle_files


def _write_readme(root: Path) -> None:
    """Write a short README describing the bundle contents."""

    readme_path = root / "README.md"
    if readme_path.exists():
        return
    template = (
        "# Community Annotation Bundle\n\n"
        f"- **Bundle ID**: {root.name}\n"
        f"- **Generated**: {utcnow().isoformat()}\n\n"
        "Edit `bundle.yml` to provide annotation text, motif tags, "
        "symbolism metadata, and references.\n"
        f"Run `poetry run corpus community validate {root}` before submitting"
        " the bundle.\n"
    )
    readme_path.write_text(template, encoding="utf-8")


def _write_notes(root: Path, notes: str | None) -> None:
    """Create notes.md so reviewers have a shared scratchpad."""

    notes_path = root / "notes.md"
    if notes_path.exists():
        return
    body = [
        "# Reviewer Notes",
        "",
        "Use this file to track review handoffs, context, and follow-ups.",
        "",
    ]
    if notes:
        body.append(notes.strip())
    else:
        body.append("_No notes captured yet._")
    body.append("")
    notes_path.write_text("\n".join(body), encoding="utf-8")


def _ensure_references_dir(root: Path) -> None:
    """Ensure references/ exists with a placeholder for git hygiene."""

    references_dir = root / "references"
    references_dir.mkdir(parents=True, exist_ok=True)
    gitkeep = references_dir / ".gitkeep"
    if not gitkeep.exists():
        gitkeep.write_text("", encoding="utf-8")


def _parse_datetime(value: str | datetime | None) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    return utcnow()


def prompt_for_motif_tags(taxonomy: MotifTaxonomy) -> list[str]:
    """Prompt the user interactively for motif tags with validation."""

    available = sorted(
        motif.slug
        for category in taxonomy.categories
        for motif in category.motifs
    )
    click.echo("Available motifs: " + ", ".join(available))
    raw = click.prompt(
        "Enter motif slugs (comma-separated)",
        default="",
        show_default=False,
    )
    tags = [tag.strip().lower() for tag in raw.split(",") if tag.strip()]
    if tags:
        taxonomy.ensure(tags)
    return tags


def prompt_for_body() -> str:
    """Open the user's editor to capture a revision body."""

    template = "# Enter annotation body below\n"
    edited = click.edit(text=template)
    if edited is None:
        raise BundleError("Annotation body entry aborted by user")
    lines = [line for line in edited.splitlines() if not line.startswith("#")]
    body = "\n".join(line.rstrip() for line in lines if line.strip())
    if not body:
        raise BundleError("Annotation body cannot be empty")
    return body


def prompt_for_notes() -> str | None:
    """Optional helper for capturing reviewer notes while scaffolding."""

    capture = click.confirm("Add reviewer notes to bundle?", default=False)
    if not capture:
        return None
    edited = click.edit(text="# Add reviewer notes below\n")
    if edited is None:
        return None
    return edited.strip() or None
