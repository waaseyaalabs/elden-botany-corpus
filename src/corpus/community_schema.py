from __future__ import annotations

"""Pydantic models for the Community Corpus annotation schema."""

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Iterable
from uuid import UUID, uuid4

import yaml
from pydantic import (
    BaseModel,
    Field,
    HttpUrl,
    field_validator,
    model_validator,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TAXONOMY_PATH = PROJECT_ROOT / "config" / "community_motifs.yml"


def utcnow() -> datetime:
    """Return a timezone-aware UTC timestamp."""

    return datetime.now(tz=timezone.utc)


class SubmissionChannel(str, Enum):
    """Channels that can submit annotations."""

    MANUAL = "manual"
    IMPORT = "import"
    LLM = "llm"
    CURATED = "curated"


class AnnotationStatus(str, Enum):
    """Lifecycle states for an annotation shell."""

    DRAFT = "draft"
    SUBMITTED = "submitted"
    APPROVED = "approved"
    REJECTED = "rejected"
    ARCHIVED = "archived"


class AnnotationReviewState(str, Enum):
    """Review decisions recorded on individual revisions."""

    PENDING = "pending"
    APPROVED = "approved"
    NEEDS_CHANGES = "needs_changes"
    REJECTED = "rejected"


class ReferenceType(str, Enum):
    """Supported citation artifact types."""

    VIDEO = "video"
    ESSAY = "essay"
    FORUM = "forum"
    IMAGE = "image"
    OTHER = "other"


class SymbolismMetadata(BaseModel):
    """Structured symbolism metadata attached to a revision."""

    colors: list[str] = Field(default_factory=list)
    botanical_signals: list[str] = Field(default_factory=list)
    archetypes: list[str] = Field(default_factory=list)
    allegories: list[str] = Field(default_factory=list)
    emotions: list[str] = Field(default_factory=list)
    rituals: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def deduplicate_lists(self) -> SymbolismMetadata:
        """Ensure every symbolism list has unique, trimmed entries."""

        for attr in (
            "colors",
            "botanical_signals",
            "archetypes",
            "allegories",
            "emotions",
            "rituals",
        ):
            values = getattr(self, attr)
            seen: list[str] = []
            for item in values:
                lowered = item.strip()
                if lowered and lowered not in seen:
                    seen.append(lowered)
            setattr(self, attr, seen)
        return self


class AnnotationReference(BaseModel):
    """External reference (video, essay, forum post) cited by a revision."""

    id: UUID = Field(default_factory=uuid4)
    reference_type: ReferenceType
    title: str | None = None
    uri: HttpUrl
    author: str | None = None
    notes: str | None = None
    published_at: datetime | None = None


def empty_reference_list() -> list[AnnotationReference]:
    """Return an empty list of references (typed helper for mypy)."""

    return []


class AnnotationProvenance(BaseModel):
    """Provenance metadata for a revision."""

    source_type: SubmissionChannel
    source_name: str | None = Field(
        default=None,
        description="Display name for the upstream source (e.g., VaatiVidya).",
    )
    source_uri: HttpUrl | None = None
    captured_at: datetime = Field(default_factory=utcnow)


class MotifEntry(BaseModel):
    """Single motif definition loaded from the taxonomy file."""

    slug: str
    label: str
    description: str
    category: str
    synonyms: list[str] = Field(default_factory=list)
    narrative_signals: list[str] = Field(default_factory=list)
    canonical_examples: list[str] = Field(default_factory=list)


class MotifCategory(BaseModel):
    """Grouping of motifs (botanical, elemental, narrative, etc.)."""

    slug: str
    label: str
    description: str
    motifs: list[MotifEntry]


class MotifTaxonomy(BaseModel):
    """Container for all motif categories + lookup helpers."""

    version: int
    updated: datetime | None = None
    categories: list[MotifCategory]

    def get(self, slug: str) -> MotifEntry | None:
        """Return the motif entry for a slug if it exists."""

        for category in self.categories:
            for motif in category.motifs:
                if motif.slug == slug:
                    return motif
        return None

    def ensure(self, slugs: Iterable[str]) -> None:
        """Raise if any slug is missing from the taxonomy."""

        missing = [slug for slug in slugs if self.get(slug) is None]
        if missing:
            raise ValueError(f"Unknown motif tags: {', '.join(missing)}")

    def categories_by_slug(self) -> dict[str, MotifCategory]:
        """Return lookup dictionary keyed by category slug."""

        return {category.slug: category for category in self.categories}


def load_motif_taxonomy(path: Path | None = None) -> MotifTaxonomy:
    """Load the motif taxonomy YAML file into typed models."""

    taxonomy_path = path or DEFAULT_TAXONOMY_PATH
    with taxonomy_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)

    categories: list[MotifCategory] = []
    for slug, raw in payload.get("categories", {}).items():
        motifs: list[MotifEntry] = []
        for motif in raw.get("motifs", []):
            motifs.append(
                MotifEntry(
                    slug=motif["slug"],
                    label=motif["label"],
                    description=motif["description"],
                    category=slug,
                    synonyms=motif.get("synonyms", []),
                    narrative_signals=motif.get("narrative_signals", []),
                    canonical_examples=motif.get("canonical_examples", []),
                )
            )
        categories.append(
            MotifCategory(
                slug=slug,
                label=raw["label"],
                description=raw["description"],
                motifs=motifs,
            )
        )

    updated_dt: datetime | None = None
    updated_raw = payload.get("updated")
    if updated_raw:
        updated_dt = datetime.fromisoformat(updated_raw)

    return MotifTaxonomy(
        version=payload["version"],
        updated=updated_dt,
        categories=categories,
    )


class CommunityAnnotationRevision(BaseModel):
    """Single revision for an annotation shell."""

    id: UUID = Field(default_factory=uuid4)
    annotation_id: UUID
    version: int = Field(gt=0)
    body: str = Field(min_length=1)
    motif_tags: list[str] = Field(
        default_factory=list,
        description="List of motif slugs",
    )
    symbolism: SymbolismMetadata = Field(default_factory=SymbolismMetadata)
    provenance: AnnotationProvenance
    references: list[AnnotationReference] = Field(
        default_factory=empty_reference_list,
    )
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    is_current: bool = False
    review_state: AnnotationReviewState = AnnotationReviewState.PENDING
    reviewer_handle: str | None = None
    review_notes: str | None = None
    submitted_at: datetime = Field(default_factory=utcnow)

    @field_validator("motif_tags")
    @classmethod
    def normalize_tags(cls, value: list[str]) -> list[str]:
        """Normalize motif tags by lowercasing and stripping whitespace."""

        normalized: list[str] = []
        for tag in value:
            slug = tag.strip().lower().replace(" ", "_")
            if slug and slug not in normalized:
                normalized.append(slug)
        return normalized

    def ensure_motifs_are_known(self, taxonomy: MotifTaxonomy) -> None:
        """Validate motif tags against a taxonomy object."""

        taxonomy.ensure(self.motif_tags)


def empty_revision_list() -> list[CommunityAnnotationRevision]:
    """Return an empty revision list (typed helper)."""

    return []


class CommunityAnnotation(BaseModel):
    """Top-level annotation shell keyed to a canonical entity or lore chunk."""

    id: UUID = Field(default_factory=uuid4)
    canonical_id: str
    chunk_id: UUID | None = None
    contributor_handle: str = Field(min_length=2, max_length=39)
    submission_channel: SubmissionChannel
    status: AnnotationStatus = AnnotationStatus.DRAFT
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)
    revisions: list[CommunityAnnotationRevision] = Field(
        default_factory=empty_revision_list,
    )

    @field_validator("contributor_handle")
    @classmethod
    def validate_handle(cls, handle: str) -> str:
        """Ensure contributor handles follow GitHub-like constraints."""

        allowed = set("abcdefghijklmnopqrstuvwxyz0123456789-_")
        slug = handle.strip().lower()
        if not 2 <= len(slug) <= 39:
            raise ValueError("Handle must be between 2 and 39 chars")
        if not set(slug) <= allowed:
            raise ValueError(
                "Handle must be alphanumeric plus - or _ characters"
            )
        return slug

    def add_revision(
        self,
        revision: CommunityAnnotationRevision,
        taxonomy: MotifTaxonomy,
    ) -> None:
        """Attach a revision after validating motif tags and IDs."""

        if revision.annotation_id != self.id:
            raise ValueError("Revision annotation_id does not match shell id")
        revision.ensure_motifs_are_known(taxonomy)
        for existing in self.revisions:
            if existing.version == revision.version:
                raise ValueError(
                    "Revision version already exists for this annotation"
                )
            existing.is_current = False
        revision.is_current = True
        self.revisions.append(revision)
        self.revisions.sort(key=lambda rev: rev.version)
        if self.status == AnnotationStatus.DRAFT:
            self.status = AnnotationStatus.SUBMITTED
        self.updated_at = utcnow()

    def current_revision(self) -> CommunityAnnotationRevision | None:
        """Return the active revision if one exists."""

        if not self.revisions:
            return None
        return max(self.revisions, key=lambda rev: rev.version)


@dataclass(slots=True)
class AnnotationDiff:
    """Simple diff structure to compare two revisions."""

    added_motifs: list[str]
    removed_motifs: list[str]
    symbolism_delta: dict[str, tuple[list[str], list[str]]]

    @classmethod
    def between(
        cls,
        previous: CommunityAnnotationRevision | None,
        current: CommunityAnnotationRevision,
    ) -> AnnotationDiff:
        """Compute the delta between revisions."""

        prev_tags = set(previous.motif_tags if previous else [])
        curr_tags = set(current.motif_tags)
        symbolism_delta: dict[str, tuple[list[str], list[str]]] = {}
        if previous:
            for field in SymbolismMetadata.model_fields:
                before = getattr(previous.symbolism, field)
                after = getattr(current.symbolism, field)
                if before != after:
                    symbolism_delta[field] = (
                        [value for value in before if value not in after],
                        [value for value in after if value not in before],
                    )
        else:
            for field in SymbolismMetadata.model_fields:
                values = getattr(current.symbolism, field)
                if values:
                    symbolism_delta[field] = ([], values)
        return cls(
            added_motifs=sorted(curr_tags - prev_tags),
            removed_motifs=sorted(prev_tags - curr_tags),
            symbolism_delta=symbolism_delta,
        )
