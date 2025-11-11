"""Shared data models for corpus entities."""

import hashlib
import re
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class Provenance(BaseModel):
    """Source provenance metadata."""

    source: str = Field(description="Source identifier (e.g., 'kaggle_base', 'github_api')")
    uri: str = Field(description="Source URI or URL")
    sha256: str | None = Field(default=None, description="SHA256 hash of source file")
    retrieved_at: datetime = Field(default_factory=datetime.utcnow, description="Retrieval timestamp")


class CorpusDocument(BaseModel):
    """Represents a source document/dataset."""

    id: UUID = Field(default_factory=uuid4)
    source_type: str = Field(description="Source type: kaggle_base, kaggle_dlc, github_api, dlc_textdump")
    source_uri: str = Field(description="Source URI")
    title: str | None = Field(default=None)
    language: str = Field(default="en")
    created_at: datetime = Field(default_factory=datetime.utcnow)


class CorpusChunk(BaseModel):
    """Represents a normalized entity/chunk for RAG."""

    id: UUID = Field(default_factory=uuid4)
    document_id: UUID | None = Field(default=None)
    entity_type: str = Field(description="Entity type: weapon, armor, boss, npc, item, etc.")
    game_entity_id: str = Field(description="Stable slug/identifier")
    is_dlc: bool = Field(default=False)
    name: str = Field(description="Entity name")
    text: str = Field(description="Full merged description/dialogue")
    meta: dict[str, Any] = Field(default_factory=dict, description="Additional metadata (stats, scaling, etc.)")
    span_start: int | None = Field(default=None)
    span_end: int | None = Field(default=None)
    embedding: list[float] | None = Field(default=None)

    def compute_hash(self) -> str:
        """Compute SHA256 hash of chunk content."""
        content = f"{self.entity_type}:{self.game_entity_id}:{self.name}:{self.text}"
        return hashlib.sha256(content.encode()).hexdigest()


class RawEntity(BaseModel):
    """Raw entity before normalization."""

    entity_type: str
    name: str
    is_dlc: bool = False
    description: str = ""
    raw_data: dict[str, Any] = Field(default_factory=dict)
    provenance: list[Provenance] = Field(default_factory=list)

    def to_slug(self) -> str:
        """Generate stable slug from name."""
        return create_slug(self.name)


def create_slug(text: str) -> str:
    """
    Create a stable, URL-safe slug from text.
    
    Examples:
        "Sword of Night and Flame" -> "sword_of_night_and_flame"
        "Rennala, Queen of the Full Moon" -> "rennala_queen_of_the_full_moon"
    """
    # Lowercase and remove punctuation except spaces
    text = text.lower()
    text = re.sub(r"[^\w\s-]", "", text)
    # Replace whitespace with underscores
    text = re.sub(r"[-\s]+", "_", text)
    # Remove leading/trailing underscores
    text = text.strip("_")
    return text


def normalize_name_for_matching(text: str) -> str:
    """
    Normalize text for fuzzy matching.
    
    More aggressive than slug creation - removes all non-alphanumeric characters
    and collapses whitespace for better matching.
    """
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()
