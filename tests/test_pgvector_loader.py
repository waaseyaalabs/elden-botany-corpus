"""Tests for PostgreSQL + pgvector loader."""

import os

import pytest

from corpus.models import CorpusChunk, CorpusDocument


@pytest.mark.integration
@pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION") != "1",
    reason="Integration tests disabled (set RUN_INTEGRATION=1 to enable)",
)
def test_pgvector_loader_init() -> None:
    """Test PgVectorLoader initialization."""
    pytest.importorskip("pgvector")
    # TODO: Implement with test database connection
    pass


@pytest.mark.integration
@pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION") != "1",
    reason="Integration tests disabled (set RUN_INTEGRATION=1 to enable)",
)
def test_create_schema() -> None:
    """Test schema creation from SQL files."""
    pytest.importorskip("pgvector")
    pytest.importorskip("psycopg")
    # TODO: Test that schema is created correctly
    pass


@pytest.mark.integration
@pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION") != "1",
    reason="Integration tests disabled (set RUN_INTEGRATION=1 to enable)",
)
def test_load_document() -> None:
    """Test loading a single document."""
    pytest.importorskip("pgvector")
    pytest.importorskip("psycopg")
    # TODO: Create test document and verify insertion
    pass


@pytest.mark.integration
@pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION") != "1",
    reason="Integration tests disabled (set RUN_INTEGRATION=1 to enable)",
)
def test_load_chunk() -> None:
    """Test loading a single chunk."""
    pytest.importorskip("pgvector")
    pytest.importorskip("psycopg")
    # TODO: Create test chunk and verify insertion
    pass


@pytest.mark.integration
@pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION") != "1",
    reason="Integration tests disabled (set RUN_INTEGRATION=1 to enable)",
)
def test_load_chunks_batch() -> None:
    """Test batch loading of chunks."""
    pytest.importorskip("pgvector")
    pytest.importorskip("psycopg")
    # TODO: Test batch insert performance and correctness
    pass


@pytest.mark.integration
@pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION") != "1",
    reason="Integration tests disabled (set RUN_INTEGRATION=1 to enable)",
)
def test_update_embeddings() -> None:
    """Test updating embeddings for chunks."""
    pytest.importorskip("pgvector")
    pytest.importorskip("psycopg")
    # TODO: Test embedding update logic
    pass


@pytest.mark.integration
@pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION") != "1",
    reason="Integration tests disabled (set RUN_INTEGRATION=1 to enable)",
)
def test_query_by_vector() -> None:
    """Test vector similarity search."""
    pytest.importorskip("pgvector")
    pytest.importorskip("psycopg")
    # TODO: Test HNSW index and similarity queries
    pass


@pytest.mark.integration
@pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION") != "1",
    reason="Integration tests disabled (set RUN_INTEGRATION=1 to enable)",
)
def test_full_text_search() -> None:
    """Test full-text search on text column."""
    pytest.importorskip("pgvector")
    pytest.importorskip("psycopg")
    # TODO: Test GIN index and full-text queries
    pass


def test_corpus_document_model() -> None:
    """Test CorpusDocument model validation."""
    doc = CorpusDocument(
        source_type="kaggle_base",
        source_uri="kaggle://test/weapons.csv",
        title="Test Weapons Dataset",
    )

    assert doc.id is not None
    assert doc.source_type == "kaggle_base"
    assert doc.language == "en"
    assert doc.created_at is not None


def test_corpus_chunk_model() -> None:
    """Test CorpusChunk model validation."""
    chunk = CorpusChunk(
        entity_type="weapon",
        game_entity_id="moonlight_greatsword",
        name="Moonlight Greatsword",
        text="A legendary weapon that glows with moonlight.",
        is_dlc=False,
        meta={"attack": 120, "scaling": "INT"},
    )

    assert chunk.id is not None
    assert chunk.entity_type == "weapon"
    assert chunk.game_entity_id == "moonlight_greatsword"
    assert chunk.is_dlc is False
    assert chunk.meta["attack"] == 120

    # Test hash computation
    hash_value = chunk.compute_hash()
    assert isinstance(hash_value, str)
    assert len(hash_value) == 64  # SHA256 hex length


def test_corpus_chunk_with_embedding() -> None:
    """Test CorpusChunk with embedding vector."""
    chunk = CorpusChunk(
        entity_type="boss",
        game_entity_id="radahn",
        name="Starscourge Radahn",
        text="A demigod general of immense power.",
        is_dlc=False,
        embedding=[0.1, 0.2, 0.3] * 512,  # Mock 1536-dim vector
    )

    assert chunk.embedding is not None
    assert len(chunk.embedding) == 1536
