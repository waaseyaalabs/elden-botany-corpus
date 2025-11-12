"""Tests for PostgreSQL + pgvector loader."""

import pytest

from corpus.models import CorpusChunk, CorpusDocument


@pytest.mark.integration
@pytest.mark.skip(reason="Requires PostgreSQL connection - integration test")
def test_pgvector_loader_init():
    """Test PgVectorLoader initialization."""
    # TODO: Implement with test database connection
    pass


@pytest.mark.integration
@pytest.mark.skip(reason="Requires PostgreSQL connection - integration test")
def test_create_schema():
    """Test schema creation from SQL files."""
    # TODO: Test that schema is created correctly
    pass


@pytest.mark.integration
@pytest.mark.skip(reason="Requires PostgreSQL connection - integration test")
def test_load_document():
    """Test loading a single document."""
    # TODO: Create test document and verify insertion
    pass


@pytest.mark.integration
@pytest.mark.skip(reason="Requires PostgreSQL connection - integration test")
def test_load_chunk():
    """Test loading a single chunk."""
    # TODO: Create test chunk and verify insertion
    pass


@pytest.mark.integration
@pytest.mark.skip(reason="Requires PostgreSQL connection - integration test")
def test_load_chunks_batch():
    """Test batch loading of chunks."""
    # TODO: Test batch insert performance and correctness
    pass


@pytest.mark.integration
@pytest.mark.skip(reason="Requires PostgreSQL connection - integration test")
def test_update_embeddings():
    """Test updating embeddings for chunks."""
    # TODO: Test embedding update logic
    pass


@pytest.mark.integration
@pytest.mark.skip(reason="Requires PostgreSQL connection - integration test")
def test_query_by_vector():
    """Test vector similarity search."""
    # TODO: Test HNSW index and similarity queries
    pass


@pytest.mark.integration
@pytest.mark.skip(reason="Requires PostgreSQL connection - integration test")
def test_full_text_search():
    """Test full-text search on text column."""
    # TODO: Test GIN index and full-text queries
    pass


def test_corpus_document_model():
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


def test_corpus_chunk_model():
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


def test_corpus_chunk_with_embedding():
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
