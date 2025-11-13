"""
Integration tests for PostgreSQL + pgvector schema and queries.

These tests verify:
- Schema creation (extensions, tables, indexes)
- Data insertion and retrieval
- Vector similarity search (HNSW index)
- Full-text search (GIN index)

Run with:
    POSTGRES_DSN=postgresql://user:pass@localhost:5432/db pytest -m integration
"""

import os

import psycopg
import pytest


@pytest.mark.integration
@pytest.mark.skipif(
    not os.getenv("POSTGRES_DSN"),
    reason="POSTGRES_DSN not set (required for integration tests)"
)
def test_pgvector_schema_and_queries() -> None:
    """
    End-to-end test: create schema, insert data, verify vector and FTS queries.

    This test recreates a minimal schema for isolation. Future PRs can refactor
    to execute the actual SQL files from /sql/*.sql.
    """
    dsn = os.environ["POSTGRES_DSN"]

    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            # ======= SETUP: Extensions + Schema =======
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            cur.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')
            cur.execute("CREATE SCHEMA IF NOT EXISTS elden;")

            # ======= SETUP: Tables =======
            # Mirror 010_schema.sql structure
            cur.execute("""
                CREATE TABLE IF NOT EXISTS elden.corpus_document (
                    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
                    source_type TEXT NOT NULL,
                    source_uri TEXT NOT NULL,
                    title TEXT,
                    language TEXT DEFAULT 'en',
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS elden.corpus_chunk (
                    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
                    document_id UUID REFERENCES elden.corpus_document(id)
                        ON DELETE CASCADE,
                    entity_type TEXT NOT NULL,
                    game_entity_id TEXT,
                    is_dlc BOOLEAN DEFAULT FALSE,
                    name TEXT,
                    text TEXT NOT NULL,
                    meta JSONB NOT NULL DEFAULT '{}',
                    span_start INT,
                    span_end INT,
                    embedding vector(1536)
                );
            """)

            # ======= SETUP: Indexes =======
            # Mirror 020_indexes.sql (minimal subset for this test)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS corpus_chunk_v_hnsw
                ON elden.corpus_chunk USING hnsw (embedding vector_l2_ops)
                WHERE embedding IS NOT NULL;
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS corpus_chunk_text_fts
                ON elden.corpus_chunk USING gin (to_tsvector('english', text));
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS corpus_chunk_type_idx
                ON elden.corpus_chunk (entity_type);
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS corpus_chunk_dlc_idx
                ON elden.corpus_chunk (is_dlc);
            """)

            # ======= TEST: Insert Document + Chunk =======
            cur.execute("""
                INSERT INTO elden.corpus_document (
                    source_type, source_uri, title
                )
                VALUES (
                    'kaggle_base',
                    'kaggle://test/weapons.csv',
                    'Test Weapons'
                )
                RETURNING id
            """)
            result = cur.fetchone()
            assert result is not None, "Document insertion failed"
            (doc_id,) = result
            assert doc_id is not None, "Document insertion failed"

            # Create a 1536-dimensional embedding vector for testing
            # First 3 values are [0.1, 0.2, 0.3], rest are zeros
            test_embedding = [0.1, 0.2, 0.3] + [0.0] * 1533

            cur.execute("""
                INSERT INTO elden.corpus_chunk (
                    document_id, entity_type, game_entity_id,
                    name, text, meta, is_dlc, embedding
                )
                VALUES (
                    %s, 'weapon', 'moonlight_greatsword',
                    'Moonlight Greatsword',
                    'A legendary weapon that glows with pale moonlight. '
                    'This greatsword was bequeathed to Ranni the Witch '
                    'by her mother.',
                    '{"attack_type": "standard", "damage_type": "magic", '
                    '"weight": 10.5}'::jsonb,
                    false,
                    %s::vector
                )
                RETURNING id
            """, (doc_id, test_embedding))
            chunk_result = cur.fetchone()
            assert chunk_result is not None, "Chunk insertion failed"
            chunk_id = chunk_result[0]
            assert chunk_id is not None, "Chunk insertion failed"

            # ======= TEST: Full-Text Search =======
            # Should find the row with "legendary" in text
            cur.execute("""
                SELECT id, name, text
                FROM elden.corpus_chunk
                WHERE to_tsvector('english', text) @@
                      plainto_tsquery('english', 'legendary')
            """)
            fts_results = cur.fetchall()
            assert len(fts_results) == 1, (
                f"Expected 1 FTS result, got {len(fts_results)}"
            )
            assert fts_results[0][0] == chunk_id, (
                "FTS returned wrong chunk"
            )
            assert "legendary" in fts_results[0][2].lower(), (
                "FTS result doesn't contain search term"
            )

            # ======= TEST: Vector Similarity Search (HNSW) =======
            # Search for nearest neighbor (should be the same row we inserted)
            query_vector = test_embedding  # Same embedding for exact match

            cur.execute("""
                SELECT id, name, embedding <-> %s::vector AS distance
                FROM elden.corpus_chunk
                WHERE embedding IS NOT NULL
                ORDER BY embedding <-> %s::vector
                LIMIT 1
            """, (query_vector, query_vector))
            knn_result = cur.fetchone()
            assert knn_result is not None, (
                "Vector KNN query returned no results"
            )
            assert knn_result[0] == chunk_id, (
                "KNN returned wrong chunk (expected exact match)"
            )
            assert knn_result[2] < 0.001, (
                f"Distance should be ~0 for exact match, "
                f"got {knn_result[2]}"
            )

            # ======= TEST: JSONB Metadata Query =======
            cur.execute("""
                SELECT id, meta->>'damage_type' AS damage_type
                FROM elden.corpus_chunk
                WHERE meta->>'damage_type' = 'magic'
            """)
            meta_results = cur.fetchall()
            assert len(meta_results) == 1, (
                f"Expected 1 JSONB result, got {len(meta_results)}"
            )
            assert meta_results[0][1] == 'magic', (
                "JSONB query returned wrong metadata"
            )

            # ======= TEST: Index Existence =======
            # Verify that our key indexes were created
            cur.execute("""
                SELECT indexname
                FROM pg_indexes
                WHERE schemaname = 'elden'
                  AND tablename = 'corpus_chunk'
                  AND indexname IN (
                      'corpus_chunk_v_hnsw',
                      'corpus_chunk_text_fts'
                  )
            """)
            indexes = {row[0] for row in cur.fetchall()}
            assert 'corpus_chunk_v_hnsw' in indexes, (
                "HNSW vector index not created"
            )
            assert 'corpus_chunk_text_fts' in indexes, (
                "FTS GIN index not created"
            )

            # ======= CLEANUP =======
            # Drop tables for cleanup (test isolation)
            cur.execute(
                "DROP TABLE IF EXISTS elden.corpus_chunk CASCADE;"
            )
            cur.execute(
                "DROP TABLE IF EXISTS elden.corpus_document CASCADE;"
            )
            cur.execute("DROP SCHEMA IF EXISTS elden CASCADE;")


@pytest.mark.integration
@pytest.mark.skipif(
    not os.getenv("POSTGRES_DSN"),
    reason="POSTGRES_DSN not set"
)
def test_vector_dimension_validation() -> None:
    """
    Test that vector dimension enforcement works correctly.

    Verifies that attempting to insert a vector with wrong dimensions fails.
    """
    dsn = os.environ["POSTGRES_DSN"]

    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            # Setup minimal schema
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            cur.execute("CREATE SCHEMA IF NOT EXISTS elden_test;")
            cur.execute("""
                CREATE TABLE elden_test.test_vectors (
                    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
                    embedding vector(1536)
                );
            """)

            # Valid 1536-dimensional vector should succeed
            valid_vector = [0.1] * 1536
            cur.execute(
                "INSERT INTO elden_test.test_vectors (embedding) "
                "VALUES (%s::vector)",
                (valid_vector,)
            )

            # Invalid dimension should fail
            invalid_vector = [0.1] * 512  # Wrong dimension
            with pytest.raises(psycopg.errors.DataException):
                cur.execute(
                    "INSERT INTO elden_test.test_vectors (embedding) "
                    "VALUES (%s::vector)",
                    (invalid_vector,)
                )

            # Cleanup
            cur.execute("DROP SCHEMA elden_test CASCADE;")


@pytest.mark.integration
@pytest.mark.skipif(
    not os.getenv("POSTGRES_DSN"),
    reason="POSTGRES_DSN not set"
)
def test_cascade_delete() -> None:
    """
    Test that deleting a document cascades to delete associated chunks.
    """
    dsn = os.environ["POSTGRES_DSN"]

    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            # Setup
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            cur.execute("CREATE SCHEMA IF NOT EXISTS elden_cascade;")
            cur.execute("""
                CREATE TABLE elden_cascade.corpus_document (
                    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
                    source_type TEXT NOT NULL,
                    source_uri TEXT NOT NULL
                );
            """)
            cur.execute("""
                CREATE TABLE elden_cascade.corpus_chunk (
                    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
                    document_id UUID
                        REFERENCES elden_cascade.corpus_document(id)
                        ON DELETE CASCADE,
                    entity_type TEXT NOT NULL,
                    text TEXT NOT NULL,
                    embedding vector(1536)
                );
            """)

            # Insert document + chunk
            cur.execute("""
                INSERT INTO elden_cascade.corpus_document (
                    source_type, source_uri
                )
                VALUES ('test', 'test://cascade')
                RETURNING id
            """)
            doc_result = cur.fetchone()
            assert doc_result is not None
            doc_id = doc_result[0]

            cur.execute("""
                INSERT INTO elden_cascade.corpus_chunk (
                    document_id, entity_type, text, embedding
                )
                VALUES (%s, 'test', 'test text', %s::vector)
            """, (doc_id, [0.0] * 1536))

            # Verify chunk exists
            cur.execute(
                "SELECT COUNT(*) FROM elden_cascade.corpus_chunk "
                "WHERE document_id = %s",
                (doc_id,)
            )
            count_result = cur.fetchone()
            assert count_result is not None
            assert count_result[0] == 1

            # Delete document
            cur.execute(
                "DELETE FROM elden_cascade.corpus_document WHERE id = %s",
                (doc_id,)
            )

            # Verify chunk was cascade-deleted
            cur.execute(
                "SELECT COUNT(*) FROM elden_cascade.corpus_chunk "
                "WHERE document_id = %s",
                (doc_id,)
            )
            cascade_result = cur.fetchone()
            assert cascade_result is not None
            assert cascade_result[0] == 0, "Cascade delete failed"

            # Cleanup
            cur.execute("DROP SCHEMA elden_cascade CASCADE;")
