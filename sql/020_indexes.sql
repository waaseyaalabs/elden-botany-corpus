-- Indexes for efficient querying

-- B-tree indexes for filtering
CREATE INDEX IF NOT EXISTS corpus_chunk_doc_idx 
    ON elden.corpus_chunk (document_id);

CREATE INDEX IF NOT EXISTS corpus_chunk_type_idx 
    ON elden.corpus_chunk (entity_type);

CREATE INDEX IF NOT EXISTS corpus_chunk_dlc_idx 
    ON elden.corpus_chunk (is_dlc);

CREATE INDEX IF NOT EXISTS corpus_chunk_entity_id_idx 
    ON elden.corpus_chunk (game_entity_id);

-- GIN index for JSONB metadata
CREATE INDEX IF NOT EXISTS corpus_chunk_meta_gin_idx 
    ON elden.corpus_chunk USING GIN (meta);

-- Full-text search index
CREATE INDEX IF NOT EXISTS corpus_chunk_text_idx 
    ON elden.corpus_chunk USING GIN (to_tsvector('english', text));

-- HNSW index for vector similarity (only if embeddings present)
-- This will be created automatically, but explicit for documentation
CREATE INDEX IF NOT EXISTS corpus_chunk_embedding_hnsw_idx 
    ON elden.corpus_chunk 
    USING hnsw (embedding vector_l2_ops)
    WHERE embedding IS NOT NULL;
