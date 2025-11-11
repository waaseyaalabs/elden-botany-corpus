-- Schema for Elden Ring corpus data

CREATE SCHEMA IF NOT EXISTS elden;

-- Documents: each source file or logical entity list
CREATE TABLE IF NOT EXISTS elden.corpus_document (
    id UUID PRIMARY KEY,
    source_type TEXT NOT NULL,         -- 'kaggle_base' | 'kaggle_dlc' | 'github_api' | 'dlc_textdump' | 'curated_corpus'
    source_uri TEXT NOT NULL,
    title TEXT,
    language TEXT DEFAULT 'en',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Chunks/records: normalized rows with rich text for RAG
CREATE TABLE IF NOT EXISTS elden.corpus_chunk (
    id UUID PRIMARY KEY,
    document_id UUID REFERENCES elden.corpus_document(id) ON DELETE CASCADE,
    entity_type TEXT NOT NULL,         -- 'weapon','armor','boss','npc','item','incantation', etc.
    game_entity_id TEXT,               -- stable slug/key when available
    is_dlc BOOLEAN DEFAULT FALSE,
    name TEXT,
    text TEXT NOT NULL,                -- merged description/dialogue
    meta JSONB NOT NULL DEFAULT '{}',  -- stats, scaling, acquisition, etc.
    span_start INT,
    span_end INT,
    embedding vector(1536)             -- optional; allow null initially
);

-- Comments for documentation
COMMENT ON TABLE elden.corpus_document IS 'Source documents/datasets for corpus';
COMMENT ON TABLE elden.corpus_chunk IS 'Individual entity chunks for RAG';
COMMENT ON COLUMN elden.corpus_chunk.embedding IS 'Vector embedding for semantic search (dimension configurable)';
