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

-- Community contributor directory
CREATE TABLE IF NOT EXISTS elden.community_contributor (
    id UUID PRIMARY KEY,
    handle TEXT NOT NULL UNIQUE CHECK (handle ~ '^[A-Za-z0-9_\-]{2,39}$'),
    display_name TEXT,
    auth_provider TEXT NOT NULL DEFAULT 'github',
    profile_uri TEXT,
    contact JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Community annotation base record (one per canonical/lore target)
CREATE TABLE IF NOT EXISTS elden.community_annotation (
    id UUID PRIMARY KEY,
    contributor_id UUID NOT NULL REFERENCES elden.community_contributor(id) ON DELETE RESTRICT,
    canonical_id TEXT NOT NULL,
    chunk_id UUID REFERENCES elden.corpus_chunk(id) ON DELETE SET NULL,
    submission_channel TEXT NOT NULL CHECK (submission_channel IN ('manual','import','llm','curated')),
    status TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft','submitted','approved','rejected','archived')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS community_annotation_canonical_idx
    ON elden.community_annotation (canonical_id);

-- Append-only revisions capturing motif + symbolism metadata
CREATE TABLE IF NOT EXISTS elden.community_annotation_revision (
    id UUID PRIMARY KEY,
    annotation_id UUID NOT NULL REFERENCES elden.community_annotation(id) ON DELETE CASCADE,
    version INT NOT NULL CHECK (version > 0),
    body TEXT NOT NULL,
    symbolism JSONB NOT NULL DEFAULT '{}'::jsonb,
    motif_summary TEXT,
    confidence NUMERIC(4,3) CHECK (confidence >= 0 AND confidence <= 1),
    provenance_type TEXT NOT NULL CHECK (provenance_type IN ('manual','import','llm','curated')),
    provenance_source TEXT,
    source_uri TEXT,
    source_sha256 TEXT,
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_current BOOLEAN NOT NULL DEFAULT FALSE,
    review_state TEXT NOT NULL DEFAULT 'pending' CHECK (review_state IN ('pending','approved','needs_changes','rejected')),
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS community_annotation_revision_version_idx
    ON elden.community_annotation_revision (annotation_id, version);

CREATE UNIQUE INDEX IF NOT EXISTS community_annotation_revision_current_idx
    ON elden.community_annotation_revision (annotation_id)
    WHERE is_current;

-- External references that support a revision (videos, essays, forum posts, etc.)
CREATE TABLE IF NOT EXISTS elden.community_annotation_reference (
    id UUID PRIMARY KEY,
    revision_id UUID NOT NULL REFERENCES elden.community_annotation_revision(id) ON DELETE CASCADE,
    reference_type TEXT NOT NULL CHECK (reference_type IN ('video','essay','forum','image','other')),
    title TEXT,
    uri TEXT NOT NULL,
    author TEXT,
    published_at TIMESTAMPTZ,
    notes TEXT
);

-- Motif taxonomy managed inside the database for referential integrity
CREATE TABLE IF NOT EXISTS elden.community_motif (
    id UUID PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    category TEXT NOT NULL,
    label TEXT NOT NULL,
    description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deprecated_at TIMESTAMPTZ,
    replacement_slug TEXT
);

-- Junction table linking revisions to motif tags with optional weighting
CREATE TABLE IF NOT EXISTS elden.community_annotation_motif (
    revision_id UUID NOT NULL REFERENCES elden.community_annotation_revision(id) ON DELETE CASCADE,
    motif_id UUID NOT NULL REFERENCES elden.community_motif(id) ON DELETE RESTRICT,
    focus TEXT NOT NULL DEFAULT 'primary' CHECK (focus IN ('primary','secondary','tertiary')),
    weight NUMERIC(4,3) DEFAULT 1.0 CHECK (weight > 0),
    PRIMARY KEY (revision_id, motif_id)
);

CREATE INDEX IF NOT EXISTS community_annotation_motif_idx
    ON elden.community_annotation_motif (motif_id);

COMMENT ON TABLE elden.community_contributor IS 'Directory of contributors adding interpretive annotations';
COMMENT ON COLUMN elden.community_contributor.handle IS 'Lowercase/slugged contributor handle (typically GitHub username)';

COMMENT ON TABLE elden.community_annotation IS 'Stable annotation shells tied to canonical entities/lore chunks';
COMMENT ON COLUMN elden.community_annotation.canonical_id IS 'Foreign reference to canonical entity or lore row identifier';

COMMENT ON TABLE elden.community_annotation_revision IS 'Append-only revision history for community annotations with provenance + symbolism metadata';
COMMENT ON COLUMN elden.community_annotation_revision.symbolism IS 'Structured symbolism metadata (colors, archetypes, botanical cues) stored as JSON';

COMMENT ON TABLE elden.community_annotation_reference IS 'External references (videos, essays, forum posts) cited by a given revision';

COMMENT ON TABLE elden.community_motif IS 'Controlled vocabulary of motif tags used across annotations';
COMMENT ON TABLE elden.community_annotation_motif IS 'Mapping table between annotation revisions and motif tags including focus/weighting';
