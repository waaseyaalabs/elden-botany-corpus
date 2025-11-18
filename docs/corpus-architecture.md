# Elden Botany Corpus Architecture

The corpus is intentionally layered so each stage builds higher-order meaning on top of the prior one. Every layer is reproducible, versioned, and validated so downstream analyses have deterministic provenance.

## Layer 1 — Canonical Corpus (Structured Entities)
- **Scope**: Weapons, armor, talismans, consumables, spirits, bosses, ashes, bell bearings, upgrade materials, DLC expansions, and supporting relational lookups.
- **Sources**: Kaggle Base + DLC datasets, GitHub `eldenring-api`, Carian Archive FMG captions, Impalers spreadsheets/HTML (structured portions), and bespoke reconciliations.
- **Processing**: Deterministic IDs, Pandera schema validation, deduplicated names, DLC override reconciliation, and provenance columns for every attribute.
- **Outputs**: Curated Parquet tables under `data/curated/`, PostgreSQL exports (`elden.canonical_*` tables), plus seed metadata for downstream embeddings.
- **Guarantees**: Deterministic canonical IDs, consistent foreign-key relationships, and tracked source lineage for every field.

## Layer 2 — Lore Corpus (Textual Lines)
- **Scope**: All textual descriptions, quotes, flavor text, effects blurbs, narrative fragments, DLC additions, GitHub fallback strings, Carian Archive dialogue lines, and Impalers HTML excerpts mapped to canonical entities.
- **Linkage**: Each lore row carries `lore_id`, `canonical_id`, `category`, `text_type`, and `source`, allowing joins back to Layer 1.
- **Pipelines**: `pipelines.build_lore_corpus` aggregates text; `pipelines.build_lore_embeddings` encodes sentences; `pipelines.build_rag_index` produces `data/embeddings/faiss_index.bin` + `rag_metadata.parquet` for retrieval.
- **Purpose**: Supplies semantically searchable text for RAG, qualitative analysis, and future motif clustering. Serves as the substrate for embedding-powered search.

## Layer 3 — Community Corpus (Interpretive + Future Work)
- **Scope**: Community interpretations (botanical readings, symbolic themes), player theories, YouTuber references (Vaati, SmoughTown, Tarnished Archaeologist), curated motifs, thematic clusters, and future user submissions.
- **Dependencies**: Built on top of Layer 2 embeddings + RAG helper so that interpretive notes can cite exact lore lines and canonical IDs.
- **Planned Features**:
  - Motif clusters (e.g., thorns, rot, celestial bodies) generated via embedding neighborhoods and manual tagging.
  - Symbolic & botanical interpretations authored by contributors, with provenance and confidence scores.
  - Community annotations referencing videos, essays, or high-signal forum posts.
  - LLM-generated summaries that stitch related lore passages, stored alongside human commentary.
- **Status**: Semantic retrieval bridge delivered via `pipelines.build_lore_embeddings`, `pipelines.build_rag_index`, and `rag.query` (see qualitative eval notebook for proof). The remaining work tracks clustering + annotation tooling for contributor workflows.

## Operational Principles
- **Determinism**: Every layer is rebuildable via `poetry run python -m pipelines.<pipeline>` commands plus Makefile shortcuts.
- **Observability**: Each pipeline writes summary metadata (counts, distributions, provenance) for quick regression detection.
- **Provenance First**: Every textual or interpretive artifact must cite upstream canonical IDs and primary sources.
- **Extendability**: Adding new DLC data or community annotations should only require appending to existing layers, never rewriting history.

With Layer 1 locked, Layer 2 query-ready, and Layer 3 scoped, contributors can reason about where a given task fits and which artifacts need to be touched or regenerated.
