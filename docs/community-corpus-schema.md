# Community Corpus Schema

Community contributions sit on top of the canonical + lore layers and track interpretive commentary, motif tags, and symbolism metadata. This document describes the storage schema, validation models, motif taxonomy, and contributor workflow for Phase 6.

## Data Model Overview

| Table / Model | Purpose |
| --- | --- |
| `elden.community_contributor` | Directory of annotators, auth providers, and contact metadata |
| `elden.community_annotation` | Stable annotation shell tied to a canonical entity or lore chunk |
| `elden.community_annotation_revision` | Append-only history that stores commentary, motif tags, symbolism JSON, provenance, and review state |
| `elden.community_annotation_reference` | External citations (videos, essays, forum posts) linked to a revision |
| `elden.community_motif` | Controlled vocabulary of motifs mirroring `config/community_motifs.yml` |
| `elden.community_annotation_motif` | Junction table connecting revisions to motif entries with focus + weight |
| `corpus.community_schema` | Pydantic layer that enforces the same constraints for tooling and tests |

### SQL Highlights

- Annotation shells (`community_annotation`) enforce `submission_channel` and `status` enums via `CHECK` constraints and store both the canonical slug and optional `corpus_chunk.id` for deterministic joins.
- Revisions (`community_annotation_revision`) carry `symbolism` JSON, `confidence`, provenance columns, review metadata, and `is_current` partial unique index so append-only history never overwrites prior work.
- Motifs live in `community_motif`, with `community_annotation_motif` preserving focus (`primary`, `secondary`, `tertiary`) and optional weight for clustering.
- References table standardizes citation types so provenance back to videos, essays, or forums is auditable.

See `sql/010_schema.sql` for the authoritative definitions.

## Programmatic Schema

`src/corpus/community_schema.py` provides strongly typed models that mirror the SQL tables:

- `CommunityAnnotation` and `CommunityAnnotationRevision` ensure contributor handles, motif tags, provenance, and symbolism metadata are normalized before serialization.
- `AnnotationDiff` offers quick insight into motif/symbolism deltas between revisions (useful for reviewer tooling).
- `load_motif_taxonomy` reads `config/community_motifs.yml` so CLI or UI layers can auto-complete valid motif tags.
- Tests covering the schema live in `tests/test_community_schema.py` and guard taxonomy loading plus revision validation.

## Motif Taxonomy (Initial Set)

Defined in `config/community_motifs.yml` and loaded into `community_motif`.

- **Botanical**: `fungus`, `thorn`, `sap`
- **Elemental**: `gravity`, `flame`, `glintstone`
- **Narrative**: `rebirth`, `martyrdom`, `betrayal`
- **Cosmic**: `scarlet_rot`, `primeval_current`, `dream`

Each entry includes synonyms, narrative signals, and canonical examples to shorten review loops. The YAML `review_log` currently records a pending sign-off entry so lore editors can append their approval notes inline.

## Provenance & Versioning Strategy

1. Contributors create an annotation shell referencing a canonical slug (`game_entity_id`) and optional `corpus_chunk.id`.
2. Every edit generates a new row in `community_annotation_revision`. Prior revisions remain immutable; `is_current` moves via SQL update or the helper method `CommunityAnnotation.add_revision`.
3. Provenance fields capture:
   - `provenance_type` (`manual`, `import`, `llm`, `curated`)
   - `provenance_source`, `source_uri`, `source_sha256`
   - `AnnotationProvenance` object in code stores `source_type`, optional `source_name`, and `captured_at`.
4. Review metadata (`review_state`, `reviewed_by`, `reviewed_at`) stays attached to the revision that was evaluated so historical context is preserved.

## Contributor Authentication

- `community_contributor` stores a `handle` (GitHub-style constraints), optional display name, auth provider (`github`, `discord`, etc.), profile URI, and `contact` JSON for encrypted emails or Matrix IDs.
- CLI tooling should require contributors to register once and reuse the same `contributor_id` when posting annotation shells. Handles are normalized via the Pydantic validator to keep comparisons deterministic.

## Workflow Example

1. **Create contributor**: insert into `community_contributor` with `handle='lorescribe'` and provider `github`.
2. **Create annotation**: insert into `community_annotation` with `canonical_id='sword_of_night_and_flame'`, `chunk_id=<uuid>`, `submission_channel='manual'`.
3. **Submit revision**:
   ```json
   {
     "annotation_id": "...",
     "version": 1,
     "body": "Interprets duality between moonlit dreams and giantsflame",
     "motif_tags": ["flame", "dream"],
     "symbolism": {
       "colors": ["gold", "cobalt"],
       "archetypes": ["twin guardians"],
       "rituals": ["covenant"],
       "botanical_signals": ["sap"]
     },
     "provenance": {
       "source_type": "manual",
       "source_name": "LoreScribe"
     },
     "references": [
       {
         "reference_type": "video",
         "uri": "https://youtu.be/example",
         "title": "Flame and Moon Lore",
         "author": "Vaati"
       }
     ]
   }
   ```
4. **Persist motifs**: insert rows into `community_annotation_motif` for each tag with appropriate `focus`/`weight`.
5. **Review**: lore editors update `review_state`, optionally append `review_notes`, and log their decision in the YAML `review_log` plus database columns.

## Lore Editor Review Checklist

- [ ] Taxonomy categories align with botanical/elemental scopes.
- [ ] Symbolism JSON fields cover required interpretive axes (colors, archetypes, rituals, emotions).
- [ ] Provenance fields sufficiently cite external media.
- [ ] Versioning strategy (append-only revisions + `is_current` index) meets reproducibility expectations.
- [ ] Example workflow validated against at least one lore passage.

Lore editors can mark the checklist directly in this doc after reviewing the schema and update the `review_log` within `config/community_motifs.yml`. Until that approval lands, contributor-facing tooling should remain behind a feature flag.

## Tooling Considerations

- `CommunityAnnotation` + `CommunityAnnotationRevision` models can be wrapped by a CLI command (`corpus community annotate`) that loads motifs, validates input, and writes rows to the database or Parquet dumps in `data/community/`.
- The join table makes motif filtering cheap for downstream RAG weighting (e.g., filter to `primary` motifs before embedding clusters).
- Use the sample tests as reference for future UI contracts and extend coverage as new fields land.
