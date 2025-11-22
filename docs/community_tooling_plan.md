# Community Tooling Plan

This document captures the now-implemented architecture for the Phase 6 community ingestion sprint.

## Goals

- Give contributors a guided workflow (CLI with optional `--tui` flag) for drafting annotation bundles that satisfy `corpus.community_schema`.
- Define bundle layout and metadata so provenance, attachments, and revisions stay portable.
- Provide an ingestion/update pipeline that writes validated bundles into `data/community/processed/` with CRUD + conflict logging.
- Expand the motif taxonomy plus coverage reporting so downstream clustering has a stable spec.

## Bundle Layout

```
data/community/bundles/<handle>/<timestamp>_<slug>/
├── bundle.yml        # Annotation shell and revision history
├── notes.md          # Reviewer scratchpad (auto-created)
├── references/       # Optional attachments + .gitkeep
└── README.md         # Generated instructions for the bundle
```

`corpus.community_bundle.scaffold_bundle` now enforces this layout by creating the directory tree, README, notes file (seeded with optional CLI `--notes` input), and `references/.gitkeep` for hygiene.

### bundle.yml Schema (current)

```yaml
bundle:
  id: "c6db0b18-..."          # UUID created during `corpus community init`
  created_at: "2025-11-21T22:15:55Z"
  updated_at: "2025-11-21T22:15:55Z"
  operation: "create"         # create | update | delete
  document_version: 1
annotation:
  id: "d8c6..."
  canonical_id: "sword_of_night_and_flame"
  chunk_id: "38b4..."         # Optional lore chunk UUID
  contributor_handle: "lorescribe"
  submission_channel: "manual"
  status: "draft"
revisions:
  - id: "..."
    version: 1
    body: |
      Interpretation text...
    motif_tags: ["flame", "dream"]
    symbolism:
      colors: ["gold", "cobalt"]
      botanical_signals: ["sap"]
    provenance:
      source_type: "manual"
      source_name: "Lore Scribe"
    references:
      - reference_type: "video"
        title: "Flame and Moon"
        uri: "https://youtu.be/..."
    confidence: 0.87
notes: "Optional reviewer notes captured via CLI"
```

This maps directly to the `CommunityBundle` dataclass (header + annotation + revisions) and keeps reviewer notes serialized at the root.

## CLI Surface (Click)

Commands live under `poetry run corpus community ...`:

1. `init`
   - Prompts for canonical slug, optional chunk UUID, contributor handle, channel, motifs, annotation body, and notes.
   - `--tui` flag prints a message today and will launch a Textual UI later.
   - Scaffolds the bundle layout described above.
2. `validate`
   - Accepts explicit bundle paths or auto-discovers under `data/community/bundles`.
   - Outputs JSON summaries with `--json-output` for CI.
3. `ingest`
   - Feeds bundles into `CommunityIngestionPipeline` with `--dry-run`, `--force`, `--allow-conflicts`, and `--actor` overrides.
   - Supports `--all` to ingest the entire bundles tree.
4. `list`
   - Emits readable bundle summaries or JSON for dashboards.
5. `motifs-report`
   - Runs motif coverage analysis against `data/curated/unified.parquet` (or `--curated` override) and writes both Parquet + Markdown docs.

## Ingestion Pipeline

`CommunityIngestionPipeline` wires CRUD + conflict detection end to end:

- Reads existing processed tables (annotations, revisions, references, symbolism) if present; otherwise creates empty DataFrames with the canonical columns.
- Persists deterministic manifest entries per annotation (`community_manifest.json`) and a JSON snapshot of the latest annotation in `processed/state/<annotation_id>.json`.
- Detects deletes via `bundle.header.operation == delete`, dropping rows, removing manifest entries, deleting state, and bumping the provenance log.
- Writes conflict artifacts under `processed/conflicts/` whenever stale bundles arrive (timestamp older than manifest). `--allow-conflicts` lets ingestion continue while still emitting JSON diagnostics.
- Logs every upsert/delete to `processed/provenance.log` with actor and timestamp.

## Conflict Strategy

- **Timestamp guard**: stale bundles are rejected unless `--force`. Each conflict spawns a JSON record with manifest snapshot + bundle summary.
- **Checksum guard**: identical bundles short-circuit to `skipped` with no writes.
- **Delete guard**: delete operations against unknown annotation IDs return `skipped` to avoid accidental data loss.
- CLI exits with non-zero status when conflicts exist unless `--allow-conflicts` is set.

## Data Layout

```
data/community/
├── bundles/                 # Contributor files (gitignored)
├── processed/
│   ├── community_annotations.parquet
│   ├── community_revisions.parquet
│   ├── community_references.parquet
│   ├── community_symbolism.parquet
│   ├── community_manifest.json
│   ├── state/<annotation>.json
│   ├── provenance.log
│   └── conflicts/<annotation_id>_<bundle_id>.json
└── README.md
```

## Testing Coverage

- `tests/test_community_bundle.py` covers scaffold + round-trip loading, including the new notes/references layout.
- `tests/test_community_pipeline.py` exercises create/update flows, stale conflict logging, and delete handling.
- `tests/test_motif_coverage.py` verifies keyword-based coverage calculations for representative motifs.
- Existing schema tests (`tests/test_community_schema.py`) still guard taxonomy loading, revision validation, and provenance constraints.

## Next Steps

- Wire the future Textual UI to the `--tui` flag.
- Extend CLI regression tests (Click runner) once the interface hardens.
- Hook coverage outputs into downstream clustering notebooks after the taxonomy doc lands (`docs/motif-taxonomy.md`).

This plan now doubles as the implementation guide and status record for issues #138, #139, and #140.
