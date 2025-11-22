# Tarnished Botanist Roadmap

## 1. Community foundations (Phase 6 – early sprint)

- Develop annotation tools (#139) and community ingestion/update pipeline (#138) – Provide CLI helpers and optional TUI/web UI for contributors, along with schema validation and scaffolding of annotation bundles. Once the annotation format is defined, implement a pipeline that ingests contributor annotations into the corpus, supports CRUD operations and handles conflict-resolution strategies with tests.
- Define the motif library and taxonomy (#140) – With community tooling in place, define the initial taxonomy of botanical, elemental and narrative motifs and map them to lore lines. This taxonomy will become the foundation for downstream analysis.

## 2. Motif analysis (Phase 7 – subsequent sprint)

- Embedding-based motif clustering (#141) – Use the defined taxonomy to build an unsupervised clustering pipeline that groups lore lines and identifies motifs via embeddings (e.g., Sentence-BERT → UMAP → HDBSCAN). Generate exemplar snippets and summarise themes.
- NPC/entity-level motif graph (#142) – Model relationships between NPCs/entities, motifs and individual lore lines in a graph database or NetworkX. Provide export hooks and query examples to traverse the graph. This task may run in parallel with clustering but will benefit from the motif library.
- Narrative summarization engine (#143) – Develop an LLM-based summarisation pipeline that narrates motifs and character relationships, generating versioned summaries per motif/entity with guardrails for hallucinations. Input to this engine should leverage clusters and graph structure to provide context.

## 3. Distribution & documentation (Phase 8 – final sprint)

- Automatic codex export (#144) – Build a job that exports a browsable codex from the curated dataset. The codex should organise content by motif/entity, include markdown or static HTML, and provide deployment instructions for GitHub Pages or similar hosting.
- API & published dataset (#145) – Package the final corpus and metadata into a public dataset (Parquet + manifest) and expose a read-only API. Ensure semantic versioning, metadata manifests and a minimal FastAPI/Cloudflare worker service that returns curated content.

## Suggested branches and initial sprint scope

For the upcoming sprint, focus on Phase 6 tasks to lay the groundwork for later analysis and distribution. Group related issues into branches so that work can proceed independently:

- `community-ingestion-tools` (issues #138 & #139) – implement the annotation tooling and ingestion/update pipeline. Establish a CLI (and optional TUI/web interface) for contributors and build the ingestion logic with CRUD operations and conflict resolution.
- `motif-taxonomy` (issue #140) – create the motif library and taxonomy, define categories (botanical, elemental, narrative, archetypes) and map them to lore lines.

After completing the above, future branches can be created for the motif-analysis tasks (`motif-clustering`, `npc-motif-graph`, `narrative-summarizer`) and for the distribution layer (`codex-export`, `public-api`).
