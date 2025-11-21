# Elden Ring Botany Corpus

[![CI](https://github.com/waaseyaalabs/elden-botany-corpus/actions/workflows/ci.yml/badge.svg)](https://github.com/waaseyaalabs/elden-botany-corpus/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

A curated, provenance-tracked dataset of **Elden Ring** game data (base game + Shadow of the Erdtree DLC) optimized for **Retrieval-Augmented Generation (RAG)** and analytics.

## 🎯 Features

- **Comprehensive Coverage**: Base game + Shadow of the Erdtree DLC entities
- **Multiple Sources**: Kaggle datasets, GitHub API, and DLC text dumps
- **Provenance Tracking**: Every record includes source attribution and SHA256 hashes
- **Deduplication**: Smart reconciliation with priority ordering (Kaggle DLC → Kaggle Base → GitHub API)
- **Fuzzy Text Matching**: DLC text dump integration using Levenshtein distance
- **PostgreSQL + pgvector**: Ready for semantic search with vector embeddings
- **Export Formats**: Parquet (partitioned), CSV, and direct Postgres loading
- **Quality Reports**: HTML/JSON profiling artifacts for every curated dataset
- **Automated Refresh**: GitHub Actions workflow for nightly updates

## 📊 Data Sources

### Base Game

1. **Kaggle: Elden Ring Ultimate Dataset** by Rob Mulla  
   📦 [robikscube/elden-ring-ultimate-dataset](https://www.kaggle.com/datasets/robikscube/elden-ring-ultimate-dataset)  
   - **License**: CC0 1.0 Universal (Public Domain)
   - **Tables**: 14 entity types (weapons, armors, bosses, npcs, items, etc.)

2. **GitHub: deliton/eldenring-api** (Fallback)  
   🔗 [github.com/deliton/eldenring-api](https://github.com/deliton/eldenring-api)  
   - **License**: MIT
   - **Format**: JSON API endpoints

### DLC (Shadow of the Erdtree)

3. **Kaggle: Ultimate Elden Ring with Shadow of the Erdtree DLC** by Pedro Altobelli  
   📦 [pedroaltobelli96/ultimate-elden-ring-with-shadow-of-the-erdtree-dlc](https://www.kaggle.com/datasets/pedroaltobelli96/ultimate-elden-ring-with-shadow-of-the-erdtree-dlc)  
   - **License**: CC BY-SA 4.0
   - **Format**: CSVs with `dlc` column

4. **GitHub: Impalers Archive** (DLC Text Dump)  
   🔗 [github.com/ividyon/Impalers-Archive](https://github.com/ividyon/Impalers-Archive)  
   - **License**: Not specified (check repository)
   - **Format**: Master.html with dialogue and item descriptions

5. **Carian Archive (FMG Localization Text)**  
   🔗 [github.com/AsteriskAmpersand/Carian-Archive](https://github.com/AsteriskAmpersand/Carian-Archive)  
   - **Files**: Weapon, armor, goods, accessory, gem, boss, magic, and talk FMG XMLs (e.g., `WeaponName.fmg.xml`, `ProtectorCaption.fmg.xml`, `TalkMsg.fmg.xml`).
   - **Usage**: Supplies authoritative names/captions plus NPC dialogue that now flow into the canonical tables and lore corpus. Run `poetry run corpus fetch --carian` (included in `--all`) to cache everything under `data/raw/carian_archive/`, or place them manually to preserve custom subdirectories.
      - **Alias handling**: Each FMG dataset has an ordered list of candidate filenames defined in `corpus/ingest_carian_fmg.py` (`CARIAN_FMG_CANDIDATES`). The downloader/loader will pick the first file that exists locally (e.g., `WeaponSkillName.fmg.xml` falls back to `ArtsName.fmg.xml`). To add another alias, append it to the relevant candidate list and rerun `poetry run corpus fetch --carian`.

**Note**: Fextralife wiki is referenced for manual validation only—no automated scraping.

## 📁 Repository Structure

```
elden-botany-corpus/
├── src/corpus/              # Core Python package
│   ├── config.py            # Configuration (env vars)
│   ├── models.py            # Pydantic data models
│   ├── utils.py             # Utilities (hashing, I/O)
│   ├── ingest_kaggle.py     # Kaggle dataset ingestion
│   ├── ingest_github_json.py  # GitHub API fallback
│   ├── ingest_impalers.py   # DLC text dump parsing
│   ├── ingest_carian_fmg.py  # Carian Archive FMG downloader
│   ├── reconcile.py         # Deduplication & fuzzy matching
│   ├── curate.py            # Curation pipeline
│   ├── export.py            # Export utilities
│   ├── embeddings.py        # Embedding generation (OpenAI/local)
│   ├── pgvector_loader.py   # PostgreSQL + pgvector loader
│   └── cli.py               # Command-line interface
├── sql/                     # PostgreSQL schema
│   ├── 001_enable_extensions.sql
│   ├── 010_schema.sql
│   └── 020_indexes.sql
├── data/                    # Data directory (gitignored)
│   ├── raw/                 # Raw downloads (cached)
│   └── curated/             # Curated outputs
│       ├── unified.parquet  # Main output
│       ├── unified.csv
│       ├── metadata.json    # Provenance & stats
│       ├── quality/         # HTML/JSON profiling artifacts
│       └── unmapped_dlc_text.csv  # Unmatched texts for review
├── docker/                  # Docker setup
│   ├── Dockerfile
│   └── compose.example.yml
├── .github/workflows/       # CI/CD
│   ├── ci.yml
│   └── nightly-refresh.yml
├── tests/                   # Unit tests
├── examples/                # Example notebooks
└── pyproject.toml           # Poetry dependencies
```

## 🚀 Quick Start

│       ├── quality/         # HTML/JSON profiling artifacts

### 1. Clone & Install

```bash
git clone https://github.com/waaseyaalabs/elden-botany-corpus.git
cd elden-botany-corpus

# Quick setup (installs Poetry if needed + dependencies)
make setup

# Or manually:
poetry install
```

### 2. Configure Kaggle API

1. Go to [https://www.kaggle.com/settings/account](https://www.kaggle.com/settings/account)
2. Click **"Create New API Token"** → downloads `kaggle.json`
3. Copy credentials to `.env`:

```bash
cp .env.example .env
# Edit .env and add:
KAGGLE_USERNAME=your_username
KAGGLE_KEY=your_api_key
```

### 3. Fetch & Curate Data

```bash
# Fetch all sources (downloads ~50-100MB)
poetry run corpus fetch --all

# Reconcile & curate (generates unified.parquet)
poetry run corpus curate
```

`corpus fetch` now includes a `--carian/--no-carian` toggle for the Carian Archive FMG XMLs (enabled by default and implied by `--all`).

**Output**: `data/curated/unified.parquet` (~5-10MB) with all entities. Per-dataset profiling summaries live under `data/curated/quality/*.json|html` and are referenced inside `data/curated/metadata.json`.

### Incremental Refreshes

Both `fetch` and `curate` support append-only refreshes:

```bash
# Skip previously processed rows/files
poetry run corpus fetch --incremental
poetry run corpus curate --incremental

# Reprocess items created on/after a timestamp (UTC)
poetry run corpus curate --incremental --since "2025-11-01T00:00:00Z"
```

- State is persisted in `data/processed/incremental_manifest.json` along with
   file hashes and record signatures per dataset.
- Curated entities are snapshotted to
   `data/curated/state/reconciled_entities.json` and reused as the baseline for
   incremental runs.
- Pass `--full` (the default) or remove the manifest/state files to force a
   clean rebuild when needed.
- Logs note how many Kaggle/Impalers records were newly ingested vs. skipped so
   you can verify the delta before exporting.
- **Lore embeddings and RAG artifacts always rebuild in full.** Changes to text
   weighting, embedding models, or reranker parameters require
   `make rag-embeddings && make rag-index` so downstream search stays consistent
   with the curated text snapshot. The incremental manifest intentionally does
   not gate these pipelines. The embedding pipeline now records a checksum guard
   at `data/embeddings/rag_rebuild_state.json`; run `make rag-guard` any time to
   confirm whether the stored embeddings/index are in-sync with the current lore
   corpus + weighting config.

### 4. (Optional) Load to PostgreSQL

```bash
# Start Postgres with pgvector (via Docker)
docker compose -f docker/compose.example.yml up -d postgres

# Load data (creates schema + inserts rows)
poetry run corpus load --dsn postgresql://elden:elden_password@localhost:5432/elden
```

## 🔍 Lore RAG Workflow

Layer 2 (lore text) can now be embedded, indexed, and queried directly from this repo. The commands below assume you already ran `corpus curate` so `data/curated/lore_corpus.parquet` exists.

```bash
# 1) Materialize lore embeddings
make rag-embeddings          # poetry run python -m pipelines.build_lore_embeddings

# 2) Build / refresh the FAISS index + metadata artifacts
make rag-index               # poetry run python -m pipelines.build_rag_index

# 3) Issue ad-hoc semantic searches (pass QUERY="...")
make rag-query QUERY="scarlet rot and decay"

# Equivalent manual invocation if you prefer not to use make
poetry run python -m rag.query "thorned death rites" --filter text_type!=dialogue --top-k 10
```

`make rag-embeddings` automatically refreshes
`data/embeddings/rag_rebuild_state.json`. `make rag-guard` surfaces the current
status (and exits with code 1 when a rebuild is required) so you can verify the
checksum guard before pushing RAG artifacts.

Artifacts are written under `data/embeddings/`:

- `lore_embeddings.parquet`: vectors + provenance columns
- `faiss_index.bin`: FAISS index (L2-normalized IP search)
- `rag_metadata.parquet`: metadata joined with embeddings for filterable search
- `rag_index_meta.json`: dimension, vector count, normalization flag, provider/model names, plus the default reranker configuration (name, model, candidate pool size)

Key query flags:

- `--top-k` now defaults to **10** results; queries internally fetch extra matches and deduplicate near-identical prose so the default window is unique-heavy.
- `--mode balanced|raw` controls the final ordering. `balanced` (default) interleaves descriptions, lore, impalers excerpts, and dialogue so no single text type dominates the top-k window unless diversity is impossible. `raw` preserves the FAISS/reranker order when you need the pure similarity list.
- `--reranker identity|cross_encoder` toggles the second-pass scorer. `cross_encoder` downloads `cross-encoder/ms-marco-MiniLM-L-6-v2`, reranks the top ~50 FAISS candidates, annotates `reranker_score`, and writes its configuration to `rag_index_meta.json`.
- `--filter` accepts repeatable expressions such as `text_type=description` or `text_type!=dialogue,effect`, enabling inclusive/exclusive filtering per column.
- `--category/--text-type/--source` remain available for quick single-column filters.

Carian Archive FMGs (TalkMsg, BossCaption, Weapon/Armor/Goods captions, etc.) are ingested during the canonical + lore builds, with fallback aliases (e.g., `ArtsName.fmg.xml`) ensuring new DLC assets land even when primary files go missing. NPC speech appears as `text_type=dialogue` rows alongside canonical descriptions and Impalers excerpts, and the additional Carian records surface throughout the RAG metadata for filtering.

### Text-Type Weighting

Narrative-heavy fields (`description`, `dialogue`, `impalers_excerpt`, `quote`, `lore`) are boosted before embedding so they surface ahead of terse `effect` lines. The default coefficients live in `config/text_type_weights.yml` and can be overridden per run:

```bash
poetry run python -m pipelines.build_lore_embeddings \
   --text-type-weights /path/to/custom_weights.yml
```

Dialogue now carries a 0.7 weight (down from 1.5) so Carian TalkMsg rows complement, rather than overwhelm, descriptive lore. Whenever you change the YAML file, rerun `make rag-embeddings && make rag-index` to regenerate `data/embeddings/*` with the new weighting. The guard file mentioned above captures the text-weight configuration in addition to the curated parquet hash, so any YAML edits show up immediately when you run `make rag-guard`.

Each embedding row records `embedding_strategy=weighted_text_types_v1`, the configured weight file, and a `text_type_components` pipe-delimited summary so downstream evaluations can confirm which snippets influenced the vector.

> ℹ️ **These pipelines do not support incremental skips.** Always run `make
> rag-embeddings && make rag-index` after changing curated text, weighting
> configs, embedding/reranker settings, or the ingestion manifest. Skipping
> these steps can leave stale vectors that no longer match the curated corpus.

### Qualitative Retrieval Evaluation

The notebook `notebooks/qualitative_rag_eval.ipynb` records five representative probes (Radahn gravitation, scarlet rot, fungal armor, gloam-eyed thorns, Messmer flame). Each run captures:

- Aggregate stats about the current index (vector count, dimension, category/source coverage)
- Ranked matches per query (top 5) to verify category diversity and provenance mixing
- Narrative observations detailing strengths (high topical precision, Impalers coverage) and weaknesses (short Kaggle blurbs, tightly clustered status tooltips)
- Follow-up ideas for weighting schemes (boost quotes, strip repeated prefixes, down-rank very short spans)

Use the notebook whenever embeddings are regenerated so Layer 3 contributors can cite concrete retrieval behavior in their annotations.

## 📖 Usage
**Output**: `data/curated/unified.parquet` (~5-10MB) with all entities. Per-dataset profiling summaries live under `data/curated/quality/*.json|html` and are referenced inside `data/curated/metadata.json`.
### CLI Commands

```bash
# Fetch data from sources
corpus fetch --all                    # All sources
corpus fetch --base --dlc             # Only Kaggle
corpus fetch --github --impalers      # Fallback + DLC text

# Curate corpus (reconcile + dedupe + export)
corpus curate                 # writes reports under data/curated/quality/
corpus curate --no-quality    # skip HTML/JSON profiling

# Load to PostgreSQL
corpus load --dsn <POSTGRES_DSN> --create-schema --embed openai
```

### Python API

```python
import polars as pl

# Load curated data
df = pl.read_parquet("data/curated/unified.parquet")

# Filter by type
bosses = df.filter(pl.col("entity_type") == "boss")
dlc_only = df.filter(pl.col("is_dlc") == True)

# Access metadata
meta = pl.read_json("data/curated/metadata.json")
print(meta["row_counts"])
```

### PostgreSQL Queries

```sql
-- Find all DLC bosses
SELECT name, text, meta->>'hp' AS hp
FROM elden.corpus_chunk
WHERE entity_type = 'boss' AND is_dlc = true;

-- Semantic search (requires embeddings)
SELECT name, entity_type, text,
       embedding <-> '[0.1, 0.2, ...]'::vector AS distance
FROM elden.corpus_chunk
WHERE embedding IS NOT NULL
ORDER BY distance
LIMIT 10;

-- Full-text search
SELECT name, ts_rank(to_tsvector('english', text), query) AS rank
FROM elden.corpus_chunk,
     to_tsquery('english', 'flame & sword') query
WHERE to_tsvector('english', text) @@ query
ORDER BY rank DESC;
```

## 🧪 Schema Overview

```
elden.corpus_document
├── id (UUID)
├── source_type (TEXT)     # 'kaggle_base' | 'kaggle_dlc' | 'github_api' | 'dlc_textdump'
├── source_uri (TEXT)
├── title (TEXT)
├── language (TEXT)
└── created_at (TIMESTAMPTZ)

elden.corpus_chunk
├── id (UUID)
├── document_id (UUID → corpus_document.id)
├── entity_type (TEXT)     # 'weapon', 'boss', 'armor', etc.
├── game_entity_id (TEXT)  # stable slug (e.g., 'sword_of_night_and_flame')
├── is_dlc (BOOLEAN)
├── name (TEXT)
├── text (TEXT)            # merged description/dialogue
├── meta (JSONB)           # stats, scaling, sources, etc.
├── span_start (INT)
├── span_end (INT)
└── embedding (VECTOR)     # optional pgvector embedding
```

**Indexes**:
- B-tree: `entity_type`, `is_dlc`, `game_entity_id`, `document_id`
- GIN: `meta` (JSONB), full-text search on `text`
- HNSW: `embedding` (vector similarity)

## 🔧 Development

### Initial Setup

```bash
# One-command setup (recommended)
make setup

# Or step-by-step:
poetry install                    # Install dependencies
poetry run ruff format src/ tests/  # Format code
```

### Running CI Checks Locally

Before pushing code, run the same checks that CI will run:

```bash
# Run all CI checks (lint + test)
make ci-local

# Or individually:
make lint      # Check formatting, linting, and types (no modifications)
make test      # Run unit tests with coverage
make format    # Auto-format code (for fixing issues)
```

**Helper Scripts** (alternative to make commands):
```bash
./scripts/lint.sh   # Run all lint checks
./scripts/test.sh   # Run unit tests
```

### Code Quality Standards

This project enforces:
- **Ruff formatting** (line length: 100)
- **Ruff linting** (pycodestyle, pyflakes, isort, bugbear, comprehensions)
- **Mypy strict type checking**

**Important**: `make lint` checks without modifying files (same as CI). Use `make format` to auto-fix issues locally.

### Run Tests

```bash
# Run all unit tests (fast, no external dependencies)
poetry run pytest -v

# Run with coverage
poetry run pytest --cov=corpus --cov-report=html

# Skip integration tests explicitly
poetry run pytest -m "not integration"

# Run only integration tests (requires services)
RUN_INTEGRATION=1 poetry run pytest -m integration
```

### Integration Tests

Integration tests require external services (PostgreSQL with pgvector, Kaggle API) and are **skipped by default**.

**To enable integration tests:**

```bash
# Set environment variables
export RUN_INTEGRATION=1
export POSTGRES_DSN="postgresql://user:pass@localhost:5432/test_db"
export KAGGLE_USERNAME="your_username"
export KAGGLE_KEY="your_api_key"

# Run integration tests
poetry run pytest -m integration
```

### Local Postgres Integration Testing

The easiest way to run Postgres integration tests is using Docker Compose:

```bash
# Start PostgreSQL 16 with pgvector extension
docker compose -f docker/compose.example.yml up -d postgres

# Set the connection string
export POSTGRES_DSN=postgresql://elden:elden_password@localhost:5432/elden

# Run integration tests only
poetry run pytest -m integration -v

# Alternative: run just the Postgres end-to-end test
poetry run pytest tests/test_pg_integration_e2e.py -v

# Clean up when done
docker compose -f docker/compose.example.yml down -v
```

**What the integration tests verify:**
- PostgreSQL extensions (pgvector, uuid-ossp) can be created
- Schema and tables are created correctly
- HNSW vector index works for similarity search
- Full-text search (FTS) with GIN indexes functions properly
- JSONB metadata queries execute correctly
- CASCADE delete behavior on foreign keys

**Using Make targets:**

```bash
# Start database
make docker-up

# Run integration tests (with POSTGRES_DSN set)
export POSTGRES_DSN=postgresql://elden:elden_password@localhost:5432/elden
poetry run pytest -m integration

# Stop database
make docker-down
```

**Fixtures and test database:**
- The `pg_connection` fixture automatically initializes the schema from `sql/*.sql` files
- Each test uses a transaction that is rolled back after completion
- Schema is dropped after the test session ends

### Linting & Type Checking

```bash
poetry run ruff check src/ tests/
poetry run mypy src/
```

### Docker Development

```bash
# Start all services (Postgres + Jupyter)
docker compose -f docker/compose.example.yml up

# Access Jupyter at http://localhost:8888
# Run CLI in worker container:
docker exec -it elden-corpus-worker bash
poetry run corpus fetch --all
```

## 📊 Data Quality

### Automated Quality Reports

- Each `corpus curate` run now emits JSON + HTML profiles under `data/curated/quality/` (one per unified + entity dataset).
- Reports capture row counts, column-level null percentages, numeric min/max/mean/std, categorical top values, and a curated list of `alerts` for quick triage.
- `data/curated/metadata.json` stores the same payload under `quality_reports`, so CI or downstream jobs can fail fast if an alert is present (e.g., `null_percent >= 50%`).
- To enforce thresholds, parse the metadata file inside your automation and exit non-zero if any alert matches your rule set.

### Reconciliation Logic

**Priority Order** (highest to lowest):
1. **Kaggle DLC** (most complete DLC entities)
2. **Kaggle Base** (most complete base game entities)
3. **GitHub API** (fallback for missing entities)

**Deduplication**:
- Key: `entity_type + slug` (e.g., `boss:rennala_queen_of_the_full_moon`)
- Higher priority sources override lower ones
- Provenance from all sources is merged

**DLC Text Matching**:
- Normalized name comparison (lowercase, remove punctuation)
- Levenshtein ratio ≥ 0.86 threshold
- Unmapped texts → `unmapped_dlc_text.csv` for manual review

### Known Limitations

1. **Incomplete DLC Boss Data**: Kaggle DLC `bosses.csv` may miss some entities; reconciled with Impalers text dump.
2. **Heuristic Text Mapping**: Impalers Archive is a text dump without entity IDs; fuzzy matching may have false positives.
3. **No Dialogue Mapping**: NPC dialogue from Impalers is not entity-mapped by default (requires manual curation).
4. **Fextralife Not Scraped**: Wiki is reference-only to avoid scraping issues.

## 📜 Licensing

### Code

- **Apache License 2.0** (see `LICENSE`)

### Data

- **Kaggle Base Dataset** (Rob Mulla): CC0 1.0 Universal
- **Kaggle DLC Dataset** (Pedro Altobelli): CC BY-SA 4.0
- **GitHub API** (deliton/eldenring-api): MIT
- **Impalers Archive**: Check repository license
- **Curated Outputs**: CC BY-SA 4.0 (most restrictive source license)

**Attribution**: When using curated data, please cite all upstream sources (see above).

## 🤝 Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 🐛 Issues

Report bugs or request features via [GitHub Issues](https://github.com/waaseyaalabs/elden-botany-corpus/issues).

## 🙏 Acknowledgments

- **Rob Mulla** - [Elden Ring Ultimate Dataset](https://www.kaggle.com/datasets/robikscube/elden-ring-ultimate-dataset)
- **Pedro Altobelli** - [DLC Dataset](https://www.kaggle.com/datasets/pedroaltobelli96/ultimate-elden-ring-with-shadow-of-the-erdtree-dlc)
- **deliton** - [eldenring-api](https://github.com/deliton/eldenring-api)
- **ividyon** - [Impalers Archive](https://github.com/ividyon/Impalers-Archive)
- **FromSoftware** - Elden Ring game content

## 📧 Contact

For questions or collaboration: [open an issue](https://github.com/waaseyaalabs/elden-botany-corpus/issues)

---

**Star ⭐ this repo if you find it useful!**
