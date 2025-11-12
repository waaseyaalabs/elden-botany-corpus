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

### Prerequisites

- **Python 3.11+**
- **Poetry** (install: `curl -sSL https://install.python-poetry.org | python3 -`)
- **Kaggle API credentials** (for dataset downloads)

### 1. Clone & Install

```bash
git clone https://github.com/waaseyaalabs/elden-botany-corpus.git
cd elden-botany-corpus

# Install dependencies
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

**Output**: `data/curated/unified.parquet` (~5-10MB) with all entities.

### 4. (Optional) Load to PostgreSQL

```bash
# Start Postgres with pgvector (via Docker)
docker compose -f docker/compose.example.yml up -d postgres

# Load data (creates schema + inserts rows)
poetry run corpus load --dsn postgresql://elden:elden_password@localhost:5432/elden
```

## 📖 Usage

### CLI Commands

```bash
# Fetch data from sources
corpus fetch --all                    # All sources
corpus fetch --base --dlc             # Only Kaggle
corpus fetch --github --impalers      # Fallback + DLC text

# Curate corpus (reconcile + dedupe + export)
corpus curate

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

**Using Docker for integration tests:**

```bash
# Start PostgreSQL with pgvector
docker compose -f docker/compose.example.yml up -d postgres

# Run integration tests
RUN_INTEGRATION=1 POSTGRES_DSN="postgresql://elden:password@localhost:5432/elden" \
  poetry run pytest -m integration
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
