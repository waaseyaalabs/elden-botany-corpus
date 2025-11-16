# GitHub Copilot Instructions: Elden Ring Botany Corpus

## Mission

Produce a curated, provenance-tracked Elden Ring + Shadow of the Erdtree corpus suitable for RAG and analysis, persistable to PostgreSQL + pgvector, with repeatable ETL and verbatim primary quotations. The agent must prefer primary sources and avoid secondary summaries.

---

## Golden Rules

### 1. Primary-text first
Use canonical game text (item descriptions, dialogue, DLC dumps). Avoid "lore essays" as sources of truth.

### 2. Verbatim quotes rule
When generating docs/scripts, include ≤ 20 words per quote (fair-use) and cite the exact source (document id + span).

### 3. Provenance everywhere
Every curated row must include `meta.provenance=[{source, uri, sha256, retrieved_at}]`.

### 4. No scraping Fextralife
You may cite Fextralife as reference in docs, but **do not crawl/scrape**.

### 5. Reproducibility > convenience
All downloads are hashed; ETL is deterministic; any change opens an auto-PR with row counts + hashes.

### 6. Prefer latest stable packages
When adding a new dependency, pin it to the latest stable release available at the time of introduction and document the rationale so downstream automation can monitor for updates.

---

## Scope of Work

### Allowed Modifications

Create/modify ETL modules under `src/corpus/`:
- `ingest_kaggle.py` - Base + DLC datasets from Kaggle
- `ingest_github_json.py` - deliton/eldenring-api JSON fallback
- `ingest_impalers.py` - Impalers "Master.html" parsing
- `reconcile.py` - Priority merge: dlc → base → api; boss text backfill
- `curate.py` - Unified long table; verbatim + normalized fields
- `export.py` - CSV/Parquet export utilities
- `pgvector_loader.py` - Create schema, load parquet, optional embeddings
- `embeddings.py` - OpenAI/local embedding generation
- `models.py` - Pydantic data models (type-safe)
- `utils.py` - Hashing, I/O utilities
- `config.py` - Pydantic settings with env vars
- `cli.py` - Click-based CLI commands

Add/maintain SQL in `/sql`:
- `001_enable_extensions.sql` → `CREATE EXTENSION vector;`
- `010_schema.sql` → `elden.corpus_document`, `elden.corpus_chunk`
- `020_indexes.sql` → HNSW vector + helper indexes (B-tree, GIN)

Maintain tests in `/tests`:
- `pytest` with fixtures in `tests/fixtures/`
- `conftest.py` for shared fixtures
- Unit tests for all core modules
- Aim for >80% code coverage

### Prohibited Actions

**Do not:**
- Introduce new external services or write secrets to the repo
- Pull content from sites blocked by policy or firewall (ask for allowlist first)
- Replace primary text with paraphrases in the curated "verbatim" field
- Scrape Fextralife or other wikis
- Commit sensitive data (API keys, tokens) - use `.env` and `.env.example`

---

## Data Sources (Allowed)

### Base Game
1. **Kaggle**: "Elden Ring Ultimate Dataset" (Rob Mulla)
   - License: CC0 1.0 Universal
   - Format: CSV tables (14 entity types)

2. **GitHub**: deliton/eldenring-api
   - License: MIT
   - Format: JSON endpoints (fallback)

### DLC (Shadow of the Erdtree)
3. **Kaggle**: "Ultimate Elden Ring with Shadow of the Erdtree DLC" (Pedro Altobelli)
   - License: CC BY-SA 4.0
   - Format: CSV with `dlc` column; bosses.csv may be incomplete

4. **GitHub**: ividyon/Impalers-Archive
   - License: Check repository
   - Format: Master.html English dump

### Reference Only
- **Fextralife wiki**: Manual validation only—no automated scraping

---

## Repository Conventions

### Technology Stack
- **Python**: 3.11+ with Poetry dependency management
- **Lint/Type/Test**: Ruff (linting), mypy --strict (type checking), pytest (testing)
- **Data Processing**: Polars (fast DataFrame ops), Parquet (storage)
- **Database**: PostgreSQL 14+ with pgvector extension
- **Embeddings**: OpenAI (text-embedding-3-small) or sentence-transformers (local)

### Directory Structure
```
data/
  raw/        # Raw zips/html (gitignored, cached downloads)
  curated/    # CSV/Parquet outputs + metadata.json
sql/
  001_enable_extensions.sql
  010_schema.sql
  020_indexes.sql
src/corpus/   # Python package
tests/        # Unit tests with fixtures
docker/       # Dockerfile + compose.example.yml
.github/
  workflows/  # CI and nightly-refresh
```

### Database Schema (pgvector)

```sql
CREATE SCHEMA IF NOT EXISTS elden;

-- Source file / logical document
CREATE TABLE IF NOT EXISTS elden.corpus_document (
  id UUID PRIMARY KEY,
  source_type TEXT NOT NULL,       -- kaggle_base | kaggle_dlc | github_api | dlc_textdump
  source_uri  TEXT NOT NULL,
  title       TEXT,
  language    TEXT DEFAULT 'en',
  created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Retrieval unit (chunk/row)
CREATE TABLE IF NOT EXISTS elden.corpus_chunk (
  id UUID PRIMARY KEY,
  document_id UUID REFERENCES elden.corpus_document(id) ON DELETE CASCADE,
  entity_type TEXT NOT NULL,       -- weapon, armor, boss, npc, item, incantation, text_snippet, ...
  game_entity_id TEXT,             -- stable slug (e.g., 'sword_of_night_and_flame')
  is_dlc BOOLEAN DEFAULT FALSE,
  name TEXT,
  text TEXT NOT NULL,              -- verbatim primary text (or DLC dump snippet)
  meta JSONB NOT NULL DEFAULT '{}',-- structured stats, acquisition, links
  span_start INT,
  span_end INT,
  embedding VECTOR(1536)           -- nullable; fill in later
);
```

---

## ETL Rules of the Road

### 1. Priority Merge Order
**Kaggle DLC → Kaggle Base → GitHub API**

Highest priority sources override lower ones for duplicates (by `entity_type + slug`).

### 2. Boss Text Gaps
Fill from Impalers (Master.html) via name matching:
- Slug normalization + normalized text comparison
- Levenshtein threshold ≥ 0.86
- Unmatched texts → `data/curated/unmapped_dlc_text.csv` for manual review

### 3. Verbatim + Normalized Fields
- Keep `text` = verbatim game text
- Store paraphrases/notes in `meta` JSONB field

### 4. Semantic Tags
Add lightweight semantic pass to `meta.semantics.tags`:
- Keywords: `["pollen", "spore", "blossom", "meteor", "seed"]` when terms appear
- Use for botanical/cosmic themes in Elden Ring lore

### 5. Hash Everything
- Save SHA-256 for raw zips/html into provenance
- Store in `data/curated/metadata.json` for integrity verification

---

## CI/CD Expectations

### Required Checks (All PRs)
- ✅ `ruff check` clean (no linting errors)
- ✅ `mypy --strict` clean (type safety)
- ✅ `pytest -q` passing (all tests pass)

### Nightly Refresh (`.github/workflows/nightly-refresh.yml`)
1. Run `corpus fetch --all` + `corpus curate`
2. If row counts/hashes changed, open auto-PR with:
   - Counts per source (base, DLC, GitHub, Impalers)
   - #deduped entities, #unmapped DLC texts
   - Changed SHA-256 hashes
3. PR title: `chore: nightly data refresh YYYY-MM-DD`

---

## Commit & PR Standards

### Branch Naming
- `feat/...` - New features
- `fix/...` - Bug fixes
- `chore/...` - Maintenance, dependencies
- `docs/...` - Documentation updates

### Commit Messages
Use imperative present tense:
- ✅ "Add DLC boss text matching"
- ✅ "Fix provenance tracking in reconcile.py"
- ❌ "Added new feature" (past tense)
- ❌ "Fixes stuff" (vague)

### PR Template (Implicit)
Include in PR description:
1. **What changed**: Brief summary of modifications
2. **Data impact**: Row counts, unmatched texts, file hash changes
3. **Testing**: Commands run and results (e.g., `pytest -v` output)
4. **Licensing**: Confirm upstream terms unchanged

**Never commit**:
- Secrets (API keys, tokens) → use `.env`
- Large binary files (>10MB) → use external storage or cache
- Personal data or credentials

---

## Networking & Environment

### Environment Variables (`.env`)
Document all required vars in `.env.example`:

```bash
# Kaggle API credentials
KAGGLE_USERNAME=your_username
KAGGLE_KEY=your_api_key

# PostgreSQL connection
POSTGRES_DSN=postgresql://user:pass@localhost:5432/elden

# OpenAI embeddings (optional)
OPENAI_API_KEY=sk-...
```

### Blocked Hosts
If the agent needs to reach blocked hosts (e.g., Poetry installer):
1. Ask maintainers to add Actions setup steps for dependencies, or
2. Request allowlist for specific hosts in Copilot coding agent settings

---

## Testing Checklist

### Local Development
```bash
poetry install               # Install dependencies
ruff check .                 # Lint code
mypy --strict src tests      # Type check
pytest -q                    # Run tests
pytest --cov=corpus --cov-report=html  # Coverage report
```

### Acceptance Criteria for New ETL Work
- ✅ Provenance stored for each row
- ✅ Verbatim text preserved and quoted appropriately in docs/scripts
- ✅ Deterministic: same inputs → same outputs; `metadata.json` updated
- ✅ No scraping of disallowed sites
- ✅ CI green (ruff, mypy, pytest)

---

## When to Ask for Help

Open a GitHub Issue if:
1. A dataset schema changed or Kaggle URL moved
2. Firewall blocks critical steps (request allowlist or pre-setup in Actions)
3. DLC text mapping reliability is low (attach examples + current threshold hits)
4. Licensing terms for upstream sources changed

**Do not**:
- Guess or hallucinate data source URLs
- Implement workarounds for network issues without approval
- Skip provenance tracking to "save time"

---

## Code Style & Quality

### Prefer
- **Polars** for ETL speed (faster than Pandas)
- **Arrow/Parquet** for storage (columnar, compressed)
- **Pure functions** where possible (separate I/O from transforms)
- **Small, tested units** (avoid large PRs mixing concerns)

### Type Safety
- Use Pydantic models for all data structures
- Enable `mypy --strict` for all new code
- Annotate all function signatures with types

### Error Handling
- Use explicit exception handling (avoid bare `except:`)
- Log errors with context (include entity type, source, etc.)
- Fail fast on data quality issues (don't silently skip)

### Performance
- Use Polars for DataFrame operations (10-100x faster than Pandas)
- Batch API calls (embeddings, downloads)
- Cache expensive operations (downloads, embeddings)

---

## Example CLI Contract

```bash
# Fetch all sources (downloads + caches)
python -m corpus fetch --all
python -m corpus fetch --base --dlc       # Only Kaggle
python -m corpus fetch --github --impalers  # Fallback + DLC text

# Curate corpus (reconcile + dedupe + export)
python -m corpus curate
# Output: data/curated/unified.parquet, metadata.json, unmapped_dlc_text.csv

# Load to PostgreSQL (with optional embeddings)
python -m corpus load --dsn $POSTGRES_DSN
python -m corpus load --dsn $POSTGRES_DSN --embed openai --model text-embedding-3-small
python -m corpus load --dsn $POSTGRES_DSN --embed local --model all-MiniLM-L6-v2
```

---

## Project-Specific Best Practices

### 1. Slug Generation
Create stable, lowercase slugs for entity IDs:
```python
# Example: "Sword of Night and Flame" → "sword_of_night_and_flame"
import re
def make_slug(name: str) -> str:
    return re.sub(r'[^a-z0-9]+', '_', name.lower()).strip('_')
```

### 2. Fuzzy Matching
Use Levenshtein distance for DLC text mapping:
```python
from Levenshtein import ratio
threshold = 0.86  # Configurable via settings
if ratio(normalized_name1, normalized_name2) >= threshold:
    # Match found
```

### 3. Provenance Tracking
Always include in `meta.provenance`:
```python
{
    "source": "kaggle_dlc",
    "uri": "https://www.kaggle.com/datasets/...",
    "sha256": "abc123...",
    "retrieved_at": "2024-01-15T10:30:00Z"
}
```

### 4. Embedding Best Practices
- Batch embeddings (e.g., 100 at a time) for API efficiency
- Store embeddings separately from text (pgvector)
- Use HNSW index for fast similarity search
- Normalize embeddings before storing (if required by model)

### 5. Data Validation
- Check for required fields before inserting to DB
- Validate entity_type against known types
- Ensure `is_dlc` is boolean (not string)
- Verify `text` is not empty after curation

---

## Common Pitfalls to Avoid

### ❌ Don't
- Hard-code file paths (use `Path` from `pathlib`)
- Mix staging and production data in same DB schema
- Skip tests for "quick fixes"
- Copy-paste code between ingest modules (use shared utilities)
- Ignore type errors from mypy
- Use `pd.read_csv()` when `pl.read_csv()` is faster

### ✅ Do
- Use environment variables for all external config
- Write integration tests for full pipeline
- Document all assumptions about data format
- Handle missing/malformed data gracefully
- Add logging for all I/O operations
- Update `metadata.json` after every curate run

---

## Quick Reference

### File Locations
- **ETL Modules**: `src/corpus/*.py`
- **Tests**: `tests/test_*.py`
- **SQL Schema**: `sql/*.sql`
- **Config**: `.env` (local), `.env.example` (template)
- **Output**: `data/curated/*.{parquet,csv,json}`
- **Raw Cache**: `data/raw/*.{zip,html}` (gitignored)

### Key Commands
```bash
make install   # poetry install
make fetch     # Download all sources
make curate    # Run full curation pipeline
make load      # Load to PostgreSQL
make test      # Run pytest
make lint      # Run ruff + mypy
make clean     # Remove cached/generated files
```

### Entity Types (14 total)
weapon, armor, shield, boss, npc, item, incantation, sorcery, talisman, spirit, ash_of_war, class, creature, location

### Priority Order (Reconciliation)
1. `kaggle_dlc` (highest priority)
2. `kaggle_base`
3. `github_api` (lowest priority)

### Embedding Dimensions
- OpenAI `text-embedding-3-small`: 1536
- sentence-transformers `all-MiniLM-L6-v2`: 384

---

## Licensing Compliance

### Code
- **This Repository**: Apache License 2.0

### Data
- **Kaggle Base** (Rob Mulla): CC0 1.0 Universal (Public Domain)
- **Kaggle DLC** (Pedro Altobelli): CC BY-SA 4.0
- **GitHub API** (deliton): MIT
- **Impalers Archive**: Check repository for license
- **Curated Outputs**: CC BY-SA 4.0 (most restrictive upstream license)

**Attribution Rule**: When using curated data, cite all upstream sources in documentation.

---

## Final Notes

This project prioritizes:
1. **Data Quality** over speed
2. **Reproducibility** over convenience
3. **Type Safety** over flexibility
4. **Provenance** over brevity

When in doubt:
- Check existing code patterns in `src/corpus/`
- Refer to test fixtures for expected data format
- Consult `PROJECT_SUMMARY.md` for architectural decisions
- Ask via GitHub Issues before making breaking changes

**Remember**: This is a scholarly corpus for RAG/analytics, not a game guide. Preserve primary sources, track provenance, and maintain deterministic pipelines.
