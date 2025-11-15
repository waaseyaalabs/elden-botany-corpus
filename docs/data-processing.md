# Data Processing Pipeline

This document describes the data processing pipeline that transforms raw Kaggle datasets into clean, validated, schema-consistent Parquet files.

## Overview

The processing pipeline:
1. Reads raw data from `data/raw/`
2. Normalizes column names and categorical values
3. Handles missing values with dataset-specific strategies
4. Validates against Pandera schemas
5. Writes processed Parquet files to `data/processed/`

## Architecture

```
data/
├── raw/              # Raw Kaggle datasets (gitignored)
│   └── <dataset-name>/
│       ├── *.csv
│       ├── *.json
│       └── *.parquet
├── processed/        # Processed outputs (gitignored)
│   ├── .cache/       # Processing cache (hashes, metadata)
│   └── <dataset-name>/
│       └── *.parquet
└── README.md

pipeline/
├── __init__.py       # Package exports
├── process.py        # Main DataProcessor class
├── schemas.py        # Pandera schema definitions
└── utils.py          # Processing utilities

scripts/
└── process_data.py   # CLI entrypoint
```

> **Version control:** `data/raw/` and `data/processed/` only track `.gitkeep` placeholders so locally generated datasets stay out of commits by default.

## Schemas

The pipeline defines strict schemas for Elden Ring datasets using [Pandera](https://pandera.readthedocs.io/):

### Weapons Schema
- **weapon_id** (int64, required, unique)
- **name** (string, required)
- **weapon_type** (string, required) - One of: sword, greatsword, katana, dagger, etc.
- **damage_physical, damage_magic, damage_fire, etc.** (int64, optional)
- **weight** (float64, required)
- **required_str, required_dex, etc.** (int64, optional)
- **scaling_str, scaling_dex, etc.** (string, optional) - One of: E, D, C, B, A, S, -

### Bosses Schema
- **boss_id** (int64, required, unique)
- **name** (string, required)
- **region** (string, optional)
- **hp** (int64, optional)
- **runes_dropped** (int64, optional)
- **optional** (bool, optional)
- **boss_type** (string, optional) - One of: main, demigod, field, dungeon, dragon, etc.

### Items Schema
- **item_id** (int64, required, unique)
- **name** (string, required)
- **category** (string, required) - One of: weapon, armor, consumable, spell, etc.
- **description** (string, optional)
- **weight** (float64, optional)
- **sell_price** (int64, optional)
- **rarity** (string, optional) - One of: common, uncommon, rare, legendary

### Armor Schema
- **armor_id** (int64, required, unique)
- **name** (string, required)
- **armor_type** (string, required) - One of: head, chest, arms, legs
- **weight** (float64, required)
- **defense_physical, defense_magic, etc.** (float64, optional)
- **resistance_immunity, resistance_robustness, etc.** (int64, optional)

### Spells Schema
- **spell_id** (int64, required, unique)
- **name** (string, required)
- **spell_type** (string, required) - One of: sorcery, incantation
- **fp_cost** (int64, required)
- **slots_required** (int64, required)
- **required_int, required_fai, etc.** (int64, optional)

## Transformations

### Column Name Normalization
- Convert to lowercase
- Replace spaces with underscores
- Remove special characters
- Strip whitespace

Example: `"Weapon Type"` → `"weapon_type"`

### Categorical Normalization
Dataset-specific mappings normalize categorical values:

**Weapons:**
- `"Straight Sword"` → `"sword"`
- `"Great Sword"` → `"greatsword"`
- `"Katanas"` → `"katana"`

**Bosses:**
- `"Demigods"` → `"demigod"`
- `"Field Boss"` → `"field"`

**Armor:**
- `"Helmet"` → `"head"`
- `"Gauntlets"` → `"arms"`
- `"Greaves"` → `"legs"`

### Missing Value Handling

**Weapons:**
- Damage fields (physical, magic, fire, etc.) → `0`
- Scaling fields → `"-"`

**Bosses:**
- `optional` → `False`

**Spells:**
- `fp_cost` → `0`
- `slots_required` → `1`

## Usage

### Local Processing

Process all datasets:
```bash
python scripts/process_data.py
```

Force reprocess everything:
```bash
python scripts/process_data.py --force
```

Dry run (validate without writing):
```bash
python scripts/process_data.py --dry-run
```

Custom paths:
```bash
python scripts/process_data.py \
  --config config/kaggle_datasets.yml \
  --raw-dir data/raw \
  --processed-dir data/processed \
  --verbose
```

Save processing statistics:
```bash
python scripts/process_data.py \
  --output-stats processing-stats.json
```

### Change Detection & Dry Runs

Every invocation of `scripts/process_data.py` now inspects the raw tree and
prints which datasets actually need work before any heavy lifting begins. The
script uses the same hash-and-mtime logic as the processor to determine if
Parquet outputs are stale, and reports the first few out-of-date files for easy
debugging. Use `--dry-run` to execute the full validation path without writing
Parquet files, and `--force` to override the cache when you explicitly need to
rebuild everything. For CI and local development this makes it obvious when no
datasets require processing (the script will say so), while still confirming
that transformations remain valid.

> **Reminder:** `data/raw/` and `data/processed/` stay gitignored artifacts.
> The pipeline only creates `.gitkeep` placeholders so you never accidentally
> commit downloaded or generated datasets.

### Using Poetry

```bash
# Install dependencies
poetry install

# Run processing
poetry run python scripts/process_data.py

# Run tests
poetry run pytest tests/test_process_data.py -v
```

### In Python

```python
from pathlib import Path
from pipeline.process import DataProcessor

processor = DataProcessor(
    config_path=Path("config/kaggle_datasets.yml"),
    raw_dir=Path("data/raw"),
    processed_dir=Path("data/processed"),
)

# Process all datasets
results = processor.process_all(force=False, dry_run=False)

# Process specific dataset
result = processor.process_dataset("elden-ring-weapons")
```

## CI/CD Integration

The pipeline runs in GitHub Actions with two jobs:

- A workflow-level concurrency group (`process-data-${{ github.ref }}`) ensures only one run per branch executes at a time; newer pushes cancel any in-progress runs.

### 1. Validate Processing (Dry Run)
Runs on:
- Pull requests affecting pipeline code
- Pushes to `main` that touch pipeline, docs, or tests
- Manual trigger

Actions:
- Creates sample test data
- Runs pipeline in dry-run mode
- Validates schema compliance
- Runs unit tests (`tests/test_process_data.py` and `tests/test_processing_pending.py`)

### 2. Process Data
Runs on:
- Manual trigger (workflow_dispatch)
- Weekly schedule (Mondays at 4 AM UTC)

Actions:
- Downloads Kaggle datasets
- Processes all enabled datasets
- Uploads processing statistics as artifacts
- Uploads sample processed data

### Triggering Manually

Via GitHub Actions UI:
1. Go to Actions tab
2. Select "Data Processing Pipeline"
3. Click "Run workflow"
4. Optional: Check "Force reprocess all datasets"

Via GitHub CLI:
```bash
# Normal processing
gh workflow run "Data Processing Pipeline"

# Force reprocess
gh workflow run "Data Processing Pipeline" -f force=true
```

## Incremental Processing

The pipeline intelligently avoids reprocessing:

1. **Modification time check**: Skip if processed file is newer than raw file
2. **Hash-based cache**: Skip if raw file hash matches cached value
3. **Force flag**: Override all checks and reprocess

Cache location: `data/processed/.cache/<dataset-name>.json`

Cache format:
```json
{
  "data/raw/weapons/weapons.csv": "sha256_hash_here"
}
```

## Reading Processed Data

### With Pandas

```python
import pandas as pd

# Read Parquet file
df = pd.read_parquet("data/processed/elden-ring-weapons/weapons.parquet")

# Read all files in a dataset
from pathlib import Path
import pandas as pd

files = Path("data/processed/elden-ring-weapons").glob("*.parquet")
dfs = [pd.read_parquet(f) for f in files]
combined = pd.concat(dfs, ignore_index=True)
```

### With Polars (faster)

```python
import polars as pl

# Read Parquet file
df = pl.read_parquet("data/processed/elden-ring-weapons/weapons.parquet")

# Read all files in a dataset
df = pl.read_parquet("data/processed/elden-ring-weapons/*.parquet")
```

### With DuckDB (SQL queries)

```python
import duckdb

# Query Parquet files with SQL
result = duckdb.query("""
    SELECT name, weapon_type, damage_physical, weight
    FROM 'data/processed/*/weapons.parquet'
    WHERE damage_physical > 100
    ORDER BY damage_physical DESC
""").to_df()
```

## Guarantees

### Schema Stability
- All processed files conform to defined Pandera schemas
- Schema violations cause processing to fail (safe by default)
- Column names are normalized and consistent
- Data types are validated and coerced

### Reproducibility
- Deterministic transformations
- Hash-based caching ensures idempotency
- Version-controlled configuration
- No manual edits to processed data

### Data Quality
- Missing values handled with documented strategies
- Categorical values normalized to standard forms
- ID uniqueness enforced
- Range validations (e.g., damage ≥ 0)

### Format Consistency
- All processed files are Parquet (columnar, compressed)
- Snappy compression (fast, balanced)
- No index column (cleaner files)
- PyArrow engine (modern, efficient)

## Troubleshooting

### Schema Validation Fails

**Error:** Column violates schema constraint

**Solution:**
1. Check raw data for invalid values
2. Update transformation logic in `pipeline/process.py`
3. Or update schema in `pipeline/schemas.py` if constraint is too strict

### Missing Dataset Schema

**Warning:** No schema found for dataset

**Solution:**
1. Add schema definition to `pipeline/schemas.py`
2. Register in `SCHEMA_REGISTRY` dict
3. Processing will continue but validation is skipped

### Processing Hangs on Large Files

**Solution:**
- Use `--verbose` to see progress
- Check memory usage
- Consider processing files individually
- Increase timeout in config

### Processed File Doesn't Update

**Cause:** File is considered up-to-date

**Solution:**
- Use `--force` flag to reprocess
- Or delete processed file and cache
- Or touch raw file to update mtime

### Import Errors

**Error:** ModuleNotFoundError: No module named 'pandera'

**Solution:**
```bash
poetry install  # Install all dependencies
# or
poetry run pip install pandera pyarrow
```

## Performance Tips

1. **Use Parquet** - 10-100x faster than CSV for large datasets
2. **Enable caching** - Avoids reprocessing unchanged files
3. **Parallel processing** - Process multiple datasets concurrently
4. **Filter early** - Remove unnecessary columns before processing
5. **Batch operations** - Process files in groups

## Future Enhancements

- [ ] Parallel dataset processing (multiprocessing)
- [ ] Data quality reports (profiling, stats)
- [ ] Schema versioning and migration
- [ ] Incremental processing for append-only datasets
- [ ] Data lineage tracking
- [ ] Custom validation rules per dataset
- [ ] Automated data drift detection
- [ ] Integration with data cataloging tools

## Related Documentation

- [Kaggle Integration](kaggle-integration.md) - Data ingestion from Kaggle
- [Project Summary](../PROJECT_SUMMARY.md) - Overall project overview
- [Contributing](../CONTRIBUTING.md) - Development guidelines
