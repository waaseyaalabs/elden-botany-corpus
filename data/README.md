# Data Directory Structure

This directory contains all raw and processed data for the Elden Ring Botany Corpus project.

## Layout

```
data/
├── raw/              # Raw data downloaded from sources (gitignored)
│   └── <dataset>/    # One directory per dataset
└── processed/        # Cleaned/transformed data (gitignored)
    └── <dataset>/    # Processed outputs per dataset
```

## Directory Purposes

### `raw/`

Contains unmodified data as downloaded from external sources:
- Kaggle datasets
- Web scraping outputs
- GitHub data exports
- Community contributions

**All files in `raw/` are gitignored** to avoid committing large binary files or CSV/JSON datasets. See `.gitignore` for specific patterns.

### `processed/`

Contains cleaned, validated, and transformed versions of raw data:
- Standardized schemas
- Deduplicated records
- Format conversions
- Feature engineering outputs

**Most files in `processed/` are also gitignored** with selective exceptions for small metadata files.

## Reproducibility

While data files themselves are not version-controlled, the **processes to generate them are**:

- Scripts in `scripts/` handle downloading raw data
- Pipeline in `pipeline/` handles data processing and validation
- Configuration in `config/` specifies dataset sources and parameters

To reproduce the data:

```bash
# 1. Download raw data from Kaggle
python -m scripts.download_kaggle_dataset \
  --config config/kaggle_datasets.yml \
  --output-dir data/raw

# 2. Process raw data into validated Parquet files
python scripts/process_data.py \
  --config config/kaggle_datasets.yml \
  --raw-dir data/raw \
  --processed-dir data/processed

# Or use Poetry
poetry run python scripts/process_data.py
```

See [docs/data-processing.md](../docs/data-processing.md) for detailed pipeline documentation.

## Git Ignore Rules

The following patterns are excluded from version control:

- `data/raw/**/*.zip`
- `data/raw/**/*.csv`
- `data/raw/**/*.json`
- `data/raw/**/*.parquet`
- `data/raw/**/*.html`
- `data/curated/*.parquet`
- `data/curated/*.csv`

Small metadata files (e.g., `metadata.json`) may be allowed via negation rules.

## Storage Recommendations

- **Local development**: Keep raw data in this directory structure
- **CI/CD**: Download on-demand per job, don't persist artifacts
- **Production**: Consider cloud storage (S3, GCS) for large datasets with local caching

## Security Notes

- Never commit API keys, credentials, or personal information
- Review data for PII before processing
- Respect data source licenses and terms of service
