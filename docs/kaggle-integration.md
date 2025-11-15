# Kaggle Integration

## Overview

This project integrates with Kaggle to download datasets for the Elden Ring Botany Corpus. The integration includes:

- **Automated dataset ingestion** via `scripts/download_kaggle_dataset.py`
- **Configuration-driven downloads** using `config/kaggle_datasets.yml`
- **CI/CD support** for scheduled and on-demand dataset refreshes
- **Local development workflow** with credential management

## Data Layout

All downloaded data follows this structure:

```
data/
├── raw/              # Raw data from Kaggle (gitignored)
│   ├── <dataset-1>/
│   │   ├── items.csv
│   │   ├── weapons.json
│   │   └── ...
│   └── <dataset-2>/
│       └── ...
└── processed/        # Cleaned/transformed data (gitignored)
    └── ...
```

**Important**: All data files in `data/raw/` and `data/processed/` are gitignored to avoid committing large binary files. See `data/README.md` for details.

## Running the Ingestion Script Locally

### Prerequisites

1. **Kaggle API credentials** configured (see [Local Setup Options](#local-setup-options))
2. **Python 3.11+** with dependencies installed
3. **Configuration file** at `config/kaggle_datasets.yml`

### Basic Usage

```bash
# Download all enabled datasets
python -m scripts.download_kaggle_dataset \
  --config config/kaggle_datasets.yml \
  --output-dir data/raw

# Force re-download even if files exist
python -m scripts.download_kaggle_dataset \
  --config config/kaggle_datasets.yml \
  --output-dir data/raw \
  --force

# Enable verbose logging
python -m scripts.download_kaggle_dataset \
  --config config/kaggle_datasets.yml \
  --output-dir data/raw \
  --verbose
```

### Configuration File

Edit `config/kaggle_datasets.yml` to define which datasets to download:

```yaml
datasets:
  - name: "elden-ring-items"
    owner: "example-owner"
    slug: "elden-ring-dataset"
    description: "Elden Ring items, weapons, armor, and spells"
    files:
      - "items.csv"
      - "weapons.json"
    enabled: true

  - name: "elden-ring-bosses"
    owner: "another-owner"
    slug: "elden-ring-bosses"
    files: []  # Empty = download all files
    enabled: false

settings:
  skip_disabled: true
  auto_unzip: true
  download_timeout: 300
```

**Key fields**:
- `name`: Local directory name for the dataset
- `owner`: Kaggle dataset owner username
- `slug`: Kaggle dataset slug (from dataset URL)
- `files`: List of specific files to download (empty = all files)
- `enabled`: Whether to download this dataset

### Expected Behavior

1. **First run**: Downloads all enabled datasets to `data/raw/<dataset-name>/`
2. **Subsequent runs**: Skips existing files unless `--force` is used
3. **Auto-unzip**: Automatically extracts `.zip` files if `auto_unzip: true`
4. **Disabled datasets**: Skipped if `skip_disabled: true` in settings

### Example Output

```
2025-11-15 12:00:00 - INFO - Loading configuration from config/kaggle_datasets.yml
2025-11-15 12:00:00 - INFO - Loaded 2 dataset(s) from config
2025-11-15 12:00:00 - INFO - Authenticating with Kaggle API
2025-11-15 12:00:01 - INFO - Successfully authenticated with Kaggle API
2025-11-15 12:00:01 - INFO - Output directory: data/raw

--- Processing dataset 1/2 ---
2025-11-15 12:00:01 - INFO - Downloading dataset: elden-ring-items (example-owner/elden-ring-dataset)
2025-11-15 12:00:01 - INFO - Output directory: data/raw/elden-ring-items
2025-11-15 12:00:01 - INFO - Downloading 2 specific file(s)
2025-11-15 12:00:02 - INFO - Downloading file: items.csv
2025-11-15 12:00:05 - INFO - Downloaded: items.csv
2025-11-15 12:00:05 - INFO - Downloading file: weapons.json
2025-11-15 12:00:07 - INFO - Downloaded: weapons.json
2025-11-15 12:00:07 - INFO - Successfully downloaded dataset: elden-ring-items

=== Download workflow completed ===
```

## Continuous Integration

The workflow in `.github/workflows/kaggle-integration.yml` provides automated testing and ingestion.

### Jobs

1. **kaggle-smoke-test** (runs on all events)
   - Verifies Kaggle credentials are configured
   - Tests basic Kaggle API connectivity
   - Runs on `push`, `pull_request`, and `workflow_dispatch`

2. **kaggle-dataset-ingestion** (runs on workflow_dispatch and schedule)
   - Downloads datasets using the ingestion script
   - Generates summary artifacts
   - Runs weekly on Sundays at 00:00 UTC (configurable)
   - Can be triggered manually with optional `--force` flag

### Triggering Ingestion in CI

#### Manual Trigger

1. Go to **Actions** → **Kaggle Integration** in GitHub
2. Click **Run workflow**
3. Select branch (usually `main`)
4. Choose whether to force re-download
5. Click **Run workflow**

#### Scheduled Refresh

By default, datasets are refreshed weekly on Sundays. Edit the cron schedule in `.github/workflows/kaggle-integration.yml`:

```yaml
schedule:
  # Run weekly on Sundays at 00:00 UTC
  - cron: '0 0 * * 0'
```

### CI Environment Requirements

- `KAGGLE_USERNAME` and `KAGGLE_KEY` must be configured as GitHub repository secrets
- The workflow automatically creates `~/.kaggle/kaggle.json` in the runner environment
- Data is **not committed** to the repository (respects `.gitignore`)
- Ingestion summary is uploaded as a workflow artifact

## Local Setup Options

### Option A: `~/.kaggle/kaggle.json`

1. Download your `kaggle.json` from Kaggle.
2. Place it at `~/.kaggle/kaggle.json` and run `chmod 600 ~/.kaggle/kaggle.json`.
3. Never commit this file. It is ignored via `.gitignore`.

### Option B: `.env` + environment variables

1. Create a local `.env` file (see `.env.example` for structure) and add:
   ```bash
   KAGGLE_USERNAME=your_username
   KAGGLE_KEY=your_key
   ```
2. Keep the file untracked (covered by `.gitignore`).
3. Export the values when needed or run the helper script below to materialize `kaggle.json` on demand.

### Helper Script

Use `scripts/setup_kaggle_creds.py` to generate the Kaggle config locally without hand-editing files:

```bash
python scripts/setup_kaggle_creds.py --env-file .env
```

- The script first checks real environment variables, then falls back to the provided `.env` file.
- It writes `~/.kaggle/kaggle.json` with `0600` permissions so only your user can read it.

## Caveats and Recommendations

### Dataset Size

- Monitor disk usage with `du -sh data/raw/*`
- Large datasets (>1GB) may take time to download
- Consider downloading specific files instead of entire datasets

### Rate Limits

- Kaggle API has rate limits for downloads
- Wait between requests if you encounter `429 Too Many Requests`
- Use `--force` sparingly to avoid unnecessary downloads

### Credentials Security

- **Never commit** `kaggle.json`, `.env`, or any credential files
- Rotate credentials immediately if accidental exposure occurs
- Use GitHub Secrets for CI/CD environments
- Keep `~/.kaggle/kaggle.json` with `0600` permissions

### Data Reproducibility

While data files aren't version-controlled, the **ingestion process is**:

- Configuration in `config/kaggle_datasets.yml` defines what to download
- Scripts in `scripts/` are versioned and testable
- Anyone with credentials can reproduce the dataset locally

### Troubleshooting

**Authentication Fails**
```
Failed to authenticate with Kaggle API
```
- Verify `~/.kaggle/kaggle.json` exists and has correct credentials
- Check file permissions: `ls -l ~/.kaggle/kaggle.json` (should be `-rw-------`)
- Ensure environment variables are exported if using `.env`

**Dataset Not Found**
```
Failed to download dataset: HTTP 404
```
- Verify the `owner` and `slug` in `config/kaggle_datasets.yml`
- Check the dataset URL on Kaggle: `https://www.kaggle.com/datasets/{owner}/{slug}`
- Ensure your Kaggle account has access to the dataset

**Out of Disk Space**
```
OSError: [Errno 28] No space left on device
```
- Check available space: `df -h`
- Remove old datasets: `rm -rf data/raw/old-dataset/`
- Download specific files instead of entire datasets

## Testing

Unit tests for the ingestion script are in `tests/test_download_kaggle_dataset.py`:

```bash
# Run tests
pytest tests/test_download_kaggle_dataset.py -v

# Run with coverage
pytest tests/test_download_kaggle_dataset.py --cov=scripts.download_kaggle_dataset
```

Tests cover:
- Configuration parsing and validation
- API authentication (mocked)
- File download logic with `--force` flag
- Auto-unzip functionality
- Error handling for missing datasets

## Security Reminders

- Never commit `kaggle.json`, `.env`, or any other secret material.
- Long-lived secrets belong either in local untracked files (for development) or in GitHub Secrets (for CI).
- Rotate credentials immediately if accidental exposure is suspected.
