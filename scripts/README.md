# Scripts Directory

This directory contains utility scripts for the Elden Ring Botany Corpus project.

## Available Scripts

### `download_kaggle_dataset.py`

Downloads Kaggle datasets based on YAML configuration.

**Usage:**
```bash
python -m scripts.download_kaggle_dataset \
  --config config/kaggle_datasets.yml \
  --output-dir data/raw \
  [--force] [--verbose]
```

**Features:**
- Configuration-driven dataset downloads
- Support for downloading specific files or entire datasets
- Skip existing files (unless `--force` is used)
- Auto-unzip compressed files
- Detailed logging and error handling

**See also:** `docs/kaggle-integration.md` for full documentation

---

### `process_data.py`

Processes raw datasets into validated Parquet outputs with optional concurrency.

**Usage:**
```bash
python scripts/process_data.py [--force] [--dry-run] [--workers N]
```

**Highlights:**
- Detects stale datasets before processing and supports dry-run validation.
- `--workers N` (or config `settings.process_workers`) parallelizes dataset
  work; use `--workers 0` to auto-detect CPU count or omit for serial mode.
- Supports writing processing statistics via `--output-stats path.json`.

Refer to `docs/data-processing.md` for the full pipeline guide.

---

### `setup_kaggle_creds.py`

Generates `~/.kaggle/kaggle.json` from environment variables.

**Usage:**
```bash
python scripts/setup_kaggle_creds.py --env-file .env
```

**Features:**
- Reads from environment variables or `.env` file
- Sets correct file permissions (0600)
- Never commits credentials to git

---

### `setup.sh`

Project setup script for initial configuration.

---

### `lint.sh`

Runs code quality checks (ruff, mypy).

**Usage:**
```bash
./scripts/lint.sh
```

---

### `test.sh`

Runs the test suite with pytest.

**Usage:**
```bash
./scripts/test.sh
```

## Development Guidelines

- Keep scripts focused on single responsibilities
- Add comprehensive error handling
- Include help text and usage examples
- Write unit tests in `tests/`
- Document in this README and relevant docs/
