# Contributing to Elden Botany Corpus

Thank you for your interest in contributing! This document provides guidelines for contributing to the project.

## Development Setup

1. **Fork and clone the repository**

```bash
git clone https://github.com/YOUR_USERNAME/elden-botany-corpus.git
cd elden-botany-corpus
```

2. **Install dependencies**

```bash
poetry install
```

3. **Set up pre-commit hooks (optional)**

```bash
poetry run pre-commit install
```

## Code Style

- **Python**: Follow PEP 8 (enforced by Ruff)
- **Type hints**: Required for all functions (checked by mypy)
- **Line length**: 100 characters (Ruff configured)
- **Imports**: Sorted with isort (via Ruff)

## Testing

### Unit Tests

Write tests for new features in `tests/`. Unit tests should run fast and have no external dependencies.

```bash
# Run all unit tests (default - integration tests are skipped)
make test
# or
poetry run pytest -v

# Explicitly skip integration tests
poetry run pytest -m "not integration"
```

Aim for >80% code coverage on core logic.

### Integration Tests

Integration tests require external services (PostgreSQL, Kaggle API, network access) and are **disabled by default**.

**To run integration tests locally:**

1. **Set up required services:**

```bash
# Start PostgreSQL with pgvector (using Docker)
docker compose -f docker/compose.example.yml up -d postgres
```

2. **Set environment variables:**

```bash
export RUN_INTEGRATION=1
export POSTGRES_DSN="postgresql://elden:password@localhost:5432/elden"
export KAGGLE_USERNAME="your_username"
export KAGGLE_KEY="your_api_key"
```

3. **Run integration tests:**

```bash
# Run only integration tests
poetry run pytest -m integration

# Run all tests (unit + integration)
RUN_INTEGRATION=1 poetry run pytest
```

**Writing integration tests:**

- Mark tests with `@pytest.mark.integration`
- Add conditional skip: `@pytest.mark.skipif(os.getenv("RUN_INTEGRATION") != "1", reason="...")`
- Use `pytest.importorskip()` for optional dependencies
- Use fixtures from `conftest.py` (e.g., `pg_connection`, `pg_cursor`)

Example:

```python
import os
import pytest

@pytest.mark.integration
@pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION") != "1",
    reason="Integration tests disabled (set RUN_INTEGRATION=1 to enable)"
)
def test_database_operation(pg_cursor):
    """Test database operation."""
    pytest.importorskip("psycopg")
    # Test implementation
    pass
```

## Pull Request Process

1. **Create a feature branch**

```bash
git checkout -b feature/your-feature-name
```

2. **Make your changes**
   - Add tests for new functionality
   - Update documentation as needed
   - Run linters and tests

```bash
make lint
make test
```

3. **Commit with descriptive messages**

```bash
git commit -m "feat: add support for new entity type"
```

Use conventional commits:
- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation only
- `test:` - Adding/updating tests
- `refactor:` - Code refactoring
- `chore:` - Maintenance tasks

4. **Push and create PR**

```bash
git push origin feature/your-feature-name
```

Then open a Pull Request on GitHub.

## Adding New Data Sources

To add a new data source:

1. Create a new ingestion module in `src/corpus/ingest_*.py`
2. Implement a fetch function returning `list[RawEntity]`
3. Add provenance tracking (source, uri, sha256)
4. Update `reconcile.py` to include the new source
5. Update documentation

Example structure:

```python
def fetch_new_source() -> list[RawEntity]:
    """Fetch data from new source."""
    entities = []
    
    # Fetch and parse data
    data = fetch_data()
    
    # Create provenance
    provenance = Provenance(
        source="new_source",
        uri="https://...",
        sha256=compute_file_hash(...)
    )
    
    # Convert to RawEntity
    for item in data:
        entities.append(
            RawEntity(
                entity_type="...",
                name="...",
                description="...",
                provenance=[provenance]
            )
        )
    
    return entities
```

## Reporting Issues

- Use GitHub Issues
- Include:
  - Clear description
  - Steps to reproduce
  - Expected vs actual behavior
  - Environment (OS, Python version)
  - Relevant logs/screenshots

## Code of Conduct

Be respectful and constructive. We're all here to learn and improve the project.

## Questions?

Open an issue or discussion on GitHub.
