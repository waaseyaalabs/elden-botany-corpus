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

- Write tests for new features in `tests/`
- Run tests before submitting:

```bash
make test
# or
poetry run pytest -v
```

- Aim for >80% code coverage

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
