#!/usr/bin/env bash
# Run all linting checks locally (mirrors CI)

set -e

echo "==> Checking code formatting..."
poetry run ruff format --check src/ tests/

echo "==> Running Ruff linter..."
poetry run ruff check src/ tests/

echo "==> Running mypy type checker..."
poetry run mypy src/

echo "✅ All lint checks passed!"
