#!/usr/bin/env bash
# Run tests locally (mirrors CI)

set -e

echo "==> Running unit tests (excluding integration tests)..."
poetry run pytest -v -m "not integration" --cov=corpus --cov-report=term

echo "✅ Unit tests passed!"
echo ""
echo "To run integration tests locally, set POSTGRES_DSN environment variable:"
echo "  export POSTGRES_DSN=postgresql://user:pass@localhost:5432/db"
echo "  poetry run pytest -v -m integration"
