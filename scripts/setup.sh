#!/usr/bin/env bash
# Setup development environment

set -e

echo "==> Setting up Elden Botany Corpus development environment..."
echo ""

# Check for Poetry
if ! command -v poetry &> /dev/null; then
    echo "Poetry not found. Installing..."
    curl -sSL https://install.python-poetry.org | python3 -
    echo ""
    echo "⚠️  Please add Poetry to your PATH:"
    echo "   export PATH=\"\$HOME/.local/bin:\$PATH\""
    echo ""
    echo "Then re-run this script."
    exit 1
fi

echo "==> Installing dependencies with Poetry..."
poetry install --no-interaction

echo ""
echo "==> Running format check to ensure code is formatted..."
poetry run ruff format src/ tests/ || {
    echo "ℹ️  Files were formatted. This is normal on first setup."
}

echo ""
echo "✅ Development environment setup complete!"
echo ""
echo "Useful commands:"
echo "  make lint      - Run linting checks (same as CI)"
echo "  make format    - Auto-format code"
echo "  make test      - Run unit tests"
echo "  poetry shell   - Activate virtual environment"
echo "  poetry run corpus --help  - Run CLI tool"
