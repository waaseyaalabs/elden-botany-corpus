# Makefile for common tasks

.PHONY: help setup install test lint format clean fetch curate load rag-embeddings rag-index rag-query rag-guard ci-local

help:
	@echo "Elden Botany Corpus - Available Commands"
	@echo ""
	@echo "  make setup      - Initial setup (install Poetry + dependencies)"
	@echo "  make install    - Install dependencies with Poetry"
	@echo "  make test       - Run tests with coverage"
	@echo "  make lint       - Run linters (ruff, mypy) - same as CI"
	@echo "  make format     - Format code with ruff"
	@echo "  make ci-local   - Run full CI checks locally"
	@echo "  make clean      - Clean generated files"
	@echo "  make fetch      - Fetch all data sources"
	@echo "  make curate     - Curate corpus"
	@echo "  make build-corpus - Fetch, curate, and regenerate motif coverage"
	@echo "  make community-report - Rebuild motif coverage report only"
	@echo "  make load       - Load to PostgreSQL (requires POSTGRES_DSN)"
	@echo "  make rag-embeddings - Build lore_embeddings.parquet"
	@echo "  make rag-index  - Build FAISS index + metadata"
	@echo "  make rag-guard  - Check checksum guard for lore corpus/RAG"
	@echo "  make rag-query  - Run semantic search (pass QUERY='...')"
	@echo ""

setup:
	@bash scripts/setup.sh

install:
	poetry install

test:
	poetry run pytest -v --cov=corpus --cov-report=term-missing --cov-report=html

# Lint target matches CI exactly - checks without modifying
lint:
	poetry run ruff format --check src/ tests/
	poetry run ruff check src/ tests/
	poetry run mypy src/

# Format target for local development - actually modifies files
format:
	poetry run ruff format src/ tests/
	poetry run ruff check --fix src/ tests/

# Run complete CI checks locally
ci-local: lint test
	@echo ""
	@echo "✅ All CI checks passed locally!"

clean:
	rm -rf data/raw/* data/curated/*
	rm -rf .pytest_cache .mypy_cache .ruff_cache
	rm -rf htmlcov .coverage
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

fetch:
	poetry run corpus fetch --all

curate:
	poetry run corpus curate

build-corpus:
	poetry run python scripts/build_corpus.py $(ARGS)

community-report:
	poetry run python scripts/build_corpus.py --skip-fetch --skip-curate $(ARGS)

load:
	poetry run corpus load --create-schema

rag-embeddings:
	poetry run python -m pipelines.build_lore_embeddings $(ARGS)

rag-index:
	poetry run python -m pipelines.build_rag_index $(ARGS)

rag-guard:
	poetry run python -m pipelines.rag_guard $(ARGS)

rag-query:
	@if [ -z "$(QUERY)" ]; then \
		echo "Usage: make rag-query QUERY='scarlet rot' [ARGS='--category item']"; \
		exit 1; \
	fi
	poetry run python -m rag.query "$(QUERY)" $(ARGS)

docker-up:
	docker compose -f docker/compose.example.yml up -d

docker-down:
	docker compose -f docker/compose.example.yml down

docker-build:
	docker compose -f docker/compose.example.yml build
