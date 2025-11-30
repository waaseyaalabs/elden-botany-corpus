Elden Botany Corpus – Development & Usage Guide

This document serves as an onboarding guide for contributors and agentic AI systems working with the Elden Botany Corpus project. It explains what the repository contains, how to set up the environment using Poetry, how to configure and use the .env file, and outlines the recommended development and analysis workflows. Where possible, citations to the upstream README.md and source code are provided.

1. Project overview

The Elden Botany Corpus is a curated, provenance‑tracked dataset of Elden Ring and Shadow of the Erdtree in‑game data. It pulls from multiple sources (Kaggle datasets, GitHub APIs and DLC text dumps) and reconciles them into a unified corpus suitable for Retrieval‑Augmented Generation (RAG) and analytics
raw.githubusercontent.com
. Each entity record carries source attribution and cryptographic hashes, and the pipeline handles deduplication and fuzzy text matching
raw.githubusercontent.com
. Downstream analysis includes motif clustering, NPC motif graphs and narrative summaries driven by large language models
raw.githubusercontent.com
.

2. Repository structure

Key directories and files:

Path	Purpose
src/corpus/	Core Python package; contains ingestion modules (ingest_kaggle.py, ingest_github_json.py, ingest_impalers.py, ingest_carian_fmg.py), reconciliation (reconcile.py), curation (curate.py), exporting (export.py), embeddings (embeddings.py), Postgres loader (pgvector_loader.py) and command‑line interface (cli.py)
raw.githubusercontent.com
.
sql/	PostgreSQL schema files (extensions, core tables, indexes)
raw.githubusercontent.com
.
data/raw/	Cached downloads of raw sources (git‑ignored).
data/curated/	Unified outputs including Parquet/CSV, profiling reports, and unmatched DLC texts
raw.githubusercontent.com
.
docker/	Docker setup and example Compose file to run PostgreSQL with pgvector.
.github/workflows/	Continuous Integration (CI) and nightly refresh workflows.
Makefile	Shortcut targets for common tasks: setup, linting, testing, fetching, curating, analysis, RAG pipelines and Docker orchestration
raw.githubusercontent.com
.
pyproject.toml	Poetry configuration, dependencies (Python 3.11), optional extras for embeddings and OpenAI, and tooling configuration
raw.githubusercontent.com
.
.env.example	Template showing environment variables for Kaggle, PostgreSQL, embedding providers and OpenAI
raw.githubusercontent.com
. Copy this to .env for local configuration.

The project uses a src/ layout, so the installed package is corpus. Entry points are exposed through the corpus CLI defined in pyproject.toml
raw.githubusercontent.com
.

3. Environment setup
3.1 Prerequisites

Python 3.11 – enforced via Poetry. Use pyenv or your system package manager to install it. Older versions are not supported
raw.githubusercontent.com
.

Poetry – used for dependency management and packaging. If you don’t have Poetry installed, running make setup will install it for you
raw.githubusercontent.com
.

Docker (optional) – required only if you wish to spin up PostgreSQL via Docker Compose for vector search or to run the provided docker services.

3.2 Installing dependencies

Clone the repository:

git clone https://github.com/waaseyaalabs/elden-botany-corpus.git
cd elden-botany-corpus


Install Poetry and project dependencies. The recommended one‑command setup uses the provided script:

# Installs Poetry if missing and then installs all dependencies
make setup


Under the hood this calls scripts/setup.sh, which ensures Poetry is on your PATH and runs poetry install
raw.githubusercontent.com
. Alternatively, you may run poetry install manually after ensuring Poetry is installed
raw.githubusercontent.com
.

Activate the virtual environment. Poetry automatically creates one. To spawn a shell within it, run:

poetry shell


Alternatively, prefix commands with poetry run to execute them inside the environment.

3.3 Configuring .env

The application reads configuration from a .env file using pydantic‑settings. A template is provided as .env.example
raw.githubusercontent.com
. Copy it to .env and fill in your credentials:

cp .env.example .env


Key variables:

Variable	Description
KAGGLE_USERNAME & KAGGLE_KEY	Required to download datasets from Kaggle. Create a Kaggle API token under “Account → API → Create new API token” and copy the values into the .env file
raw.githubusercontent.com
.
POSTGRES_DSN	PostgreSQL connection string, e.g. postgresql://user:pass@localhost:5432/elden. This is used by the loader (corpus load) and by pipelines that need database access. The default in config.py points to a local database
raw.githubusercontent.com
.
EMBED_PROVIDER	Embedding provider: openai, local, or none
raw.githubusercontent.com
. Set to openai to use OpenAI’s embeddings, or local to use sentence‑transformers. When set to none, embedding steps are skipped.
EMBED_MODEL & EMBED_DIMENSION	Name and dimension of the embedding model. Defaults are text-embedding-3-small and 1536 respectively
raw.githubusercontent.com
. When using local embeddings, specify a supported sentence-transformers model.
OPENAI_API_KEY	Only needed when EMBED_PROVIDER is openai or when you run narrative summarization. Obtain an API key from OpenAI and keep it secret.
TB_LLM_PROVIDER, TB_LLM_MODEL	Optional variables controlling which language model is used for narrative summaries. The CLI defaults to cost‑aware OpenAI models (bulk gpt-5-mini, premium gpt-5.1 or debug gpt-4o-mini)
raw.githubusercontent.com
.

Do not commit your .env file to version control. It contains secrets and is excluded via .gitignore. The template .env.example is safe to commit and documents default values.

When Kaggle credentials are provided, the pipeline can write them to ~/.kaggle/kaggle.json. Use:

from corpus.config import settings
settings.write_kaggle_config()  # writes kaggle.json with 0600 permissions


This step is automatically performed by some ingestion commands.

4. Workflow overview

Most tasks can be executed via the corpus CLI (exposed through Poetry) or the provided Makefile. Below is an end‑to‑end workflow.

4.1 Fetching and curating data

Fetch raw sources – downloads Kaggle datasets, Carian FMG files and DLC text dumps:

poetry run corpus fetch --all
# or use make for the shorthand
make fetch


You can selectively enable/disable the Carian Archive via --carian/--no-carian. The fetch command is read‑only: it doesn’t modify state files
raw.githubusercontent.com
. For incremental refreshes, pass --incremental or --since <ISO8601>
raw.githubusercontent.com
.

Curate the corpus – deduplicates and reconciles raw rows, producing data/curated/unified.parquet and associated profiling reports:

poetry run corpus curate
# or
make curate


Use --incremental on the curate command to perform append‑only refreshes
raw.githubusercontent.com
. The curation step writes a manifest to data/curated/state/reconciled_entities.json to track processed items.

All‑in‑one build & motif coverage – the helper script scripts/build_corpus.py can fetch, curate and produce community motif coverage in a single step:

make build-corpus             # runs fetch → curate → motif coverage
make community-report         # regenerates motif coverage only


Pass flags via the ARGS variable to override default behaviour (e.g., make build-corpus ARGS="--fetch-arg --base --curate-arg --no-quality")
raw.githubusercontent.com
.

4.2 Loading into PostgreSQL (optional)

To explore the data via SQL or to host embeddings for vector search, you can load the curated corpus into a PostgreSQL database. A docker-compose.example.yml is provided to spin up a local instance with the pgvector extension.

# Start Postgres via Docker (in the background)
docker compose -f docker/compose.example.yml up -d postgres

# Create tables and insert rows
poetry run corpus load --dsn postgresql://user:password@localhost:5432/elden --create-schema


Adjust the DSN to match your database credentials. The loader creates the required schema and populates all tables
raw.githubusercontent.com
.

4.3 Building embeddings and RAG artifacts

The repository includes pipelines to embed curated lore and build a FAISS index for retrieval. These tasks are optional but recommended if you plan to perform semantic search or integrate the corpus into downstream applications.

Generate embeddings:

make rag-embeddings          # runs python -m pipelines.build_lore_embeddings


The embeddings pipeline reads data/curated/lore_corpus.parquet, applies text‑type weighting (configured in config/text_type_weights.yml) and writes vectors to data/embeddings/lore_embeddings.parquet
raw.githubusercontent.com
.

Build the FAISS index and metadata:

make rag-index               # runs python -m pipelines.build_rag_index


Output files include faiss_index.bin, rag_metadata.parquet and rag_index_meta.json which store vector dimensions and reranker configuration
raw.githubusercontent.com
.

Query the index (optional):

make rag-query QUERY="scarlet rot and decay"
# or manual call
poetry run python -m rag.query "thorned death rites" --top-k 10


Flags such as --mode balanced|raw, --reranker identity|cross_encoder and --filter control search behaviour
raw.githubusercontent.com
.

After modifying curated text, weighting config or embedding settings, you must run both make rag-embeddings and make rag-index to refresh the vectors and index
raw.githubusercontent.com
.

4.4 Phase 7 analysis (motif clusters, graph and summaries)

The analysis layer extracts higher‑order patterns from the curated lore. The CLI offers commands for each step and the Makefile wraps them for convenience.

Motif clustering – reduces high‑dimensional motif hits into clusters using UMAP and HDBSCAN:

make analysis-clusters
# underlying command:
poetry run corpus analysis clusters --export


This writes artifacts to data/analysis/motif_clustering/. Use ARGS to override e.g. --max-rows or input paths
raw.githubusercontent.com
.

NPC motif graph – builds a graph of motif interactions between NPCs:

make analysis-graph
# calls: poetry run corpus analysis graph


Graphs are saved in data/analysis/npc_motif_graph/. Regenerate after each curation refresh
raw.githubusercontent.com
.

Narrative summaries – generates in‑universe summaries for each NPC using a large language model. The recommended process uses OpenAI’s batch API to minimize cost:

# Build and optionally submit the JSONL batch
make analysis-summaries-batch

# After OpenAI job completes, ingest the results and produce outputs
make analysis-summaries


Use ARGS="--llm-model gpt-5.1" for premium polish or --llm-mode per-entity for synchronous debugging
raw.githubusercontent.com
. Use --llm-mode heuristic to skip LLM calls entirely. The summarization pipeline records the provider, model and whether an LLM was used in each summary entry
raw.githubusercontent.com
.

4.5 Motif detection and clustering internals

Motif detection currently relies on a taxonomy of motif labels, synonyms and narrative signals. Regex patterns are compiled for each motif and applied to lore text to detect “hits”
raw.githubusercontent.com
. Counts of these hits feed the clustering and graph pipelines. While adequate for baseline analysis, motifs are better interpreted at the level of complete speeches; see discussion in project conversations for proposals to enhance this pipeline.

5. Development best practices
5.1 Code quality and tests

Formatting and linting: Use make lint to run Ruff (style and import sorting) and MyPy (type checking)
raw.githubusercontent.com
. Use make format to automatically format the codebase.

Tests: Run make test to execute the Pytest suite with coverage
raw.githubusercontent.com
. Unit tests live in tests/. To run the full CI checks locally, use make ci-local which runs linting and tests
raw.githubusercontent.com
.

Pre‑commit hooks: Consider installing pre‑commit hooks (pre-commit install) configured to run Ruff and MyPy before each commit.

Line length: The project enforces a 79‑character line limit via Ruff
raw.githubusercontent.com
. Keep docstrings and comments concise.

Type hints: The code is type‑checked under MyPy’s strict mode
raw.githubusercontent.com
. Provide type annotations on public functions and classes and run make lint regularly to catch issues.

5.2 Dependency management

Poetry: Add new dependencies with poetry add <package> and commit the updated pyproject.toml and poetry.lock. Avoid modifying requirements.txt—this project uses Poetry exclusively.

Optional extras: Two extras are defined: embeddings-local (for sentence-transformers) and embeddings-openai (for openai)
raw.githubusercontent.com
. If you plan to use local embeddings or OpenAI features, install the appropriate extra: poetry install --with embeddings-openai.

5.3 Data handling and privacy

Do not commit large data files. The data/ directory is git‑ignored; curated and raw data should remain local unless specifically publishing small test fixtures. Use Git LFS if you must version large artifacts.

Protect secrets. Never commit your .env file or API keys. Only commit .env.example with placeholder values. CI pipelines should load secrets from GitHub Actions secrets or environment variables.

Versioning external content. When ingesting Kaggle or GitHub data, record the sources and licenses. All ingested rows track SHA‑256 hashes to preserve provenance
raw.githubusercontent.com
.

5.4 Working with Docker

The docker/compose.example.yml defines a PostgreSQL service with pgvector. Use make docker-up to start services and make docker-down to stop them
raw.githubusercontent.com
.

When using Docker for development, mount your repository volume into the container to allow code editing outside the container.

5.5 Extending the analysis pipelines

Motif taxonomy: New motifs should be added to the taxonomy with clear labels, synonyms and examples to improve regex‑based detection. See pipelines/motif_taxonomy_utils.py for helper functions
raw.githubusercontent.com
.

LLM configuration: The narrative summarizer consults the environment variables TB_LLM_PROVIDER and TB_LLM_MODEL to select the model. Adjust these variables to switch between cost tiers or providers. Avoid hard‑coding API keys in code.

New features: When adding new CLI commands, register them under corpus/cli.py and expose them via the corpus script in pyproject.toml.

Documentation: Document any new commands or environment variables in both the README.md and this instructions.md.

6. Troubleshooting

Missing Kaggle data: If poetry run corpus fetch fails due to missing Kaggle credentials, confirm that KAGGLE_USERNAME and KAGGLE_KEY are set in .env and that ~/.kaggle/kaggle.json exists. Regenerate your API token if necessary.

Environment not found: If poetry complains about a missing virtual environment, run poetry install again or create a fresh environment with poetry env use 3.11.

Database connection errors: Ensure PostgreSQL is running, the DSN in .env is correct, and the pgvector extension is installed. When using Docker Compose, run make docker-up before loading data.

OpenAI API errors: Narrative summaries and OpenAI embeddings require a valid API key. Make sure OPENAI_API_KEY and, if used, TB_LLM_PROVIDER/TB_LLM_MODEL are set. Check rate limits and quotas if requests fail.

Long run times: Clustering, embedding, and summarization are compute‑intensive. For development, you can limit the number of rows (--max-rows) or choose a cheaper model (--llm-mode heuristic).

7. Conclusion

This repository provides a full pipeline from raw data ingestion to high‑level narrative analysis of Elden Ring lore. By following the setup instructions, configuring your environment variables, and leveraging the provided Makefile targets, you can fetch, curate, explore and analyse the corpus. Adhering to the development practices outlined above will help maintain code quality and ensure that new features integrate smoothly into the existing system.