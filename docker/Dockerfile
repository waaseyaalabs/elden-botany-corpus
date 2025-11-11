# Multi-stage Dockerfile for elden-botany-corpus

FROM python:3.11-slim as base

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="/root/.local/bin:$PATH"

WORKDIR /app

# Copy project files
COPY pyproject.toml poetry.lock* ./

# Install dependencies (without dev dependencies)
RUN poetry config virtualenvs.create false \
    && poetry install --no-dev --no-interaction --no-ansi

# Copy application code
COPY src/ ./src/
COPY sql/ ./sql/

# Create data directories
RUN mkdir -p data/raw data/curated

# Set Python path
ENV PYTHONPATH=/app

# Default command
CMD ["python", "-m", "corpus.cli", "--help"]


# Development stage with dev dependencies
FROM base as dev

RUN poetry install --no-interaction --no-ansi

# Install Jupyter for notebooks
RUN pip install jupyter jupyterlab

COPY examples/ ./examples/
COPY tests/ ./tests/

CMD ["bash"]
