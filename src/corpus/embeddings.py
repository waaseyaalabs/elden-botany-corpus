"""Embedding generation for corpus chunks."""

import json
from typing import Literal

import polars as pl
from tqdm import tqdm

from corpus.config import settings


def generate_embeddings(
    df: pl.DataFrame,
    provider: Literal["openai", "local"] = "openai",
    model: str | None = None,
    batch_size: int | None = None,
) -> pl.DataFrame:
    """
    Generate embeddings for corpus text.

    Args:
        df: DataFrame with 'description' column
        provider: Embedding provider ('openai' or 'local')
        model: Model name (default from settings)
        batch_size: Batch size (default from settings)

    Returns:
        DataFrame with 'embedding' column added
    """
    model = model or settings.embed_model
    batch_size = batch_size or settings.embed_batch_size

    if provider == "openai":
        return _generate_openai_embeddings(df, model, batch_size)
    elif provider == "local":
        return _generate_local_embeddings(df, model, batch_size)
    else:
        raise ValueError(f"Unknown provider: {provider}")


def _generate_openai_embeddings(
    df: pl.DataFrame, model: str, batch_size: int
) -> pl.DataFrame:
    """Generate embeddings using OpenAI API."""
    try:
        from openai import OpenAI
    except ImportError:
        raise ImportError(
            "OpenAI package not installed. "
            "Install with: poetry add openai"
        )

    if not settings.openai_api_key:
        raise ValueError("OPENAI_API_KEY not set in environment")

    client = OpenAI(api_key=settings.openai_api_key)

    texts = df.select("description").to_series().to_list()
    embeddings = []

    print(
        f"Generating embeddings for {len(texts)} texts using {model}..."
    )

    for i in tqdm(range(0, len(texts), batch_size), desc="Embedding"):
        batch = texts[i : i + batch_size]

        response = client.embeddings.create(
            input=batch,
            model=model,
        )

        batch_embeddings = [
            item.embedding for item in response.data
        ]
        embeddings.extend(batch_embeddings)

    # Add embeddings as JSON strings for Polars
    df = df.with_columns(
        pl.Series(
            "embedding",
            [json.dumps(emb) for emb in embeddings],
        )
    )

    return df


def _generate_local_embeddings(
    df: pl.DataFrame, model: str, batch_size: int
) -> pl.DataFrame:
    """Generate embeddings using local sentence-transformers."""
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        raise ImportError(
            "sentence-transformers not installed. "
            "Install with: poetry add sentence-transformers"
        )

    print(f"Loading local embedding model: {model}...")
    encoder = SentenceTransformer(model)

    texts = df.select("description").to_series().to_list()

    print(f"Generating embeddings for {len(texts)} texts...")
    embeddings = encoder.encode(
        texts,
        batch_size=batch_size,
        show_progress_bar=True,
        convert_to_numpy=True,
    )

    # Convert to list of lists
    embedding_lists = embeddings.tolist()

    # Add embeddings as JSON strings
    df = df.with_columns(
        pl.Series(
            "embedding",
            [json.dumps(emb) for emb in embedding_lists],
        )
    )

    return df
