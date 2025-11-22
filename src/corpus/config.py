"""Configuration management using pydantic-settings."""

import json
from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Kaggle API credentials
    kaggle_username: str = Field(default="", description="Kaggle username")
    kaggle_key: str = Field(default="", description="Kaggle API key")

    # PostgreSQL connection
    postgres_dsn: str = Field(
        default="postgresql://postgres:postgres@localhost:5432/elden",
        description="PostgreSQL connection string",
    )

    # Embedding configuration
    embed_provider: Literal["openai", "local", "none"] = Field(
        default="none", description="Embedding provider"
    )
    embed_dimension: int = Field(
        default=1536,
        description="Embedding vector dimension",
    )
    embed_model: str = Field(
        default="text-embedding-3-small",
        description="Embedding model name",
    )

    # Reranker configuration
    reranker_name: str = Field(
        default="identity",
        description="Default reranker to apply when querying",
    )
    reranker_model: str = Field(
        default="cross-encoder/ms-marco-MiniLM-L-6-v2",
        description="Sentence-transformers cross-encoder model for reranking",
    )
    reranker_batch_size: int = Field(
        default=16,
        description="Batch size used when scoring reranker candidates",
    )
    reranker_candidate_pool: int = Field(
        default=50,
        description=(
            "Number of FAISS candidates that should be reranked before "
            "selecting the final top_k list"
        ),
    )

    # OpenAI API key (when using openai provider)
    openai_api_key: str = Field(default="", description="OpenAI API key")

    # Data paths
    data_dir: Path = Field(
        default=Path("data"),
        description="Root data directory",
    )
    raw_dir: Path = Field(
        default=Path("data/raw"),
        description="Raw data directory",
    )
    curated_dir: Path = Field(
        default=Path("data/curated"),
        description="Curated data directory",
    )
    curated_unified_csv_path: Path = Field(
        default=Path("data/curated/unified.csv"),
        description=(
            "CSV fallback used when curated Parquet files fail to load"
        ),
    )
    community_dir: Path = Field(
        default=Path("data/community"),
        description="Community bundles + processed outputs",
    )
    community_bundles_dir: Path = Field(
        default=Path("data/community/bundles"),
        description="Directory where contributor bundles live",
    )
    community_processed_dir: Path = Field(
        default=Path("data/community/processed"),
        description="Directory storing processed community artifacts",
    )

    # String matching threshold for DLC text reconciliation
    match_threshold: float = Field(
        default=0.86, description="Minimum similarity for entity-text matching"
    )

    # Batch size for embedding generation
    embed_batch_size: int = Field(
        default=128,
        description="Batch size for embedding generation",
    )

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        # Ensure directories exist
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.curated_dir.mkdir(parents=True, exist_ok=True)
        self.community_dir.mkdir(parents=True, exist_ok=True)
        self.community_bundles_dir.mkdir(parents=True, exist_ok=True)
        self.community_processed_dir.mkdir(parents=True, exist_ok=True)

    @property
    def kaggle_credentials_set(self) -> bool:
        """Check if Kaggle credentials are configured."""
        return bool(self.kaggle_username and self.kaggle_key)

    def write_kaggle_config(self) -> None:
        """Write Kaggle credentials to ~/.kaggle/kaggle.json."""
        if not self.kaggle_credentials_set:
            raise ValueError("Kaggle credentials not set in environment")

        kaggle_dir = Path.home() / ".kaggle"
        kaggle_dir.mkdir(exist_ok=True)

        config_file = kaggle_dir / "kaggle.json"
        payload = {
            "username": self.kaggle_username,
            "key": self.kaggle_key,
        }
        config_file.write_text(json.dumps(payload))
        config_file.chmod(0o600)


# Global settings instance
settings = Settings()
