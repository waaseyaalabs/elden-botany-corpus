"""Configuration management using pydantic-settings."""

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
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
        default=1536, description="Embedding vector dimension"
    )
    embed_model: str = Field(
        default="text-embedding-3-small", description="Embedding model name"
    )

    # OpenAI API key (when using openai provider)
    openai_api_key: str = Field(default="", description="OpenAI API key")

    # Data paths
    data_dir: Path = Field(
        default=Path("data"), description="Root data directory"
    )
    raw_dir: Path = Field(
        default=Path("data/raw"), description="Raw data directory"
    )
    curated_dir: Path = Field(
        default=Path("data/curated"), description="Curated data directory"
    )

    # String matching threshold for DLC text reconciliation
    match_threshold: float = Field(
        default=0.86, description="Minimum similarity for entity-text matching"
    )

    # Batch size for embedding generation
    embed_batch_size: int = Field(
        default=128, description="Batch size for embedding generation"
    )

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)
        # Ensure directories exist
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.curated_dir.mkdir(parents=True, exist_ok=True)

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
        config_content = (
            f'{{"username":"{self.kaggle_username}",'
            f'"key":"{self.kaggle_key}"}}'
        )
        config_file.write_text(config_content)
        config_file.chmod(0o600)


# Global settings instance
settings = Settings()
