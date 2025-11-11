"""Pipeline configuration for corpus curation."""

from pydantic import BaseModel, Field


class PipelineConfig(BaseModel):
    """Configuration for curation pipeline."""

    fetch_base: bool = Field(default=True, description="Fetch base game data")
    fetch_dlc: bool = Field(default=True, description="Fetch DLC data")
    fetch_github: bool = Field(default=True, description="Fetch GitHub API")
    fetch_impalers: bool = Field(default=True, description="Fetch Impalers text")

    reconcile_threshold: float = Field(default=0.86, description="Fuzzy match threshold")

    export_parquet: bool = Field(default=True, description="Export to Parquet")
    export_csv: bool = Field(default=True, description="Export to CSV")
    export_by_type: bool = Field(default=True, description="Export separate files per type")
