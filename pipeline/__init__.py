"""Data processing pipeline for Elden Ring corpus.

This package provides:
- Data schemas for validation (schemas.py)
- Processing utilities (utils.py)
- Main processing logic (process.py)
- Common result types (ProcessingResult)
"""

from pipeline.process import DataProcessor, ProcessingResult
from pipeline.schemas import (
    get_active_schema_version,
    get_dataset_schema,
    list_schema_versions,
)

__all__ = [
    "DataProcessor",
    "ProcessingResult",
    "get_dataset_schema",
    "get_active_schema_version",
    "list_schema_versions",
]
