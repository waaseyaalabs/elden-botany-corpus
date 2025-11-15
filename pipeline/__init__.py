"""Data processing pipeline for Elden Ring corpus.

This package provides:
- Data schemas for validation (schemas.py)
- Processing utilities (utils.py)
- Main processing logic (process.py)
- Common result types (ProcessingResult)
"""

from pipeline.process import DataProcessor, ProcessingResult
from pipeline.schemas import get_dataset_schema

__all__ = ["DataProcessor", "ProcessingResult", "get_dataset_schema"]
