"""Helpers for loading pipeline schema utilities without static imports."""

from __future__ import annotations

from importlib import import_module
from typing import Any


def _schemas_module() -> Any:
    return import_module("pipeline.schemas")


def get_dataset_schema(dataset: str) -> Any:
    """Runtime proxy to pipeline.schemas.get_dataset_schema."""

    return _schemas_module().get_dataset_schema(dataset)


def get_active_schema_version(dataset: str) -> Any:
    """Runtime proxy to pipeline.schemas.get_active_schema_version."""

    return _schemas_module().get_active_schema_version(dataset)


def list_schema_versions(dataset: str | None = None) -> Any:
    """Runtime proxy to pipeline.schemas.list_schema_versions."""

    module = _schemas_module()
    if dataset is None:
        return module.list_schema_versions()
    return module.list_schema_versions(dataset)
