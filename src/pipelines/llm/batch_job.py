"""Helpers for orchestrating OpenAI batch jobs for narrative summaries."""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

try:  # pragma: no cover - optional dependency guard
    from openai import OpenAI
except ModuleNotFoundError as exc:  # pragma: no cover - handled by caller
    message = (
        "The 'openai' extra is required: "
        "poetry install --with embeddings-openai"
    )
    raise RuntimeError(message) from exc

LOGGER = logging.getLogger(__name__)


class OpenAIBatchJob:
    """Utility wrapper for OpenAI's batch file workflow."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        client: Any | None = None,
    ) -> None:
        key = api_key or os.environ.get("OPENAI_API_KEY")
        if not key:
            raise RuntimeError(
                "OPENAI_API_KEY is required to submit OpenAI batch jobs"
            )
        self._client: Any = client or OpenAI(api_key=key)

    def submit(
        self,
        batch_file: Path,
        *,
        completion_window: str = "24h",
        metadata: dict[str, str] | None = None,
    ) -> Any:
        """Upload the JSONL payload and create a batch job."""

        LOGGER.info("Uploading batch payload: %s", batch_file)
        with batch_file.open("rb") as handle:
            uploaded = self._client.files.create(file=handle, purpose="batch")
        LOGGER.info("Creating batch job for file %s", uploaded.id)
        batch = self._client.batches.create(
            input_file_id=uploaded.id,
            endpoint="/v1/responses",
            completion_window=completion_window,
            metadata=metadata or {},
        )
        return batch

    def retrieve(self, batch_id: str) -> Any:
        """Fetch the latest batch metadata from OpenAI."""

        return self._client.batches.retrieve(batch_id)

    def poll(
        self,
        batch_id: str,
        *,
        interval: float = 10.0,
    ) -> Any:
        """Poll until the batch reaches a terminal state."""

        terminal_states = {"completed", "failed", "cancelled", "expired"}
        while True:
            batch = self.retrieve(batch_id)
            status = self._batch_field(batch, "status")
            LOGGER.info("Batch %s status=%s", batch_id, status)
            if status in terminal_states:
                return batch
            time.sleep(interval)

    def download_output(self, batch: Any, destination: Path) -> Path:
        """Download the batch output JSONL to the requested path."""

        output_file_id = self._batch_field(batch, "output_file_id")
        if not output_file_id:
            raise RuntimeError(
                "Batch did not expose output_file_id; ensure it completed"
                " successfully"
            )
        destination.parent.mkdir(parents=True, exist_ok=True)
        LOGGER.info(
            "Downloading batch output %s -> %s", output_file_id, destination
        )
        response = self._client.files.content(output_file_id)
        with destination.open("wb") as handle:
            handle.write(response.read())
        return destination

    def _batch_field(self, batch: Any, field: str) -> Any:
        if hasattr(batch, field):
            return getattr(batch, field)
        if hasattr(batch, "model_dump"):
            try:
                data = batch.model_dump()
            except Exception:  # pragma: no cover - defensive fallback
                data = None
            if isinstance(data, Mapping):
                data_mapping = cast(Mapping[str, Any], data)
                return data_mapping.get(field)
        if isinstance(batch, Mapping):
            batch_mapping = cast(Mapping[str, Any], batch)
            return batch_mapping.get(field)
        return None
