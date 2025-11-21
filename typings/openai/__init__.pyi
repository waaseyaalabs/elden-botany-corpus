from collections.abc import Sequence
from typing import Any


class _EmbeddingsClient:
    def create(self, *, input: Sequence[str], model: str) -> Any: ...


class OpenAI:
    embeddings: _EmbeddingsClient

    def __init__(self, api_key: str | None = ...) -> None: ...
