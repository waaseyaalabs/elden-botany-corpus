from collections.abc import Sequence
from typing import Any


class SentenceTransformer:
    def __init__(self, model_name: str, device: str | None = ...) -> None: ...

    def encode(
        self,
        texts: Sequence[str],
        *,
        batch_size: int | None = ...,
        show_progress_bar: bool | None = ...,
        convert_to_numpy: bool | None = ...,
    ) -> Any: ...
