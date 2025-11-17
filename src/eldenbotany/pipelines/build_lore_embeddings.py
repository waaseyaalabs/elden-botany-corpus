"""Expose lore embedding builder under the eldenbotany namespace."""

from __future__ import annotations

import pipelines.build_lore_embeddings as _embedding_module

__all__ = getattr(_embedding_module, "__all__", [])

for _name in __all__:
    globals()[_name] = getattr(_embedding_module, _name)


def main() -> None:
    """Delegate execution to the canonical lore embedding entrypoint."""

    _embedding_module.main()


if __name__ == "__main__":
    main()
