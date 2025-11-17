"""Expose lore embedding builder under the eldenbotany namespace."""

from __future__ import annotations

import pipelines.build_lore_embeddings as _module

__all__ = getattr(_module, "__all__", [])

for _name in __all__:
    globals()[_name] = getattr(_module, _name)


def main() -> None:
    """Delegate execution to the canonical lore embedding pipeline."""

    _module.main()


if __name__ == "__main__":  # pragma: no cover
    main()
