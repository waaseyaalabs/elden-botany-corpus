"""Expose lore RAG index builder under the eldenbotany namespace."""

from __future__ import annotations

import pipelines.build_rag_index as _rag_module

__all__ = getattr(_rag_module, "__all__", [])

for _name in __all__:
    globals()[_name] = getattr(_rag_module, _name)


def main() -> None:
    """Delegate execution to the canonical RAG index entrypoint."""

    _rag_module.main()


if __name__ == "__main__":
    main()
