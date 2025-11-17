"""Expose rag.query under the eldenbotany namespace."""

from __future__ import annotations

from rag.query import LoreMatch, main, query_lore

__all__ = ["LoreMatch", "main", "query_lore"]


if __name__ == "__main__":  # pragma: no cover
    main()
