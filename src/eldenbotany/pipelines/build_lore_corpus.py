"""Expose lore corpus builder under the eldenbotany namespace."""

import pipelines.build_lore_corpus as _lore_module

__all__ = getattr(_lore_module, "__all__", [])

for _name in __all__:
    globals()[_name] = getattr(_lore_module, _name)


def main() -> None:
    """Delegate execution to the canonical lore corpus entrypoint."""

    _lore_module.main()


if __name__ == "__main__":
    main()
