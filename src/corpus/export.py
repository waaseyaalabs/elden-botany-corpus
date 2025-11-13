"""Export utilities for different formats."""

from pathlib import Path

import polars as pl

from corpus.config import settings
from corpus.utils import save_parquet


def pack_all_to_parquet(
    input_parquet: str | None = None, output_parquet: str | None = None
) -> None:
    """
    Pack all entities into a single partitioned Parquet file.

    Args:
        input_parquet: Input unified parquet (default: curated/unified.parquet)
        output_parquet: Output packed parquet (default: curated/packed.parquet)
    """
    if input_parquet is None:
        input_parquet = str(settings.curated_dir / "unified.parquet")

    if output_parquet is None:
        output_parquet = str(settings.curated_dir / "packed.parquet")

    print(f"Loading {input_parquet}...")
    df = pl.read_parquet(input_parquet)

    print(f"Packing {len(df)} entities by entity_type...")
    save_parquet(df, Path(output_parquet))

    print(f"Saved: {output_parquet}")
