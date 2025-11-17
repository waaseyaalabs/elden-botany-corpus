"""Processing utilities for data transformation and validation.

Provides helper functions for:
- Column name normalization
- Missing value handling
- Type coercion
- File change detection
"""

import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize DataFrame column names.

    - Convert to lowercase
    - Replace spaces with underscores
    - Remove special characters
    - Strip whitespace

    Args:
        df: pandas DataFrame

    Returns:
        DataFrame with normalized column names
    """
    df = df.copy()
    df.columns = (
        df.columns.str.lower()
        .str.strip()
        .str.replace(r"[^\w\s]", "", regex=True)
        .str.replace(r"\s+", "_", regex=True)
    )
    return df


def handle_missing_values(df: pd.DataFrame, strategy: dict[str, Any]) -> pd.DataFrame:
    """Handle missing values based on column-specific strategies.

    Args:
        df: pandas DataFrame
        strategy: Dict mapping column names to fill strategies
            - 'drop': drop rows with missing values
            - value: fill with specific value
            - 'mean', 'median', 'mode': fill with statistical measure

    Returns:
        DataFrame with missing values handled
    """
    df = df.copy()

    for col, action in strategy.items():
        if col not in df.columns:
            continue

        if action == "drop":
            df = df.dropna(subset=[col])
        elif action == "mean":
            df[col] = df[col].fillna(df[col].mean())
        elif action == "median":
            df[col] = df[col].fillna(df[col].median())
        elif action == "mode":
            mode_val = df[col].mode()
            if len(mode_val) > 0:
                df[col] = df[col].fillna(mode_val[0])
        else:
            df[col] = df[col].fillna(action)

    return df


def normalize_categorical(df: pd.DataFrame, mappings: dict[str, dict[str, str]]) -> pd.DataFrame:
    """Normalize categorical values using predefined mappings.

    Args:
        df: pandas DataFrame
        mappings: Dict of {column: {old_value: new_value}}

    Returns:
        DataFrame with normalized categorical values
    """
    df = df.copy()

    for col, mapping in mappings.items():
        if col in df.columns:
            # Case-insensitive mapping
            df[col] = df[col].astype(str).str.lower().str.strip()
            df[col] = df[col].replace(mapping)

    return df


def coerce_types(df: pd.DataFrame, type_map: dict[str, str]) -> pd.DataFrame:
    """Coerce DataFrame columns to specified types.

    Args:
        df: pandas DataFrame
        type_map: Dict mapping column names to target types
            ('int', 'float', 'bool', 'str', 'datetime')

    Returns:
        DataFrame with coerced types
    """
    df = df.copy()

    for col, target_type in type_map.items():
        if col not in df.columns:
            continue

        try:
            if target_type == "int":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif target_type == "float":
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif target_type == "bool":
                df[col] = df[col].astype(bool)
            elif target_type == "str":
                df[col] = df[col].astype(str)
            elif target_type == "datetime":
                df[col] = pd.to_datetime(df[col], errors="coerce")
        except Exception:
            # Skip columns that can't be coerced
            continue

    return df


def calculate_file_hash(filepath: Path) -> str:
    """Calculate SHA256 hash of a file.

    Args:
        filepath: Path to file

    Returns:
        Hex digest of file hash
    """
    sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def needs_processing(raw_path: Path, processed_path: Path, cache_file: Path | None = None) -> bool:
    """Check if raw file needs to be processed.

    Compares:
    1. File existence
    2. Modification time
    3. File hash (if cache available)

    Args:
        raw_path: Path to raw input file
        processed_path: Path to processed output file
        cache_file: Optional path to cache file with hashes

    Returns:
        True if processing is needed
    """
    # Processed file doesn't exist yet
    if not processed_path.exists():
        return True

    cached_hash: str | None = None
    if cache_file and cache_file.exists():
        try:
            with open(cache_file) as f:
                cache = json.load(f)
            cached_hash = cache.get(str(raw_path))
        except Exception:
            cached_hash = None

    if cached_hash is not None:
        try:
            raw_hash = calculate_file_hash(raw_path)
        except Exception:
            return True
        return raw_hash != cached_hash

    # Fall back to modification time when no cache exists
    return raw_path.stat().st_mtime > processed_path.stat().st_mtime


def update_cache(cache_file: Path, file_path: Path):
    """Update cache with current file hash.

    Args:
        cache_file: Path to cache JSON file
        file_path: Path to file to cache
    """
    cache = {}
    if cache_file.exists():
        try:
            with open(cache_file) as f:
                cache = json.load(f)
        except Exception:
            cache = {}

    cache[str(file_path)] = calculate_file_hash(file_path)

    cache_file.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_file, "w") as f:
        json.dump(cache, f, indent=2)


def detect_delimiter(filepath: Path, sample_size: int = 10) -> str:
    """Auto-detect CSV delimiter.

    Args:
        filepath: Path to CSV file
        sample_size: Number of lines to sample

    Returns:
        Detected delimiter character
    """
    with open(filepath) as f:
        sample = "".join([f.readline() for _ in range(sample_size)])

    # Count occurrences of common delimiters
    delimiters = [",", "\t", "|", ";"]
    counts = {d: sample.count(d) for d in delimiters}

    # Return delimiter with highest count
    return max(counts, key=counts.get)


def read_data_file(filepath: Path) -> pd.DataFrame:
    """Read data file (CSV, JSON, or Parquet).

    Args:
        filepath: Path to data file

    Returns:
        pandas DataFrame

    Raises:
        ValueError: If file format is not supported
    """
    suffix = filepath.suffix.lower()

    if suffix == ".csv":
        delimiter = detect_delimiter(filepath)
        return pd.read_csv(filepath, delimiter=delimiter, low_memory=False)
    elif suffix == ".json":
        return pd.read_json(filepath)
    elif suffix == ".parquet":
        return pd.read_parquet(filepath)
    else:
        raise ValueError(f"Unsupported file format: {suffix}")


def write_parquet(df: pd.DataFrame, output_path: Path, compression: str = "snappy"):
    """Write DataFrame to Parquet with consistent settings.

    Args:
        df: pandas DataFrame
        output_path: Path to output file
        compression: Compression algorithm ('snappy', 'gzip', 'brotli')
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, compression=compression, index=False, engine="pyarrow")


def get_processing_stats(df: pd.DataFrame) -> dict[str, Any]:
    """Generate processing statistics for a DataFrame.

    Args:
        df: pandas DataFrame

    Returns:
        Dict with statistics
    """
    return {
        "num_rows": len(df),
        "num_columns": len(df.columns),
        "columns": list(df.columns),
        "dtypes": df.dtypes.astype(str).to_dict(),
        "missing_values": df.isnull().sum().to_dict(),
        "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024**2,
    }
