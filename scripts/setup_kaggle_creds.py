#!/usr/bin/env python3
"""Materialize Kaggle credentials locally without tracking secrets."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path


def parse_env_file(path: Path) -> dict[str, str]:
    """Best-effort parser for KEY=VALUE lines inside a .env file."""
    values: dict[str, str] = {}
    if not path.exists():
        return values

    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"\'')
    return values


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Create ~/.kaggle/kaggle.json from env vars or a .env file."
        )
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help=(
            "Optional .env file to read when environment variables "
            "are not already set."
        ),
    )
    args = parser.parse_args()

    env_file = Path(args.env_file)
    env_values = parse_env_file(env_file)

    username = os.environ.get("KAGGLE_USERNAME") or env_values.get(
        "KAGGLE_USERNAME"
    )
    key = os.environ.get("KAGGLE_KEY") or env_values.get(
        "KAGGLE_KEY"
    )

    if not username or not key:
        raise SystemExit(
            "KAGGLE_USERNAME and KAGGLE_KEY must come from env vars "
            "or the specified .env file."
        )

    kaggle_dir = Path.home() / ".kaggle"
    kaggle_dir.mkdir(parents=True, exist_ok=True)
    kaggle_json = kaggle_dir / "kaggle.json"

    kaggle_json.write_text(
        json.dumps({"username": username, "key": key}, indent=2)
    )
    os.chmod(kaggle_json, 0o600)

    print(f"Wrote credentials to {kaggle_json} with 0600 permissions.")


if __name__ == "__main__":
    main()
