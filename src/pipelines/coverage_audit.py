# pyright: reportUnknownArgumentType=false, reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false

"""Utilities for validating NPC summary coverage."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd

from pipelines.aliasing import AliasResolver, load_alias_map

try:  # pragma: no cover - optional dependency detection
    import pyarrow  # noqa: F401
except ImportError:  # pyarrow is optional when using fastparquet
    _PARQUET_READ_KWARGS: dict[str, Any] = {}
else:
    _PARQUET_READ_KWARGS = {
        "engine": "pyarrow",
        "use_threads": False,
    }


@dataclass(slots=True)
class CoverageReport:
    """Diff describing curated vs. summarized NPC coverage."""

    curated_entities: int
    summarized_entities: int
    missing_ids: tuple[str, ...]
    extra_ids: tuple[str, ...]


def compare_summary_coverage(
    lore_path: Path,
    summary_path: Path,
    *,
    alias_table: Path | None = None,
    categories: Sequence[str] | None = ("npc",),
) -> CoverageReport:
    """Compare curated lore speakers to generated summaries."""

    alias_map = load_alias_map(alias_table)
    curated_ids = _load_curated_ids(lore_path, alias_map, categories)
    summary_ids = _load_summary_ids(summary_path, alias_map)

    missing = tuple(sorted(curated_ids - summary_ids))
    extra = tuple(sorted(summary_ids - curated_ids))
    return CoverageReport(
        curated_entities=len(curated_ids),
        summarized_entities=len(summary_ids),
        missing_ids=missing,
        extra_ids=extra,
    )


def _load_curated_ids(
    path: Path,
    alias_map: AliasResolver,
    categories: Sequence[str] | None,
) -> set[str]:
    if not path.exists():
        raise FileNotFoundError(
            f"Curated lore corpus not found at {path}. Run 'poetry run corpus "
            "curate' first."
        )
    try:
        frame = _read_parquet(
            path,
            columns=["canonical_id", "category", "text"],
        )
    except Exception as exc:  # pragma: no cover - defensive
        raise ValueError(
            f"Unable to read curated lore corpus at {path}: {exc}"
        ) from exc

    subset = frame.copy()
    subset["category"] = subset["category"].astype(str).str.strip().str.lower()
    subset["canonical_id"] = subset["canonical_id"].astype(str).str.strip()
    subset["text"] = subset["text"].astype(str).str.strip()

    if categories:
        targets = {cat.strip().lower() for cat in categories if cat.strip()}
        subset = subset.loc[subset["category"].isin(targets)]

    subset = subset.loc[
        (subset["canonical_id"] != "") & (subset["text"] != "")
    ]

    identifiers: set[str] = set()
    for value in subset["canonical_id"].tolist():
        resolved = alias_map.resolve(value)
        if resolved:
            identifiers.add(resolved)
    return identifiers


def _load_summary_ids(path: Path, alias_map: AliasResolver) -> set[str]:
    if not path.exists():
        raise FileNotFoundError(
            "Narrative summaries file not found at "
            f"{path}. Run 'corpus analysis summaries' first."
        )
    suffix = path.suffix.lower()
    if suffix in {".parquet", ".pq"}:
        try:
            frame = _read_parquet(path, columns=["canonical_id"])
        except Exception as exc:  # pragma: no cover - defensive
            raise ValueError(
                f"Unable to read narrative summaries parquet at {path}: {exc}"
            ) from exc
        return _canonical_id_set(frame["canonical_id"].tolist(), alias_map)
    if suffix == ".jsonl":
        records: list[Mapping[str, Any]] = []
        with path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError as exc:  # pragma: no cover
                    raise ValueError(
                        f"Invalid JSONL line in {path}: {exc}"
                    ) from exc
                if isinstance(payload, Mapping):
                    records.append(payload)
        return _canonical_id_from_records(records, alias_map)
    if suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        records = _extract_json_records(payload)
        return _canonical_id_from_records(records, alias_map)
    raise ValueError(
        "Unsupported summary format. Provide a parquet, json, or jsonl file."
    )


def _canonical_id_set(
    values: Iterable[Any],
    alias_map: AliasResolver,
) -> set[str]:
    identifiers: set[str] = set()
    for value in values:
        normalized = str(value or "").strip()
        if not normalized:
            continue
        identifiers.add(alias_map.resolve(normalized))
    return identifiers


def _canonical_id_from_records(
    records: Iterable[Mapping[str, Any]],
    alias_map: AliasResolver,
) -> set[str]:
    identifiers: set[str] = set()
    for record in records:
        raw_id = record.get("canonical_id")
        normalized = str(raw_id or "").strip()
        if not normalized:
            continue
        identifiers.add(alias_map.resolve(normalized))
    return identifiers


def _extract_json_records(payload: Any) -> list[Mapping[str, Any]]:
    if isinstance(payload, Mapping):
        summaries = payload.get("summaries")
        if isinstance(summaries, Sequence) and not isinstance(
            summaries, (str, bytes)
        ):
            return [item for item in summaries if isinstance(item, Mapping)]
        raise ValueError("Summary JSON must contain a 'summaries' array")
    if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes)):
        return [item for item in payload if isinstance(item, Mapping)]
    raise ValueError(
        "Summary JSON must be an object with 'summaries' or a list of records"
    )


def _read_parquet(path: Path, *, columns: list[str]) -> pd.DataFrame:
    """Load Parquet deterministically to avoid pyarrow thread finalizers."""

    try:
        return pd.read_parquet(
            path,
            columns=columns,
            **_PARQUET_READ_KWARGS,
        )
    except TypeError:
        # Older pandas/engines might not accept use_threads/engine args.
        return pd.read_parquet(path, columns=columns)


__all__ = ["CoverageReport", "compare_summary_coverage"]
