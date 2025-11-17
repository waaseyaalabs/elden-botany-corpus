"""Build a unified lore corpus from canonical tables and Impalers excerpts."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd  # type: ignore[import]
import pandera as pa  # type: ignore[import]
from bs4 import BeautifulSoup  # type: ignore[import]
from corpus.models import create_slug, normalize_name_for_matching
from Levenshtein import ratio as levenshtein_ratio  # type: ignore[import]
from pandera import Check, Column, DataFrameSchema

LOGGER = logging.getLogger(__name__)

DEFAULT_CURATED_ROOT = Path("data/curated")
DEFAULT_RAW_ROOT = Path("data/raw")
DEFAULT_OUTPUT = Path("data/curated/lore_corpus.parquet")

ALLOWED_SOURCES = {"kaggle_base", "kaggle_dlc", "github_api", "impalers"}
ALLOWED_CATEGORIES = {"item", "weapon", "armor", "boss", "spell", "npc"}
ALLOWED_TEXT_TYPES = {
    "description",
    "effect",
    "obtained_from",
    "quote",
    "lore",
    "skill",
    "special_effect",
    "drops",
    "impalers_excerpt",
}


@dataclass(frozen=True)
class TextField:
    column: str
    text_type: str


@dataclass(frozen=True)
class DomainSpec:
    dataset: str
    file_name: str
    id_column: str
    category: str
    text_fields: tuple[TextField, ...]


DOMAIN_SPECS: tuple[DomainSpec, ...] = (
    DomainSpec(
        dataset="items",
        file_name="items_canonical.parquet",
        id_column="item_id",
        category="item",
        text_fields=(
            TextField("description", "description"),
            TextField("effect", "effect"),
            TextField("obtained_from", "obtained_from"),
        ),
    ),
    DomainSpec(
        dataset="weapons",
        file_name="weapons_canonical.parquet",
        id_column="weapon_id",
        category="weapon",
        text_fields=(
            TextField("description", "description"),
            TextField("skill_description", "skill"),
            TextField("special_effect", "special_effect"),
        ),
    ),
    DomainSpec(
        dataset="armor",
        file_name="armor_canonical.parquet",
        id_column="armor_id",
        category="armor",
        text_fields=(TextField("description", "description"),),
    ),
    DomainSpec(
        dataset="bosses",
        file_name="bosses_canonical.parquet",
        id_column="boss_id",
        category="boss",
        text_fields=(
            TextField("description", "description"),
            TextField("quote", "quote"),
            TextField("lore", "lore"),
            TextField("drops", "drops"),
        ),
    ),
    DomainSpec(
        dataset="spells",
        file_name="spells_canonical.parquet",
        id_column="spell_id",
        category="spell",
        text_fields=(
            TextField("description", "description"),
            TextField("effects", "effect"),
        ),
    ),
)

SPEC_BY_CATEGORY = {spec.category: spec for spec in DOMAIN_SPECS}


SECTION_CATEGORY = {
    "AccessoryName": "item",
    "GoodsName": "item",
    "WeaponName": "weapon",
    "ProtectorName": "armor",
    "MagicName": "spell",
}


LORE_SCHEMA = DataFrameSchema(
    {
        "lore_id": Column(pa.String, nullable=False, unique=True),
        "canonical_id": Column(pa.String, nullable=False),
        "category": Column(pa.String, Check.isin(sorted(ALLOWED_CATEGORIES))),
        "source": Column(pa.String, Check.isin(sorted(ALLOWED_SOURCES))),
        "text_type": Column(pa.String, Check.isin(sorted(ALLOWED_TEXT_TYPES))),
        "language": Column(pa.String, Check.isin(["en"])),
        "text": Column(pa.String, Check.str_length(min_value=1)),
        "provenance": Column(pa.String, nullable=False),
    },
    strict=True,
    coerce=True,
)


def build_lore_corpus(
    *,
    curated_root: Path,
    raw_root: Path,
    output_path: Path = DEFAULT_OUTPUT,
    dry_run: bool = False,
) -> pd.DataFrame:
    """Aggregate lore lines from canonical datasets and Impalers dump."""

    canonical_frames = {
        spec.category: _load_domain_frame(spec, curated_root) for spec in DOMAIN_SPECS
    }
    canonical_lookup = _build_canonical_lookup(canonical_frames)

    lore_rows: list[dict[str, Any]] = []
    for spec in DOMAIN_SPECS:
        frame = canonical_frames[spec.category]
        lore_rows.extend(_extract_canonical_lore(frame, spec))

    impalers_entries = _parse_impalers_dump(raw_root / "impalers" / "Master.html")
    matched_impalers, unmatched = _match_impalers_entries(
        entries=impalers_entries,
        canonical_lookup=canonical_lookup,
    )
    lore_rows.extend(matched_impalers)

    if not lore_rows:
        raise RuntimeError("No lore rows were produced")

    lore_df = pd.DataFrame(lore_rows)
    lore_df.drop_duplicates(subset=["lore_id"], inplace=True)
    lore_df.sort_values(["canonical_id", "text_type", "source"], inplace=True)
    lore_df.reset_index(drop=True, inplace=True)

    try:
        validated = LORE_SCHEMA.validate(lore_df, lazy=True)
    except pa.errors.SchemaErrors as exc:  # type: ignore[attr-defined]
        LOGGER.error("Lore schema validation failed: %s", exc)
        raise

    _log_corpus_stats(validated, unmatched)

    if dry_run:
        LOGGER.info("Dry run enabled; skipping parquet write")
        return validated

    output_path.parent.mkdir(parents=True, exist_ok=True)
    validated.to_parquet(output_path, index=False)
    LOGGER.info("Wrote lore corpus to %s", output_path)

    return validated


def _load_domain_frame(spec: DomainSpec, curated_root: Path) -> pd.DataFrame:
    path = curated_root / spec.file_name
    if not path.exists():
        message = f"Missing canonical dataset: {path}"
        raise FileNotFoundError(message)

    frame = pd.read_parquet(path)
    if spec.id_column not in frame.columns:
        message = f"{spec.dataset} dataset is missing id column {spec.id_column}"
        raise RuntimeError(message)
    return frame


def _extract_canonical_lore(frame: pd.DataFrame, spec: DomainSpec) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for record in frame.to_dict("records"):
        canonical_id = _build_canonical_id(record, spec)
        for field in spec.text_fields:
            raw_text = record.get(field.column)
            text = _normalize_text(raw_text)
            if not text:
                continue

            source = str(record.get("source") or "").strip() or "github_api"
            if source not in ALLOWED_SOURCES:
                source = "github_api"

            payload = {
                "source": source,
                "source_id": record.get("source_id"),
                "source_priority": record.get("source_priority"),
                "text_column": field.column,
            }

            provenance_value = record.get("provenance")
            if provenance_value:
                try:
                    payload["canonical_provenance"] = json.loads(provenance_value)
                except (TypeError, json.JSONDecodeError):
                    payload["canonical_provenance"] = provenance_value

            lore_id = _compute_lore_id(canonical_id, field.text_type, text)
            rows.append(
                {
                    "lore_id": lore_id,
                    "canonical_id": canonical_id,
                    "category": spec.category,
                    "source": source,
                    "text_type": field.text_type,
                    "language": "en",
                    "text": text,
                    "provenance": json.dumps(
                        payload,
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                }
            )

    return rows


def _parse_impalers_dump(html_path: Path) -> list[dict[str, Any]]:
    if not html_path.exists():
        LOGGER.warning("Impalers HTML not found at %s", html_path)
        return []

    soup = BeautifulSoup(html_path.read_text(encoding="utf-8"), "html.parser")
    entries: list[dict[str, Any]] = []

    current_section: str | None = None
    current_category: str | None = None
    current_entry: dict[str, Any] | None = None

    for node in soup.find_all(["h2", "h3", "p"]):
        if node.name == "h2":
            current_section = node.get_text(strip=True)
            section_base = _section_base(current_section)
            current_category = SECTION_CATEGORY.get(section_base) if section_base else None
            current_entry = None
            continue

        if current_category is None:
            continue

        if node.name == "h3":
            if current_entry and current_entry.get("paragraphs"):
                entries.append(current_entry)
            name, entry_id = _parse_heading(node.get_text(" ", strip=True))
            current_entry = {
                "name": name,
                "entry_id": entry_id,
                "section": current_section,
                "category": current_category,
                "paragraphs": [],
            }
            continue

        if node.name == "p":
            text = node.get_text(" ", strip=True)
            if not text:
                continue

            if current_entry is None:
                # Some sections emit names via <p>[123] Name
                parsed = _parse_name_from_paragraph(text)
                if parsed:
                    name, entry_id = parsed
                    current_entry = {
                        "name": name,
                        "entry_id": entry_id,
                        "section": current_section,
                        "category": current_category,
                        "paragraphs": [],
                    }
                    continue

            if current_entry is None:
                continue

            current_entry.setdefault("paragraphs", []).append(text)

    if current_entry and current_entry.get("paragraphs"):
        entries.append(current_entry)

    LOGGER.info("Parsed %s Impalers entries", len(entries))
    return entries


def _match_impalers_entries(
    *,
    entries: list[dict[str, Any]],
    canonical_lookup: dict[str, list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    matched_rows: list[dict[str, Any]] = []
    unmatched_entries: list[dict[str, Any]] = []

    by_category: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for bucket in canonical_lookup.values():
        for record in bucket:
            by_category[record["category"]].append(record)

    for entry in entries:
        category = entry["category"]
        candidates = by_category.get(category, [])
        if not candidates:
            unmatched_entries.append(entry)
            continue

        name_slug = normalize_name_for_matching(entry["name"])
        best_record: dict[str, Any] | None = None
        best_score = 0.0
        for record in candidates:
            slug = record["match_slug"]
            if slug == name_slug:
                best_record = record
                best_score = 1.0
                break
            score = levenshtein_ratio(name_slug, slug)
            if score > best_score:
                best_record = record
                best_score = score

        if best_record is None or best_score < 0.82:
            entry["match_score"] = best_score
            unmatched_entries.append(entry)
            continue

        text = _normalize_text("\n".join(entry.get("paragraphs", [])))
        if not text:
            continue

        canonical_id = best_record["canonical_id"]
        lore_id = _compute_lore_id(canonical_id, "impalers_excerpt", text)
        provenance = {
            "source": "impalers",
            "section": entry.get("section"),
            "entry_id": entry.get("entry_id"),
            "match_score": round(best_score, 4),
        }
        matched_rows.append(
            {
                "lore_id": lore_id,
                "canonical_id": canonical_id,
                "category": category,
                "source": "impalers",
                "text_type": "impalers_excerpt",
                "language": "en",
                "text": text,
                "provenance": json.dumps(
                    provenance,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            }
        )

    return matched_rows, unmatched_entries


def _build_canonical_lookup(frames: dict[str, pd.DataFrame]) -> dict[str, list[dict[str, Any]]]:
    lookup: dict[str, list[dict[str, Any]]] = {}
    for category, frame in frames.items():
        entries: list[dict[str, Any]] = []
        for record in frame.to_dict("records"):
            slug = record.get("canonical_slug") or record.get("name")
            if not isinstance(slug, str) or not slug:
                slug = create_slug(str(record.get("name", "")))
            normalized = normalize_name_for_matching(slug)
            canonical_id = _build_canonical_id(
                record,
                SPEC_BY_CATEGORY[category],
            )
            entries.append(
                {
                    "canonical_id": canonical_id,
                    "category": category,
                    "name": record.get("name"),
                    "match_slug": normalized,
                }
            )
        lookup[category] = entries
    return lookup


def _build_canonical_id(record: dict[str, Any], spec: DomainSpec) -> str:
    value = record.get(spec.id_column)
    if value is None or pd.isna(value):
        slug = record.get("canonical_slug") or create_slug(str(record.get("name", "")))
        return f"{spec.category}:{slug}"
    return f"{spec.category}:{int(value)}"


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    if not text.strip():
        return ""
    text = re.sub(r"\r\n?", "\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{2,}", "\n", text)
    text = text.strip()
    return text


def _compute_lore_id(canonical_id: str, text_type: str, text: str) -> str:
    payload = f"{canonical_id}|{text_type}|{text}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _parse_heading(raw: str) -> tuple[str, str | None]:
    match = re.match(r"^(.*)\[(\d+)]$", raw)
    if match:
        name = match.group(1).strip()
        entry_id = match.group(2)
    else:
        name = raw.strip()
        entry_id = None
    return name, entry_id


def _parse_name_from_paragraph(raw: str) -> tuple[str, str] | None:
    match = re.match(r"^\[(\d+)]\s*(.+)$", raw)
    if not match:
        return None
    return match.group(2).strip(), match.group(1)


def _section_base(section: str | None) -> str | None:
    if section is None:
        return None
    return section.split("_")[0]


def _log_corpus_stats(df: pd.DataFrame, unmatched: list[dict[str, Any]]) -> None:
    LOGGER.info("Lore lines total: %s", len(df))
    for label, series in (
        ("by source", df["source"]),
        ("by category", df["category"]),
        ("by text_type", df["text_type"]),
    ):
        counts = Counter(series)
        formatted = ", ".join(f"{key}={value}" for key, value in counts.items())
        LOGGER.info("Lore lines %s: %s", label, formatted)

    impaler_count = len(df[df["source"] == "impalers"])
    LOGGER.info("Impalers matched entries: %s", impaler_count)
    if unmatched:
        LOGGER.warning("Impalers unmatched entries: %s", len(unmatched))

    multi_source = df.groupby("canonical_id")["source"].nunique()
    enriched = multi_source[multi_source > 1].head(5).index.tolist()
    if enriched:
        LOGGER.debug("Multi-source examples: %s", ", ".join(enriched))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build unified lore corpus")
    parser.add_argument(
        "--curated-root",
        type=Path,
        default=DEFAULT_CURATED_ROOT,
        help="Directory containing canonical parquet outputs",
    )
    parser.add_argument(
        "--raw-root",
        type=Path,
        default=DEFAULT_RAW_ROOT,
        help="Root directory for raw assets (Impalers dump)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Lore corpus parquet destination",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip writes and report stats only",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    return parser.parse_args()


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s %(message)s")


def main() -> None:
    args = parse_args()
    configure_logging(args.verbose)

    try:
        build_lore_corpus(
            curated_root=args.curated_root,
            raw_root=args.raw_root,
            output_path=args.output,
            dry_run=args.dry_run,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.error("Lore corpus pipeline failed: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":  # pragma: no cover
    main()
