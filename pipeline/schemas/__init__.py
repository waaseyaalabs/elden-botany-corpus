"""Schema definitions for Elden Ring datasets with version metadata."""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import pandas as pd
import pandera
import pandera.pandas as pa
from pandera.pandas import Check, Column, DataFrameSchema

# Common field types and validations
COMMON_CHECKS = {
    "positive": Check.greater_than_or_equal_to(0),
    "non_empty_str": Check(lambda s: s.str.len() > 0),
    "percentage": Check.in_range(0, 100),
}


# Elden Ring Items Schema
ITEMS_SCHEMA = DataFrameSchema(
    {
        "item_id": Column(pa.Int64, nullable=False, unique=True, coerce=True),
        "name": Column(
            pa.String,
            nullable=False,
            checks=COMMON_CHECKS["non_empty_str"],
        ),
        "category": Column(
            pa.String,
            nullable=False,
            checks=Check.isin(
                [
                    "weapon",
                    "armor",
                    "consumable",
                    "key_item",
                    "spell",
                    "ash_of_war",
                    "talisman",
                    "tool",
                    "material",
                    "spirit",
                    "other",
                ]
            ),
        ),
        "description": Column(pa.String, nullable=True),
        "weight": Column(
            pa.Float64,
            nullable=True,
            checks=COMMON_CHECKS["positive"],
            coerce=True,
        ),
        "sell_price": Column(
            pa.Int64,
            nullable=True,
            checks=COMMON_CHECKS["positive"],
            coerce=True,
        ),
        "max_stack": Column(
            pa.Int64,
            nullable=True,
            checks=COMMON_CHECKS["positive"],
            coerce=True,
        ),
        "rarity": Column(
            pa.String,
            nullable=True,
            checks=Check.isin(["common", "uncommon", "rare", "legendary"]),
        ),
    },
    strict=False,  # Allow additional columns
    coerce=True,  # Auto-convert types where possible
)


# Elden Ring Weapons Schema
WEAPONS_SCHEMA = DataFrameSchema(
    {
        "weapon_id": Column(
            pa.Int64,
            nullable=False,
            unique=True,
            coerce=True,
        ),
        "name": Column(
            pa.String,
            nullable=False,
            checks=COMMON_CHECKS["non_empty_str"],
        ),
        "weapon_type": Column(
            pa.String,
            nullable=False,
            checks=Check.isin(
                [
                    "sword",
                    "greatsword",
                    "colossal_sword",
                    "dagger",
                    "katana",
                    "spear",
                    "halberd",
                    "axe",
                    "hammer",
                    "flail",
                    "bow",
                    "crossbow",
                    "staff",
                    "seal",
                    "fist",
                    "claw",
                    "whip",
                    "other",
                ]
            ),
        ),
    },
    strict=False,  # Allow additional columns
    coerce=True,  # Auto-convert types where possible
)


# Elden Ring Bosses Schema
BOSSES_SCHEMA = DataFrameSchema(
    {
        "boss_id": Column(pa.Int64, nullable=False, unique=True, coerce=True),
        "name": Column(
            pa.String,
            nullable=False,
            checks=COMMON_CHECKS["non_empty_str"],
        ),
    },
    strict=False,
    coerce=True,
)


# Armor Schema
ARMOR_SCHEMA = DataFrameSchema(
    {
        "armor_id": Column(
            pa.Int64,
            nullable=False,
            unique=True,
            coerce=True,
        ),
        "name": Column(
            pa.String,
            nullable=False,
            checks=COMMON_CHECKS["non_empty_str"],
        ),
        "armor_type": Column(
            pa.String,
            nullable=False,
            checks=Check.isin(["head", "chest", "arms", "legs"]),
        ),
        "weight": Column(
            pa.Float64,
            nullable=False,
            checks=COMMON_CHECKS["positive"],
            coerce=True,
        ),
        "defense_physical": Column(
            pa.Float64,
            nullable=True,
            checks=COMMON_CHECKS["positive"],
            coerce=True,
        ),
        "defense_strike": Column(
            pa.Float64,
            nullable=True,
            checks=COMMON_CHECKS["positive"],
            coerce=True,
        ),
        "defense_slash": Column(
            pa.Float64,
            nullable=True,
            checks=COMMON_CHECKS["positive"],
            coerce=True,
        ),
        "defense_pierce": Column(
            pa.Float64,
            nullable=True,
            checks=COMMON_CHECKS["positive"],
            coerce=True,
        ),
        "defense_magic": Column(
            pa.Float64,
            nullable=True,
            checks=COMMON_CHECKS["positive"],
            coerce=True,
        ),
        "defense_fire": Column(
            pa.Float64,
            nullable=True,
            checks=COMMON_CHECKS["positive"],
            coerce=True,
        ),
        "defense_lightning": Column(
            pa.Float64,
            nullable=True,
            checks=COMMON_CHECKS["positive"],
            coerce=True,
        ),
        "defense_holy": Column(
            pa.Float64,
            nullable=True,
            checks=COMMON_CHECKS["positive"],
            coerce=True,
        ),
        "resistance_immunity": Column(
            pa.Int64,
            nullable=True,
            checks=COMMON_CHECKS["positive"],
            coerce=True,
        ),
        "resistance_robustness": Column(
            pa.Int64,
            nullable=True,
            checks=COMMON_CHECKS["positive"],
            coerce=True,
        ),
        "resistance_focus": Column(
            pa.Int64,
            nullable=True,
            checks=COMMON_CHECKS["positive"],
            coerce=True,
        ),
        "resistance_vitality": Column(
            pa.Int64,
            nullable=True,
            checks=COMMON_CHECKS["positive"],
            coerce=True,
        ),
        "poise": Column(
            pa.Float64,
            nullable=True,
            checks=COMMON_CHECKS["positive"],
            coerce=True,
        ),
    },
    strict=False,
    coerce=True,
)


# Spells Schema
SPELLS_SCHEMA = DataFrameSchema(
    {
        "spell_id": Column(
            pa.Int64,
            nullable=False,
            unique=True,
            coerce=True,
        ),
        "name": Column(
            pa.String,
            nullable=False,
            checks=COMMON_CHECKS["non_empty_str"],
        ),
        "spell_type": Column(
            pa.String,
            nullable=False,
            checks=Check.isin(["sorcery", "incantation"]),
        ),
        "fp_cost": Column(
            pa.Int64,
            nullable=False,
            checks=COMMON_CHECKS["positive"],
            coerce=True,
        ),
        "stamina_cost": Column(
            pa.Int64,
            nullable=True,
            checks=COMMON_CHECKS["positive"],
            coerce=True,
        ),
        "slots_required": Column(
            pa.Int64,
            nullable=False,
            checks=COMMON_CHECKS["positive"],
            coerce=True,
        ),
        "required_int": Column(
            pa.Int64,
            nullable=True,
            checks=COMMON_CHECKS["positive"],
            coerce=True,
        ),
        "required_fai": Column(
            pa.Int64,
            nullable=True,
            checks=COMMON_CHECKS["positive"],
            coerce=True,
        ),
        "required_arc": Column(
            pa.Int64,
            nullable=True,
            checks=COMMON_CHECKS["positive"],
            coerce=True,
        ),
        "description": Column(pa.String, nullable=True),
    },
    strict=False,
    coerce=True,
)


VERSION_SUFFIX_RE = re.compile(
    r"^(?P<dataset>[a-z0-9_]+)_v(?P<version>[a-z0-9][a-z0-9_.-]*)$"
)


def _normalize_dataset_name(name: str) -> str:
    return name.strip().lower().replace("-", "_").replace(" ", "_")


@dataclass(frozen=True)
class SchemaVersion:
    """Metadata wrapper for a Pandera schema version."""

    dataset: str
    version: str
    schema: DataFrameSchema
    migration_notes: str | None = None
    compatibility: list[str] | None = None
    deprecated: bool = False

    def __post_init__(self) -> None:
        normalized_dataset = _normalize_dataset_name(self.dataset)
        canonical_version = self.version.lower()
        if not canonical_version.startswith("v"):
            canonical_version = f"v{canonical_version}"
        object.__setattr__(self, "dataset", normalized_dataset)
        object.__setattr__(self, "version", canonical_version)

    @property
    def tag(self) -> str:
        """Return canonical identifier (e.g., 'weapons_v1')."""

        return f"{self.dataset}_{self.version}"

    @property
    def is_active(self) -> bool:
        """Whether this schema version is active (not deprecated)."""

        return not self.deprecated

    def to_metadata(self) -> dict[str, Any]:
        """Serialize schema metadata for downstream tracking."""

        payload: dict[str, Any] = {
            "tag": self.tag,
            "version": self.version,
            "dataset": self.dataset,
            "deprecated": self.deprecated,
        }
        if self.migration_notes:
            payload["migration_notes"] = self.migration_notes
        if self.compatibility:
            payload["compatibility"] = self.compatibility
        return payload


SCHEMA_REGISTRY: dict[str, list[SchemaVersion]] = {
    "items": [
        SchemaVersion(
            dataset="items",
            version="v1",
            schema=ITEMS_SCHEMA,
            migration_notes="Initial canonical schema",
            compatibility=["initial_release"],
        ),
    ],
    "weapons": [
        SchemaVersion(
            dataset="weapons",
            version="v1",
            schema=WEAPONS_SCHEMA,
            migration_notes="Initial canonical schema",
            compatibility=["initial_release"],
        ),
    ],
    "bosses": [
        SchemaVersion(
            dataset="bosses",
            version="v1",
            schema=BOSSES_SCHEMA,
            migration_notes="Initial canonical schema",
            compatibility=["initial_release"],
        ),
    ],
    "armor": [
        SchemaVersion(
            dataset="armor",
            version="v1",
            schema=ARMOR_SCHEMA,
            migration_notes="Initial canonical schema",
            compatibility=["initial_release"],
        ),
    ],
    "spells": [
        SchemaVersion(
            dataset="spells",
            version="v1",
            schema=SPELLS_SCHEMA,
            migration_notes="Initial canonical schema",
            compatibility=["initial_release"],
        ),
    ],
}


def _match_dataset_key(dataset_name: str) -> tuple[str | None, str | None]:
    """Return dataset key and optional explicit version from input name."""

    normalized = _normalize_dataset_name(dataset_name)
    if normalized in SCHEMA_REGISTRY:
        return normalized, None

    match = VERSION_SUFFIX_RE.match(normalized)
    if match:
        dataset_key = match.group("dataset")
        version = f"v{match.group('version')}"
        if dataset_key in SCHEMA_REGISTRY:
            return dataset_key, version

    # Fallback to fuzzy matching
    for key in SCHEMA_REGISTRY:
        if key in normalized or normalized in key:
            return key, None

    return None, None


def _iter_schema_candidates(
    dataset_key: str,
    allow_deprecated: bool,
) -> Iterable[SchemaVersion]:
    for schema_version in SCHEMA_REGISTRY.get(dataset_key, []):
        if schema_version.deprecated and not allow_deprecated:
            continue
        yield schema_version


def get_dataset_schema(
    dataset_name: str,
    version: str | None = None,
    allow_deprecated: bool = False,
) -> SchemaVersion | None:
    """Get schema metadata for a dataset.

    Args:
        dataset_name: Dataset identifier (e.g., 'items', 'weapons_v1')
        version: Optional explicit version string ('v1' or 'items_v1')
        allow_deprecated: When True, include deprecated versions in lookup

    Returns:
        ``SchemaVersion`` if found, ``None`` otherwise
    """

    dataset_key, inline_version = _match_dataset_key(dataset_name)
    if dataset_key is None:
        return None

    requested_version = version or inline_version
    if requested_version:
        requested_version = requested_version.lower()
        if not requested_version.startswith("v"):
            requested_version = f"v{requested_version}"

    for schema_version in _iter_schema_candidates(
        dataset_key,
        allow_deprecated,
    ):
        if requested_version and schema_version.version != requested_version:
            continue
        return schema_version

    return None


def list_schema_versions(
    dataset_name: str | None = None,
) -> dict[str, list[SchemaVersion]] | list[SchemaVersion]:
    """List schema versions for a dataset or the entire registry."""

    if dataset_name is None:
        return SCHEMA_REGISTRY

    dataset_key, _ = _match_dataset_key(dataset_name)
    if dataset_key is None:
        return []
    return SCHEMA_REGISTRY.get(dataset_key, [])


def get_active_schema_version(dataset_name: str) -> SchemaVersion | None:
    """Return the first non-deprecated schema version for a dataset."""

    dataset_key, _ = _match_dataset_key(dataset_name)
    if dataset_key is None:
        return None

    for schema_version in _iter_schema_candidates(
        dataset_key,
        allow_deprecated=False,
    ):
        return schema_version
    return None


def validate_dataframe(
    df: pd.DataFrame,
    schema: DataFrameSchema | SchemaVersion,
) -> tuple[bool, str | None, pd.DataFrame]:
    """Validate a DataFrame against a schema.

    Args:
        df: pandas DataFrame to validate
        schema: Pandera DataFrameSchema

    Returns:
        Tuple of (is_valid, error_message, validated_df)
        The validated_df contains the coerced types when validation succeeds
    """
    try:
        schema_obj = (
            schema.schema if isinstance(schema, SchemaVersion) else schema
        )
        validated_df = schema_obj.validate(df, lazy=True)
        return True, None, validated_df
    except pandera.errors.SchemaErrors as e:
        return False, str(e), df
