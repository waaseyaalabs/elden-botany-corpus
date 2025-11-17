"""Schema definitions for Elden Ring datasets.

Uses Pandera for runtime validation and type checking of DataFrames.
"""

from typing import Any

import pandera as pa
from pandera import Check, Column, DataFrameSchema

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
        "weapon_id": Column(pa.Int64, nullable=False, unique=True, coerce=True),
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


# Schema registry
SCHEMA_REGISTRY: dict[str, DataFrameSchema] = {
    "items": ITEMS_SCHEMA,
    "weapons": WEAPONS_SCHEMA,
    "bosses": BOSSES_SCHEMA,
    "armor": ARMOR_SCHEMA,
    "spells": SPELLS_SCHEMA,
}


def get_dataset_schema(dataset_name: str) -> DataFrameSchema | None:
    """Get the Pandera schema for a dataset by name.

    Args:
        dataset_name: Name of the dataset (e.g., 'items', 'weapons')

    Returns:
        DataFrameSchema if found, None otherwise
    """
    # Normalize dataset name
    normalized = dataset_name.lower().replace("-", "_").replace(" ", "_")

    # Try exact match first
    if normalized in SCHEMA_REGISTRY:
        return SCHEMA_REGISTRY[normalized]

    # Try fuzzy match (contains)
    for key, schema in SCHEMA_REGISTRY.items():
        if key in normalized or normalized in key:
            return schema

    return None


def validate_dataframe(df, schema: DataFrameSchema) -> tuple[bool, str | None, Any]:
    """Validate a DataFrame against a schema.

    Args:
        df: pandas DataFrame to validate
        schema: Pandera DataFrameSchema

    Returns:
        Tuple of (is_valid, error_message, validated_df)
        The validated_df contains the coerced types when validation succeeds
    """
    try:
        validated_df = schema.validate(df, lazy=True)
        return True, None, validated_df
    except pa.errors.SchemaErrors as e:
        return False, str(e), df
