from __future__ import annotations

import ast
import json
import math
from typing import Any

DAMAGE_KEY_MAP = {
    "phy": "damage_physical",
    "mag": "damage_magic",
    "fire": "damage_fire",
    "ligt": "damage_lightning",
    "lit": "damage_lightning",
    "lightning": "damage_lightning",
    "holy": "damage_holy",
    "crit": "damage_critical",
}

ATTRIBUTE_KEY_MAP = {
    "str": "required_str",
    "strength": "required_str",
    "dex": "required_dex",
    "dexterity": "required_dex",
    "int": "required_int",
    "intelligence": "required_int",
    "fai": "required_fai",
    "faith": "required_fai",
    "arc": "required_arc",
    "arcane": "required_arc",
}

SCALING_KEY_MAP = {
    "str": "scaling_str",
    "dex": "scaling_dex",
    "int": "scaling_int",
    "fai": "scaling_fai",
    "arc": "scaling_arc",
}

SCALING_DEFAULT = {value: "-" for value in SCALING_KEY_MAP.values()}

WEAPON_TYPE_MAPPING = {
    "straight sword": "sword",
    "curved sword": "sword",
    "thrusting sword": "sword",
    "sword": "sword",
    "greatsword": "greatsword",
    "great sword": "greatsword",
    "colossal sword": "colossal_sword",
    "colossal weapon": "colossal_sword",
    "dagger": "dagger",
    "twinblade": "spear",
    "spear": "spear",
    "great spear": "spear",
    "halberd": "halberd",
    "axe": "axe",
    "great axe": "axe",
    "hammer": "hammer",
    "great hammer": "hammer",
    "flail": "flail",
    "bow": "bow",
    "crossbow": "crossbow",
    "staff": "staff",
    "glintstone staff": "staff",
    "seal": "seal",
    "sacred seal": "seal",
    "fist": "fist",
    "claw": "claw",
    "whip": "whip",
    "torch": "other",
    "shield": "other",
    "thrusting shield": "other",
    "thrusting shields": "other",
    "ballista": "other",
    "other": "other",
}

ARMOR_DAMAGE_KEY_MAP = {
    "phy": "defense_physical",
    "physical": "defense_physical",
    "strike": "defense_strike",
    "vs str.": "defense_strike",
    "slash": "defense_slash",
    "vs sla.": "defense_slash",
    "pierce": "defense_pierce",
    "vs pie.": "defense_pierce",
    "mag": "defense_magic",
    "magic": "defense_magic",
    "fir": "defense_fire",
    "fire": "defense_fire",
    "lit": "defense_lightning",
    "ligt": "defense_lightning",
    "lightning": "defense_lightning",
    "hol": "defense_holy",
    "holy": "defense_holy",
}

ARMOR_RESISTANCE_KEY_MAP = {
    "imm.": "resistance_immunity",
    "immunity": "resistance_immunity",
    "rob.": "resistance_robustness",
    "robustness": "resistance_robustness",
    "foc.": "resistance_focus",
    "focus": "resistance_focus",
    "vit.": "resistance_vitality",
    "vitality": "resistance_vitality",
    "poi.": "poise",
    "poise": "poise",
}

ITEM_CATEGORY_ALIASES = {
    "weapon": "weapon",
    "armor": "armor",
    "helm": "armor",
    "chest armor": "armor",
    "gauntlets": "armor",
    "leg armor": "armor",
    "consumable": "consumable",
    "reusable": "tool",
    "tool": "tool",
    "cookbook": "tool",
    "crafting": "tool",
    "talisman": "talisman",
    "ash of war": "ash_of_war",
    "ashes of war": "ash_of_war",
    "key item": "key_item",
    "keyitem": "key_item",
    "bell bearing": "key_item",
    "bell": "key_item",
    "great rune": "key_item",
    "remembrance": "key_item",
    "material": "material",
    "upgrade material": "material",
    "crystal tear": "tool",
    "ammo": "tool",
    "cookbooks": "tool",
    "multi": "other",
    "whetblade": "tool",
    "sorcery": "spell",
    "incantation": "spell",
    "spell": "spell",
}

RARITY_CHOICES = {
    "common",
    "uncommon",
    "rare",
    "legendary",
}


def safe_float(value: Any) -> float | None:
    """Best-effort conversion to float."""

    if value is None:
        return None

    if isinstance(value, float | int) and not (
        isinstance(value, float) and math.isnan(value)
    ):
        return float(value)

    if isinstance(value, str):
        text = value.strip().replace(",", "")
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None

    return None


def safe_int(value: Any) -> int | None:
    """Best-effort conversion to Int64-friendly int."""

    if value is None:
        return None

    if isinstance(value, float | int):
        if isinstance(value, float) and math.isnan(value):
            return None
        return int(value)

    if isinstance(value, str):
        text = value.strip().replace(",", "")
        if not text:
            return None
        try:
            return int(float(text))
        except ValueError:
            return None

    return None


def parse_structured_data(value: Any) -> Any:
    """Parse raw JSON-like blobs coming from CSV/JSON sources."""

    if value is None:
        return None

    if isinstance(value, list | dict):
        return value

    if isinstance(value, float) and math.isnan(value):
        return None

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            try:
                return ast.literal_eval(text)
            except (ValueError, SyntaxError):
                return None

    return None


def to_entry_list(value: Any) -> list[dict[str, Any]]:
    """Coerce structured values into a list of dict entries."""

    data = parse_structured_data(value)

    if data is None:
        return []

    if isinstance(data, list):
        return [entry for entry in data if isinstance(entry, dict)]

    if isinstance(data, dict):
        return [{"name": key, "amount": data[key]} for key in data]

    return []


def extract_damage(entries: list[dict[str, Any]]) -> dict[str, float | None]:
    """Extract damage columns from structured attack stats."""

    result: dict[str, float | None] = {
        value: None for value in DAMAGE_KEY_MAP.values()
    }

    for entry in entries:
        name = str(entry.get("name", "")).strip().lower()
        key = DAMAGE_KEY_MAP.get(name)
        if key:
            result[key] = safe_float(entry.get("amount"))

    return result


def extract_requirements(
    entries: list[dict[str, Any]]
) -> dict[str, int | None]:
    """Extract attribute requirements as canonical columns."""

    result: dict[str, int | None] = {
        value: None for value in ATTRIBUTE_KEY_MAP.values()
    }

    for entry in entries:
        name = str(entry.get("name", "")).strip().lower()
        key = ATTRIBUTE_KEY_MAP.get(name)
        if key:
            result[key] = safe_int(entry.get("amount") or entry.get("value"))

    return result


def extract_scaling(entries: list[dict[str, Any]]) -> dict[str, str]:
    """Extract scaling grades with defaults."""

    result = SCALING_DEFAULT.copy()

    for entry in entries:
        name = str(entry.get("name", "")).strip().lower()
        key = SCALING_KEY_MAP.get(name)
        if key:
            scaling = entry.get("scaling")
            if scaling is None:
                continue
            result[key] = str(scaling).strip().upper() or "-"

    return result


def normalize_weapon_type(raw_value: str | None) -> str:
    """Normalize arbitrary weapon categories into canonical schema choices."""

    if not raw_value:
        return "other"

    cleaned = (
        raw_value.lower()
        .replace("-", " ")
        .replace("_", " ")
        .replace("/", " ")
        .strip()
    )

    if cleaned.endswith("s") and cleaned[:-1] in WEAPON_TYPE_MAPPING:
        cleaned = cleaned[:-1]

    return WEAPON_TYPE_MAPPING.get(
        cleaned,
        WEAPON_TYPE_MAPPING.get(cleaned.rstrip("s"), "other"),
    )


def serialize_payload(payload: dict[str, Any]) -> str:
    """Serialize arbitrary dict payloads for provenance logging."""

    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def extract_armor_damage(
    entries: list[dict[str, Any]]
) -> dict[str, float | None]:
    """Map structured damage negation entries to canonical armor columns."""

    result: dict[str, float | None] = {
        value: None for value in ARMOR_DAMAGE_KEY_MAP.values()
    }

    for entry in entries:
        if "name" in entry:
            candidates = [(entry.get("name"), entry.get("amount"))]
        else:
            candidates = list(entry.items())

        for raw_key, raw_value in candidates:
            name = str(raw_key or "").strip().lower()
            key = ARMOR_DAMAGE_KEY_MAP.get(name)
            if key is None:
                continue
            result[key] = safe_float(raw_value)

    return result


def extract_armor_resistances(
    entries: list[dict[str, Any]]
) -> dict[str, float | int | None]:
    """Map resistance entries to canonical armor resistance columns."""

    result: dict[str, float | int | None] = {
        value: None for value in ARMOR_RESISTANCE_KEY_MAP.values()
    }

    for entry in entries:
        if "name" in entry:
            candidates = [(entry.get("name"), entry.get("amount"))]
        else:
            candidates = list(entry.items())

        for raw_key, raw_value in candidates:
            name = str(raw_key or "").strip().lower()
            key = ARMOR_RESISTANCE_KEY_MAP.get(name)
            if key is None:
                continue
            if key == "poise":
                result[key] = safe_float(raw_value)
            else:
                int_value = safe_int(raw_value)
                result[key] = int_value

    return result


def normalize_armor_type(raw_value: str | None) -> str:
    """Normalize free-form armor categories to schema choices."""

    if not raw_value:
        return "other"

    cleaned = raw_value.strip().lower()
    if "helm" in cleaned or "head" in cleaned:
        return "head"
    if any(token in cleaned for token in ("gauntlet", "glove", "arm")):
        return "arms"
    if any(token in cleaned for token in ("leg", "greave", "boot")):
        return "legs"
    if any(token in cleaned for token in ("chest", "armor", "robe", "body")):
        return "chest"
    return "other"


def normalize_item_category(
    raw_value: str | None,
    hint: str | None = None,
) -> str:
    """Normalize item categorization across disparate sources."""

    candidates = [raw_value, hint]
    for candidate in candidates:
        if not candidate:
            continue
        lowered = candidate.strip().lower()
        normalized = lowered.replace("-", " ").replace("_", " ")
        normalized = " ".join(normalized.split())
        if normalized in ITEM_CATEGORY_ALIASES:
            return ITEM_CATEGORY_ALIASES[normalized]
        for key, value in ITEM_CATEGORY_ALIASES.items():
            if key in normalized or normalized in key:
                return value
    return "other"


def normalize_rarity(raw_value: Any) -> str | None:
    """Normalize rarity values to schema-approved labels."""

    if raw_value is None:
        return None

    text = str(raw_value).strip().lower()
    if not text:
        return None

    return text if text in RARITY_CHOICES else None


def stringify_structured_field(value: Any) -> str | None:
    """Best-effort conversion of nested data into readable text."""

    if value is None:
        return None

    if isinstance(value, str) and not value.strip():
        return None

    data = parse_structured_data(value)

    if data is None:
        text = str(value).strip()
        return text or None

    if isinstance(data, list):
        parts: list[str] = []
        for entry in data:
            if isinstance(entry, dict):
                name = entry.get("name")
                amount = entry.get("amount") or entry.get("value")
                if name is not None and amount is not None:
                    parts.append(f"{name}: {amount}")
                else:
                    parts.append(json.dumps(entry, ensure_ascii=False))
            else:
                parts.append(str(entry))
        joined = "; ".join(part for part in parts if part)
        return joined or None

    if isinstance(data, dict):
        parts = []
        for key, raw in data.items():
            key_text = str(key).strip()
            if isinstance(raw, list | dict):
                formatted = stringify_structured_field(raw)
            else:
                formatted = str(raw).strip()
            if formatted:
                parts.append(f"{key_text}: {formatted}")
            else:
                parts.append(key_text)
        joined = "; ".join(part for part in parts if part)
        return joined or None

    text = str(data).strip()
    return text or None


def build_item_record(
    *,
    name: str,
    description: str | None,
    category_raw: str | None = None,
    category_hint: str | None = None,
    weight: Any = None,
    sell_price: Any = None,
    max_stack: Any = None,
    rarity: Any = None,
    effect: str | None = None,
    obtained_from: str | None = None,
    is_dlc: bool,
    source: str,
    source_id: str,
    source_priority: int,
    payload: dict[str, Any],
    extra_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create a normalized item record with shared metadata fields."""

    record: dict[str, Any] = {
        "name": name.strip(),
        "category": normalize_item_category(category_raw, hint=category_hint),
        "description": description or None,
        "weight": safe_float(weight),
        "sell_price": safe_int(sell_price),
        "max_stack": safe_int(max_stack),
        "rarity": normalize_rarity(rarity),
        "effect": effect or None,
        "obtained_from": obtained_from or None,
        "is_dlc": is_dlc,
        "source": source,
        "source_id": source_id,
        "source_priority": source_priority,
        "source_payload": serialize_payload(payload),
    }

    if extra_fields:
        record.update(extra_fields)

    return record


def build_boss_record(
    *,
    name: str,
    description: str | None,
    region: str | None,
    location: str | None,
    drops: Any,
    health_points: Any,
    quote: str | None,
    is_dlc: bool,
    source: str,
    source_id: str,
    source_priority: int,
    payload: dict[str, Any],
    extra_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create a normalized boss record."""

    record: dict[str, Any] = {
        "name": name.strip(),
        "description": description or None,
        "region": region or None,
        "location": location or None,
        "drops": stringify_structured_field(drops),
        "health_points": safe_int(health_points),
        "quote": quote or None,
        "is_dlc": is_dlc,
        "source": source,
        "source_id": source_id,
        "source_priority": source_priority,
        "source_payload": serialize_payload(payload),
    }

    if extra_fields:
        record.update(extra_fields)

    return record


def build_armor_record(
    *,
    name: str,
    description: str | None,
    armor_type: str | None,
    weight: Any,
    damage_entries: list[dict[str, Any]],
    resistance_entries: list[dict[str, Any]],
    is_dlc: bool,
    source: str,
    source_id: str,
    source_priority: int,
    payload: dict[str, Any],
    extra_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create a normalized armor record."""

    record: dict[str, Any] = {
        "name": name.strip(),
        "description": description or None,
        "armor_type": normalize_armor_type(armor_type),
        "weight": safe_float(weight),
        "is_dlc": is_dlc,
        "source": source,
        "source_id": source_id,
        "source_priority": source_priority,
        "source_payload": serialize_payload(payload),
    }
    record.update(extract_armor_damage(damage_entries))
    record.update(extract_armor_resistances(resistance_entries))

    if extra_fields:
        record.update(extra_fields)

    return record


def build_spell_record(
    *,
    name: str,
    description: str | None,
    spell_type: str | None,
    fp_cost: Any,
    stamina_cost: Any,
    slots_required: Any,
    required_int: Any,
    required_fai: Any,
    required_arc: Any,
    is_dlc: bool,
    source: str,
    source_id: str,
    source_priority: int,
    payload: dict[str, Any],
    extra_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create a normalized spell record."""

    spell_type_clean = str(spell_type or "").strip().lower()
    if spell_type_clean not in {"sorcery", "incantation"}:
        spell_type_clean = "incantation"

    record: dict[str, Any] = {
        "name": name.strip(),
        "description": description or None,
        "spell_type": spell_type_clean,
        "fp_cost": safe_int(fp_cost) or 0,
        "stamina_cost": safe_int(stamina_cost),
        "slots_required": safe_int(slots_required) or 1,
        "required_int": safe_int(required_int),
        "required_fai": safe_int(required_fai),
        "required_arc": safe_int(required_arc),
        "is_dlc": is_dlc,
        "source": source,
        "source_id": source_id,
        "source_priority": source_priority,
        "source_payload": serialize_payload(payload),
    }

    if extra_fields:
        record.update(extra_fields)

    return record
