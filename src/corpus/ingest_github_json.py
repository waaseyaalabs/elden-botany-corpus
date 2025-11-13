"""GitHub API JSON ingestion module (fallback for base game)."""

import json
from typing import Any

import requests

from corpus.config import settings
from corpus.models import Provenance, RawEntity
from corpus.utils import compute_file_hash, progress_bar

# GitHub repository
GITHUB_REPO = "deliton/eldenring-api"
GITHUB_RAW_BASE = f"https://raw.githubusercontent.com/{GITHUB_REPO}/main"

# Entity types available in the API
API_ENTITIES = [
    "weapons",
    "armors",
    "shields",
    "ashes",
    "bosses",
    "classes",
    "creatures",
    "incantations",
    "items",
    "locations",
    "npcs",
    "sorceries",
    "spirits",
    "talismans",
]


class GitHubAPIIngester:
    """Ingest data from eldenring-api GitHub repository."""

    def __init__(self) -> None:
        self.base_dir = settings.raw_dir / "github_api"
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def fetch_entity_list(self, entity_type: str) -> dict[str, Any]:
        """
        Fetch entity list from GitHub API.

        Args:
            entity_type: Type of entity (e.g., 'weapons', 'bosses')

        Returns:
            JSON response as dictionary
        """
        url = f"{GITHUB_RAW_BASE}/data/{entity_type}.json"
        cache_file = self.base_dir / f"{entity_type}.json"

        # Use cache if exists
        if cache_file.exists():
            print(f"Loading {entity_type} from cache...")
            with open(cache_file, encoding="utf-8") as f:
                result: dict[str, Any] = json.load(f)
                return result

        print(f"Fetching {entity_type} from {url}...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        data: dict[str, Any] = response.json()

        # Save to cache
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        return data

    def ingest_all(self) -> list[RawEntity]:
        """
        Ingest all available entity types.

        Returns:
            List of RawEntity objects
        """
        entities: list[RawEntity] = []

        for entity_type in progress_bar(
            API_ENTITIES, desc="Fetching entity types"
        ):
            try:
                data = self.fetch_entity_list(entity_type)

                # Handle different response formats
                if isinstance(data, dict):
                    items = data.get("data", [])
                elif isinstance(data, list):
                    items = data
                else:
                    print(f"Unexpected format for {entity_type}: {type(data)}")
                    continue

                # Create provenance
                cache_file = self.base_dir / f"{entity_type}.json"
                provenance = Provenance(
                    source="github_api",
                    uri=f"{GITHUB_RAW_BASE}/data/{entity_type}.json",
                    sha256=(
                        compute_file_hash(cache_file)
                        if cache_file.exists()
                        else None
                    ),
                )

                # Convert to RawEntity
                singular = entity_type.rstrip("s")
                for item in items:
                    name = item.get("name", "")
                    if not name:
                        continue

                    description = self._extract_description(item)

                    entities.append(
                        RawEntity(
                            entity_type=singular,
                            name=name,
                            is_dlc=False,
                            description=description,
                            raw_data=item,
                            provenance=[provenance],
                        )
                    )

            except Exception as e:
                print(f"Error fetching {entity_type}: {e}")
                continue

        print(f"\nTotal entities from GitHub API: {len(entities)}")
        return entities

    def _extract_description(self, item: dict[str, Any]) -> str:
        """Extract description from API item."""
        parts = []

        # Common description fields
        for field in ["description", "effect", "passive"]:
            if field in item and item[field]:
                parts.append(str(item[field]))

        # Nested fields
        if "stats" in item and item["stats"]:
            stats_text = self._format_stats(item["stats"])
            if stats_text:
                parts.append(f"Stats: {stats_text}")

        if "location" in item and item["location"]:
            parts.append(f"Location: {item['location']}")

        return "\n\n".join(parts)

    def _format_stats(self, stats: dict[str, Any]) -> str:
        """Format stats dictionary into readable text."""
        if not isinstance(stats, dict):
            return str(stats)

        # Format key-value pairs
        formatted = []
        for key, value in stats.items():
            if value is not None:
                formatted.append(f"{key}: {value}")

        return ", ".join(formatted)


def fetch_github_api_data() -> list[RawEntity]:
    """
    Fetch data from GitHub API as fallback.

    Returns:
        List of RawEntity objects
    """
    print("\n=== Ingesting GitHub API Data ===")
    ingester = GitHubAPIIngester()
    return ingester.ingest_all()
