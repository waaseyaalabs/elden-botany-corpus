"""Fallback ingester pulling JSON from the Elden Ring fan API."""

import json
from typing import Any

import requests

from corpus.config import settings
from corpus.models import Provenance, RawEntity
from corpus.utils import compute_file_hash, progress_bar

# Elden Ring fan API (formerly mirrored via GitHub)
ELDEN_RING_API_BASE = "https://eldenring.fanapis.com/api"
API_PAGE_SIZE = 100
API_TIMEOUT_SECONDS = 30

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
    """Ingest Elden Ring reference data from the public fan API."""

    def __init__(self) -> None:
        self.base_dir = settings.raw_dir / "github_api"
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.page_size = API_PAGE_SIZE

    def fetch_entity_list(self, entity_type: str) -> dict[str, Any]:
        """
        Fetch entity list from GitHub API.

        Args:
            entity_type: Type of entity (e.g., 'weapons', 'bosses')

        Returns:
            JSON response as dictionary
        """
        url = f"{ELDEN_RING_API_BASE}/{entity_type}"
        cache_file = self.base_dir / f"{entity_type}.json"

        # Use cache if exists
        if cache_file.exists():
            print(f"Loading {entity_type} from cache...")
            with open(cache_file, encoding="utf-8") as f:
                result: dict[str, Any] = json.load(f)
                return result

        print(f"Fetching {entity_type} from {url} (paged)...")
        data = self._download_all_pages(url, entity_type)

        # Save to cache
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        return data

    def _download_all_pages(
        self, url: str, entity_type: str
    ) -> dict[str, Any]:
        """Download and combine every page for a given entity type."""

        aggregated: list[dict[str, Any]] = []
        total_items: int | None = None
        page = 0

        while True:
            params = {"limit": self.page_size, "page": page}
            response = requests.get(
                url,
                params=params,
                timeout=API_TIMEOUT_SECONDS,
            )
            response.raise_for_status()

            payload = response.json()
            items = payload.get("data") or []
            # API may return strings for counts; coerce to int when possible.
            page_total = payload.get("total")
            if page_total is not None:
                try:
                    total_items = int(page_total)
                except (TypeError, ValueError):
                    total_items = total_items or None

            aggregated.extend(items)

            count = payload.get("count")
            if count is not None:
                try:
                    count = int(count)
                except (TypeError, ValueError):
                    count = len(items)
            else:
                count = len(items)

            if not items:
                break

            if total_items is not None and len(aggregated) >= total_items:
                break

            if count < self.page_size:
                break

            page += 1

            if page > 1000:
                raise RuntimeError(
                    f"Exceeded pagination limit while fetching {entity_type}"
                )

        return {
            "success": True,
            "total": total_items or len(aggregated),
            "count": len(aggregated),
            "data": aggregated,
            "source": ELDEN_RING_API_BASE,
        }

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
                    uri=f"{ELDEN_RING_API_BASE}/{entity_type}",
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
