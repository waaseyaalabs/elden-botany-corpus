"""Impalers Archive DLC text dump ingestion."""

from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

from corpus.config import settings
from corpus.models import Provenance, RawEntity
from corpus.utils import compute_file_hash

# Impalers Archive repository
IMPALERS_REPO = "ividyon/Impalers-Archive"
MASTER_HTML_URL = f"https://raw.githubusercontent.com/{IMPALERS_REPO}/main/Master.html"


class ImpalersIngester:
    """Ingest DLC text dump from Impalers Archive."""

    def __init__(self) -> None:
        self.base_dir = settings.raw_dir / "impalers"
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def fetch_master_html(self) -> Path:
        """
        Download Master.html from Impalers Archive.

        Returns:
            Path to downloaded file
        """
        cache_file = self.base_dir / "Master.html"

        # Use cache if exists
        if cache_file.exists():
            print("Using cached Master.html from Impalers Archive")
            return cache_file

        print(f"Downloading Master.html from {MASTER_HTML_URL}...")
        response = requests.get(MASTER_HTML_URL, timeout=30)
        response.raise_for_status()

        cache_file.write_text(response.text, encoding="utf-8")
        print(f"Downloaded Master.html ({len(response.text)} bytes)")

        return cache_file

    def parse_master_html(self, html_path: Path) -> list[dict[str, Any]]:
        """
        Parse Master.html into structured text records.

        Args:
            html_path: Path to Master.html

        Returns:
            List of text records with name, text, section
        """
        with open(html_path, encoding="utf-8") as f:
            soup = BeautifulSoup(f, "html.parser")

        records: list[dict[str, Any]] = []

        # Parse structure - this depends on actual HTML format
        # The Impalers Archive typically has sections with headers and text
        # This is a generic parser that can be adjusted

        current_section = "Unknown"

        # Find all major sections (h1, h2, h3)
        for header in soup.find_all(["h1", "h2", "h3"]):
            section_name = header.get_text(strip=True)
            if section_name:
                current_section = section_name

        # Find text blocks (this varies by actual format)
        # Common patterns: divs with class, paragraphs, tables

        # Strategy 1: Look for tables with name/description columns
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            for row in rows[1:]:  # Skip header
                cells = row.find_all(["td", "th"])
                if len(cells) >= 2:
                    name = cells[0].get_text(strip=True)
                    text = cells[1].get_text(strip=True)

                    if name and text and len(text) > 10:
                        records.append(
                            {
                                "name": name,
                                "text": text,
                                "section": current_section,
                                "language": "en",
                            }
                        )

        # Strategy 2: Look for definition lists (dt/dd pairs)
        for dl in soup.find_all("dl"):
            terms = dl.find_all("dt")
            definitions = dl.find_all("dd")

            for term, definition in zip(terms, definitions, strict=True):
                name = term.get_text(strip=True)
                text = definition.get_text(strip=True)

                if name and text and len(text) > 10:
                    records.append(
                        {
                            "name": name,
                            "text": text,
                            "section": current_section,
                            "language": "en",
                        }
                    )

        # Strategy 3: Look for heading + paragraph pairs
        for heading in soup.find_all(["h4", "h5", "h6"]):
            name = heading.get_text(strip=True)
            # Get next sibling paragraphs
            text_parts = []
            for sibling in heading.find_next_siblings():
                if sibling.name in ["h1", "h2", "h3", "h4", "h5", "h6"]:
                    break
                if sibling.name == "p":
                    text_parts.append(sibling.get_text(strip=True))

            if name and text_parts:
                text = "\n\n".join(text_parts)
                if len(text) > 10:
                    records.append(
                        {
                            "name": name,
                            "text": text,
                            "section": current_section,
                            "language": "en",
                        }
                    )

        print(f"Parsed {len(records)} text records from Master.html")
        return records

    def ingest(self) -> list[RawEntity]:
        """
        Ingest DLC text dump.

        Returns:
            List of RawEntity objects (unmapped text)
        """
        html_path = self.fetch_master_html()
        records = self.parse_master_html(html_path)

        # Create provenance
        provenance = Provenance(
            source="dlc_textdump",
            dataset=IMPALERS_REPO,
            source_file=html_path.name,
            uri=MASTER_HTML_URL,
            sha256=compute_file_hash(html_path),
        )

        entities: list[RawEntity] = []

        for record in records:
            # These are raw text snippets without clear entity type
            # They'll be matched to entities in the reconciliation phase
            entities.append(
                RawEntity(
                    entity_type="text_snippet",
                    name=record["name"],
                    is_dlc=True,
                    description=record["text"],
                    raw_data=record,
                    provenance=[provenance],
                )
            )

        print(f"\nTotal DLC text snippets: {len(entities)}")
        return entities


def fetch_impalers_data() -> list[RawEntity]:
    """
    Fetch DLC text dump from Impalers Archive.

    Returns:
        List of RawEntity objects (text snippets)
    """
    print("\n=== Ingesting Impalers Archive DLC Text ===")
    ingester = ImpalersIngester()
    return ingester.ingest()
