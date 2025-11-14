"""Tests for Impalers Archive DLC text dump ingestion."""

import os

import pytest

from corpus.models import normalize_name_for_matching


def test_normalize_name_for_matching() -> None:
    """Test name normalization for fuzzy matching."""
    assert normalize_name_for_matching("Messmer the Impaler") == "messmer the impaler"
    assert normalize_name_for_matching("Bayle, the Dread") == "bayle the dread"
    assert normalize_name_for_matching("Rellana - Twin Moon Knight") == "rellana twin moon knight"


@pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION") != "1",
    reason="Integration tests disabled (set RUN_INTEGRATION=1 to enable)",
)
def test_impalers_ingester_init() -> None:
    """Test ImpalersIngester initialization."""
    # TODO: Implement with fixture data
    pass


@pytest.mark.integration
@pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION") != "1",
    reason="Integration tests disabled (set RUN_INTEGRATION=1 to enable)",
)
def test_download_impalers_archive() -> None:
    """Test downloading Impalers Archive."""
    # TODO: Implement integration test with mock HTML data
    pass


@pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION") != "1",
    reason="Integration tests disabled (set RUN_INTEGRATION=1 to enable)",
)
def test_parse_html_text_dump() -> None:
    """Test parsing HTML text dump."""
    # TODO: Create fixture HTML and test parsing logic
    pass


@pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION") != "1",
    reason="Integration tests disabled (set RUN_INTEGRATION=1 to enable)",
)
def test_extract_entity_names_from_text() -> None:
    """Test entity name extraction from text dump."""
    # TODO: Test extraction of boss names, item names, etc.
    pass


@pytest.mark.integration
@pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION") != "1",
    reason="Integration tests disabled (set RUN_INTEGRATION=1 to enable)",
)
def test_fetch_impalers_data() -> None:
    """Test complete Impalers data fetch pipeline."""
    # TODO: Implement integration test with network access or mock
    pass


@pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION") != "1",
    reason="Integration tests disabled (set RUN_INTEGRATION=1 to enable)",
)
def test_filter_dlc_content() -> None:
    """Test filtering DLC-specific content from base game text."""
    # TODO: Test that only DLC content is extracted
    pass
