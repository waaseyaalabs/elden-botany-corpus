"""Tests for Impalers Archive DLC text dump ingestion."""

import pytest

from corpus.models import normalize_name_for_matching


def test_normalize_name_for_matching():
    """Test name normalization for fuzzy matching."""
    assert normalize_name_for_matching("Messmer the Impaler") == "messmer the impaler"
    assert normalize_name_for_matching("Bayle, the Dread") == "bayle the dread"
    assert normalize_name_for_matching("Rellana - Twin Moon Knight") == "rellana twin moon knight"


@pytest.mark.skip(reason="Module implementation pending")
def test_impalers_ingester_init():
    """Test ImpalersIngester initialization."""
    # TODO: Implement when module is ready
    pass


@pytest.mark.skip(reason="Requires network access to GitHub - integration test")
def test_download_impalers_archive():
    """Test downloading Impalers Archive."""
    # TODO: Implement integration test with mock HTML data
    pass


@pytest.mark.skip(reason="Requires test fixture HTML data")
def test_parse_html_text_dump():
    """Test parsing HTML text dump."""
    # TODO: Create fixture HTML and test parsing logic
    pass


@pytest.mark.skip(reason="Requires test fixture data")
def test_extract_entity_names_from_text():
    """Test entity name extraction from text dump."""
    # TODO: Test extraction of boss names, item names, etc.
    pass


@pytest.mark.skip(reason="Module implementation pending")
def test_fetch_impalers_data():
    """Test complete Impalers data fetch pipeline."""
    # TODO: Implement when module is ready
    pass


@pytest.mark.skip(reason="Requires fixture data")
def test_filter_dlc_content():
    """Test filtering DLC-specific content from base game text."""
    # TODO: Test that only DLC content is extracted
    pass
