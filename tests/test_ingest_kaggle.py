"""Tests for Kaggle ingestion module."""

import pytest

from corpus.ingest_kaggle import KaggleIngester


def test_kaggle_ingester_init():
    """Test KaggleIngester initialization."""
    ingester = KaggleIngester()
    assert ingester.base_dir.exists()
    assert ingester.base_dir.name == "kaggle"


def test_extract_name():
    """Test name extraction from various row formats."""
    ingester = KaggleIngester()

    # Test with 'name' field
    row = {"name": "Test Weapon", "other": "data"}
    assert ingester._extract_name(row) == "Test Weapon"

    # Test with 'item_name' field
    row = {"item_name": "Test Item"}
    assert ingester._extract_name(row) == "Test Item"

    # Test with 'boss_name' field
    row = {"boss_name": "Test Boss"}
    assert ingester._extract_name(row) == "Test Boss"

    # Test with no name fields
    row = {"description": "No name here"}
    assert ingester._extract_name(row) == ""


def test_extract_description():
    """Test description extraction and merging."""
    ingester = KaggleIngester()

    row = {
        "description": "A powerful weapon",
        "effect": "Deals fire damage",
        "location": "Found in Limgrave",
        "other_field": "ignored",
        "passive": None,
    }

    description = ingester._extract_description(row)

    assert "A powerful weapon" in description
    assert "Deals fire damage" in description
    assert "Found in Limgrave" in description
    assert "ignored" not in description


@pytest.mark.integration
@pytest.mark.skip(reason="Requires Kaggle API credentials - integration test")
def test_download_dataset():
    """Test dataset download (integration test)."""
    # TODO: Implement integration test with mock data or test dataset
    pass


@pytest.mark.integration
@pytest.mark.skip(reason="Requires Kaggle data - integration test")
def test_ingest_base_game():
    """Test base game ingestion (integration test)."""
    # TODO: Implement integration test with fixture data
    pass


@pytest.mark.integration
@pytest.mark.skip(reason="Requires Kaggle data - integration test")
def test_ingest_dlc():
    """Test DLC ingestion (integration test)."""
    # TODO: Implement integration test with fixture data
    pass


@pytest.mark.integration
@pytest.mark.skip(reason="Requires Kaggle API credentials")
def test_fetch_kaggle_data():
    """Test complete Kaggle data fetch pipeline."""
    # TODO: Add integration test when credentials available
    pass
