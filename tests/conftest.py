"""Test configuration and fixtures."""


import pytest


@pytest.fixture
def sample_entity_data():
    """Sample entity data for testing."""
    return {
        "name": "Sword of Night and Flame",
        "description": "Legendary armament of Carian royalty.",
        "entity_type": "weapon",
        "is_dlc": False,
        "stats": {
            "attack": 100,
            "magic": 50,
        }
    }


@pytest.fixture
def sample_boss_data():
    """Sample boss data."""
    return {
        "name": "Rennala, Queen of the Full Moon",
        "description": "Boss of the Academy of Raya Lucaria",
        "entity_type": "boss",
        "is_dlc": False,
        "hp": 3500,
    }


@pytest.fixture
def sample_dlc_entity():
    """Sample DLC entity."""
    return {
        "name": "Messmer the Impaler",
        "description": "DLC boss from Shadow of the Erdtree",
        "entity_type": "boss",
        "is_dlc": True,
    }


@pytest.fixture
def temp_data_dir(tmp_path):
    """Temporary data directory."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "raw").mkdir()
    (data_dir / "curated").mkdir()
    return data_dir
