"""Test configuration and fixtures."""

import os
from pathlib import Path

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


@pytest.fixture(scope="session")
def postgres_dsn():
    """
    PostgreSQL connection string for integration tests.

    Returns DSN from environment variable POSTGRES_DSN, or None if not set.
    Tests requiring this fixture should be marked with @pytest.mark.integration
    and will be skipped if RUN_INTEGRATION != "1".
    """
    return os.getenv("POSTGRES_DSN")


@pytest.fixture(scope="session")
def pg_connection(postgres_dsn):
    """
    PostgreSQL connection for integration tests.

    Requires POSTGRES_DSN environment variable.
    Optional: Use testcontainers for ephemeral database.
    """
    if not postgres_dsn:
        pytest.skip("POSTGRES_DSN not set - skipping database tests")

    psycopg = pytest.importorskip("psycopg")

    # Create connection
    conn = psycopg.connect(postgres_dsn)

    # Initialize schema from SQL files
    sql_dir = Path(__file__).parent.parent / "sql"
    with conn.cursor() as cur:
        # Load extensions
        extensions_sql = (sql_dir / "001_enable_extensions.sql").read_text()
        cur.execute(extensions_sql)

        # Create schema
        schema_sql = (sql_dir / "010_schema.sql").read_text()
        cur.execute(schema_sql)

        # Create indexes
        indexes_sql = (sql_dir / "020_indexes.sql").read_text()
        cur.execute(indexes_sql)

        conn.commit()

    yield conn

    # Cleanup: drop schema
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS elden CASCADE")
        conn.commit()

    conn.close()


@pytest.fixture
def pg_cursor(pg_connection):
    """PostgreSQL cursor for integration tests."""
    cursor = pg_connection.cursor()
    yield cursor
    cursor.close()
    pg_connection.rollback()  # Rollback any uncommitted changes
