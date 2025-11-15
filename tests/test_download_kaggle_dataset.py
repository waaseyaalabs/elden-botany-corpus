"""
Tests for scripts/download_kaggle_dataset.py

Validates configuration parsing, directory handling, and Kaggle API integration
with mocked API calls.
"""

import tempfile
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
import yaml


# Mock kaggle module before importing download_kaggle_dataset
@pytest.fixture(autouse=True)
def mock_kaggle_module():
    """Mock the kaggle module for all tests."""
    with patch.dict("sys.modules", {"kaggle": MagicMock()}):
        with patch.dict(
            "sys.modules", {"kaggle.api": MagicMock()}
        ):
            with patch.dict(
                "sys.modules",
                {"kaggle.api.kaggle_api_extended": MagicMock()},
            ):
                yield


@pytest.fixture
def sample_config():
    """Sample Kaggle datasets configuration."""
    return {
        "datasets": [
            {
                "name": "test-dataset-1",
                "owner": "test-owner",
                "slug": "test-dataset",
                "description": "Test dataset",
                "files": ["data.csv", "metadata.json"],
                "enabled": True,
            },
            {
                "name": "test-dataset-2",
                "owner": "test-owner-2",
                "slug": "another-dataset",
                "description": "Another test dataset",
                "files": [],
                "enabled": True,
            },
            {
                "name": "disabled-dataset",
                "owner": "test-owner",
                "slug": "disabled",
                "description": "Disabled dataset",
                "enabled": False,
            },
        ],
        "settings": {
            "skip_disabled": True,
            "verify_integrity": True,
            "download_timeout": 300,
            "auto_unzip": True,
        },
    }


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def config_file(temp_dir, sample_config):
    """Create a temporary config file."""
    config_path = temp_dir / "config.yml"
    with open(config_path, "w") as f:
        yaml.dump(sample_config, f)
    return config_path


@pytest.fixture
def mock_kaggle_api():
    """Mock Kaggle API object."""
    api = Mock()
    api.authenticate = Mock()
    api.dataset_download_file = Mock()
    api.dataset_download_files = Mock()
    return api


class TestKaggleDatasetDownloader:
    """Test suite for KaggleDatasetDownloader class."""

    def test_load_config_valid(self, config_file, temp_dir):
        """Test loading a valid configuration file."""
        from scripts.download_kaggle_dataset import (
            KaggleDatasetDownloader,
        )

        with patch(
            "scripts.download_kaggle_dataset.KaggleApi"
        ) as MockApi:
            MockApi.return_value = Mock()
            downloader = KaggleDatasetDownloader(
                config_file, temp_dir, force=False
            )
            downloader.load_config()

            assert "datasets" in downloader.config
            assert len(downloader.config["datasets"]) == 3
            assert downloader.config["settings"]["auto_unzip"] is True

    def test_load_config_missing_file(self, temp_dir):
        """Test loading a non-existent configuration file."""
        from scripts.download_kaggle_dataset import (
            KaggleDatasetDownloader,
        )

        missing_config = temp_dir / "missing.yml"

        with patch(
            "scripts.download_kaggle_dataset.KaggleApi"
        ) as MockApi:
            MockApi.return_value = Mock()
            downloader = KaggleDatasetDownloader(
                missing_config, temp_dir, force=False
            )

            with pytest.raises(FileNotFoundError):
                downloader.load_config()

    def test_load_config_invalid_yaml(self, temp_dir):
        """Test loading an invalid YAML file."""
        from scripts.download_kaggle_dataset import (
            KaggleDatasetDownloader,
        )

        invalid_config = temp_dir / "invalid.yml"
        with open(invalid_config, "w") as f:
            f.write("datasets:\n  - name: test\n    invalid yaml {{")

        with patch(
            "scripts.download_kaggle_dataset.KaggleApi"
        ) as MockApi:
            MockApi.return_value = Mock()
            downloader = KaggleDatasetDownloader(
                invalid_config, temp_dir, force=False
            )

            with pytest.raises(yaml.YAMLError):
                downloader.load_config()

    def test_authenticate_success(self, config_file, temp_dir):
        """Test successful Kaggle API authentication."""
        from scripts.download_kaggle_dataset import (
            KaggleDatasetDownloader,
        )

        with patch(
            "scripts.download_kaggle_dataset.KaggleApi"
        ) as MockApi:
            mock_api = Mock()
            mock_api.authenticate = Mock()
            MockApi.return_value = mock_api

            downloader = KaggleDatasetDownloader(
                config_file, temp_dir, force=False
            )
            downloader.authenticate()

            mock_api.authenticate.assert_called_once()

    def test_authenticate_failure(self, config_file, temp_dir):
        """Test failed Kaggle API authentication."""
        from scripts.download_kaggle_dataset import (
            KaggleDatasetDownloader,
        )

        with patch(
            "scripts.download_kaggle_dataset.KaggleApi"
        ) as MockApi:
            mock_api = Mock()
            mock_api.authenticate = Mock(
                side_effect=Exception("Auth failed")
            )
            MockApi.return_value = mock_api

            downloader = KaggleDatasetDownloader(
                config_file, temp_dir, force=False
            )

            with pytest.raises(Exception, match="Auth failed"):
                downloader.authenticate()

    def test_download_specific_files(
        self, config_file, temp_dir, sample_config
    ):
        """Test downloading specific files from a dataset."""
        from scripts.download_kaggle_dataset import (
            KaggleDatasetDownloader,
        )

        with patch(
            "scripts.download_kaggle_dataset.KaggleApi"
        ) as MockApi:
            mock_api = Mock()
            mock_api.authenticate = Mock()
            mock_api.dataset_download_file = Mock()
            MockApi.return_value = mock_api

            downloader = KaggleDatasetDownloader(
                config_file, temp_dir, force=False
            )
            downloader.load_config()

            dataset_config = sample_config["datasets"][0]
            downloader.download_dataset(dataset_config)

            # Should download 2 files
            assert mock_api.dataset_download_file.call_count == 2

    def test_download_all_files(
        self, config_file, temp_dir, sample_config
    ):
        """Test downloading all files from a dataset."""
        from scripts.download_kaggle_dataset import (
            KaggleDatasetDownloader,
        )

        with patch(
            "scripts.download_kaggle_dataset.KaggleApi"
        ) as MockApi:
            mock_api = Mock()
            mock_api.authenticate = Mock()
            mock_api.dataset_download_files = Mock()
            MockApi.return_value = mock_api

            downloader = KaggleDatasetDownloader(
                config_file, temp_dir, force=False
            )
            downloader.load_config()

            # Dataset with empty files list = download all
            dataset_config = sample_config["datasets"][1]
            downloader.download_dataset(dataset_config)

            mock_api.dataset_download_files.assert_called_once()

    def test_skip_disabled_dataset(
        self, config_file, temp_dir, sample_config
    ):
        """Test that disabled datasets are skipped."""
        from scripts.download_kaggle_dataset import (
            KaggleDatasetDownloader,
        )

        with patch(
            "scripts.download_kaggle_dataset.KaggleApi"
        ) as MockApi:
            mock_api = Mock()
            mock_api.authenticate = Mock()
            mock_api.dataset_download_file = Mock()
            mock_api.dataset_download_files = Mock()
            MockApi.return_value = mock_api

            downloader = KaggleDatasetDownloader(
                config_file, temp_dir, force=False
            )
            downloader.load_config()

            # Dataset with enabled=False
            dataset_config = sample_config["datasets"][2]
            downloader.download_dataset(dataset_config)

            # Should not call any download methods
            mock_api.dataset_download_file.assert_not_called()
            mock_api.dataset_download_files.assert_not_called()

    def test_force_redownload(self, config_file, temp_dir, sample_config):
        """Test force re-download overwrites existing files."""
        from scripts.download_kaggle_dataset import (
            KaggleDatasetDownloader,
        )

        # Create existing file
        dataset_dir = temp_dir / "test-dataset-1"
        dataset_dir.mkdir(parents=True)
        existing_file = dataset_dir / "data.csv"
        existing_file.write_text("old data")

        with patch(
            "scripts.download_kaggle_dataset.KaggleApi"
        ) as MockApi:
            mock_api = Mock()
            mock_api.authenticate = Mock()
            mock_api.dataset_download_file = Mock()
            MockApi.return_value = mock_api

            downloader = KaggleDatasetDownloader(
                config_file, temp_dir, force=True
            )
            downloader.load_config()

            dataset_config = sample_config["datasets"][0]
            downloader.download_dataset(dataset_config)

            # Should download even though file exists
            assert mock_api.dataset_download_file.call_count == 2

    def test_skip_existing_files_without_force(
        self, config_file, temp_dir, sample_config
    ):
        """Test that existing files are skipped without --force."""
        from scripts.download_kaggle_dataset import (
            KaggleDatasetDownloader,
        )

        # Create existing file
        dataset_dir = temp_dir / "test-dataset-1"
        dataset_dir.mkdir(parents=True)
        existing_file = dataset_dir / "data.csv"
        existing_file.write_text("existing data")

        with patch(
            "scripts.download_kaggle_dataset.KaggleApi"
        ) as MockApi:
            mock_api = Mock()
            mock_api.authenticate = Mock()
            mock_api.dataset_download_file = Mock()
            MockApi.return_value = mock_api

            downloader = KaggleDatasetDownloader(
                config_file, temp_dir, force=False
            )
            downloader.load_config()

            dataset_config = sample_config["datasets"][0]
            downloader.download_dataset(dataset_config)

            # Should only download metadata.json, skip data.csv
            assert mock_api.dataset_download_file.call_count == 1

    def test_unzip_files(self, temp_dir):
        """Test auto-unzip functionality."""
        from scripts.download_kaggle_dataset import (
            KaggleDatasetDownloader,
        )

        # Create a test zip file
        zip_path = temp_dir / "test.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("file1.txt", "content1")
            zf.writestr("file2.csv", "content2")

        with patch(
            "scripts.download_kaggle_dataset.KaggleApi"
        ) as MockApi:
            MockApi.return_value = Mock()
            downloader = KaggleDatasetDownloader(
                temp_dir / "config.yml", temp_dir, force=False
            )
            downloader._unzip_files(temp_dir)

            # Check that files were extracted
            assert (temp_dir / "file1.txt").exists()
            assert (temp_dir / "file2.csv").exists()

    def test_dataset_exists_check(self, temp_dir):
        """Test checking if dataset already exists."""
        from scripts.download_kaggle_dataset import (
            KaggleDatasetDownloader,
        )

        with patch(
            "scripts.download_kaggle_dataset.KaggleApi"
        ) as MockApi:
            MockApi.return_value = Mock()
            downloader = KaggleDatasetDownloader(
                temp_dir / "config.yml", temp_dir, force=False
            )

            # Non-existent directory
            assert not downloader._dataset_exists(temp_dir / "nonexistent")

            # Empty directory
            empty_dir = temp_dir / "empty"
            empty_dir.mkdir()
            assert not downloader._dataset_exists(empty_dir)

            # Directory with files
            file_dir = temp_dir / "with_files"
            file_dir.mkdir()
            (file_dir / "data.csv").write_text("data")
            assert downloader._dataset_exists(file_dir)

    def test_missing_owner_or_slug(self, config_file, temp_dir):
        """Test handling datasets with missing owner or slug."""
        from scripts.download_kaggle_dataset import (
            KaggleDatasetDownloader,
        )

        invalid_dataset = {
            "name": "invalid",
            "owner": "",  # Missing owner
            "slug": "test-slug",
            "enabled": True,
        }

        with patch(
            "scripts.download_kaggle_dataset.KaggleApi"
        ) as MockApi:
            mock_api = Mock()
            MockApi.return_value = mock_api

            downloader = KaggleDatasetDownloader(
                config_file, temp_dir, force=False
            )
            downloader.load_config()

            # Should log warning and skip
            downloader.download_dataset(invalid_dataset)

            # No API calls should be made
            mock_api.dataset_download_file.assert_not_called()
            mock_api.dataset_download_files.assert_not_called()
