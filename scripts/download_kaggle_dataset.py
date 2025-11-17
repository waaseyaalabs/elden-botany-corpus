#!/usr/bin/env python3
"""
Download Kaggle datasets based on configuration file.

This script reads a YAML configuration file specifying Kaggle datasets,
downloads them using the Kaggle API, and organizes them in the
data/raw directory.

Usage:
    python -m scripts.download_kaggle_dataset \\
        --config config/kaggle_datasets.yml \\
        --output-dir data/raw

    # Force re-download even if files exist
    python -m scripts.download_kaggle_dataset \\
        --config config/kaggle_datasets.yml \\
        --output-dir data/raw \\
        --force

Prerequisites:
    - Kaggle API credentials configured at ~/.kaggle/kaggle.json
    - Or KAGGLE_USERNAME and KAGGLE_KEY environment variables set
"""

import argparse
import logging
import sys
import zipfile
from pathlib import Path
from typing import Any

import yaml

try:
    from kaggle.api.kaggle_api_extended import KaggleApi
except ImportError as e:
    raise ImportError("kaggle package not installed. Run: pip install kaggle") from e

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class KaggleDatasetDownloader:
    """Handles downloading and organizing Kaggle datasets."""

    def __init__(self, config_path: Path, output_dir: Path, force: bool = False):
        """
        Initialize the downloader.

        Args:
            config_path: Path to YAML configuration file
            output_dir: Base directory for downloaded datasets
            force: If True, re-download even if files exist
        """
        self.config_path = config_path
        self.output_dir = output_dir
        self.force = force
        self.api = KaggleApi()
        self.config: dict[str, Any] = {}

    def load_config(self) -> None:
        """Load and validate configuration file."""
        logger.info(f"Loading configuration from {self.config_path}")

        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")

        with open(self.config_path) as f:
            self.config = yaml.safe_load(f)

        if not self.config or "datasets" not in self.config:
            raise ValueError("Configuration must contain 'datasets' key")

        logger.info(f"Loaded {len(self.config['datasets'])} dataset(s) from config")

    def authenticate(self) -> None:
        """Authenticate with Kaggle API."""
        logger.info("Authenticating with Kaggle API")
        try:
            self.api.authenticate()
            logger.info("Successfully authenticated with Kaggle API")
        except Exception as e:
            logger.error(f"Failed to authenticate with Kaggle API: {e}")
            logger.error(
                "Ensure ~/.kaggle/kaggle.json exists or " "KAGGLE_USERNAME and KAGGLE_KEY are set"
            )
            raise

    def download_dataset(self, dataset_config: dict[str, Any]) -> None:
        """
        Download a single dataset based on configuration.

        Args:
            dataset_config: Dictionary with dataset configuration
        """
        name = dataset_config.get("name", "unnamed")
        owner = dataset_config.get("owner")
        slug = dataset_config.get("slug")
        enabled = dataset_config.get("enabled", True)
        files = dataset_config.get("files", [])

        # Check if dataset should be skipped
        settings = self.config.get("settings", {})
        if not enabled and settings.get("skip_disabled", True):
            logger.info(f"Skipping disabled dataset: {name}")
            return

        if not owner or not slug:
            logger.warning(f"Skipping dataset '{name}': missing owner or slug")
            return

        dataset_id = f"{owner}/{slug}"
        output_path = self.output_dir / name

        # Create output directory
        output_path.mkdir(parents=True, exist_ok=True)

        logger.info(f"Downloading dataset: {name} ({dataset_id})")
        logger.info(f"Output directory: {output_path}")

        try:
            if files:
                # Download specific files
                logger.info(f"Downloading {len(files)} specific file(s)")
                for file in files:
                    self._download_file(dataset_id, file, output_path)
            else:
                # Download entire dataset
                logger.info("Downloading all dataset files")
                self._download_all_files(dataset_id, output_path)

            # Auto-unzip if configured
            if settings.get("auto_unzip", True):
                self._unzip_files(output_path)

            logger.info(f"Successfully downloaded dataset: {name}")

        except Exception as e:
            logger.error(f"Failed to download dataset '{name}': {e}")
            raise

    def _download_file(self, dataset_id: str, file_name: str, output_path: Path) -> None:
        """
        Download a specific file from a dataset.

        Args:
            dataset_id: Kaggle dataset ID (owner/slug)
            file_name: Name of file to download
            output_path: Directory to save file
        """
        file_path = output_path / file_name

        # Skip if file exists and force is False
        if file_path.exists() and not self.force:
            logger.info(f"File already exists (use --force to " f"re-download): {file_name}")
            return

        logger.info(f"Downloading file: {file_name}")
        try:
            self.api.dataset_download_file(
                dataset_id,
                file_name,
                path=str(output_path),
                force=self.force,
            )
            logger.info(f"Downloaded: {file_name}")
        except Exception as e:
            logger.error(f"Failed to download file '{file_name}': {e}")
            raise

    def _download_all_files(self, dataset_id: str, output_path: Path) -> None:
        """
        Download all files from a dataset.

        Args:
            dataset_id: Kaggle dataset ID (owner/slug)
            output_path: Directory to save files
        """
        # Check if dataset already downloaded
        if not self.force and self._dataset_exists(output_path):
            logger.info("Dataset already downloaded (use --force to re-download)")
            return

        logger.info("Downloading entire dataset")
        try:
            self.api.dataset_download_files(
                dataset_id,
                path=str(output_path),
                force=self.force,
                unzip=False,  # We handle unzipping separately
            )
            logger.info("Downloaded all files")
        except Exception as e:
            logger.error(f"Failed to download dataset: {e}")
            raise

    def _dataset_exists(self, output_path: Path) -> bool:
        """
        Check if dataset files already exist.

        Args:
            output_path: Directory to check

        Returns:
            True if directory contains files
        """
        if not output_path.exists():
            return False

        # Check if directory has any non-hidden files
        files = [f for f in output_path.iterdir() if not f.name.startswith(".")]
        return len(files) > 0

    def _unzip_files(self, output_path: Path) -> None:
        """
        Unzip any .zip files in the output directory.

        Args:
            output_path: Directory containing zip files
        """
        zip_files = list(output_path.glob("*.zip"))

        if not zip_files:
            return

        logger.info(f"Found {len(zip_files)} zip file(s) to extract")

        for zip_file in zip_files:
            try:
                logger.info(f"Extracting: {zip_file.name}")
                with zipfile.ZipFile(zip_file, "r") as zf:
                    zf.extractall(output_path)
                logger.info(f"Extracted: {zip_file.name}")

                # Optionally remove zip file after extraction
                # Keeping it for now in case extraction fails
                # zip_file.unlink()

            except Exception as e:
                logger.error(f"Failed to extract {zip_file.name}: {e}")
                # Continue with other files

    def run(self) -> None:
        """Execute the download workflow."""
        try:
            self.load_config()
            self.authenticate()

            # Create output directory
            self.output_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Output directory: {self.output_dir}")

            # Download each dataset
            datasets = self.config.get("datasets", [])
            for i, dataset_config in enumerate(datasets, 1):
                logger.info(f"\n--- Processing dataset {i}/{len(datasets)} ---")
                try:
                    self.download_dataset(dataset_config)
                except Exception as e:
                    logger.error(f"Failed to process dataset {i}: {e}")
                    # Continue with next dataset instead of failing completely
                    continue

            logger.info("\n=== Download workflow completed ===")

        except Exception as e:
            logger.error(f"Download workflow failed: {e}")
            sys.exit(1)


def main() -> None:
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Download Kaggle datasets based on configuration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to YAML configuration file",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Base directory for downloaded datasets",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-download even if files exist",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Run downloader
    downloader = KaggleDatasetDownloader(
        config_path=args.config,
        output_dir=args.output_dir,
        force=args.force,
    )
    downloader.run()


if __name__ == "__main__":
    main()
