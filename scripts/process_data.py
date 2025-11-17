#!/usr/bin/env python3
"""Process raw Kaggle datasets into validated Parquet files.

This script reads raw data from data/raw/, applies transformations
and validations, and writes processed outputs to data/processed/.
"""

import argparse
import json
import logging
import sys
from pathlib import Path

from pipeline.process import DataProcessor


def setup_logging(verbose: bool = False):
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def main():
    """Main entrypoint."""
    parser = argparse.ArgumentParser(
        description="Process raw datasets into validated Parquet files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all datasets
  python scripts/process_data.py

  # Force reprocess everything
  python scripts/process_data.py --force

  # Dry run (validate only, no writes)
  python scripts/process_data.py --dry-run

  # Custom paths
  python scripts/process_data.py \\
    --config config/kaggle_datasets.yml \\
    --raw-dir data/raw \\
    --processed-dir data/processed
        """,
    )

    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/kaggle_datasets.yml"),
        help="Path to dataset configuration file (default: config/kaggle_datasets.yml)",
    )

    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=Path("data/raw"),
        help="Directory containing raw data (default: data/raw)",
    )

    parser.add_argument(
        "--processed-dir",
        type=Path,
        default=Path("data/processed"),
        help="Directory for processed outputs (default: data/processed)",
    )

    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=None,
        help="Directory for processing cache (default: <processed-dir>/.cache)",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Force reprocessing even if files are up-to-date",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate transformations without writing files",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    parser.add_argument(
        "--output-stats",
        type=Path,
        default=None,
        help="Write processing statistics to JSON file",
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help=(
            "Number of worker processes to use (default: serial or value "
            "from config settings). Use 0 to auto-detect CPU count."
        ),
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    # Validate inputs
    if not args.config.exists():
        logger.error(f"Config file not found: {args.config}")
        sys.exit(1)

    if not args.raw_dir.exists():
        logger.error(f"Raw data directory not found: {args.raw_dir}")
        sys.exit(1)

    # Log configuration
    logger.info("=" * 60)
    logger.info("Data Processing Pipeline")
    logger.info("=" * 60)
    logger.info(f"Config: {args.config}")
    logger.info(f"Raw directory: {args.raw_dir}")
    logger.info(f"Processed directory: {args.processed_dir}")
    logger.info(f"Force reprocess: {args.force}")
    logger.info(f"Dry run: {args.dry_run}")
    logger.info(
        "Workers: %s",
        "auto" if args.workers == 0 else args.workers or "config",
    )
    logger.info("=" * 60)

    # Create processor
    processor = DataProcessor(
        config_path=args.config,
        raw_dir=args.raw_dir,
        processed_dir=args.processed_dir,
        cache_dir=args.cache_dir,
    )

    pending = processor.get_pending_datasets(force=args.force)
    if args.force:
        logger.info("Force flag enabled; processing every dataset.")
    elif pending:
        logger.info("Detected %d dataset(s) with stale raw files:", len(pending))
        for dataset, payload in pending.items():
            logger.info("  - %s (%s)", dataset, payload.get("reason", "unknown"))
            for raw_file in payload.get("files", [])[:5]:
                logger.debug("      - %s", raw_file)
            extra_files = max(len(payload.get("files", [])) - 5, 0)
            if extra_files:
                logger.debug("      ...and %d more", extra_files)
    else:
        logger.info("No datasets require processing; performing validation run only.")

    # Process all datasets
    try:
        results = processor.process_all(
            force=args.force,
            dry_run=args.dry_run,
            workers=args.workers,
        )

        # Print summary
        logger.info("=" * 60)
        logger.info("Processing Summary")
        logger.info("=" * 60)
        summary = results["summary"]
        logger.info(f"Total datasets: {summary['total']}")
        logger.info(f"Succeeded: {summary['succeeded']}")
        logger.info(f"Failed: {summary['failed']}")
        logger.info(f"Skipped: {summary['skipped']}")
        logger.info("=" * 60)

        # Save statistics if requested
        if args.output_stats:
            logger.info(f"Writing statistics to {args.output_stats}")
            args.output_stats.parent.mkdir(parents=True, exist_ok=True)
            with open(args.output_stats, "w") as f:
                json.dump(results, f, indent=2)

        # Exit with appropriate code
        if summary["failed"] > 0:
            logger.error("Some datasets failed to process")
            sys.exit(1)
        else:
            logger.info("All datasets processed successfully!")
            sys.exit(0)

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
