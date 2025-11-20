"""Data curation and export pipeline."""

import json
from pathlib import Path

import polars as pl  # type: ignore[import]

from corpus.config import settings
from corpus.lineage import LineageManifestBuilder
from corpus.models import RawEntity
from corpus.quality import QualityReporter
from corpus.reconcile import entities_to_dataframe
from corpus.utils import (
    MetadataTracker,
    save_csv,
    save_parquet,
)
from pipeline.schemas import get_active_schema_version


class CorpusCurator:
    """Curate and export final corpus datasets."""

    def __init__(
        self,
        output_dir: Path | None = None,
        enable_quality_reports: bool = True,
    ) -> None:
        """
        Initialize curator.

        Args:
            output_dir: Output directory (default: settings.curated_dir)
            enable_quality_reports: Generate HTML/JSON quality reports for
                each curated dataset when True.
        """
        self.output_dir = output_dir or settings.curated_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.metadata = MetadataTracker()
        self.lineage_builder = LineageManifestBuilder(
            output_root=self.output_dir / "lineage",
            relative_root=self.output_dir,
        )
        self.quality_reporter: QualityReporter | None = None
        if enable_quality_reports:
            self.quality_reporter = QualityReporter(
                output_dir=self.output_dir / "quality",
                relative_root=self.output_dir,
            )

    def curate(
        self,
        entities: list[RawEntity],
        unmapped_texts: list[RawEntity] | None = None,
    ) -> pl.DataFrame:
        """
        Curate entities into final dataset.

        Args:
            entities: Reconciled entities
            unmapped_texts: Unmapped DLC text snippets

        Returns:
            Unified DataFrame
        """
        print("\n=== Curating Corpus ===")

        # Convert to DataFrame
        df = entities_to_dataframe(entities)

        # Track metadata
        self.metadata.add_row_count("total_entities", len(df))

        # Count by entity type
        entity_counts = df.group_by("entity_type").agg(
            pl.count().alias("count"),
        )
        for row in entity_counts.iter_rows(named=True):
            self.metadata.add_entity_count(row["entity_type"], row["count"])

        # Count DLC vs base
        dlc_count = df.filter(pl.col("is_dlc")).height
        base_count = df.filter(~pl.col("is_dlc")).height

        self.metadata.add_row_count("dlc_entities", dlc_count)
        self.metadata.add_row_count("base_entities", base_count)

        # Count by source
        source_counts: dict[str, int] = {}
        for row in df.iter_rows(named=True):
            for source in row["sources"]:
                source_counts[source] = source_counts.get(source, 0) + 1

        for source, count in source_counts.items():
            self.metadata.add_provenance_summary(source, count)

        self._emit_lineage_manifests(entities)

        # Handle unmapped texts
        if unmapped_texts:
            self.metadata.set_unmapped_texts(len(unmapped_texts))
            self._export_unmapped_texts(unmapped_texts)

        print(f"\nCurated {len(df)} entities:")
        print(f"  Base game: {base_count}")
        print(f"  DLC: {dlc_count}")

        return df

    def export_unified(self, df: pl.DataFrame) -> None:
        """
        Export unified dataset in multiple formats.

        Args:
            df: Unified DataFrame
        """
        print("\n=== Exporting Unified Dataset ===")

        # Parquet (preferred format)
        parquet_path = self.output_dir / "unified.parquet"
        save_parquet(df, parquet_path)
        print(f"Saved: {parquet_path}")

        # CSV (for compatibility)
        # Convert meta_json and sources to JSON strings for CSV
        df_csv = df.with_columns(
            [
                pl.when(pl.col("meta_json").is_null())
                .then(pl.lit("{}"))
                .otherwise(pl.col("meta_json").struct.json_encode())
                .alias("meta_json"),
                pl.col("sources")
                .map_elements(
                    lambda x: (
                        json.dumps(x.to_list())
                        if isinstance(x, pl.Series)
                        else json.dumps(x if x is not None else [])
                    ),
                    return_dtype=pl.Utf8,
                )
                .alias("sources"),
            ]
        )

        csv_path = self.output_dir / "unified.csv"
        save_csv(df_csv, csv_path)
        print(f"Saved: {csv_path}")

        self._record_quality_report("unified", df)

    def export_by_entity_type(self, df: pl.DataFrame) -> None:
        """
        Export separate files per entity type.

        Args:
            df: Unified DataFrame
        """
        print("\n=== Exporting by Entity Type ===")

        entity_types = df.select("entity_type").unique().to_series()

        for entity_type in entity_types:
            entity_df = df.filter(pl.col("entity_type") == entity_type)

            # Parquet
            parquet_path = self.output_dir / f"{entity_type}.parquet"
            save_parquet(entity_df, parquet_path)

            # CSV
            entity_df_csv = entity_df.with_columns(
                [
                    pl.when(pl.col("meta_json").is_null())
                    .then(pl.lit("{}"))
                    .otherwise(pl.col("meta_json").struct.json_encode())
                    .alias("meta_json"),
                    pl.col("sources")
                    .map_elements(
                        lambda x: (
                            json.dumps(x.to_list())
                            if isinstance(x, pl.Series)
                            else json.dumps(x if x is not None else [])
                        ),
                        return_dtype=pl.Utf8,
                    )
                    .alias("sources"),
                ]
            )

            csv_path = self.output_dir / f"{entity_type}.csv"
            save_csv(entity_df_csv, csv_path)

            print(f"Exported {len(entity_df)} {entity_type} entities")
            self._record_quality_report(entity_type, entity_df)
            self._track_schema_version(entity_type)

    def export_metadata(self) -> None:
        """Export metadata about the curation process."""
        metadata_path = self.output_dir / "metadata.json"
        self.metadata.save(metadata_path)
        print(f"\nSaved metadata: {metadata_path}")

    def _emit_lineage_manifests(self, entities: list[RawEntity]) -> None:
        """Generate lineage manifests and register them in metadata."""

        if not entities:
            return

        summary = self.lineage_builder.build(entities)
        self.metadata.set_lineage_manifests(summary)

    def _export_unmapped_texts(self, unmapped_texts: list[RawEntity]) -> None:
        """Export unmapped DLC texts for manual review."""
        rows = [
            {
                "name": text.name,
                "description": text.description,
                "section": text.raw_data.get("section", ""),
            }
            for text in unmapped_texts
        ]

        df = pl.DataFrame(rows)
        csv_path = self.output_dir / "unmapped_dlc_text.csv"
        save_csv(df, csv_path)
        print(f"Saved {len(df)} unmapped texts: {csv_path}")

    def _record_quality_report(
        self,
        dataset_name: str,
        df: pl.DataFrame,
    ) -> None:
        """Generate and track quality reports for curated datasets."""

        if not self.quality_reporter:
            return

        summary = self.quality_reporter.generate_report(dataset_name, df)
        self.metadata.add_quality_report(dataset_name, summary)

    def _track_schema_version(self, dataset_name: str) -> None:
        """Record schema version metadata for a curated dataset."""

        schema_version = get_active_schema_version(dataset_name)
        if not schema_version:
            return

        self.metadata.add_schema_version(
            dataset_name,
            schema_version.to_metadata(),
        )


def curate_corpus(
    entities: list[RawEntity],
    unmapped_texts: list[RawEntity] | None = None,
    enable_quality_reports: bool = True,
) -> pl.DataFrame:
    """
    Main curation pipeline.

    Args:
        entities: Reconciled entities
        unmapped_texts: Unmapped DLC text snippets
        enable_quality_reports: When True, emit HTML/JSON quality diagnostics
            for unified + per-entity exports.

    Returns:
        Unified DataFrame
    """
    curator = CorpusCurator(enable_quality_reports=enable_quality_reports)

    # Curate
    df = curator.curate(entities, unmapped_texts)

    # Export in multiple formats
    curator.export_unified(df)
    curator.export_by_entity_type(df)
    curator.export_metadata()

    return df
