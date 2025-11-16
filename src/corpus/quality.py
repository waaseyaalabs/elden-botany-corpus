"""Utilities for generating quality reports for curated datasets."""

from __future__ import annotations

from datetime import UTC, datetime
from html import escape
from pathlib import Path
from typing import Any

import polars as pl  # type: ignore[import]

from corpus.utils import save_json


class QualityReporter:
    """Build JSON + HTML quality reports for Polars DataFrames."""

    def __init__(
        self,
        output_dir: Path,
        relative_root: Path | None = None,
    ) -> None:
        """Create a reporter that writes reports to ``output_dir``.

        Args:
            output_dir: Directory where report artifacts are stored.
            relative_root: Base path used for computing metadata-friendly
                relative paths. Defaults to ``output_dir.parent``.
        """

        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.relative_root = relative_root or output_dir.parent

    def generate_report(
        self,
        dataset_name: str,
        df: pl.DataFrame,
    ) -> dict[str, Any]:
        """Profile a DataFrame and emit JSON + HTML reports.

        Args:
            dataset_name: Logical dataset name (e.g., ``"unified"`` or
                ``"weapon"``).
            df: Curated Polars DataFrame to profile.

        Returns:
            Summary metadata describing the generated reports.
        """

        safe_name = self._safe_name(dataset_name)
        json_path = self.output_dir / f"{safe_name}.json"
        html_path = self.output_dir / f"{safe_name}.html"

        profile = self._build_profile(dataset_name, df)
        save_json(profile, json_path, indent=2)
        html_path.write_text(self._render_html(profile), encoding="utf-8")

        return {
            "dataset": dataset_name,
            "rows": profile["row_count"],
            "columns": profile["column_count"],
            "null_pct_avg": profile["summary"]["avg_null_percentage"],
            "json_report": str(json_path.relative_to(self.relative_root)),
            "html_report": str(html_path.relative_to(self.relative_root)),
        }

    def _build_profile(
        self,
        dataset_name: str,
        df: pl.DataFrame,
    ) -> dict[str, Any]:
        """Compute column-level statistics for the provided DataFrame."""

        row_count = df.height
        columns: list[dict[str, Any]] = []
        total_null_pct = 0.0

        for column in df.columns:
            series = df[column]
            null_count = series.null_count()
            null_pct = round(
                (null_count / row_count * 100) if row_count else 0.0,
                2,
            )
            total_null_pct += null_pct

            column_profile: dict[str, Any] = {
                "name": column,
                "dtype": str(series.dtype),
                "null_count": int(null_count),
                "null_percentage": null_pct,
            }

            if self._is_numeric(series.dtype):
                stats = self._numeric_stats(series)
                if stats:
                    column_profile["stats"] = stats
            elif self._is_categorical(series):
                top_categories = self._top_categories(
                    series,
                    row_count - null_count,
                )
                if top_categories:
                    column_profile["top_categories"] = top_categories

            columns.append(column_profile)

        avg_null_pct = (
            round(
                total_null_pct / len(df.columns),
                2,
            )
            if df.columns
            else 0.0
        )

        return {
            "dataset": dataset_name,
            "generated_at": datetime.now(UTC).isoformat(),
            "row_count": row_count,
            "column_count": len(df.columns),
            "summary": {
                "avg_null_percentage": avg_null_pct,
            },
            "columns": columns,
        }

    @staticmethod
    def _numeric_stats(series: pl.Series) -> dict[str, float] | None:
        """Return min/max/mean statistics for numeric columns."""

        numeric = series.drop_nulls()
        if numeric.is_empty():
            return None

        return {
            "min": float(numeric.min()),
            "max": float(numeric.max()),
            "mean": float(numeric.mean()),
        }

    @staticmethod
    def _is_numeric(dtype: pl.DataType) -> bool:
        """Heuristic numeric detection for various Polars dtypes."""

        dtype_name = dtype.__class__.__name__.lower()
        return dtype_name.startswith(("int", "uint", "float", "decimal"))

    @staticmethod
    def _is_categorical(series: pl.Series) -> bool:
        """Heuristic check for categorical/text columns."""

        return series.dtype in {pl.Utf8, pl.Categorical, pl.Boolean}

    @staticmethod
    def _top_categories(
        series: pl.Series,
        non_null_rows: int,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        """Return value counts for string-like columns."""

        if non_null_rows <= 0:
            return []

        value_counts = (
            series.drop_nulls().value_counts().sort("counts", descending=True).head(limit)
        )
        value_column = value_counts.columns[0] if value_counts.columns else "value"

        top_values: list[dict[str, Any]] = []
        for row in value_counts.iter_rows(named=True):
            count = int(row["counts"])
            pct = round((count / non_null_rows) * 100, 2)
            top_values.append(
                {
                    "value": row[value_column],
                    "count": count,
                    "percentage": pct,
                }
            )
        return top_values

    @staticmethod
    def _safe_name(dataset_name: str) -> str:
        """Create filesystem-friendly file names for datasets."""

        return dataset_name.lower().replace(" ", "_")

    @staticmethod
    def _render_html(profile: dict[str, Any]) -> str:
        """Render a lightweight HTML summary of the quality report."""

        head = (
            "<!DOCTYPE html>\n"
            '<html lang="en">\n'
            "<head>\n"
            '  <meta charset="utf-8" />\n'
            "  <title>Quality Report - {title}</title>\n"
            "  <style>\n"
            "    body {{ font-family: Arial, sans-serif; padding: 1.5rem; }}\n"
            "    table {{ border-collapse: collapse; width: 100%;"
            " margin-top: 1rem; }}\n"
            "    th, td {{ border: 1px solid #ddd; padding: 0.5rem;"
            " text-align: left; }}\n"
            "    th {{ background-color: #f4f4f4; }}\n"
            "  </style>\n"
            "</head>\n"
        ).format(title=escape(str(profile["dataset"])))

        header_section = (
            "<body>\n"
            f"  <h1>Quality Report: {escape(str(profile['dataset']))}</h1>\n"
            f"  <p>Generated at: {escape(str(profile['generated_at']))}</p>\n"
            f"  <p>Rows: {profile['row_count']} • Columns:"
            f" {profile['column_count']} • Avg. null %:"
            f" {profile['summary']['avg_null_percentage']}%</p>\n"
        )

        table_rows = []
        for column in profile["columns"]:
            stats_html = ""
            if "stats" in column:
                stats = column["stats"]
                stats_html = (
                    f"min={stats['min']:.2f}, max={stats['max']:.2f}, " f"mean={stats['mean']:.2f}"
                )
            elif "top_categories" in column:
                cats = ", ".join(
                    f"{escape(str(item['value']))} ({item['percentage']}%)"
                    for item in column["top_categories"]
                )
                stats_html = cats

            table_rows.append(
                "    <tr>\n"
                f"      <td>{escape(column['name'])}</td>\n"
                f"      <td>{escape(column['dtype'])}</td>\n"
                f"      <td>{column['null_percentage']}%</td>\n"
                f"      <td>{escape(stats_html) if stats_html else '-'}</td>\n"
                "    </tr>\n"
            )

        table_html = (
            "  <table>\n"
            "    <thead>\n"
            "      <tr><th>Column</th><th>Type</th><th>Null %</th>"
            "<th>Details</th></tr>\n"
            "    </thead>\n"
            "    <tbody>\n"
            f"{''.join(table_rows)}"
            "    </tbody>\n"
            "  </table>\n"
        )

        return head + header_section + table_html + "</body>\n</html>\n"
