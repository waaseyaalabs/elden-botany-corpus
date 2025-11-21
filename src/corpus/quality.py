"""Lightweight quality reporting for curated datasets."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl
from polars import exceptions as pl_exceptions

from corpus.config import settings
from corpus.utils import save_json


def _numeric_dtypes() -> set[pl.PolarsDataType]:
    """Collect numeric dtype classes available in the installed Polars."""

    numeric: set[pl.PolarsDataType] = set(getattr(pl, "NUMERIC_DTYPES", []))
    for attr in ("Decimal", "Decimal128", "Decimal256"):
        dtype = getattr(pl, attr, None)
        if dtype is not None:
            numeric.add(dtype)
    return numeric


NUMERIC_DTYPES = _numeric_dtypes()


def _is_numeric_dtype(dtype: pl.PolarsDataType) -> bool:
    """Return True when dtype represents a numeric column."""

    return dtype in NUMERIC_DTYPES


def _is_categorical_dtype(dtype: pl.PolarsDataType) -> bool:
    """Return True for textual/categorical columns."""

    return dtype in {pl.Utf8, pl.Categorical, pl.Boolean}


class QualityReporter:
    """Create machine-readable and HTML quality reports for curated outputs."""

    def __init__(
        self,
        output_dir: Path | None = None,
        *,
        top_k: int = 5,
        relative_root: Path | None = None,
        base_dir: Path | None = None,
    ) -> None:
        self.output_dir = output_dir or settings.curated_dir / "quality"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.top_k = top_k
        self.relative_root = (
            relative_root or base_dir or self.output_dir.parent
        )

    def profile(self, dataset_name: str, df: pl.DataFrame) -> dict[str, Any]:
        """Backward-compatible alias for :meth:`generate_report`."""

        return self.generate_report(dataset_name, df)

    def generate_report(
        self,
        dataset_name: str,
        df: pl.DataFrame,
    ) -> dict[str, Any]:
        """Generate quality report artifacts for a dataset."""

        summary = self._summarize(dataset_name, df)
        file_slug = dataset_name.replace(" ", "_").lower()
        json_path = self.output_dir / f"{file_slug}.json"
        html_path = self.output_dir / f"{file_slug}.html"

        save_json(summary, json_path, indent=2)
        html_path.write_text(self._render_html(summary), encoding="utf-8")

        rel_json = self._relative_path(json_path)
        rel_html = self._relative_path(html_path)

        report_refs = {
            "dataset": dataset_name,
            "json": str(rel_json),
            "html": str(rel_html),
            "generated_at": summary["generated_at"],
            "row_count": summary["row_count"],
            "column_count": summary["column_count"],
            "alerts": summary["alerts"],
        }

        summary["reports"] = report_refs
        summary["rows"] = summary["row_count"]
        summary["columns"] = summary["column_count"]
        summary["null_pct_avg"] = summary["overall_null_percent"]
        summary["json_report"] = str(rel_json)
        summary["html_report"] = str(rel_html)
        return summary

    # fmt: off

    def _summarize(
        self, dataset_name: str, df: pl.DataFrame
    ) -> dict[str, Any]:
        """Build the JSON-friendly summary for a dataset."""

        row_count = df.height
        column_names = df.columns
        total_cells = row_count * len(column_names)
        total_nulls = sum(df[col].null_count() for col in column_names)
        overall_null_percent = (
            round((total_nulls / total_cells) * 100, 4) if total_cells else 0.0
        )

        columns: dict[str, dict[str, Any]] = {}
        alerts: list[str] = []

        for column in column_names:
            series = df[column]
            dtype = series.dtype
            null_percent = (
                round((series.null_count() / row_count) * 100, 4)
                if row_count
                else 0.0
            )
            unique_count = None
            summary: dict[str, Any] = {}

            non_null_series = series.drop_nulls()
            if _is_numeric_dtype(dtype):
                if not non_null_series.is_empty():
                    summary.update(
                        {
                            "min": non_null_series.min(),
                            "max": non_null_series.max(),
                        }
                    )
                    mean_raw = non_null_series.mean()
                    mean_value = 0.0 if mean_raw is None else float(mean_raw)
                    summary["mean"] = round(mean_value, 4)

                    std_raw = (
                        non_null_series.std()
                        if non_null_series.len() > 1
                        else 0.0
                    )
                    std_value = 0.0 if std_raw is None else float(std_raw)
                    summary["std"] = round(std_value, 4)
            elif _is_categorical_dtype(dtype):
                value_counts = (
                    non_null_series.value_counts(sort=True)
                    .head(self.top_k)
                    .to_dict(as_series=False)
                )
                if value_counts:
                    summary["top_values"] = [
                        {"value": value, "count": count}
                        for value, count in zip(
                            value_counts.get(series.name, []) or [],
                            value_counts.get("count", []) or [],
                            strict=False,
                        )
                    ]

            if not non_null_series.is_empty():
                try:
                    unique_count = non_null_series.n_unique()
                except pl_exceptions.InvalidOperationError:
                    unique_count = None

            if null_percent >= 50.0:
                alerts.append(f"{column} has {null_percent:.2f}% nulls")

            columns[column] = {
                "dtype": str(dtype),
                "null_percent": null_percent,
                "unique_count": unique_count,
                "summary": summary,
            }

        # Add high-level alerts
        if row_count == 0:
            alerts.append("Dataset is empty")
        if overall_null_percent >= 25.0:
            alerts.append("Overall null percentage exceeds 25%")

        return {
            "dataset": dataset_name,
            "generated_at": datetime.now(UTC).isoformat(),
            "row_count": row_count,
            "column_count": len(column_names),
            "overall_null_percent": overall_null_percent,
            "columns": columns,
            "alerts": alerts,
        }

    # fmt: on
    # fmt: off

    def _render_html(self, summary: dict[str, Any]) -> str:
        """Render a minimal HTML report so analysts can browse locally."""

        rows: list[str] = []
        for column, metrics in summary["columns"].items():
            column_html = [
                f"<tr><td>{column}</td>",
                f"<td>{metrics['dtype']}</td>",
                f"<td>{metrics['null_percent']:.2f}%</td>",
                f"<td>{metrics.get('unique_count', '—')}</td>",
                "<td>",
            ]

            summary_stats = metrics["summary"]
            if "min" in summary_stats:
                column_html.append(
                    "Min: {min}, Max: {max}, Mean: {mean}, Std: {std}".format(
                        **summary_stats
                    )
                )
            elif "top_values" in summary_stats:
                top_values = ", ".join(
                    f"{item['value']} ({item['count']})"
                    for item in summary_stats["top_values"]
                )
                column_html.append(top_values)
            else:
                column_html.append("—")

            column_html.append("</td></tr>")
            rows.append("".join(column_html))

        alerts_section = "".join(
            f"<li>{alert}</li>" for alert in summary["alerts"]
        )

        return f"""<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <title>Quality Report - {summary['dataset']}</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 1.5rem; }}
    table {{ border-collapse: collapse; width: 100%; margin-top: 1rem; }}
    th, td {{ border: 1px solid #ddd; padding: 0.5rem; text-align: left; }}
    th {{ background-color: #f5f5f5; }}
  </style>
 </head>
 <body>
   <h1>Quality Report - {summary['dataset']}</h1>
   <p>Generated: {summary['generated_at']}</p>
    <p>Rows: {summary['row_count']} | Columns: {summary['column_count']} |
        Overall Null %: {summary['overall_null_percent']:.2f}%</p>
   <h2>Alerts</h2>
   <ul>{alerts_section or '<li>No alerts detected.</li>'}</ul>
   <h2>Columns</h2>
   <table>
     <thead>
       <tr>
         <th>Column</th>
         <th>Type</th>
         <th>Null %</th>
         <th>Unique</th>
         <th>Summary</th>
       </tr>
     </thead>
     <tbody>
       {''.join(rows)}
     </tbody>
   </table>
 </body>
</html>
"""

    # fmt: on

    def _relative_path(self, path: Path) -> Path:
        """Return a path relative to the curated base directory."""

        try:
            return path.relative_to(self.relative_root)
        except ValueError:
            return path
