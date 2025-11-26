"""Generate an audit report for Carian FMG speaker IDs."""

# pyright: reportGeneralTypeIssues=false

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def _load_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        message = f"Missing required dataset: {path}"
        raise FileNotFoundError(message)
    return pd.read_parquet(path)


def build_audit(
    *,
    lore_path: Path,
    motif_stats_path: Path,
    parquet_output: Path,
    markdown_output: Path,
) -> pd.DataFrame:
    lore_df = _load_frame(lore_path)
    motif_df = _load_frame(motif_stats_path)

    mask = lore_df["canonical_id"].str.startswith("npc:carian_speaker_")
    carian_df = lore_df.loc[mask].copy()
    if carian_df.empty:
        raise RuntimeError("No Carian speaker rows found in lore corpus")

    grouped = (
        carian_df.groupby("canonical_id")
        .agg(lore_count=("lore_id", "nunique"))
        .sort_values("lore_count", ascending=False)
    )

    text_counts_series = (
        carian_df.groupby(["canonical_id", "text_type"]).size().rename("count")
    )
    text_counts_dict: dict[str, dict[str, int]] = {}
    for (cid, text_type), count in text_counts_series.items():
        bucket = text_counts_dict.setdefault(cid, {})
        bucket[text_type] = int(count)

    grouped["text_type_counts"] = grouped.index.map(
        lambda cid: json.dumps(
            text_counts_dict.get(cid, {}),
            sort_keys=True,
        )
    )

    motif_mask = motif_df["canonical_id"].isin(grouped.index)
    motif_subset = motif_df.loc[motif_mask].copy()

    motif_lists: dict[str, list[str]] = {}
    for cid, frame in motif_subset.groupby("canonical_id"):
        motifs = sorted(set(frame["motif_slug"].tolist()))
        motif_lists[cid] = motifs

    grouped["motif_slugs"] = grouped.index.map(
        lambda cid: ", ".join(motif_lists.get(cid, [])) or ""
    )
    grouped["motif_count"] = grouped["motif_slugs"].apply(
        lambda s: 0 if not s else len(s.split(", "))
    )

    parquet_output.parent.mkdir(parents=True, exist_ok=True)
    grouped.reset_index().to_parquet(parquet_output, index=False)

    _write_markdown_report(
        grouped=grouped,
        markdown_output=markdown_output,
    )

    return grouped


def _write_markdown_report(
    *, grouped: pd.DataFrame, markdown_output: Path
) -> None:
    markdown_output.parent.mkdir(parents=True, exist_ok=True)

    total_ids = len(grouped)
    total_lore = int(grouped["lore_count"].sum())
    top5 = grouped.head(5).reset_index()

    lines = ["# Carian Speaker Audit", ""]
    lines.append(f"Total synthetic speaker IDs: **{total_ids}**")
    lines.append(f"Total lore lines attached: **{total_lore}**")
    lines.append("")
    lines.append("## Top 5 speaker buckets by lore count")
    lines.append("")
    lines.append("| canonical_id | lore_count | motifs | text_types |")
    lines.append("| --- | ---: | --- | --- |")
    for _, row in top5.iterrows():
        motifs = row["motif_slugs"] or "—"
        text_counts = row["text_type_counts"]
        lines.append(
            "| {cid} | {count} | {motifs} | {text_types} |".format(
                cid=row["canonical_id"],
                count=row["lore_count"],
                motifs=motifs,
                text_types=text_counts,
            )
        )

    lines.append("")
    lines.append(
        "> Full details stored in `data/debug/carian_speaker_audit.parquet`."
    )

    markdown_output.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--lore-path",
        type=Path,
        default=Path("data/curated/lore_corpus.parquet"),
    )
    parser.add_argument(
        "--motif-stats-path",
        type=Path,
        default=Path(
            "data/analysis/npc_motif_graph/entity_motif_stats.parquet"
        ),
    )
    parser.add_argument(
        "--parquet-output",
        type=Path,
        default=Path("data/debug/carian_speaker_audit.parquet"),
    )
    parser.add_argument(
        "--markdown-output",
        type=Path,
        default=Path("docs/debug/carian_speaker_audit.md"),
    )
    args = parser.parse_args()
    build_audit(
        lore_path=args.lore_path,
        motif_stats_path=args.motif_stats_path,
        parquet_output=args.parquet_output,
        markdown_output=args.markdown_output,
    )


if __name__ == "__main__":
    main()
