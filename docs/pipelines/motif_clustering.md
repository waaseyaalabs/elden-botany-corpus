# Motif clustering pipeline

Phase 7 introduces an analysis layer that groups lore lines by thematic
similarity and surfaces motif-rich clusters for downstream tooling.

## Inputs

- `data/curated/lore_corpus.parquet` (automatically read, CSV fallback is
  supported when the Parquet export is missing)
- `data/community/processed/motif_coverage.parquet` for global motif coverage
  context (optional but recommended)
- Motif taxonomy definitions under `config/community_motifs.yml`

## Outputs

Artifacts are written to `data/analysis/motif_clustering/` (configurable via
`--output-dir`):

| File | Description |
| --- | --- |
| `motif_clusters.parquet` | Per-lore row assignments with cluster id,
UMAP positions, and HDBSCAN membership probability |
| `motif_cluster_density.parquet` | Long-form motif density table with
cluster-level match counts, local percentages, and delta vs. global coverage |
| `motif_cluster_samples.json` | Human-readable summary containing exemplar
lore lines and top motifs per cluster |
| `motif_clusters.png` | 2D UMAP scatter plot labeled by cluster id |

## CLI usage

```bash
poetry run corpus analysis clusters --export \
  --model sentence-transformers/all-MiniLM-L6-v2 \
  --max-rows 20000 \
  --min-cluster 15
```

Key options:

- `--provider`: switch between `local` sentence-transformers and `openai`
  embeddings.
- `--max-rows`: deterministically subsample large corpora to control runtime.
- `--min-cluster` / `--min-samples`: tune HDBSCAN density thresholds.
- `--components`: adjust the dimensionality used for clustering projections.

`make analysis-clusters` is a convenient wrapper that simply runs
`poetry run corpus analysis clusters --export` with any extra `ARGS`
forwarded to the CLI.

## Implementation details

1. Curated lore text is embedded with the configured encoder (local MiniLM by
   default).
2. Embeddings are projected into a lower-dimensional manifold using UMAP and
   clustered with HDBSCAN (deterministic seeds ensure repeatability).
3. Each cluster receives exemplar lore lines and motif density stats by
   reapplying the taxonomy keyword matchers, enabling comparison against the
   Phase 6 coverage report.
4. Visualization helpers persist a static PNG scatterplot so reviewers can
   quickly scan the thematic layout without rebuilding notebooks.

## Testing

Unit tests cover deterministic sampling, exemplar selection, and the motif
correlation report to ensure regressions are caught without requiring the full
pipeline to run during CI.
