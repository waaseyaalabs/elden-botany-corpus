#!/usr/bin/env bash
set -euo pipefail

# Optional: allow caller to override model/provider
: "${TB_LLM_PROVIDER:=openai}"
: "${TB_LLM_MODEL:=gpt-5-mini}"

echo "=== TarnishedBotanist Analysis Smoke Test ==="
echo "LLM provider: ${TB_LLM_PROVIDER}"
echo "LLM model   : ${TB_LLM_MODEL}"
echo

# 0. Preconditions
if [[ ! -d ".venv" ]]; then
  echo "[ERR] .venv not found. Run 'make setup' or 'poetry install' first." >&2
  exit 1
fi

if ! command -v poetry >/dev/null 2>&1; then
  echo "[ERR] poetry is not on PATH." >&2
  exit 1
fi

# 1. Build curated + community coverage (if not already done)
echo ">>> Step 1: Build curated corpus and community coverage"
make build-corpus
make community-report

# At this point we expect:
# - data/curated/lore_corpus.parquet (or unified parquet depending on config)
# - data/community/processed/motif_coverage.parquet
echo "✓ Curated + community coverage built"
echo

# 2. Motif clustering (Phase 7 – Branch 1)
echo ">>> Step 2: Motif clustering analysis"
make analysis-clusters

# Expected artifacts under:
# - data/analysis/motif_clustering/*.parquet
# - data/analysis/motif_clustering/*.json
# - data/analysis/motif_clustering/*.png
echo "✓ Motif clustering artifacts generated"
echo

# 3. NPC motif graph (Phase 7 – Branch 2)
echo ">>> Step 3: NPC motif graph"
# If your curated corpus lives under a different filename, override here with ARGS="--curated ..."
make analysis-graph

# Expected artifacts under:
# - data/analysis/npc_motif_graph/entity_summary.parquet
# - data/analysis/npc_motif_graph/entity_motif_stats.parquet
# - data/analysis/npc_motif_graph/lore_motif_hits.parquet
# - data/analysis/npc_motif_graph/npc_motif_graph.gpickle
# - data/analysis/npc_motif_graph/npc_motif_graph.graphml
# - data/analysis/npc_motif_graph/graph_report.json
echo "✓ NPC motif graph artifacts generated"
echo

# 4. Dry-run narrative summaries (no LLM)
echo ">>> Step 4: Narrative summaries (dry-run, no LLM calls)"
make analysis-summaries ARGS="--dry-run-llm"

# Expected artifacts under:
# - data/analysis/narrative_summaries/npc_narrative_summaries.json
# - data/analysis/narrative_summaries/npc_narrative_summaries.parquet
# - data/analysis/narrative_summaries/npc_narrative_summaries.md
# and entries should include `llm_used=false`.
echo "✓ Dry-run narrative summaries complete (heuristic mode)"
echo

# 5. Single real LLM run (same corpus, different output dir)
echo ">>> Step 5: Narrative summaries with real LLM (one batch run)"

if [[ -z "${OPENAI_API_KEY:-}" && "${TB_LLM_PROVIDER}" == "openai" ]]; then
  echo "[WARN] OPENAI_API_KEY is not set; skipping real LLM run." >&2
  echo "       Set OPENAI_API_KEY and rerun this script to exercise real LLM integration."
  exit 0
fi

# Use a separate output dir so reviewers can diff against dry-run artifacts
poetry run corpus analysis summaries \
  --graph-dir data/analysis/npc_motif_graph \
  --output-dir data/analysis/narrative_summaries_llm

echo "✓ LLM-backed narrative summaries refreshed"
echo "Artifacts written to data/analysis/narrative_summaries_llm"
echo
echo "=== Smoke test complete ==="
