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

# 5. Build OpenAI batch payload (submission is manual/optional)
echo ">>> Step 5: Prepare OpenAI batch payload for LLM summaries"
LLM_OUTPUT_DIR="data/analysis/narrative_summaries_llm"
BATCH_INPUT_PATH="${LLM_OUTPUT_DIR}/batch_input.jsonl"
BATCH_OUTPUT_PATH="${LLM_OUTPUT_DIR}/batch_output.jsonl"

make analysis-summaries-batch ARGS="--graph-dir data/analysis/npc_motif_graph --output-dir ${LLM_OUTPUT_DIR}"

cat <<'EOF'
ℹ To submit the batch to OpenAI manually run:
  make analysis-summaries-batch ARGS="--graph-dir data/analysis/npc_motif_graph --output-dir data/analysis/narrative_summaries_llm --submit --completion-window 24h"
This prints the batch ID. Once it completes, download the results with:
  make analysis-summaries-batch ARGS="--graph-dir data/analysis/npc_motif_graph --output-dir data/analysis/narrative_summaries_llm --batch-id <BATCH_ID> --wait --download-output"
The downloaded file will live at data/analysis/narrative_summaries_llm/batch_output.jsonl.
EOF

echo

if [[ ! -f "${BATCH_OUTPUT_PATH}" ]]; then
  echo "[WARN] Batch output ${BATCH_OUTPUT_PATH} not found; skipping live LLM verification." >&2
  echo "       After downloading the batch output, rerun this script to validate end-to-end."
  exit 0
fi

# 6. Materialize summaries from the completed batch output
echo ">>> Step 6: Materialize LLM-backed summaries from batch output"
poetry run corpus analysis summaries \
  --graph-dir data/analysis/npc_motif_graph \
  --output-dir "${LLM_OUTPUT_DIR}"

python - <<'PY'
import json
import sys
from pathlib import Path

summary_path = Path("data/analysis/narrative_summaries_llm/npc_narrative_summaries.json")
if not summary_path.exists():
  print(f"[ERR] Expected summary JSON at {summary_path}", file=sys.stderr)
  sys.exit(1)

payload = json.loads(summary_path.read_text())
llm_count = sum(1 for entry in payload.get("summaries", []) if entry.get("llm_used"))
if llm_count == 0:
  print(
    "[ERR] No entries reported llm_used=true. Ensure the batch output contained valid responses.",
    file=sys.stderr,
  )
  sys.exit(1)

print(f"✓ Verified {llm_count} LLM-backed summaries in {summary_path}")
PY

echo "✓ LLM-backed narrative summaries refreshed"
echo "Artifacts written to ${LLM_OUTPUT_DIR}"
echo
echo "=== Smoke test complete ==="
