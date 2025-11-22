# Analysis Layer Smoke Test

To validate the full Phase 7 analysis layer (clusters → graph → summaries) on a development workstation, follow this sequence verbatim. The commands assume you are at the repository root and have access to the required Kaggle/OpenAI credentials when exercising the LLM path.

```bash
# 0. Ensure dependencies
poetry install

# 1. Curated + community coverage
make build-corpus
make community-report

# 2. Motif clustering
make analysis-clusters

# 3. NPC motif graph
make analysis-graph

# 4. Dry-run summaries (no LLM)
make analysis-summaries ARGS="--dry-run-llm"

# 5. Optional: real LLM-backed summaries
# (requires OPENAI_API_KEY; uses TB_LLM_MODEL if set, otherwise gpt-5-mini)
OPENAI_API_KEY=... TB_LLM_MODEL=gpt-5-mini make analysis-summaries
```

This sequence should complete without errors and produce artifacts under:

- `data/analysis/motif_clustering/`
- `data/analysis/npc_motif_graph/`
- `data/analysis/narrative_summaries/` (and optionally `data/analysis/narrative_summaries_llm/` if you direct LLM runs to a separate output directory)

For convenience, you can run `make analysis-smoke`, which executes `scripts/analysis_smoke_test.sh`. That helper script walks the same flow end-to-end, emits progress markers, and performs pre-flight checks for Poetry, the virtual environment, and optional LLM credentials.
