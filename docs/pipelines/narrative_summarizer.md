# Narrative Summaries Pipeline

Phase 7 introduces an LLM-backed analysis layer that turns motif graphs into
narrative briefs with citations. This document captures how the pipeline is
wired, how to configure the connector, and how to run the CLI in both LLM and
heuristic modes.

## Architecture Overview

1. `pipelines.npc_motif_graph` produces three parquet artifacts containing
   entity stats, motif stats, and lore hits.
2. `pipelines.narrative_summarizer.NarrativeSummariesPipeline` loads those
   artifacts, selects top motifs/quotes per canonical NPC, and then asks an LLM
   to produce a structured JSON summary.
3. The LLM layer is provider-agnostic via `pipelines.llm.base`:
   - `LLMConfig`: provider/model/reasoning configuration.
   - `LLMClient` protocol: requires `summarize_entity(payload)` returning a
     JSON-safe dict with `canonical_id`, `summary_text`, `motif_slugs`, and
     `supporting_quotes`.
   - `create_llm_client_from_env()`: builds a client based on environment
     variables or CLI overrides.
4. The default implementation is `pipelines.llm.openai_client.OpenAILLMClient`,
   which uses OpenAI's Responses API in JSON mode and enforces our schema.

### Model Strategy & Environment Variables

The connector favors economical bulk runs while still allowing premium or
ultra-cheap overrides:

| Mode | Model | When to use |
| --- | --- | --- |
| Default bulk (recommended) | `gpt-5-mini` | Routine batches (10k+ entities); balances quality + cost. |
| Hero / premium | `gpt-5.1` | Final codex exports or small, high-stakes runs. |
| Ultra-cheap debug | `gpt-4o-mini` | Prompt wiring + JSON-shape smoke tests. |

| Variable | Description | Default |
| --- | --- | --- |
| `OPENAI_API_KEY` | Secret used by the OpenAI connector (required for LLM mode). | _none_ |
| `TB_LLM_PROVIDER` | Provider slug passed to the factory. | `openai` |
| `TB_LLM_MODEL` | Model name for the provider. | `gpt-5-mini` |
| `TB_LLM_REASONING` | Optional reasoning effort (`none|low|medium|high`). | unset |
| `TB_LLM_MAX_OUTPUT_TOKENS` | Optional token cap per response. | unset |

Secrets should be injected through GitHub Actions or local shells (e.g., via
`direnv`). They are never stored in the repository.

### Artifact Schema Additions

Each summary entry now includes:

```json
{
  "summary_text": "...",
  "summary_motif_slugs": ["scarlet_rot", "dream_cycle"],
  "supporting_quotes": ["lore-010", "lore-011"],
  "llm_provider": "openai",
  "llm_model": "gpt-5-mini",
  "llm_used": true
}
```

`llm_used=false` indicates we fell back to the local heuristic summary for that
entity (either because `--dry-run-llm` was set or the LLM raised an
`LLMResponseError`).

## CLI Usage

Run the workflow after the motif graph has been generated:

```bash
poetry run corpus analysis graph  # generates graph artifacts
poetry run corpus analysis summaries-batch --graph-dir data/analysis/npc_motif_graph
# (optionally submit/poll/download the batch)
poetry run corpus analysis summaries --graph-dir data/analysis/npc_motif_graph
```

The CLI now has three execution modes controlled by `--llm-mode` (default
`batch`):

- `batch`: consume a downloaded OpenAI batch output (`batch_output.jsonl`).
- `per-entity`: call the LLM synchronously for the requested subset (debug).
- `heuristic`: skip LLM calls entirely (alias `--dry-run-llm`).

Key flags:

- `--llm-provider`, `--llm-model`, `--llm-reasoning`: override env defaults for
  both builder + ingestion steps.
- `--llm-mode per-entity`: force synchronous Runs for tiny subsets / debugging.
- `--llm-mode heuristic` or `--dry-run-llm`: emit heuristic summaries only.
- `--max-motifs`, `--max-quotes`: cap the context passed to the LLM and stored
  in artifacts.

Make targets forward the options via `ARGS`:

```bash
make analysis-summaries-batch                        # build payload
make analysis-summaries                              # ingest batch output
make analysis-summaries ARGS="--llm-model gpt-5.1"   # hero batch ingest
make analysis-summaries ARGS="--llm-mode per-entity --llm-model gpt-4o-mini"
make analysis-summaries ARGS="--llm-mode heuristic"  # offline fallback
```

## Testing & Local Development

- Unit tests inject fake LLM clients (`tests/test_narrative_summarizer.py`) so
  no network calls or secrets are required.
- To exercise the real connector locally, export `OPENAI_API_KEY` and run the
  CLI without `--dry-run-llm`.
- When adding new providers, implement `LLMClient`, register it inside
  `create_llm_client_from_env`, and document any additional environment
  variables.

## Failure Handling

- All LLM parsing issues raise `LLMResponseError`.
- The pipeline logs a warning and falls back to the heuristic summary, but it
  still records the attempted provider/model for observability.
- Supporting quotes are validated against the limited lore context we pass to
  the model, preventing hallucinated citations.
