# Weighted Text-Type Benchmark — 2025-11-18 (evening refresh)

## Method
- Canonical + lore rebuilds (Nov 18 nightly): `poetry run python -m pipelines.build_lore_corpus` (15,992 rows after filtering) followed by `EMBED_PROVIDER=local EMBED_MODEL=all-MiniLM-L6-v2 poetry run python -m pipelines.build_lore_embeddings --provider local --model all-MiniLM-L6-v2` (14,454 vectors written with `embedding_strategy=weighted_text_types_v1`).
- Text-type weights: `config/text_type_weights.yml` (description 1.4, impalers_excerpt 1.7, quote 1.3, lore 1.2, effect 0.7, obtained_from 0.8, drops 0.8). Inline overrides were not used in this pass.
- FAISS refresh: `poetry run python -m pipelines.build_rag_index` (14,454 vectors, dim=384, cosine via normalized IP). `rag_metadata.parquet` + `rag_index_meta.json` now record the local encoder and weighting strategy.
- Benchmark queries executed with `poetry run python -m rag.query --top-k 5 --verbose` immediately after the index rebuild (Radahn, Scarlet Rot, Fungus, Gloam-Eyed Queen, Messmer). Tables below capture the raw outputs.

## Query 1 — “Radahn gravity comet”
| Rank | Score | Category | Text Type | Source | Notes |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.468 | npc | dialogue | carian_dialogue_fmg | “If General Radahn were to die, the stars would resume their movement.”
| 2 | 0.462 | npc | dialogue | carian_dialogue_fmg | “Radahn challenged the swirling constellations … arrested their cycles.”
| 3 | 0.460 | npc | dialogue | carian_dialogue_fmg | Dialogue about Radahn’s defeat letting stars move again.
| 4 | 0.455 | npc | dialogue | carian_dialogue_fmg | Another Carian line repeating the star-cycle warning.
| 5 | 0.445 | npc | dialogue | carian_dialogue_fmg | “But General Radahn is the conqueror of the stars.”

Observations:
- Carian NPC dialogue now monopolizes the top-5 for Radahn queries; descriptive weapon/spell entries no longer appear without additional filtering.
- All five hits are unique NPC IDs, but their prose is repetitive—consider weighting dialogue slightly lower or filtering by category for lore-focused prompts.
- Spread 0.468 → 0.445 (Δ=0.023) indicates tight clustering once the model latches onto dialog phrasing.

## Query 2 — “Scarlet rot and decay”
| Rank | Score | Category | Text Type | Source | Notes |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.754 | npc | dialogue | carian_dialogue_fmg | “The scarlet rot has stilled, since last we met.”
| 2 | 0.730 | npc | dialogue | carian_dialogue_fmg | “Not one of them decayed when faced with the scarlet rot…”
| 3 | 0.690 | npc | dialogue | carian_dialogue_fmg | “The scarlet rot writhes now, worse than ever.”
| 4 | 0.677 | npc | dialogue | carian_dialogue_fmg | “If the scarlet rot hasn’t eaten her away completely.”
| 5 | 0.657 | npc | dialogue | carian_dialogue_fmg | “By the scarlet rot.”

Observations:
- Dialogue dominance persists for Scarlet Rot queries; every hit is from the Carian TalkMsg corpus.
- Score spread expands (0.754 → 0.657, Δ=0.097) but still within conversational paraphrases. Need weighting tweaks if we expect item descriptions to surface again.

## Query 3 — “fungus mushroom armor”
| Rank | Score | Category | Text Type | Source | Notes |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.586 | item | description | kaggle_dlc | Toxic mold mushroom (rot-ward crafting use).
| 2 | 0.530 | item | description | kaggle_dlc | False-night mushroom dripping oil in Eternal City.
| 3 | 0.529 | item | description | kaggle_dlc | Spongy fungal growth for throwing pots.
| 4 | 0.512 | item | impalers_excerpt | kaggle_dlc\|impalers | Red mushroom excerpt; raw meat texture copy.
| 5 | 0.510 | item | impalers_excerpt | kaggle_dlc\|impalers | Milky-white mushroom excerpt; same pot lore.

Observations:
- Narrative Impalers excerpts surface naturally (ranks 4–5) providing richer prose than plain tooltips.
- No duplicate canonical rows; each result covers a different crafting material variant.
- Spread 0.586 → 0.510 (Δ=0.076). Sources mix kaggle_dlc and kaggle_dlc\|impalers.

## Query 4 — “thorns death gloam-eyed queen”
| Rank | Score | Category | Text Type | Source | Notes |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.524 | npc | dialogue | carian_dialogue_fmg | “The thorns are impenetrable.”
| 2 | 0.501 | item | description | carian_goods_fmg | Godskin Apostle black-flame circle lore snippet.
| 3 | 0.490 | spell | description | kaggle_dlc | Same black flame incantation (duplicate wording vs. #2).
| 4 | 0.471 | item | description | carian_accessory_fmg | Legendary talisman/Marika engraving passage.
| 5 | 0.462 | npc | dialogue | carian_dialogue_fmg | “To pass the impenetrable thorns…” guidance line.

Observations:
- Dialogue still claims two slots, and the remaining entries include duplicate prose for the same incantation (item vs. spell row).
- Spread 0.524 → 0.462 (Δ=0.062). We now mix Carian + Kaggle sources but still see redundancy; dedupe heuristics across carian_goods vs. spells might help.

## Query 5 — “Messmer flame serpent blood”
| Rank | Score | Category | Text Type | Source | Notes |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.551 | armor | impalers_excerpt | kaggle_dlc\|impalers | Messmer helm excerpt (winged serpents guarding the base serpent).
| 2 | 0.531 | spell | description | kaggle_dlc | Messmer flame orb incantation (“he despised his own fire”).
| 3 | 0.531 | armor | impalers_excerpt | kaggle_dlc\|impalers | Fire Knight armor excerpt (“only ones who truly knew Messmer”).
| 4 | 0.531 | armor | impalers_excerpt | kaggle_dlc\|impalers | Duplicate helm excerpt (alternate canonical id).
| 5 | 0.530 | armor | impalers_excerpt | kaggle_dlc\|impalers | Captain Kood helm excerpt (winged serpent companion).

Observations:
- Four of five rows include `text_type_components` with `impalers_excerpt`, surfacing the requested long-form DLC lore.
- Multiple armor rows share near-identical Impalers prose; consider grouping by normalized text to reduce redundant hits.
- Spread 0.551 → 0.530 (Δ=0.021) remains tight while still mixing incantation + armor contexts.

## Conclusions vs. Acceptance Criteria
1. **Weighted embeddings produced (AC1)**: `data/embeddings/lore_embeddings.parquet` now stores 14,454 weighted vectors (384-dim MiniLM, `embedding_strategy=weighted_text_types_v1`).
2. **RAG index rebuilt (AC2)**: `faiss_index.bin`, `rag_metadata.parquet`, and `rag_index_meta.json` were regenerated from the refreshed embeddings (normalized IP search, metadata tracks provider/model).
3. **Retrieval quality**: Narrative coverage improved for mushrooms/Messmer, but Carian dialogue now overwhelms broader lore queries (Radahn, Scarlet Rot). Follow-up tuning (dialogue weight downgrade or category filters) is recommended before declaring success.
4. **Configurability / reproducibility (AC4)**: Weight config remains centralized; rerunning the pipelines with the same ENV/flags reproduces the artifacts (local MiniLM backend installed via optional `embeddings-local` extra).
5. **No regressions (AC5)**: Lore corpus + embedding pipelines completed without errors, and `rag.query` exercised the index successfully. Additional pytest coverage will run as part of the overall CI sweep.
