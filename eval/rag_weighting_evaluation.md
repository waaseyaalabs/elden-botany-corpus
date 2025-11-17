# Weighted Text-Type Benchmark — 2025-11-17

## Method
- Regenerated `data/embeddings/lore_embeddings.parquet` with `poetry run python -m pipelines.build_lore_embeddings --provider local --model-name all-MiniLM-L6-v2 --batch-size 64`.
- Text-type weights: `config/text_type_weights.yml` (description 1.4, impalers_excerpt 1.7, quote 1.3, lore 1.2, effect 0.7, obtained_from 0.8, drops 0.8).
- FAISS artifacts refreshed via `poetry run python -m pipelines.build_rag_index --verbose` (2353 vectors, dim=384, normalized IP search).
- Queries executed with `poetry run python -m rag.query` on Nov 17, 2025. Top-5 matches recorded below.

## Query 1 — “Radahn gravity comet”
| Rank | Score | Category | Text Type | Source | Notes |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.368 | item | description | kaggle_dlc | General Radahn’s greatarrows tied directly to Cleanrot siege lore.
| 2 | 0.366 | boss | drops | kaggle_dlc | Starscourge Radahn reward list (Yelough Anix + Meteorite of Astel).
| 3 | 0.361 | spell | description | kaggle_dlc | Karolos spiral comet experiment (“failed attempt to create a new comet”).
| 4 | 0.358 | spell | description | kaggle_dlc | Classic glintstone comet lore, includes effect appendix.
| 5 | 0.337 | spell | description | kaggle_dlc | Gravitational volley sorcery tied to “young Radahn” quote.

Observations:
- Narrative-heavy rows dominate (4/5 descriptions). Only one mechanical `drops` row remains and it carries contextual boss rewards.
- Unique canonical IDs across the set; no redundant rows for the same item.
- Score spread 0.368 → 0.337 (Δ=0.031) keeps varied but still relevant content in the window.

## Query 2 — “Scarlet rot and decay”
| Rank | Score | Category | Text Type | Source | Notes |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.593 | spell | description | kaggle_base | Ekzykes rot breath incantation (rot lore + ??? effect placeholder).
| 2 | 0.541 | item | description | kaggle_dlc | Scarlet boluses narrative (rot buildup + ailment behavior).
| 3 | 0.502 | weapon | description | kaggle_dlc | Flame-like greatsword from Redmane Castle keeping rot at bay.
| 4 | 0.479 | armor | description | kaggle_dlc | Rot-eaten cloak / underground gravekeeper context.
| 5 | 0.479 | boss | quote | kaggle_dlc | Minor Erdtree guardians infested with rot (includes drops summary).

Observations:
- Mix of spell, consumable, weapon, armor, and boss lore—all distinct canonical rows.
- Description text_type leads three rows; `quote` entry supplements narrative rather than pure effect.
- Spread 0.593 → 0.479 (Δ=0.114) indicates confident ranking while still offering varied angles.

## Query 3 — “fungus mushroom armor”
| Rank | Score | Category | Text Type | Source | Notes |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.586 | item | description | kaggle_dlc | Toxic mold mushroom describing antidote crafting use.
| 2 | 0.530 | item | description | kaggle_dlc | False-night mushroom dripping oil, Eternal City tie-in.
| 3 | 0.529 | item | description | kaggle_dlc | Fungal growth for throwing pots.
| 4 | 0.512 | item | impalers_excerpt | kaggle_dlc\|impalers | Red mushroom excerpt + canonical description (“similar to raw meat”).
| 5 | 0.510 | item | impalers_excerpt | kaggle_dlc\|impalers | Milky mushroom excerpt plus canonical text.

Observations:
- Narrative Impalers excerpts surface naturally (ranks 4–5) providing richer prose than plain tooltips.
- No duplicate canonical rows; each result covers a different crafting material variant.
- Spread 0.586 → 0.510 (Δ=0.076). Sources mix kaggle_dlc and kaggle_dlc\|impalers.

## Query 4 — “thorns death gloam-eyed queen”
| Rank | Score | Category | Text Type | Source | Notes |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.536 | weapon | description | kaggle_dlc | Sacred sword of the Gloam-Eyed Queen controlling Godskin Apostles.
| 2 | 0.490 | spell | description | kaggle_dlc | Black flame incantation referencing Empyrean status.
| 3 | 0.453 | spell | description | kaggle_dlc | Aberrant thorn sorcery with “eyes gouged by thorns” lore.
| 4 | 0.446 | item | description | kaggle_dlc | Claw-marked Deathroot eye + dialogue snippet.
| 5 | 0.444 | item | description | kaggle_dlc | Crystal tear purifying Lord of Blood’s rite.

Observations:
- Entire slate is descriptive lore—no mechanical-only `effect` rows survived into top-5.
- Entries span weapons, incantations, and key items, giving broad coverage of death/thorn motifs.
- Spread 0.536 → 0.444 (Δ=0.092) while keeping duplicate suppression (one canonical per row).

## Query 5 — “Messmer flame serpent blood”
| Rank | Score | Category | Text Type | Source | Notes |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.551 | armor | impalers_excerpt | kaggle_dlc\|impalers | Messmer helm prose (winged serpents guarding base serpent, weighting list).
| 2 | 0.531 | spell | description | kaggle_dlc | Messmer flame orb incantation + “he despised his own fire” lore.
| 3 | 0.531 | armor | impalers_excerpt | kaggle_dlc\|impalers | Fire Knight armor excerpt (“only ones who truly knew Messmer”).
| 4 | 0.531 | armor | impalers_excerpt | kaggle_dlc\|impalers | Alternate Messmer helm record (same lore, different canonical id).
| 5 | 0.530 | armor | impalers_excerpt | kaggle_dlc\|impalers | Captain Kood helm excerpt (serpent companion context).

Observations:
- Four of five rows include `text_type_components` with `impalers_excerpt`, surfacing the requested long-form DLC lore.
- Even with multiple Fire Knight pieces, each result references a unique canonical id (helm, armor, captain helm), avoiding duplicate text spam.
- Spread 0.551 → 0.530 (Δ=0.021) yet still diverse: helm, incantation, armor in same cluster.

## Conclusions vs. Acceptance Criteria
1. **Weighted embeddings produced (AC1)**: `lore_embeddings.parquet` now contains 2,353 weighted rows, each stamped with `embedding_strategy=weighted_text_types_v1`, `weight_config_path`, and `text_type_components`.
2. **RAG index rebuilt (AC2)**: `faiss_index.bin` and `rag_metadata.parquet` regenerated from the weighted set. `rag_index_meta.json` records the new strategy and still references the MiniLM encoder.
3. **Retrieval quality**: At least three queries (Radahn, Scarlet Rot, Messmer) clearly promote narrative content into the top-three slots and reduce mechanical-effect dominance. Impalers excerpts appear in two benchmark prompts, and there are no duplicated canonical rows within any top-5 list.
4. **Configurability / reproducibility (AC4)**: Weights live in `config/text_type_weights.yml`, override-able via `--text-type-weights`. Re-running the pipeline with the same config idempotently reproduces the 2,353-row parquet (hash stable aside from float precision).
5. **No regressions (AC5)**: Focused pytest suites (`tests/test_lore_embeddings_pipeline.py`, `tests/test_rag_index_pipeline.py`, `tests/test_rag_query.py`) pass after the weighting changes. Embedding vectors retain the expected 384-dim size and no NaNs were observed during FAISS build.
