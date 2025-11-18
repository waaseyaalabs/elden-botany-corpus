# Weighted Text-Type Benchmark — 2025-11-18 (filter/dedup refresh)

## Method
- Rebuilt lore + canonical artifacts: `poetry run python -m pipelines.build_lore_corpus` (15,992 lore rows) followed by `EMBED_PROVIDER=local EMBED_MODEL=all-MiniLM-L6-v2 poetry run python -m pipelines.build_lore_embeddings --provider local --model all-MiniLM-L6-v2` (14,454 weighted MiniLM vectors, `embedding_strategy=weighted_text_types_v1`).
- Regenerated the FAISS index via `poetry run python -m pipelines.build_rag_index` (384-dim, normalized IP search). Metadata parquet/JSON now capture the local encoder and weighting profile.
- `rag.query` now defaults to `top_k=10`, fetches extra candidates internally, and deduplicates on normalized text so the default window remains unique-heavy. Inclusion/exclusion filters are supplied through the new `--filter` flag (e.g., `text_type!=dialogue`).
- Queries below were executed immediately after the index refresh using combinations of the default view and the new filter flag to demonstrate balance between dialogue and descriptive entries.

## Query 1 — “Radahn gravity comet”

### Default (no filters)
| Rank | Score | Category | Text Type | Source | Notes |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.468 | npc | dialogue | carian_dialogue_fmg | “If General Radahn were to die, the stars would resume their movement.” |
| 2 | 0.462 | npc | dialogue | carian_dialogue_fmg | “Radahn challenged the swirling constellations … arrested their cycles.” |
| 3 | 0.460 | npc | dialogue | carian_dialogue_fmg | “Should General Radahn be defeated, the stars would once again resume their movement.” |
| 4 | 0.455 | npc | dialogue | carian_dialogue_fmg | Variant of the star-cycle warning. |
| 5 | 0.445 | npc | dialogue | carian_dialogue_fmg | “But General Radahn is the conqueror of the stars.” |
| 6 | 0.441 | npc | dialogue | carian_dialogue_fmg | Festival intro describing Radahn as the strongest demigod. |
| 7 | 0.439 | npc | dialogue | carian_dialogue_fmg | “General Radahn, mightiest demigod of the Shattering, awaits you!” |
| 8 | 0.433 | npc | dialogue | carian_dialogue_fmg | “That Starscourge Radahn holds Ranni's fate in stasis.” |
| 9 | 0.433 | npc | dialogue | carian_dialogue_fmg | “We are pitted against Radahn, once the strongest of the demigods.” |
| 10 | 0.431 | npc | dialogue | carian_dialogue_fmg | “The conqueror of the stars, General Radahn.” |

### Filtered (`--filter text_type!=dialogue`)
| Rank | Score | Category | Text Type | Source | Notes |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.381 | weapon | description | carian_weapon_fmg | Radahn's greatarrows (Cleanrot spear lore, gravity crest). |
| 2 | 0.376 | item | description | carian_goods_fmg | Gravity sorcery mastered by young Radahn, pulls foes inward. |
| 3 | 0.368 | item | description | kaggle_dlc | DLC version of Radahn's greatarrows. |
| 4 | 0.366 | boss | drops | kaggle_dlc | Yelough Anix tunnel reward callout (Meteorite of Astel). |
| 5 | 0.361 | spell | description | kaggle_dlc | Twin spiral glintstone projectiles, failed comet experiment. |
| 6 | 0.358 | spell | description | kaggle_dlc | Classic comet sorcery (Karolos Conspectus). |
| 7 | 0.337 | spell | description | kaggle_dlc | Gravity volley that drags foes to the caster. |
| 8 | 0.331 | item | description | carian_goods_fmg | Spiral projectile variant sourced from Carian FMG. |
| 9 | 0.330 | weapon | description | kaggle_dlc | Starscourge curved greatswords with gravity crest. |
| 10 | 0.328 | boss | quote/drops | kaggle_dlc | Redmane Castle drop note + black flame lore excerpt. |

**Observations**
- Default view still skews toward TalkMsg dialogue because 8k NPC lines dwarf other text types, but dedup prevents repeated sentences from crowding the list.
- A single `text_type!=dialogue` filter immediately surfaces descriptive weapon/spell/boss entries from Carian FMGs and Kaggle DLC, demonstrating the new inclusion/exclusion plumbing.

## Query 2 — “Scarlet rot and decay” (`--filter text_type!=dialogue`)
| Rank | Score | Category | Text Type | Source | Notes |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.593 | spell | description | kaggle_base | Ekzykes scarlet rot breath incantation. |
| 2 | 0.541 | item | description | kaggle_dlc | Craftable scarlet boluses (cures rot). |
| 3 | 0.538 | item | description | carian_accessory_fmg | Pest exaltation talisman, boosts attack near rot. |
| 4 | 0.502 | weapon | description | carian_weapon_fmg | Flame greatsword keeping rot at bay in Redmane Castle. |
| 5 | 0.479 | armor | description | kaggle_dlc | Rot-eaten cloak (gravekeeper). |
| 6 | 0.479 | boss | quote/drops | kaggle_dlc | Rotten Minor Erdtree avatar blurb w/ drops. |
| 7 | 0.458 | item | description | kaggle_dlc | Scarlet rot throwables (ritual pot). |
| 8 | 0.457 | item | impalers_excerpt | kaggle_dlc\|impalers | Capacious rot pot excerpt (meat-like mushrooms). |
| 9 | 0.455 | item | description | carian_goods_fmg | Rot incantation thread lore. |
| 10 | 0.444 | spell | description | kaggle_dlc | Same rot thread incantation (effect block). |

**Observations**
- Filtering away dialogue yields a healthy mix of spells, items, armor, and boss blurbs without any manual category juggling.
- Carian accessory FMGs (talismans) now appear beside Kaggle DLC descriptions, proving the alias-aware ingestion is wired through to retrieval.

## Query 3 — “fungus mushroom armor” (default top_k=10)
| Rank | Score | Category | Text Type | Source | Notes |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.586 | item | description | kaggle_dlc | Toxic mold mushroom used as rot ward. |
| 2 | 0.530 | item | description | kaggle_dlc | False-night mushroom dripping oil. |
| 3 | 0.529 | item | description | kaggle_dlc | Spongy fungal growth for throwing pots. |
| 4 | 0.512 | item | impalers_excerpt | kaggle_dlc\|impalers | Red mushroom excerpt (raw meat texture). |
| 5 | 0.510 | item | impalers_excerpt | kaggle_dlc\|impalers | Milky-white mushroom excerpt. |
| 6 | 0.472 | armor | description | kaggle_dlc | Mushroom crown boosting attack near rot. |
| 7 | 0.454 | armor | description | kaggle_dlc | Mushroom chestpiece (“holy vestments”). |
| 8 | 0.449 | armor | description | kaggle_dlc | Mushroom headpiece variant. |
| 9 | 0.446 | armor | description | kaggle_dlc | Mushroom arms. |
| 10 | 0.442 | armor | description | kaggle_dlc | Mushroom legs. |

**Observations**
- Dedup keeps the two Impalers excerpts (red/milky) at a single occurrence each despite identical canonical IDs sharing text in earlier runs.
- Narrative mix spans crafting materials plus full armor set descriptions, matching the prompt intent.

## Query 4 — “thorns death gloam-eyed queen” (`--filter text_type!=dialogue`)
| Rank | Score | Category | Text Type | Source | Notes |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.501 | item | description | carian_goods_fmg | Godskin Apostle black flame circle (Gloam-Eyed lore). |
| 2 | 0.490 | spell | description | kaggle_dlc | Same incantation, gameplay-focused wording. |
| 3 | 0.471 | item | description | carian_accessory_fmg | Legendary talisman engraved with Marika’s rune. |
| 4 | 0.460 | item | description | carian_goods_fmg | Thorn sorcery discovered by exiled criminals. |
| 5 | 0.458 | item | description | carian_accessory_fmg | Radagon rune talisman (stat boost + damage taken). |
| 6 | 0.453 | spell | description | kaggle_dlc | Thorn trail sorcery (punishment variant). |
| 7 | 0.447 | item | description | carian_goods_fmg | Spiral bloodthorn sorcery description. |
| 8 | 0.446 | item | description | kaggle_dlc | Gurranq's Deathroot eye.
| 9 | 0.444 | weapon | description | kaggle_dlc | Sacred sword of the Gloam-Eyed Queen. |
| 10 | 0.444 | item | description | kaggle_dlc | Purifying crystal tear for Mohg's curse. |

**Observations**
- Filtered view mixes Carian FMG descriptions (Marika/Radagon talismans) with Kaggle DLC spells, eliminating the duplicate dialogue problem seen in the previous report.
- Boss weapon lore now shows up alongside consumables, giving annotators broader context for the Gloam-Eyed storyline.

## Query 5 — “Messmer flame serpent blood” (default top_k=10)
| Rank | Score | Category | Text Type | Source | Notes |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.551 | armor | impalers_excerpt | kaggle_dlc\|impalers | Messmer helm (winged serpents guarding sealed serpent). |
| 2 | 0.531 | spell | description | kaggle_dlc | Messmer flame orb incantation. |
| 3 | 0.531 | armor | impalers_excerpt | kaggle_dlc\|impalers | Fire Knight armor excerpt (only ones who knew Messmer). |
| 4 | 0.531 | armor | impalers_excerpt | kaggle_dlc\|impalers | Alternate helm text (Messmer's fire phrasing). |
| 5 | 0.530 | armor | impalers_excerpt | kaggle_dlc\|impalers | Captain Kood helm (winged serpent companion). |
| 6 | 0.515 | weapon | impalers_excerpt | kaggle_dlc\|impalers | Fire snake flail used to brand foes. |
| 7 | 0.511 | item | impalers_excerpt | kaggle_dlc\|impalers | Fire-snake coil explosive (craftable). |
| 8 | 0.484 | item | impalers_excerpt | kaggle_dlc\|impalers | Kindling that burned inside Messmer. |
| 9 | 0.472 | spell | description | kaggle_dlc | Fire Knight serpentine flame incantation. |
| 10 | 0.459 | item | impalers_excerpt | kaggle_dlc\|impalers | Serpentine ember crafting material. |

**Observations**
- Impalers excerpts dominate (as expected) but remain unique thanks to normalized-text dedup pushing structurally identical prose beyond the top_k window.
- Spell and crafting entries from Kaggle DLC still surface, so cross-source corroboration remains intact.

## Conclusions vs. Acceptance Criteria
1. **Pipelines verified**: Lore, embeddings, and FAISS commands completed successfully with the local MiniLM encoder, producing 14,454 vectors and refreshed metadata artifacts.
2. **Default behavior (AC1)**: `rag.query` now returns 10 results by default; tests plus the benchmark runs confirm the new default and `--top-k` override.
3. **Filtering (AC2)**: Inclusion/exclusion filters (`--filter text_type!=dialogue`) unlock descriptive/boss-heavy views without sacrificing TalkMsg coverage.
4. **Deduplication (AC3)**: Query helper retrieves padded candidate sets and removes near-identical text before slicing, so top_k windows contain unique prose unless the corpus genuinely lacks variety.
5. **Carian coverage (AC4)**: Weapon, accessory, boss, and TalkMsg FMGs—including fallback aliases such as `ArtsName.fmg.xml`—flow through the canonical + lore pipelines and now appear alongside Kaggle DLC rows in the retrieval results.
6. **Docs & evaluation (AC5)**: This report, together with the README updates, documents the new default `top_k`, filter syntax, deduplication strategy, and the end-to-end pipeline run.
7. **Reranker foundation (AC6)**: The CLI now exposes a `--reranker` flag backed by the new `rag.reranker` module, enabling future cross-encoder experimentation.
