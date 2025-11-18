# Weighted Text-Type Benchmark — 2025-11-18 (filter/dedup + reranker refresh)

## Method
- Rebuilt lore + canonical artifacts: `poetry run python -m pipelines.build_lore_corpus` (15,992 lore rows) followed by `EMBED_PROVIDER=local EMBED_MODEL=all-MiniLM-L6-v2 poetry run python -m pipelines.build_lore_embeddings --provider local --model all-MiniLM-L6-v2` (14,454 weighted MiniLM vectors, `embedding_strategy=weighted_text_types_v1`).
- Regenerated the FAISS index via `poetry run python -m pipelines.build_rag_index` (384-dim, normalized IP search). Metadata parquet/JSON now capture the local encoder and weighting profile.
- `rag.query` now defaults to `top_k=10`, fetches extra candidates internally, and deduplicates on normalized text so the default window remains unique-heavy. Inclusion/exclusion filters are supplied through the new `--filter` flag (e.g., `text_type!=dialogue`).
- Queries below were executed immediately after the index refresh using combinations of the default view and the new filter flag to demonstrate balance between dialogue and descriptive entries.
- Captured embedding-only vs. cross-encoder (`cross-encoder/ms-marco-MiniLM-L-6-v2`) reranked outputs for every prompt via a helper script. Raw captures live in `eval/reranker_benchmark.json` for reproducibility.

## Query 1 — “Radahn gravity comet”

### Embedding-only (balanced, no filters)
| Rank | Score | Category | Text Type | Source | Snippet |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.381 | weapon | description | carian_weapon_fmg | Description: Greatarrows used by General Radahn during the festival of combat... |
| 2 | 0.468 | npc | dialogue | carian_dialogue_fmg | Dialogue: If General Radahn were to die, the stars would resume their movement. |
| 3 | 0.376 | item | description | carian_goods_fmg | Description: One of the glintstone sorceries that manipulates gravitational f... |
| 4 | 0.462 | npc | dialogue | carian_dialogue_fmg | Dialogue: But long ago, General Radahn challenged the swirling constellations... |
| 5 | 0.460 | npc | dialogue | carian_dialogue_fmg | Dialogue: Should General Radahn be defeated, the stars would once again resum... |
| 6 | 0.455 | npc | dialogue | carian_dialogue_fmg | Dialogue: And so, if General Radahn were defeated, the stars would once again... |
| 7 | 0.445 | npc | dialogue | carian_dialogue_fmg | Dialogue: But General Radahn is the conqueror of the stars. |
| 8 | 0.441 | npc | dialogue | carian_dialogue_fmg | Dialogue: Now, we find ourselves at a festival of combat, pitted against Rada... |
| 9 | 0.439 | npc | dialogue | carian_dialogue_fmg | Dialogue: General Radahn, mightiest demigod of the Shattering, awaits you! |
| 10 | 0.433 | npc | dialogue | carian_dialogue_fmg | Dialogue: That Starscourge Radahn holds Ranni's fate in stasis. |

### Cross-encoder reranked (balanced, no filters)
| Rank | Score | Category | Text Type | Source | Snippet |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.376 | item | description | carian_goods_fmg | Description: One of the glintstone sorceries that manipulates gravitational f... |
| 2 | 0.462 | npc | dialogue | carian_dialogue_fmg | Dialogue: But long ago, General Radahn challenged the swirling constellations... |
| 3 | 0.366 | boss | drops | kaggle_dlc | Drops: Yelough Anix Tunnel: 120,000; Meteorite of Astel |
| 4 | 0.381 | weapon | description | carian_weapon_fmg | Description: Greatarrows used by General Radahn during the festival of combat... |
| 5 | 0.445 | npc | dialogue | carian_dialogue_fmg | Dialogue: But General Radahn is the conqueror of the stars. |
| 6 | 0.368 | item | description | kaggle_dlc | Description: Greatarrows used by the General Radahns during the festival of c... |
| 7 | 0.401 | npc | dialogue | carian_dialogue_fmg | Dialogue: General Radahn. |
| 8 | 0.361 | spell | description | kaggle_dlc | Description: One of the glintstone sorceries of the Academy of Raya Lucaria.... |
| 9 | 0.439 | npc | dialogue | carian_dialogue_fmg | Dialogue: General Radahn, mightiest demigod of the Shattering, awaits you! |
| 10 | 0.358 | spell | description | kaggle_dlc | Description: One of the glintstone sorceries of the Academy of Raya Lucaria.F... |

### Filtered (text_type!=dialogue)
| Rank | Score | Category | Text Type | Source | Snippet |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.381 | weapon | description | carian_weapon_fmg | Description: Greatarrows used by General Radahn during the festival of combat... |
| 2 | 0.366 | boss | drops | kaggle_dlc | Drops: Yelough Anix Tunnel: 120,000; Meteorite of Astel |
| 3 | 0.328 | boss | quote | kaggle_dlc | Quote: The Red Lion General wielded gravitational powers which he learned in... |
| 4 | 0.376 | item | description | carian_goods_fmg | Description: One of the glintstone sorceries that manipulates gravitational f... |
| 5 | 0.285 | boss | drops | kaggle_dlc | Drops: Volcano Cave: 11,000; Jar Cannon |
| 6 | 0.368 | item | description | kaggle_dlc | Description: Greatarrows used by the General Radahns during the festival of c... |
| 7 | 0.281 | boss | drops | kaggle_dlc | Drops: Rauh Base: 210,000; Roar of Rugalea |
| 8 | 0.361 | spell | description | kaggle_dlc | Description: One of the glintstone sorceries of the Academy of Raya Lucaria.... |
| 9 | 0.358 | spell | description | kaggle_dlc | Description: One of the glintstone sorceries of the Academy of Raya Lucaria.F... |
| 10 | 0.337 | spell | description | kaggle_dlc | Description: One of the glintstone sorceries that manipulates gravitational f... |

### Filtered + cross-encoder reranked
| Rank | Score | Category | Text Type | Source | Snippet |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.337 | spell | description | kaggle_dlc | Description: One of the glintstone sorceries that manipulates gravitational f... |
| 2 | 0.328 | boss | quote | kaggle_dlc | Quote: The Red Lion General wielded gravitational powers which he learned in... |
| 3 | 0.281 | boss | drops | kaggle_dlc | Drops: Rauh Base: 210,000; Roar of Rugalea |
| 4 | 0.376 | item | description | carian_goods_fmg | Description: One of the glintstone sorceries that manipulates gravitational f... |
| 5 | 0.366 | boss | drops | kaggle_dlc | Drops: Yelough Anix Tunnel: 120,000; Meteorite of Astel |
| 6 | 0.305 | spell | description | kaggle_dlc | Description: One of the glintstone sorceries that manipulates gravitational f... |
| 7 | 0.285 | boss | drops | kaggle_dlc | Drops: Volcano Cave: 11,000; Jar Cannon |
| 8 | 0.330 | weapon | description | kaggle_dlc | Description: Curved greatswords of black steel wielded by General Radahn. A p... |
| 9 | 0.322 | item | description | carian_goods_fmg | Description: One of the glintstone sorceries that manipulates gravitational f... |
| 10 | 0.312 | item | description | kaggle_dlc | Description: Remembrance of Starscourge Radahn, hewn into the Erdtree. The po... |

**Observations**
- Default view still skews toward TalkMsg dialogue because 8k NPC lines dominate, but dedup prevents repeated sentences. The cross-encoder reranker pulls descriptive Carian FMG lore, Kaggle drops, and meta-gravity spells back into the top 10 without filters.
- A single `text_type!=dialogue` filter surfaces descriptive weapon/spell/boss entries. With reranking enabled the gravitational sorceries, great rune lore, and boss loot share roughly equal slots, demonstrating the diversity target for Phase 4/5.

## Query 2 — “Scarlet rot and decay” (`text_type!=dialogue`)

### Embedding-only
| Rank | Score | Category | Text Type | Source | Snippet |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.593 | spell | description | kaggle_base | Description: Spews scarlet rot breath of Ekzykes from above Effect: ??? |
| 2 | 0.457 | item | impalers_excerpt | kaggle_dlc|impalers | Impalers Excerpt: Craftable item prepared using a capacious cracked pot. A go... |
| 3 | 0.479 | boss | quote | kaggle_dlc | Quote: Guardians of the Minor Erdtrees, infested with Scarlet Rot. Their larg... |
| 4 | 0.541 | item | description | kaggle_dlc | Description: Scarlet boluses made of cave moss.Craftable item. Alleviates sca... |
| 5 | 0.538 | item | description | carian_accessory_fmg | Description: A talisman depicting the exultation of pests. Raises attack powe... |
| 6 | 0.502 | weapon | description | carian_weapon_fmg | Description: Greatsword featuring a flame-like undulation. Shreds enemy flesh... |
| 7 | 0.479 | armor | description | kaggle_dlc | Description: Thick, bristly cloak eaten through by scarlet rot.The symbol of... |
| 8 | 0.458 | item | description | kaggle_dlc | Description: Craftable item prepared using a ritual pot. Decorated with the c... |
| 9 | 0.455 | item | description | carian_goods_fmg | Description: Incantation of the servants of rot. Secretes countless sticky th... |
| 10 | 0.444 | spell | description | kaggle_dlc | Description: Incantation of the servants of rot. Secretes countless sticky th... |

### Cross-encoder reranked
| Rank | Score | Category | Text Type | Source | Snippet |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.541 | item | description | kaggle_dlc | Description: Scarlet boluses made of cave moss.Craftable item. Alleviates sca... |
| 2 | 0.457 | item | impalers_excerpt | kaggle_dlc|impalers | Impalers Excerpt: Craftable item prepared using a capacious cracked pot. A go... |
| 3 | 0.479 | boss | quote | kaggle_dlc | Quote: Guardians of the Minor Erdtrees, infested with Scarlet Rot. Their larg... |
| 4 | 0.392 | boss | drops | kaggle_dlc | Drops: Shadow Keep: 150,000; Aspects of the Crucible: Thorns; Scadutree Fragm... |
| 5 | 0.432 | spell | description | kaggle_dlc | Description: Technique of Malenia, the Goddess of Rot. Creates a gigantic flo... |
| 6 | 0.433 | item | impalers_excerpt | kaggle_dlc|impalers | Impalers Excerpt: A large, rotten bud that will never come into bloom. Materi... |
| 7 | 0.390 | boss | quote | kaggle_dlc | Quote: In the time when there was no Erdtree, death was burned in ghostflame.... |
| 8 | 0.434 | item | description | carian_goods_fmg | Description: Technique of Malenia, the Goddess of Rot. Creates a gigantic flo... |
| 9 | 0.378 | weapon | impalers_excerpt | kaggle_dlc|impalers | Impalers Excerpt: Weapon of Romina, Saint of the Bud. A scarlet glaive with a... |
| 10 | 0.458 | item | description | kaggle_dlc | Description: Craftable item prepared using a ritual pot. Decorated with the c... |

**Observations**
- Filtering away dialogue yields a healthy mix of spells, items, armor, and boss blurbs without manual category juggling.
- The reranker further amplifies restorative items (scarlet boluses) and Romina/Malenia story beats, improving thematic continuity for rot-heavy prompts without reintroducing dialogue noise.

## Query 3 — “fungus mushroom armor”

### Embedding-only (balanced, no filters)
| Rank | Score | Category | Text Type | Source | Snippet |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.586 | item | description | kaggle_dlc | Description: A mushroom covered in toxic mold that grows in rotten lands.Mate... |
| 2 | 0.512 | item | impalers_excerpt | kaggle_dlc|impalers | Impalers Excerpt: Red mushroom that grows in sullen lands. Material used for... |
| 3 | 0.530 | item | description | kaggle_dlc | Description: A mushroom that grows in the false night in and around the Etern... |
| 4 | 0.510 | item | impalers_excerpt | kaggle_dlc|impalers | Impalers Excerpt: Milky-white mushroom that grows in sullen lands. Material u... |
| 5 | 0.529 | item | description | kaggle_dlc | Description: A fungal growth that thrives in damp thickets and elsewhere.Mate... |
| 6 | 0.419 | item | impalers_excerpt | kaggle_dlc|impalers | Impalers Excerpt: A species of fungus known for its deathly sweet stench. Mat... |
| 7 | 0.472 | armor | description | kaggle_dlc | Description: Mushrooms found growing all over the body. These overgrown mushr... |
| 8 | 0.405 | armor | impalers_excerpt | kaggle_dlc|impalers | Impalers Excerpt: Armor of the broken drake warrior Igon, from a set comprise... |
| 9 | 0.454 | armor | description | kaggle_dlc | Description: Mushrooms found growing all over the body. These overgrown mushr... |
| 10 | 0.449 | armor | description | kaggle_dlc | Description: Mushrooms found growing all over the body. These overgrown mushr... |

### Cross-encoder reranked (balanced, no filters)
| Rank | Score | Category | Text Type | Source | Snippet |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.472 | armor | description | kaggle_dlc | Description: Mushrooms found growing all over the body. These overgrown mushr... |
| 2 | 0.512 | item | impalers_excerpt | kaggle_dlc|impalers | Impalers Excerpt: Red mushroom that grows in sullen lands. Material used for... |
| 3 | 0.586 | item | description | kaggle_dlc | Description: A mushroom covered in toxic mold that grows in rotten lands.Mate... |
| 4 | 0.510 | item | impalers_excerpt | kaggle_dlc|impalers | Impalers Excerpt: Milky-white mushroom that grows in sullen lands. Material u... |
| 5 | 0.446 | armor | description | kaggle_dlc | Description: Mushrooms found growing all over the body. These overgrown mushr... |
| 6 | 0.419 | item | impalers_excerpt | kaggle_dlc|impalers | Impalers Excerpt: A species of fungus known for its deathly sweet stench. Mat... |
| 7 | 0.454 | armor | description | kaggle_dlc | Description: Mushrooms found growing all over the body. These overgrown mushr... |
| 8 | 0.405 | armor | impalers_excerpt | kaggle_dlc|impalers | Impalers Excerpt: Armor of the broken drake warrior Igon, from a set comprise... |
| 9 | 0.530 | item | description | kaggle_dlc | Description: A mushroom that grows in the false night in and around the Etern... |
| 10 | 0.449 | armor | description | kaggle_dlc | Description: Mushrooms found growing all over the body. These overgrown mushr... |

**Observations**
- Dedup keeps Impalers excerpts (red/milky) at one occurrence each despite identical canonical IDs sharing text in earlier runs.
- Reranking nudges the armor descriptions slightly higher, keeping the mushroom set adjacent to crafting ingredients so annotators see gear context first.

## Query 4 — “thorns death gloam-eyed queen” (`text_type!=dialogue`)

### Embedding-only
| Rank | Score | Category | Text Type | Source | Snippet |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.501 | item | description | carian_goods_fmg | Description: Superior black flame incantation of the Godskin Apostles. Summon... |
| 2 | 0.410 | weapon | impalers_excerpt | kaggle_dlc|impalers | Impalers Excerpt: Light greatsword with gold inlaid. Weapon of Leda, the Need... |
| 3 | 0.422 | boss | drops | kaggle_dlc | Drops: War-Dead Catacombs: 64,000; Redmane Knight Ogha Spirit Ashes; Golden S... |
| 4 | 0.409 | boss | quote | kaggle_dlc | Quote: Valiant Gargoyles, mended with blackened corpse wax. Such is the mark... |
| 5 | 0.490 | spell | description | kaggle_dlc | Description: Superior black flame incantation of the Godskin Apostles. Summon... |
| 6 | 0.408 | boss | quote | kaggle_dlc | Quote: Guardians of the Minor Erdtrees, infested with Scarlet Rot. Their larg... |
| 7 | 0.471 | item | description | carian_accessory_fmg | Description: This legendary talisman is an eye engraved with an Elden Rune, s... |
| 8 | 0.404 | boss | quote | kaggle_dlc | Quote: A Vicious beast that has lost all reason, attacking indiscriminately.... |
| 9 | 0.460 | item | description | carian_goods_fmg | Description: An aberrant sorcery discovered by exiled criminals. Theirs are t... |
| 10 | 0.458 | item | description | carian_accessory_fmg | Description: This legendary talisman is an eye engraved with an Elden Rune, s... |

### Cross-encoder reranked
| Rank | Score | Category | Text Type | Source | Snippet |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.414 | item | description | carian_accessory_fmg | Description: Sacred cloth of the Godskin Apostles, made from supple skin sewn... |
| 2 | 0.400 | item | impalers_excerpt | kaggle_dlc|impalers | Impalers Excerpt: A Great Rune relinquished by Miquella. Broken and bereft of... |
| 3 | 0.408 | boss | quote | kaggle_dlc | Quote: Guardians of the Minor Erdtrees, infested with Scarlet Rot. Their larg... |
| 4 | 0.422 | boss | drops | kaggle_dlc | Drops: War-Dead Catacombs: 64,000; Redmane Knight Ogha Spirit Ashes; Golden S... |
| 5 | 0.490 | spell | description | kaggle_dlc | Description: Superior black flame incantation of the Godskin Apostles. Summon... |
| 6 | 0.410 | weapon | impalers_excerpt | kaggle_dlc|impalers | Impalers Excerpt: Light greatsword with gold inlaid. Weapon of Leda, the Need... |
| 7 | 0.409 | boss | quote | kaggle_dlc | Quote: Valiant Gargoyles, mended with blackened corpse wax. Such is the mark... |
| 8 | 0.501 | item | description | carian_goods_fmg | Description: Superior black flame incantation of the Godskin Apostles. Summon... |
| 9 | 0.387 | armor | impalers_excerpt | kaggle_dlc|impalers | Impalers Excerpt: Robe of Count Ymir, High Priest. The front is open, exposin... |
| 10 | 0.404 | boss | quote | kaggle_dlc | Quote: A Vicious beast that has lost all reason, attacking indiscriminately.... |

**Observations**
- Filtered view mixes Carian FMG descriptions (Marika/Radagon talismans) with Kaggle DLC spells, eliminating the duplicate dialogue problem from the previous report.
- The reranker prefers lore-rich Godskin textiles and Miquella’s rune over raw drop tables, so thorn-centric narratives lead the page but drops remain present for sourcing.

## Query 5 — “Messmer flame serpent blood”

### Embedding-only (balanced, no filters)
| Rank | Score | Category | Text Type | Source | Snippet |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.531 | spell | description | kaggle_dlc | Description: An incantation of Messmer the Impaler. Summons Messmer's flame t... |
| 2 | 0.551 | armor | impalers_excerpt | kaggle_dlc|impalers | Impalers Excerpt: Black helm of Messmer the Impaler, crowned with two intertw... |
| 3 | 0.448 | npc | dialogue | carian_dialogue_fmg | Dialogue: Join the Serpent King, as family... |
| 4 | 0.436 | boss | quote | kaggle_dlc | Quote: Join the Serpent King, as family... Together, we will devour the very... |
| 5 | 0.472 | spell | description | kaggle_dlc | Description: Incantation of the Fire Knights under Messmer the Impaler's pers... |
| 6 | 0.531 | armor | impalers_excerpt | kaggle_dlc|impalers | Impalers Excerpt: Armor of the Fire Knights under Messmer the Impaler's perso... |
| 7 | 0.439 | npc | dialogue | carian_dialogue_fmg | Dialogue: Ahh, the flame of chaos has nestled within you. |
| 8 | 0.424 | item | description | carian_gem_fmg | Description: This Ash of War grants an armament the Blood affinity and the fo... |
| 9 | 0.531 | armor | impalers_excerpt | kaggle_dlc|impalers | Impalers Excerpt: Black helm of Messmer the Impaler, crowned with two intertw... |
| 10 | 0.437 | npc | dialogue | carian_dialogue_fmg | Dialogue: Let me be your serpent... |

### Cross-encoder reranked (balanced, no filters)
| Rank | Score | Category | Text Type | Source | Snippet |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.472 | spell | description | kaggle_dlc | Description: Incantation of the Fire Knights under Messmer the Impaler's pers... |
| 2 | 0.551 | armor | impalers_excerpt | kaggle_dlc|impalers | Impalers Excerpt: Black helm of Messmer the Impaler, crowned with two intertw... |
| 3 | 0.416 | npc | dialogue | carian_dialogue_fmg | Dialogue: The serpent that lurked in the shadows that night... |
| 4 | 0.411 | boss | quote | kaggle_dlc | Quote: The great serpent of Mt. Gelmir that swallowed Praetor Rykard. Attacks... |
| 5 | 0.531 | spell | description | kaggle_dlc | Description: An incantation of Messmer the Impaler. Summons Messmer's flame t... |
| 6 | 0.531 | armor | impalers_excerpt | kaggle_dlc|impalers | Impalers Excerpt: Armor of the Fire Knights under Messmer the Impaler's perso... |
| 7 | 0.422 | npc | dialogue | carian_dialogue_fmg | Dialogue: Then please. Kill the great serpent. |
| 8 | 0.436 | boss | quote | kaggle_dlc | Quote: Join the Serpent King, as family... Together, we will devour the very... |
| 9 | 0.424 | spell | description | kaggle_dlc | Description: The terrible power of Rykard, Lord of Blasphemy. Summons searing... |
| 10 | 0.531 | armor | impalers_excerpt | kaggle_dlc|impalers | Impalers Excerpt: Black helm of Messmer the Impaler, crowned with two intertw... |

**Observations**
- Impalers excerpts dominate (as expected) but remain unique thanks to normalized-text dedup pushing structurally identical prose beyond the top_k window.
- The reranker boosts Fire Knight incantations/armor to the very top, so narrative-heavy Messmer lore frames the remaining dialogue instead of the other way around.

## Conclusions vs. Acceptance Criteria
1. **Pipelines verified**: Lore, embeddings, and FAISS commands completed successfully with the local MiniLM encoder, producing 14,454 vectors and refreshed metadata artifacts.
2. **Default behavior (AC1)**: `rag.query` now returns 10 results by default; tests plus the benchmark runs confirm the new default and `--top-k` override.
3. **Filtering (AC2)**: Inclusion/exclusion filters (`--filter text_type!=dialogue`) unlock descriptive/boss-heavy views without sacrificing TalkMsg coverage.
4. **Deduplication (AC3)**: Query helper retrieves padded candidate sets and removes near-identical text before slicing, so top_k windows contain unique prose unless the corpus genuinely lacks variety.
5. **Carian coverage (AC4)**: Weapon, accessory, boss, and TalkMsg FMGs—including fallback aliases such as `ArtsName.fmg.xml`—flow through the canonical + lore pipelines and now appear alongside Kaggle DLC rows in the retrieval results.
6. **Docs & evaluation (AC5)**: This report, together with the README updates, documents the new default `top_k`, filter syntax, deduplication strategy, and the end-to-end pipeline run.
7. **Reranker validation (AC6)**: The CLI exposes a `--reranker` flag backed by `rag.reranker`, the benchmark tables document embedding-only vs. cross-encoder ordering for all five prompts, and raw captures are stored in `eval/reranker_benchmark.json` for auditability.
