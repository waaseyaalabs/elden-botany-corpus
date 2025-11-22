# Motif Taxonomy Coverage

This report summarizes how often each motif appears in the current curated lore corpus.

| Motif | Category | Matches | Coverage % | Sample IDs |
| --- | --- | --- | --- | --- |
| Trickster & Mask | Archetypal | 55 | 1.30 | thiolliers_mask, caterpillar_mask, wise_mans_mask, death_mask_helm, death_knight_helm |
| Guardian & Threshold | Archetypal | 54 | 1.28 | blackflame_monk_hood, fire_monk_hood, guardian_mask, blackflame_monk_armor, fire_prelate_armor |
| Pilgrim & Journey | Archetypal | 55 | 1.30 | aristocrat_hat, aristocrat_headband, braves_cord_circlet, crimson_hood, fias_hood |
| Fungus & Mycelia | Botanical | 46 | 1.09 | mushroom_crown, mushroom_head, mushroom_body, mushroom_arms, mushroom_legs |
| Thorns & Brambles | Botanical | 20 | 0.47 | alberichs_pointed_hat, alberichs_robe, alberichs_bracers, fire_prelate, oracle_envoy_giant |
| Sap & Flow | Botanical | 75 | 1.77 | juvenile_scholar_cap, haligtree_crest_surcoat, juvenile_scholar_robe, raya_lucarian_robe, haligtree_knight_armor |
| Lichen & Bark | Botanical | 28 | 0.66 | perfumer, depraved_perfumer, divine_fortification, stanching_boluses, neutralizing_boluses |
| Roots & Veins | Botanical | 46 | 1.09 | mushroom_head, commoners_garb, commoners_simple_garb, mushroom_body, commoners_garb_altered |
| Scarlet Rot Divinity | Cosmic | 327 | 7.73 | caterpillar_mask, igons_helm, freyjas_helm, black_knight_helm, young_lions_helm |
| Primeval Current | Cosmic | 57 | 1.35 | messmers_helm, messmers_helm_altered, astrologer_hood, azurs_glintstone_crown, cleanrot_helm |
| Dream & Reverie | Cosmic | 20 | 0.47 | owl, sleep_pot, stimulating_boluses, pureblood_knights_medal, fevors_cookbook_1 |
| Gravity Wells | Elemental | 33 | 0.78 | fire_prelate_armor, fire_prelate_armor_altered, fire_prelate_gauntlets, fire_prelate_greaves, ash_of_war_spinning_gravity_thrust |
| Sacred Flame | Elemental | 440 | 10.39 | dancers_hood, fire_knight_helm, death_mask_helm, winged_serpent_helm, salzas_hood |
| Glintstone & Crystalline | Elemental | 295 | 6.97 | alberichs_pointed_hat, astrologer_hood, azurs_glintstone_crown, haima_glintstone_crown, hierodas_glintstone_crown |
| Storm & Tempest | Elemental | 104 | 2.46 | divine_beast_helm, divine_beast_head, elden_lord_crown, elden_lord_armor, elden_lord_armor_altered |
| Frost & Hoarfrost | Elemental | 238 | 5.62 | alberichs_pointed_hat, alberichs_pointed_hat_altered, blue_silver_mail_hood, depraved_perfumer_headscarf, fingerprint_helm |
| Rebirth & Cycles | Narrative | 14 | 0.33 | rakshasa_helm, greatjar, juvenile_scholar_cap, rakshasa_armor, juvenile_scholar_robe |
| Martyrdom & Sacrifice | Narrative | 14 | 0.33 | alberichs_pointed_hat, alberichs_robe, alberichs_bracers, priestess_heart, chrysalids_memento |
| Betrayal & Fracture | Narrative | 2 | 0.05 | ijis_mirrorhelm, millicents_prosthesis |
| Oaths & Vows | Narrative | 105 | 2.48 | oathseeker_knight_helm, wise_mans_mask, beast_champion_helm, cleanrot_helm, cleanrot_helm_altered |
| Twinship & Mirrors | Narrative | 154 | 3.64 | messmers_helm, messmers_helm_altered, rellanas_helm, blackguards_iron_mask, dialloss_mask |

## Analysis layer overview

The Phase 7 analysis layer builds on this taxonomy to produce derived visuals and
reports:

- `poetry run corpus analysis clusters --export` embeds curated lore lines,
	projects them with UMAP, clusters via HDBSCAN, and saves
	`data/analysis/motif_clustering/*.parquet|json|png`.
- The resulting JSON report highlights exemplar lore lines per cluster, motif
	density deltas relative to the global coverage metrics above, and the
	associated visualization for qualitative review.
- `poetry run corpus analysis graph` (Phase 7 Branch 2) will assemble the NPC
	motif interaction graph, exposing NetworkX exports plus diagnostics that tie
	motif co-occurrence back to this taxonomy.
- `poetry run corpus analysis summaries` (Phase 7 Branch 3) will feed graph and
	clustering outputs into the narrative summarizer, generating clan-level
	briefings with guardrailed LLM prompts and citations into curated lore.