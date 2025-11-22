# Entity Aliasing

## Why this exists
The community and Impaler FMG dumps emit tens of thousands of dialogue rows under synthetic speaker IDs such as `npc:carian_speaker_220001000`. Downstream pipelines (motif graphs, LLM summarizers, guardrails) need real Elden Ring entities, so we mine deterministic alias rules that rewrite those synthetic IDs into canonical NPC slugs such as `npc:iron_fist_alexander`. Each alias row records a wildcard, canonical slug, confidence, and the evidence we used while auditing.

## Data sources sampled
- `data/curated/lore_corpus.parquet` – primary source for canonical_id, raw_canonical_id, text, and source columns. Pandas sampling/filters were used to enumerate speaker buckets and spot-check representative lines.
- `data/reference/entity_aliases.csv` – authoritative alias table consumed by `pipelines.build_lore_corpus`.
- Kaggle Elden Ring datasets (`robikscube/elden-ring-ultimate-dataset`, `pedroaltobelli/ultimate-elden-ring-with-shadow-of-the-erdtree-dlc`) – cross-referenced NPC names and questlines.
- Prior FMG carian speaker audit artifacts (`docs/debug/carian_speaker_audit.md`, `data/debug/carian_speaker_audit.parquet`) to prioritize high-volume unmapped buckets.

## Heuristics and evidence
1. **Prefix fan-out** – grouped `raw_canonical_id` by the three-digit prefix (e.g., `npc:carian_speaker_220`) to find the highest-traffic buckets.
2. **Self-identification** – many speakers introduce themselves verbatim (“I am Alexander, also known as the Iron Fist”). These received `confidence=1.0`.
3. **Quest context** – when no name is spoken, unique quest details (Potentate trials, Beast Clergyman “Feed me Death”) anchored the mapping. These received 0.8–0.95 confidence.
4. **Melina vs Marika** – Melina’s voice actor also narrates Queen Marika’s decrees. We split buckets by raw ID ranges: accords/level-up lines stay Melina, while the `100041*`, `100044*`, `100045*`, `10010*`, `2054*`, and `2056*` blocks map to `npc:marika` because the text is explicitly framed as Marika’s words.
5. **Guardrail feedback** – verified alias coverage by counting remaining `npc:carian_speaker_*` rows in the regenerated lore corpus (15,828 total rows → 6,883 synthetic speakers after this pass).

## Newly confirmed mappings
| Raw prefix / pattern | Canonical NPC | Confidence | Evidence (representative quote) |
| --- | --- | --- | --- |
| `npc:carian_speaker_220*` | `npc:iron_fist_alexander` | 1.0 | “I am Alexander, also known as the Iron Fist. And as you can see, I’m stuck here.” |
| `npc:carian_speaker_309*` | `npc:patches` | 1.0 | “You scheming little thief… The gods demand repentance!” |
| `npc:carian_speaker_307*` | `npc:preceptor_seluvis` | 1.0 | “I am Seluvis, preceptor in the sorcerous arts.” |
| `npc:carian_speaker_316*` | `npc:sorceress_sellen` | 1.0 | “I am Sellen, a sorcerer, quite plainly.” |
| `npc:carian_speaker_223*` | `npc:boc_the_seamster` | 1.0 | “Thank you. The name’s Boc… Boc the seamster, at your service.” |
| `npc:carian_speaker_202*` | `npc:gurranq_beast_clergyman` | 0.9 | Unique Bestial Sanctum lines: “I smell it… Death… Feed it me… I shall grant thee eye and claw.” |
| `npc:carian_speaker_322*` | `npc:fia` | 1.0 | “Greetings, great champion… I am Fia. Would you allow me to hold you?” |
| `npc:carian_speaker_320*` | `npc:roderika` | 1.0 | “Everyone’s been grafted… Only two peas in a pod, eh?” |
| `npc:carian_speaker_348*` | `npc:millicent` | 1.0 | “If you are wise, you will leave immediately. My flesh writhes with scarlet rot.” |
| `npc:carian_speaker_349*` | `npc:gowry` | 1.0 | “I am Gowry. I’d hoped to ask a favour… find the needle for Millicent.” |
| `npc:carian_speaker_10014*` | `npc:melina` | 1.0 | “Hello again, old friend. It’s me, Melina. Your travelling companion.” |
| `npc:carian_speaker_100045*` | `npc:marika` | 0.95 | Spoken as Marika’s edict: “The Erdtree governs all. The choice is thine.” |
| `npc:carian_speaker_2054*` | `npc:marika` | 0.95 | Prayer that culminates in “Elden Ring, O Elden Ring. Beget Order most elegant.” |

The full set of rows lives in `data/reference/entity_aliases.csv` and includes additional NPCs (Varre, Jar Bairn, Nepheli Loux, Merchant Kale, Brother Corhyn, Sorcerer Rogier, etc.). Comments on each row summarize the evidence trail for quick diff reviews.

## Melina vs Marika rules
- **Melina buckets**: everyday companion and accord dialogue is isolated to `npc:carian_speaker_10001* – 10003*`, `10011* – 10019*`, `2050*`, and `2058*`. These blocks include explicit “I am Melina” / “It’s me, Melina” lines or unique references to her accord duties.
- **Marika buckets**: the narrator-style decrees and seedlings prayer sequences (`100041*`, `100044*`, `100045*`, `10010*`, `2054*`, `2056*`) are framed in-universe as Queen Marika’s own words. Even though Melina voices them in-game, the curation pipeline now rewrites those rows to `npc:marika` so downstream guardrails reason about the correct entity.

## Ambiguous or deferred speakers
| Raw prefix | Why unresolved | Next action |
| --- | --- | --- |
| `npc:carian_speaker_20680*` | Lines such as “Dearest Marika…” appear during Farum Azula cutscenes but oscillate between Maliketh narration and Radagon memories. Needs video cross-check to avoid mislabeling. | Review FMG speaker tables alongside cutscene IDs before mapping. |
| `npc:carian_speaker_3310*` | Contains battlefield cries around Leyndell siege soldiers with no clear named speaker. | Only alias once we can tie them to specific knights/commanders. |
| `npc:carian_speaker_21313*` | “Ahh Radahn…” style laments appear both in Redmane Castle NPC chatter and Ghost NPCs. Unsure if the bucket mixes multiple speakers. | Run `scripts/audit_carian_speakers.py` filtered to this prefix and compare against Kaggle NPC metadata. |

We intentionally leave these unmapped so analytics continue to flag them for curator review instead of misattributing lore to the wrong NPC.

## Next steps
1. **Expand coverage** – remaining 6.8k `npc:carian_speaker_*` rows concentrate in mid-game bucket IDs (e.g., 223xxx side quests). Continue the sample → evidence → alias loop focusing on high-frequency prefixes.
2. **Automated validation** – add a lightweight lint that fails CI when any lore corpus rebuild still contains `npc:carian_speaker_` rows for characters listed in `docs/entity_aliasing.md`.
3. **Cross-reference FMG tables** – integrate the raw FMG speaker metadata (location, talk ID) to auto-suggest canonical candidates before human review.
4. **Monitor guardrails** – re-run the graph/summarizer pipelines after each alias batch to confirm that canonical nodes (Ranni, Gideon, Hyetta, Melina, Marika, etc.) accumulate the expected lore lines and that LLM prompt runs no longer crash on anonymous speaker IDs.
