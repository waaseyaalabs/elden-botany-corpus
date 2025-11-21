# TODO

## Canonical builder integration
- [x] Wire GitHub loaders into canonical builders for items, bosses, armor, spells.
- [x] Ensure shared logging covers GitHub row counts and schema failures.
- [x] Normalize Kaggle and GitHub rows through the same parsing helpers.
- [x] Default missing GitHub fields so Pandera schemas pass.

## Testing
- [x] Add github_api loader coverage for each domain.
- [x] Add canonical builder fallback tests spanning Kaggle/GitHub mixes.
- [x] Run `poetry run ruff check --fix .` and `poetry run pytest` before delivery.

## Follow-ups
- [ ] Review Pandera warning about imports in a future PR.
- [ ] Phase in `check_untyped_defs` for `src/tarnishedbotanist/*` only; once stable, extend via `[[tool.mypy.overrides]]` entries (see requested config snippet) before turning it on for tests.
- [ ] Grow `tests/test_cli_incremental.py` to cover additional CLI flags
	(e.g., `--all`, error handling) whenever incremental plumbing changes.
- [ ] Add a docs checklist to PR templates ensuring README/AGENTS/TODO stay in
	sync when manifest or guard behavior shifts.

## RAG quality improvements
- [x] Reweight text types so `description` and `impalers_excerpt` rows score higher than terse `effect` snippets for thematic prompts (Radahn, Scarlet Rot, etc.).
- [x] Augment preprocessing by concatenating complementary fields (e.g., effect + description) before embedding to blend mechanics with lore context.
- [x] Prototype a lightweight reranker (cosine or cross-encoder) prioritizing narrative-rich passages for story-heavy searches.
- [ ] Audit corpus coverage for Radahn/Malenia/Rot dialogue in Impalers/GitHub sources and schedule ingestion if gaps remain.
