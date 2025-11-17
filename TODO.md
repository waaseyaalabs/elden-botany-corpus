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
