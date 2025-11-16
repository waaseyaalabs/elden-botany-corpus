# Security Policy

## Secret Handling

- Never commit API keys, tokens, or other secrets to this repository.
- Kaggle credentials must only live in GitHub repository secrets (`KAGGLE_USERNAME`, `KAGGLE_KEY`) or in local untracked files such as `~/.kaggle/kaggle.json` or a `.env` file covered by `.gitignore`.

## Dependency Updates

- 2025-11-15: Pinned `requests==2.32.4` to remediate [CVE-2024-47081](https://nvd.nist.gov/vuln/detail/CVE-2024-47081). See `pyproject.toml` for the exact requirement and run `poetry lock && poetry install` to refresh local environments.
