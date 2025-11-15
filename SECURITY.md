# Security Policy

## Secret Handling

- Never commit API keys, tokens, or other secrets to this repository.
- Kaggle credentials must only live in GitHub repository secrets (`KAGGLE_USERNAME`, `KAGGLE_KEY`) or in local untracked files such as `~/.kaggle/kaggle.json` or a `.env` file covered by `.gitignore`.
