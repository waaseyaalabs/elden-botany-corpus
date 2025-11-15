# Kaggle Integration

## Continuous Integration

- The workflow in `.github/workflows/kaggle-integration.yml` runs on `push`, `pull_request`, and manual `workflow_dispatch` events.
- It expects `KAGGLE_USERNAME` and `KAGGLE_KEY` to be configured as GitHub repository secrets. These values are injected into the job environment and used to create `~/.kaggle/kaggle.json` on the runner.
- The workflow installs the official Kaggle CLI and runs a smoke test (`kaggle datasets list -s "titanic" | head -n 5`) to verify that the credentials are valid.

## Local Setup Options

### Option A: `~/.kaggle/kaggle.json`

1. Download your `kaggle.json` from Kaggle.
2. Place it at `~/.kaggle/kaggle.json` and run `chmod 600 ~/.kaggle/kaggle.json`.
3. Never commit this file. It is ignored via `.gitignore`.

### Option B: `.env` + environment variables

1. Create a local `.env` file (see `.env.example` for structure) and add:
   ```bash
   KAGGLE_USERNAME=your_username
   KAGGLE_KEY=your_key
   ```
2. Keep the file untracked (covered by `.gitignore`).
3. Export the values when needed or run the helper script below to materialize `kaggle.json` on demand.

### Helper Script

Use `scripts/setup_kaggle_creds.py` to generate the Kaggle config locally without hand-editing files:

```bash
python scripts/setup_kaggle_creds.py --env-file .env
```

- The script first checks real environment variables, then falls back to the provided `.env` file.
- It writes `~/.kaggle/kaggle.json` with `0600` permissions so only your user can read it.

## Security Reminders

- Never commit `kaggle.json`, `.env`, or any other secret material.
- Long-lived secrets belong either in local untracked files (for development) or in GitHub Secrets (for CI).
- Rotate credentials immediately if accidental exposure is suspected.
