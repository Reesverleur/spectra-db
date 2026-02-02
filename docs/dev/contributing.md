# Contributing

## Setup (developer install)

From the repo root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e ".[dev,docs,scrape]"
```

## Style and tests

- Ruff is the formatter + linter:
  ```bash
  ruff check .
  ruff format --check .
  ```
- Tests:
  ```bash
  pytest -q
  ```
- Docs build:
  ```bash
  mkdocs build --strict
  ```

## Project conventions

- Source code lives under `src/spectra_db/` (src layout).
- CLI is `spectra-db` (entry point: `spectra_db.cli:main`).
- Data assets are optional wheels:
  - `spectra-db-assets` (DuckDB files)
  - `spectra-db-sources` (NDJSON sources)
- Scrapers live in `spectra_db.scrapers.*` and should always write via `get_paths()` (no hardcoded paths).
