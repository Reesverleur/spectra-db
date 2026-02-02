# Data layout

Spectra-DB uses the same directory structure in two contexts:

1) **Repo mode** (developer checkout): `./data/...`
2) **Installed mode** (end users): per-user data directory via `platformdirs` (or `SPECTRA_DB_DATA_DIR` override)

## Repo layout (developer)

- `data/raw/`: cached/raw snapshots (HTML/CSV) by source
- `data/normalized/`: atomic canonical NDJSON
- `data/normalized_molecular/`: molecular canonical NDJSON
- `data/db/`: regeneratable DuckDB files

## Installed layout (end user)

Defaults to a per-user directory (platform-dependent), unless `SPECTRA_DB_DATA_DIR` is set.

It contains:

- `db/spectra.duckdb`
- `db/spectra_molecular.duckdb`
- `normalized/` and `normalized_molecular/` (created if sources are installed or scrapers are run)

## Release wheels

- `spectra-db` (code + CLI)
- `spectra-db-assets` (DuckDB files)
- `spectra-db-sources` (NDJSON sources)

The code auto-copies DB assets into the user data directory on first query if missing.
