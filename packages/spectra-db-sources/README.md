# spectra-db-sources

This package contains the NDJSON “source of truth” for Spectra-DB.

It is intended to be installed alongside `spectra-db` when users want to:
- inspect/modify NDJSON locally
- rebuild DuckDB databases offline via `spectra-db bootstrap`

Most users only need `spectra-db` + `spectra-db-assets` (DuckDB files).
