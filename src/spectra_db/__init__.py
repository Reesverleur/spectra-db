"""spectra_db package.

Spectra-DB is a local-first spectroscopy database + query API backed by DuckDB.

Key ideas:
- Reproducible ingestion pipeline: fetch/cache → normalize (NDJSON) → bootstrap (DuckDB) → query
- Separate profiles:
  - Atomic (NIST ASD)
  - Molecular (NIST WebBook diatomic constants; designed to expand)

Public API:
- spectra_db.query.open_default_api
- spectra_db.query.api.QueryAPI

Scrapers / ingestion tools live under:
- spectra_db.scrapers.*
"""

from __future__ import annotations

__all__ = ["__version__"]
__version__ = "0.0.2"
