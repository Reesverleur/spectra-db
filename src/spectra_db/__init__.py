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
- Convenience helpers:
  - spectra_db.get_atomic_levels
  - spectra_db.get_atomic_lines
  - spectra_db.get_diatomic_constants
"""

from __future__ import annotations

from spectra_db.db_query import get_atomic_levels, get_atomic_lines, get_diatomic_constants

__all__ = [
    "__version__",
    "get_atomic_levels",
    "get_atomic_lines",
    "get_diatomic_constants",
]

__version__ = "0.0.2"
