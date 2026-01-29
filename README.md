# Spectra DB

Local spectroscopic database + fast query API, with ingestion tools for NIST sources.

This repo is organized so you can:
- scrape or import spectroscopic data into a **canonical, human-editable** format
- build a **fast local database** (DuckDB) for efficient searching
- evolve the code into a proper Python package over time
- generate a documentation site later (MkDocs → GitHub Pages)

> Note: NIST and other upstream sources have their own terms/disclaimers. This project stores **snapshots + provenance** and does not claim ownership of source data.

---

## Goals

- **Canonical schema** capable of representing:
  - rotational/vibrational constants (including rovibrational coupling, anharmonicity)
  - electronic energy levels
  - hyperfine / nuclear spin effects (where available)
  - isotopologues, quantum-number conventions, uncertainties, references
- **Two-layer data model**
  1. `data/normalized/` — diffable, editable canonical datasets (NDJSON/CSV)
  2. `data/db/` — regeneratable fast query artifacts (DuckDB/Parquet)
- **Scrapers live in-repo but not in the package**
  - scraping dependencies stay isolated in `tools/`

---

## Repository Layout

```

src/spectra_db/          # installable package (query + schema + utilities)
tools/scrapers/          # scrapers/ETL tools (NOT part of the package)
data/raw/                # immutable raw snapshots (HTML/CSV/etc.)
data/normalized/         # canonical editable datasets (NDJSON/CSV)
data/db/                 # generated DB artifacts (DuckDB)
docs/                    # MkDocs documentation
tests/                   # tests (schema + query smoke tests)
scripts/                 # one-off scripts (bootstrap, validation, etc.)

```

---

## Installation

Create and activate a venv (recommended):

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
```

Install the package in editable mode + extras:

```bash
pip install -e ".[dev,docs,scrape]"
```

Extras:

* `dev`: pytest, ruff, mypy, pre-commit
* `docs`: mkdocs + mkdocstrings
* `scrape`: requests/bs4/lxml/mechanicalsoup for ingestion tools

---

## Quick Start (local DB)

This repo uses a “normalized → DB” flow:

1. Put canonical records into `data/normalized/` (NDJSON/CSV).
2. Build or refresh the local DuckDB in `data/db/`.

Once `scripts/bootstrap_db.py` exists (we’ll add it next), the workflow will look like:

```bash
python scripts/bootstrap_db.py
```

Then query via the Python API:

```python
from spectra_db.query import api

# Example (API will be implemented as we go)
# api.open_default_db()
# api.find_species("CO")
```

---

## Scrapers / Ingestion Tools

Scrapers are in `tools/scrapers/` and are intentionally **not** installed as part of the package.
They should:

* download raw snapshots into `data/raw/` (cached, rate-limited)
* parse raw content into canonical `data/normalized/` records
* record provenance (source URL, retrieval date, reference IDs)

Example usage pattern (will vary by scraper):

```bash
python tools/scrapers/nist_asd/fetch_levels.py --out data/raw/nist_asd/levels/
python tools/scrapers/nist_asd/parse_levels.py --in data/raw/... --out data/normalized/
```

---

## Development

### Formatting / Lint

We use `ruff`:

```bash
ruff check .
ruff format .
```

### Tests

```bash
pytest -q
```

### Pre-commit (optional but recommended)

```bash
pre-commit install
```

---

## Documentation

Docs are built with MkDocs + mkdocstrings.

Local preview:

```bash
mkdocs serve
```

Strict build (like CI):

```bash
mkdocs build --strict
```

---

## Data & Provenance

* `data/raw/` contains *snapshots* of upstream pages/exports to support reproducibility.
* `data/normalized/` is the canonical layer meant to be editable and reviewable in PRs.
* `data/db/` is generated and can be tracked with Git LFS if desired.

---

## Disclaimer

This project is not affiliated with NIST. Upstream databases include their own disclaimers and usage notes. Keep source references and do not represent ingested data as authoritative without consulting original sources.

---

## License

MIT License. See `LICENSE`.
