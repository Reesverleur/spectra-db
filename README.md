# Spectra DB

Local spectroscopic database + fast query API, with ingestion tools for NIST sources.

This repo is organized so you can:
- scrape or import spectroscopic data into a **canonical, provenance-preserving** format
- build a **fast local database** (DuckDB) for efficient searching
- query both from the **command line** and from **Python**
- keep “everything NIST provides” (including optional columns that vary by species/ion)

> Note: This project is not affiliated with NIST. NIST content may include disclaimers and usage notes. This repo stores snapshots + provenance and does not claim ownership of upstream data.

---

## Goals

- **Atomic data (NIST ASD):**
  - Energy levels (configuration/term/J, energies, uncertainties, Landé g-factor when present)
  - Spectral lines (observed/ritz wavelengths, uncertainties, wavenumber, Ei/Ek, Aki/f/log(gf) when present)
  - Bibliographic references via ASBib “popup” endpoints (captured URLs + optional later enrichment)
- **Molecular data (later):**
  - rotational/vibrational constants, rovib coupling, hyperfine, etc.
- **Future-proof preservation:**
  - Store key physics fields in columns
  - Store all other table columns in `extra_json` so nothing is lost

---

## Repository Layout

```
src/spectra_db/          # installable package (schema + query + export helpers)
tools/scrapers/          # scrapers/ETL tools (NOT part of the package)
data/raw/                # cached raw snapshots (HTML/CSV/etc.)
data/normalized/         # canonical normalized NDJSON
data/db/                 # generated DuckDB
docs/                    # MkDocs docs
tests/                   # unit tests
scripts/                 # CLI utilities (bootstrap + query)
examples/                # example Python scripts
```

---

## Installation

Create and activate a venv:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
```

Install in editable mode + extras:

```bash
pip install -e ".[dev,docs,scrape]"
```

Extras:
- `dev`: pytest, ruff, mypy, pre-commit
- `docs`: mkdocs + mkdocstrings
- `scrape`: requests/bs4/lxml for ingestion tools

---

## Quick Start: Ingest → Build DB → Query (not necessary if data directory is pulled correctly)

### A) (Optional) Start fresh
If you want a clean rebuild:

```bash
rm -rf data/db/*
rm -f data/normalized/*.ndjson
```

(Keeping `data/raw/` is fine; it speeds up restarts via cache.)

### B) Ingest NIST ASD data

You should already have these spectrum lists:
- `data/normalized/asd_spectra_levels.txt`
- `data/normalized/asd_spectra_lines.txt`

If you need to regenerate them (holdings pages):

```bash
python -m tools.scrapers.nist_asd.list_spectra --kind levels --out data/normalized/asd_spectra_levels.txt
python -m tools.scrapers.nist_asd.list_spectra --kind lines  --out data/normalized/asd_spectra_lines.txt
```

#### Ingest all levels
```bash
python -m tools.scrapers.nist_asd.bulk_ingest \
  --mode levels \
  --spectra-file data/normalized/asd_spectra_levels.txt \
  --units-levels cm-1
```

#### Ingest all lines (adaptive splitting, resumable)
This sweeps a wide wavelength range and automatically splits bins that appear truncated.

```bash
python -m tools.scrapers.nist_asd.bulk_ingest \
  --mode lines \
  --spectra-file data/normalized/asd_spectra_lines.txt \
  --wav-min 0 \
  --wav-max 200000 \
  --initial-bin 2000 \
  --min-bin 0.5 \
  --line-unit nm \
  --wavelength-type vacuum
```

**Stop / restart behavior**
- HTTP responses are cached in `data/raw/` (re-running won’t re-download unless `--force`)
- NDJSON writing dedupes by stable IDs
- The adaptive bulk ingester writes a checkpoint log and will skip already-completed bins
So you can stop (Ctrl-C) and re-run the same command to resume.

### C) Build / rebuild the local DuckDB
```bash
python scripts/bootstrap_db.py --truncate-all
```

### D) Query from CLI
List species:
```bash
python scripts/query.py species He
```

Show levels:
```bash
python scripts/query.py levels "H I" --limit 20
python scripts/query.py levels "Fe II" --limit 20
```

Show lines (in nm):
```bash
python scripts/query.py lines "H I" --min-wav 400 --max-wav 700 --unit nm --limit 30
```

Export a machine-friendly JSON bundle:
```bash
python scripts/query.py export "H I" --levels-max-energy 90000 --lines-min-wav 400 --lines-max-wav 700 --out h_i_bundle.json
```

---

## Query from Python

### Open the DB and run queries
```python
from spectra_db.query import open_default_api

api = open_default_api()

# Search species
print(api.find_species("He"))

# Get isotopologues for a species_id
isos = api.isotopologues_for_species("ASD:He:+0")
iso_id = isos[0]["iso_id"]

# Levels (atomic)
levels = api.atomic_levels(iso_id, limit=50, max_energy=100000.0)
print(levels[0])

# Lines (atomic)
lines = api.lines(iso_id, unit="nm", min_wav=400, max_wav=700, limit=100, parse_payload=True)
print(lines[0])
```

### Export a bundle (best for downstream usage)
```python
from spectra_db.query.export import export_species_bundle

bundle = export_species_bundle(
    query="H I",
    levels_max_energy=90000,
    lines_min_wav=400,
    lines_max_wav=700,
    lines_unit="nm",
)
# JSON-serializable dict:
print(bundle.keys())
```

---

## Reference Enrichment (optional, recommended)

ASD references are pop-up links with `onclick="popded('...get_ASBib_ref.cgi?...')"`.

During ingest we capture:
- `ref_id` (e.g., `L8672c99`)
- `refs.url` (real ASBib endpoint URL)

Then you can enrich citation/DOI later:

```bash
python -m tools.scrapers.nist_asd.enrich_refs
python scripts/bootstrap_db.py --truncate-all
```

---

## Data & Provenance Notes

- Levels and lines store key fields in columns.
- Any additional or ion-specific columns are preserved in `extra_json` so nothing is lost.
- Large scraped datasets are typically not committed to Git (use `.gitignore` or Git LFS).

---

## Development

Format / lint:
```bash
ruff check .
ruff format .
```

Tests:
```bash
pytest -q
```

Docs:
```bash
mkdocs serve
```

---

## License

MIT License. See `LICENSE`.
