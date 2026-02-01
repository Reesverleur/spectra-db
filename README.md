# Spectra DB

**Spectra DB** is a **local-first spectroscopy database + query API** backed by **DuckDB**, built around a
**cache → normalize (NDJSON) → bootstrap (DuckDB)** pipeline.

This repo currently supports **two separated data profiles**:

- **Atomic (NIST ASD)** — stable, working well
- **Molecular (NIST Chemistry WebBook: diatomic constants)** — working and designed to expand (ExoMol, HITRAN, …)

> This project is not affiliated with NIST. Use at your own risk and subject to upstream terms/disclaimers.

---

## Profiles, storage layout, and separation

### Atomic profile (NIST ASD)
- Normalized NDJSON: `data/normalized/`
- DuckDB: `data/spectra.duckdb`
- CLI: `scripts/query.py` (levels/lines/export)

### Molecular profile (NIST WebBook diatomic constants; Mask=1000)
- Cache: `data/raw/nist_webbook/cbook/` (`*.body` + `*.meta.json`, written by `fetch_webbook`)
- Normalized NDJSON: `data/normalized_molecular/`
- DuckDB: `data/db/spectra_molecular.duckdb` (separate from atomic DB)
- CLI: `scripts/query.py diatomic ...` (molecular-only query)

Hard rule: **atomic behavior is preserved**. Shared utilities must remain backward-compatible.

---

## What this repository provides today

## Atomic (NIST ASD)

### Energy levels (`energy1.pl`)
- Configuration, Term, J
- Level energy and uncertainty
- Landé g-factor and leading percentages when present
- **References**: supports **multiple references per level** (comma-separated in ASD tables)

### Spectral lines (`lines1.pl`)
- Observed and Ritz wavelengths + uncertainties
- Relative intensity, Aki, accuracy (when present)
- Endpoint energies (Ei/Ek) and lower/upper level designations
- **References**: supports **multiple refs per line** for both:
  - **TP Ref** (transition probabilities; `type=T`)
  - **Line Ref** (line classification; `type=L`)

### Bibliographic reference enrichment (optional)
- ASD table cells often contain comma-separated reference codes
- We preserve raw ref codes and normalized ref keys (`E:<code>`, `L:<code>`, `T:<code>`)
- `enrich_refs` (optional) fetches each unique ASBib page and extracts DOI/citation metadata

---

## Molecular (NIST Chemistry WebBook)

### What we ingest
**Diatomic constants** (WebBook “Constants of diatomic molecules”, fetched with `Mask=1000`).

### How WebBook “links” are interpreted
WebBook diatomic tables include hyperlink-like markers that look like references, but they are actually:

- **Footnotes / annotations:** `DiaNN` anchors (linked from table cells)
- **Bibliographic references:** `ref-N` anchors listed in the **References** section at the bottom of the page

We preserve both faithfully:

- **Footnotes** are stored per-species in `species.extra_json`:
  - `webbook_footnotes_by_id["Dia53"] -> {"text": "...", "ref_targets": ["ref-1", ...], "dia_targets": ["Dia88", ...]}`
- **Citations** (bibliography) are normalized into the `refs` table:
  - `refs.ref_id = "WB:<webbook_id>:ref-1"` and includes citation text + DOI (when present)
- Table cells keep their link markers out of the numeric value:
  - Per-cell linkage is stored in `spectroscopic_parameters.context_json["cell_note_targets"] = ["Dia53", ...]`
  - CLI displays these as square-bracket markers like `64748.48 Z [Dia53]`

### Normalized molecular tables
Written to `data/normalized_molecular/`:

- `species.ndjson`
- `isotopologues.ndjson`
- `states.ndjson` (molecular electronic states; Te stored as `energy_value`)
- `parameters.ndjson` (`spectroscopic_parameters` in DuckDB)
- `refs.ndjson` (bibliography for WebBook pages; `WB:<webbook_id>:ref-N`)

---

## Repository layout (high level)

- `tools/scrapers/nist_asd/` — ASD scrapers + parsers + enrichment
- `tools/scrapers/nist_webbook/` — WebBook fetch + bulk ingest + normalizers
- `data/raw/` — cached HTML responses (traceability)
- `data/normalized/` — atomic NDJSON
- `data/normalized_molecular/` — molecular NDJSON
- `src/spectra_db/` — DuckDB schema + query API
- `scripts/query.py` — CLI querying / verification
- `examples/` — small Python examples (`asd_demo.py`, `molecular_demo.py`)

---

## Install

From repo root (Python 3.10+ recommended):

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
```

---

## Tooling, code quality, and Git strategy

### Ruff + Pytest
This repo uses:
- **ruff** for linting/style checks
- **pytest** for unit/integration tests on scrapers, normalization, and DB bootstrap/query behavior

Run them:

```bash
ruff check .
pytest -q
```

### Suggested repository strategy
Raw caches and database artifacts can be large. Recommended approach:

- Keep source code + tests in Git
- **Do not commit** (or commit rarely) the following by default:
  - `data/raw/**`
  - `data/normalized/*.ndjson`
  - `data/normalized_molecular/*.ndjson`
  - `data/*.duckdb`, `data/db/*.duckdb`
- If you need to share large artifacts:
  - use Git LFS, or
  - publish DB snapshots / NDJSON bundles as GitHub Release assets

The pipeline is designed so DuckDB is fully rebuildable from NDJSON.

---

## End-to-end ingestion workflows

## Atomic ingestion (ASD)

(Optional) start fresh:
```bash
rm -f data/normalized/*.ndjson
rm -f data/spectra.duckdb
```

Fetch levels:
```bash
python -m tools.scrapers.nist_asd.fetch_levels --spectrum "Fe II"
```

Fetch lines (small window):
```bash
python -m tools.scrapers.nist_asd.fetch_lines --spectrum "Fe II" --min-wav 380 --max-wav 381 --unit nm --wavelength-type vacuum
```

Bootstrap atomic DB:
```bash
python scripts/bootstrap_db.py --profile atomic --truncate-all
```

---

## Molecular ingestion (WebBook diatomics)

### Fetch a single species page
Example for CO (WebBook ID `C630080`), Mask=1000:

```bash
python -m tools.scrapers.nist_webbook.fetch_webbook --id C630080 --mask 1000
```

Normalize cached pages → NDJSON:
```bash
python -m tools.scrapers.nist_webbook.normalize_cache
```

Bootstrap molecular DB:
```bash
python scripts/bootstrap_db.py --profile molecular --truncate-all
```

### Bulk ingest all diatomic-constants pages from WebBook
This performs:
1) discovery via the WebBook formula search pattern
2) fetch of each discovered WebBook ID via the canonical `fetch_webbook` cache layer

```bash
python -m tools.scrapers.nist_webbook.bulk_ingest_diatomics --sleep 0.5
```

Then normalize + bootstrap:
```bash
python -m tools.scrapers.nist_webbook.normalize_cache
python scripts/bootstrap_db.py --profile molecular --truncate-all
```

---

## Query from the command line

Run from repo root:

```bash
python scripts/query.py --help
```

### Species search
```bash
python scripts/query.py species He
python scripts/query.py species Iron
```

### Atomic levels
```bash
python scripts/query.py levels "Fe II" --limit 30
python scripts/query.py levels "Fe II" --max-energy 90000 --limit 50
```

### Atomic lines
```bash
python scripts/query.py lines "H I" --min-wav 400 --max-wav 700 --unit nm --limit 30
python scripts/query.py lines "Fe II" --min-wav 380 --max-wav 381 --unit nm --limit 30
```

### Molecular diatomic constants
This switches internally to the **molecular profile**.

```bash
python scripts/query.py diatomic "CO"
python scripts/query.py diatomic "CH"
python scripts/query.py diatomic "H2"
```

Include footnotes and bibliography (linked by square-bracket markers in the table):
```bash
python scripts/query.py diatomic "CO" --footnotes --citations
```

---

## Query from Python

### Atomic
```python
from spectra_db.query import open_default_api

api = open_default_api(profile="atomic")
matches = api.find_species("He", limit=10)
```

### Molecular
```python
from spectra_db.query import open_default_api

api = open_default_api(profile="molecular")
matches = api.find_species("CO", limit=10)
```

Examples:
- `examples/asd_demo.py` — atomic walkthrough (levels, lines, export)
- `examples/molecular_demo.py` — molecular walkthrough + validation (footnotes + citations)

---

## License

MIT. See `LICENSE`.
