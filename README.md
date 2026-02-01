# Spectra-DB

Spectra-DB is a **local-first spectroscopy database + query API** backed by **DuckDB**, built around a reproducible pipeline:

**fetch/cache HTML → normalize to NDJSON → bootstrap into DuckDB → query via CLI + Python API**

> This project is not affiliated with NIST. Use at your own risk and subject to upstream terms/disclaimers.

---

## Profiles and storage layout

Spectra-DB supports **two separated “profiles”** with hard separation (**different normalized dirs and DB files**).

### Atomic profile — NIST Atomic Spectra Database (ASD)

- **Normalized NDJSON:** `data/normalized/`
- **DuckDB:** `data/db/spectra.duckdb`
- **CLI:** `python scripts/query.py` (`species`, `levels`, `lines`, `export`)

Hard requirement: **Do not break atomic behavior**. Shared utilities must remain backward-compatible.

### Molecular profile — NIST Chemistry WebBook (diatomic constants, Mask=1000)

- **Cache:** `data/raw/nist_webbook/cbook/` (`*.body` + `*.meta.json`)
- **Normalized NDJSON:** `data/normalized_molecular/`
- **DuckDB:** `data/db/spectra_molecular.duckdb`
- **CLI:** `python scripts/query.py diatomic ...` (always uses molecular profile)

---

## Molecular (WebBook) semantics and storage

WebBook pages have two “reference-like” systems:

- **DiaNN anchors**: footnotes/annotations referenced from table cells
- **ref-N anchors**: bibliographic citations in the “References” section (often with DOI)

We preserve both:

### Footnotes (DiaNN)

Stored per species in `species.extra_json["webbook_footnotes_by_id"]` as structured objects:

```json
{
  "Dia53": {
    "text": "...",
    "ref_targets": ["ref-1", "..."],
    "dia_targets": ["Dia88", "..."]
  }
}
```

### Bibliographic references (ref-N)

Normalized into the `refs` table:

- `refs.ref_id = "WB:<webbook_id>:ref-1"`
- `refs.citation` contains the reference text
- `refs.doi` is extracted when present

### Table cell linkage (markers without contaminating numeric values)

Per-cell linkage is stored separately from numeric parsing:

- `spectroscopic_parameters.context_json["cell_note_targets"] = ["Dia53", ...]`

The CLI renders this as square-bracket markers, e.g. `64748.48 Z [Dia53]`.

### Mixed-token behavior (WebBook quirks we preserve)

- `nu00` numeric value may include a trailing letter (e.g. `Z`): stored as numeric value + `value_suffix`
- `Trans` is stored as **text in the state `extra_json`**, not as a numeric parameter
- numeric parsing strips `<sub>…</sub>` markers from values but preserves them in `context_json`

---

## Installation

Python **3.11+** is required.

From repo root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
```

Optional scrape dependencies:

```bash
pip install -e ".[scrape]"
```

---

## Tooling (Ruff + pytest)

```bash
ruff check .
pytest -q
```

(Tests add `src/` to `sys.path`, so they work whether or not you installed the package editable.)

---

## End-to-end ingestion workflows

### Atomic ingestion (ASD)

(Optional) start fresh:

```bash
rm -f data/normalized/*.ndjson
rm -f data/db/spectra.duckdb
```

Fetch levels:

```bash
python -m spectra_db.scrapers.nist_asd.fetch_levels --spectrum "Fe II"
```

Fetch lines (small window):

```bash
python -m spectra_db.scrapers.nist_asd.fetch_lines --spectrum "Fe II" --min-wav 380 --max-wav 381 --unit nm --wavelength-type vacuum
```

Bootstrap atomic DB:

```bash
python scripts/bootstrap_db.py --profile atomic --truncate-all
```

### Molecular ingestion (WebBook diatomics)

Fetch a single species page (example: CO WebBook ID `C630080`), `Mask=1000`:

```bash
python -m spectra_db.scrapers.nist_webbook.fetch_webbook --id C630080 --mask 1000
```

Normalize cached pages → NDJSON:

```bash
python -m spectra_db.scrapers.nist_webbook.normalize_cache
```

Bootstrap molecular DB:

```bash
python scripts/bootstrap_db.py --profile molecular --truncate-all
```

#### Bulk ingest all diatomic-constants pages from WebBook

This performs:

1) discovery via the WebBook formula search pattern
2) fetch of each discovered WebBook ID via the canonical `fetch_webbook` cache layer

```bash
python -m spectra_db.scrapers.nist_webbook.bulk_ingest_diatomics --sleep 0.5
```

Then normalize + bootstrap:

```bash
python -m spectra_db.scrapers.nist_webbook.normalize_cache
python scripts/bootstrap_db.py --profile molecular --truncate-all
```

Note: some discovered pages are legitimate **“no data”** cases (no diatomic constants table). These are expected and are logged as ingested.

---

## Query from the command line

```bash
python scripts/query.py --help
```

### Species search (smart fuzzy + formula reversal)

```bash
python scripts/query.py species He
python scripts/query.py species Iron
python scripts/query.py species HF
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
python scripts/query.py diatomic "CH" --footnotes --citations
```

#### Exact matching + formula order reversal (important)

Some WebBook entries store formulas in an unusual order (example: user query `HF` stored as `FH`). This can cause fuzzy ambiguity (e.g., `HF` incorrectly matching `HfO`).

The project implements an exact resolution order:

1) `species_id`
2) `formula` (case-insensitive; also tries **reversed token order**, e.g. `HF` ⇄ `FH`)
3) `name` (case-insensitive)

In the CLI, use `--exact` with `diatomic` when you want exact resolution first:

```bash
python scripts/query.py diatomic HF --exact --footnotes --citations
python scripts/query.py diatomic "Hydrogen fluoride" --exact --footnotes --citations
```

---

## Query from Python

```python
from spectra_db.query import open_default_api

api_atomic = open_default_api(profile="atomic")          # data/db/spectra.duckdb
api_mol    = open_default_api(profile="molecular")       # data/db/spectra_molecular.duckdb
```

### Exact helpers (recommended for molecular)

```python
sid = api_mol.resolve_species_id("HF", exact_first=True, fuzzy_fallback=True)
rows = api_mol.find_species_exact("CO", by=("species_id", "formula", "name"))
```

---

## Repository strategy (Git vs data artifacts)

Raw caches and database artifacts can be large. Recommended approach:

**Keep in Git**
- source code (`src/`, `tools/`, `scripts/`)
- schemas (`src/spectra_db/db/*.sql`)
- tests (`tests/`)
- small examples (`examples/`)

**Do not commit by default**
- `data/raw/**`
- `data/normalized/**`
- `data/normalized_molecular/**`
- `data/db/*.duckdb`

If you need to share large artifacts, prefer **Git LFS** or **GitHub Release assets**.

---

## Future direction (brief)

Planned expansions (ExoMol, HITRAN/HITEMP, …) should remain **profile/dataset separated**, likely with on-demand fetch + local caching due to scale (billions of lines). The atomic pipeline and DB behavior should remain stable as new datasets are added.

---

## License

MIT. See `LICENSE`.
