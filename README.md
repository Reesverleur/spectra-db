# Spectra-DB

Spectra-DB is a **local-first spectroscopy database + query API** backed by **DuckDB**, built around a reproducible pipeline:

**fetch/cache HTML → normalize to NDJSON → bootstrap into DuckDB → query via CLI + Python API**

> This project is not affiliated with NIST. Use at your own risk and subject to upstream terms/disclaimers.
Docs Site: https://reesverleur.github.io/spectra-db/
---

## Profiles and storage layout

Spectra-DB supports **two separated “profiles”** with hard separation (**different normalized dirs and DB files**).

### Atomic profile — NIST Atomic Spectra Database (ASD)

- **Normalized NDJSON:** `data/normalized/`
- **DuckDB:** `data/db/spectra.duckdb`
- **CLI:** `spectra-db` (`species`, `levels`, `lines`, `export`)

### Molecular profile — NIST Chemistry WebBook (diatomic constants, Mask=1000)

- **Cache:** `data/raw/nist_webbook/cbook/` (`*.body` + `*.meta.json`)
- **Normalized NDJSON:** `data/normalized_molecular/`
- **DuckDB:** `data/db/spectra_molecular.duckdb`
- **CLI:** `spectra-db diatomic ...` (always uses molecular profile)

> `diatomic` always queries the molecular DB internally, regardless of `--profile`.

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

- `nu00` numeric value may include a trailing letter (e.g. `Z`): stored as numeric value + `value_suffix` and displayed as `... Z`
- `Trans` is stored as **text in the state `extra_json`**, not as a numeric parameter
- numeric parsing strips `<sub>…</sub>` markers from values but preserves markers in `context_json`

---

## Installation

Python **3.11+** is required.

There are two supported install styles:

1) **End-user install from Release wheels** (recommended for most users)
2) **Developer install from a repo checkout**

### 1) End-user install (Release wheels)

Download the wheels from the GitHub Release page (three separate wheels):

- `spectra_db_assets-<ver>-*.whl`  → DuckDB files (required for “it just works” querying)
- `spectra_db_sources-<ver>-*.whl` → NDJSON “source of truth” (optional, for rebuild/edit workflows)
- `spectra_db-<ver>-*.whl`         → code + CLI

Install them (order doesn’t strictly matter, but this makes intent clear):

```bash
pip install spectra_db_assets-<ver>-*.whl
pip install spectra_db_sources-<ver>-*.whl   # optional
pip install spectra_db-<ver>-*.whl
```

Now you can run from **any directory**:

```bash
spectra-db species HF
spectra-db levels "Fe II" --limit 10
spectra-db diatomic CO --limit 20
```

#### Where the data lives (installed usage)

By default Spectra-DB uses a per-user data directory (via `platformdirs`) and stores:

- `db/spectra.duckdb`
- `db/spectra_molecular.duckdb`
- (optionally) `normalized/` and `normalized_molecular/` when sources are installed or scrapers are run

On first query, if the DB does not exist yet in the per-user location, Spectra-DB **copies the DuckDB files from `spectra-db-assets`** into the per-user data directory automatically.

You can override the data directory with an environment variable:

```bash
export SPECTRA_DB_DATA_DIR=/path/to/your/data_dir
```

> Tip: if you ever set `SPECTRA_DB_DATA_DIR` for testing and later want “repo mode” again, run `unset SPECTRA_DB_DATA_DIR`.

### 2) Developer install (repo checkout)

From the repo root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e ".[dev,scrape,docs]"
```

For developer convenience, it’s recommended to keep local artifacts in the repo:

- `data/db/*.duckdb`
- `data/normalized/*.ndjson`
- `data/normalized_molecular/*.ndjson`

These can be untracked (recommended) or stored via Git LFS (your choice). The Release wheels do **not** depend on repo-local artifacts.

---

## Tooling (Ruff + pytest)

```bash
ruff check .
ruff format --check .
pytest -q
```

Docs:

```bash
mkdocs build --strict
```

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
spectra-db --profile atomic bootstrap --truncate-all
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
spectra-db --profile molecular bootstrap --truncate-all
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
spectra-db --profile molecular bootstrap --truncate-all
```

Note: some discovered pages are legitimate **“no data”** cases (no diatomic constants table). These are expected and are logged as ingested.

---

## Query from the command line

```bash
spectra-db --help
```

### About `--profile`
`--profile` is a **global flag** and must appear **before** the subcommand:

```bash
spectra-db --profile atomic levels "Fe II"
spectra-db --profile molecular bootstrap --truncate-all
```

### Species search (smart fuzzy + formula reversal)

```bash
spectra-db species He
spectra-db species Iron
spectra-db species HF
```

### Atomic levels

By default, **reference URL columns are hidden**. Opt in with `--references`.

```bash
spectra-db levels "Fe II" --limit 30
spectra-db levels "Fe II" --max-energy 90000 --limit 50
spectra-db levels "Fe II" --references
```

You can explicitly control columns with `--columns` (overrides `--references` / `--compact`):

```bash
spectra-db levels "Fe II" --columns Energy,J,g,Configuration,Term
spectra-db levels "Fe II" --columns Energy,J,g,RefURL
```

### Atomic lines

By default, **reference URL columns are hidden**. Opt in with `--references`.

```bash
spectra-db lines "H I" --min-wav 400 --max-wav 700 --unit nm --limit 30
spectra-db lines "Fe II" --min-wav 380 --max-wav 381 --unit nm --limit 30
spectra-db lines "H I" --references
```

Column override:

```bash
spectra-db lines "H I" --columns Obs,Lower,Upper,Type,LineRefURL
```

### Molecular diatomic constants

This switches internally to the **molecular profile**.

```bash
spectra-db diatomic "CO"
spectra-db diatomic HF --exact --footnotes --citations
```

#### Exact matching + formula order reversal (important)

Some WebBook entries store formulas in an unusual order (example: user query `HF` stored as `FH`). This can cause fuzzy ambiguity (e.g., `HF` incorrectly matching `HfO`).

The project implements an exact resolution order:

1) `species_id`
2) `formula` (case-insensitive; also tries **reversed token order**, e.g. `HF` ⇄ `FH`)
3) `name` (case-insensitive)

In the CLI, use `--exact` with `diatomic` when you want exact resolution first.

---

## Rebuild databases from NDJSON sources (optional)

If you installed `spectra-db-sources`, you can rebuild the DuckDB files offline.

Bootstrap will automatically copy NDJSON sources into the active data directory if the NDJSON inputs are missing.

```bash
spectra-db --profile atomic bootstrap --truncate-all
spectra-db --profile molecular bootstrap --truncate-all
```

You can override the NDJSON directory explicitly:

```bash
spectra-db --profile atomic bootstrap --normalized /path/to/normalized --truncate-all
```

---

## Query from Python

```python
from spectra_db.query import open_default_api

api_atomic = open_default_api(profile="atomic")          # atomic DB
api_mol    = open_default_api(profile="molecular")       # molecular DB
```

### Exact helpers (recommended for molecular)

```python
sid = api_mol.resolve_species_id("HF", exact_first=True, fuzzy_fallback=True)
rows = api_mol.find_species_exact("CO", by=("species_id", "formula", "name"))
```

### Read-only access (recommended for analysis scripts)

```python
api = open_default_api(profile="atomic", read_only=True, ensure_schema=False)
```

---

## Repository strategy (Git vs data artifacts)

Raw caches and database artifacts can be large. Recommended approach:

**Keep in Git**
- source code (`src/`)
- schemas (`src/spectra_db/db/*.sql`)
- tests (`tests/`)
- docs (`docs/`, `mkdocs.yml`)
- small examples (`examples/`)

**Do not commit by default**
- `data/raw/**`
- `data/normalized/**`
- `data/normalized_molecular/**`
- `data/db/*.duckdb`
- build artifacts: `dist/`, `build/`, `*.egg-info/`

If you need to share large artifacts in-repo, prefer **Git LFS**. For end users, prefer the Release wheels described above.

---

## Future direction (brief)

Planned expansions (ExoMol, HITRAN/HITEMP, …) should remain **profile/dataset separated**, likely with on-demand fetch + local caching due to scale (billions of lines). The atomic pipeline and DB behavior should remain stable as new datasets are added.

---

## License

MIT. See `LICENSE`.
