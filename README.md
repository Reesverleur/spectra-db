# Spectra-DB

Spectra-DB is a **local-first spectroscopy database + query API** backed by **DuckDB**. It is designed so you can query spectroscopy data **offline** on your own machine.

It’s built around a reproducible pipeline:

**fetch/cache → normalize to NDJSON → bootstrap into DuckDB → query via CLI + Python API**

Docs Site: https://reesverleur.github.io/spectra-db/

> This project is not affiliated with NIST. Use at your own risk and subject to upstream terms/disclaimers.
---

## Key features

- **Two separate datasets (“profiles”)**
  - **Atomic (NIST ASD)**: atomic species, energy levels, spectral lines
  - **Molecular (NIST WebBook diatomics)**: diatomic constants (Mask=1000)
- **Local-first data**: everything runs from files on your machine (DuckDB + optional NDJSON sources).
- **Easy CLI**: `spectra-db species …`, `spectra-db levels …`, `spectra-db lines …`, `spectra-db diatomic …`
- **Easy Python helpers** (imported by default):
  - `get_atomic_levels("H I", n_excited=1)` → ground + excited levels
  - `get_atomic_lines("H I", n_excited=1)` → lines from low-lying levels
  - `get_diatomic_constants("HF", n_excited=0)` → ground-state diatomic constants
- **Optional “source-of-truth” NDJSON** (for rebuilding/editing):
  - install the *sources* package and rebuild the DuckDB locally via `spectra-db bootstrap`

---

## Profiles and data layout

### Atomic profile — NIST Atomic Spectra Database (ASD)
- **NDJSON (canonical):** `normalized/` (installed) or `data/normalized/` (repo checkout)
- **DuckDB (fast query DB):** `db/spectra.duckdb` (installed) or `data/db/spectra.duckdb` (repo checkout)

### Molecular profile — NIST Chemistry WebBook (diatomic constants, Mask=1000)
- **Cache (HTML):** `raw/nist_webbook/cbook/` (installed) or `data/raw/nist_webbook/cbook/` (repo checkout)
- **NDJSON (canonical):** `normalized_molecular/` (installed) or `data/normalized_molecular/` (repo checkout)
- **DuckDB (fast query DB):** `db/spectra_molecular.duckdb` (installed) or `data/db/spectra_molecular.duckdb` (repo checkout)

### Where files live for end users
By default, Spectra-DB stores data in a **per-user data directory** (via `platformdirs`). You do not have to manage paths.

If you ever want to override the location, set:

```bash
export SPECTRA_DB_DATA_DIR=/path/to/your/data_dir
```

> If you set `SPECTRA_DB_DATA_DIR` temporarily for testing, you can return to normal behavior with:
> `unset SPECTRA_DB_DATA_DIR`

---

## Get started (recommended for most users)

Spectra-DB is distributed as **three wheels** (three installable packages):

1) **`spectra-db`** (code + CLI) — required
2) **`spectra-db-assets`** (DuckDB files) — required for “query immediately”
3) **`spectra-db-sources`** (NDJSON files) — optional (only needed if you want to rebuild/edit the DB)

You download these wheels from the GitHub Releases page for the project.

### Step 1 — Create a fresh Python environment

You can use either **venv** (standard Python) or **conda** (Anaconda/Miniconda). Choose one.

### Option A: Standard Python venv (recommended if you’re new)

You can run these commands from **any directory**.

```bash
# Create a new environment folder (named .venv)
python -m venv .venv
# Activate it (macOS/Linux)
source .venv/bin/activate
# Upgrade pip
python -m pip install -U pip
```

### Option B: Conda (Anaconda/Miniconda)

You can run these commands from **any directory**.

```bash
conda create -n spectra-db python=3.11 -y
conda activate spectra-db
python -m pip install -U pip
```

### Step 2 — Install the wheels into your environment

Run these commands from the directory where you downloaded the wheel files (or provide full paths).

```bash
pip install spectra_db_assets-<ver>-*.whl
pip install spectra_db-<ver>-*.whl
# Optional: install NDJSON sources (for rebuild/edit workflows)
pip install spectra_db_sources-<ver>-*.whl
```

### Step 3 — Verify it works

You can run these commands from **any directory**:

```bash
spectra-db species HF
spectra-db levels "Fe II" --limit 10
spectra-db diatomic CO --limit 20
```

---

## Python usage (easy helpers)

These helper functions are available directly from `spectra_db`:

### Molecular: diatomic constants

```python
from spectra_db import get_diatomic_constants

# Ground electronic state only
hf = get_diatomic_constants("HF", n_excited=0)

# Ground + 2 excited electronic states (sorted by Te)
co = get_diatomic_constants("CO", n_excited=2, exact_first=True)
```

### Atomic: levels

```python
from spectra_db import get_atomic_levels

# Ground only
h0 = get_atomic_levels("H I", n_excited=0)

# Ground + 1 excited
h1 = get_atomic_levels("H I", n_excited=1)
```

### Atomic: lines

```python
from spectra_db import get_atomic_lines

lines = get_atomic_lines("H I", n_excited=1, unit="nm", max_lines=500)
```

**How `n_excited` affects atomic lines:**
`get_atomic_lines` uses the *(ground + n_excited)-th* level energy as a threshold and keeps lines whose lower-state energy (`Ei_cm-1` in the payload) is ≤ that threshold when available.

---

## CLI usage

Show help:

```bash
spectra-db --help
```

### Global `--profile`
`--profile` is a **global flag** and must appear **before** the subcommand:

```bash
spectra-db --profile atomic levels "Fe II"
spectra-db --profile molecular bootstrap --truncate-all
```

### Atomic levels and lines: references are opt-in
By default, **reference URL columns are hidden**. Opt in with `--references`:

```bash
spectra-db levels "Fe II" --limit 30
spectra-db levels "Fe II" --references

spectra-db lines "H I" --limit 30
spectra-db lines "H I" --references
```

### Molecular diatomic constants
`diatomic` always queries the molecular DB:

```bash
spectra-db diatomic "CO"
spectra-db diatomic HF --exact --footnotes --citations
```

---

## Rebuilding the databases from NDJSON (optional)

If you installed **`spectra-db-sources`**, you can rebuild the DuckDB files locally.
Bootstrap will automatically copy NDJSON sources into your active data directory if the NDJSON inputs are missing.

```bash
spectra-db --profile atomic bootstrap --truncate-all
spectra-db --profile molecular bootstrap --truncate-all
```

You can also specify an NDJSON directory explicitly:

```bash
spectra-db --profile atomic bootstrap --normalized /path/to/normalized --truncate-all
```

---

## Developer install (repo checkout)

If you want to develop or run scrapers, clone the repo and install editable.

### Step 1 — Create/activate an environment (any directory)
Use venv or conda, same as above.

### Step 2 — Install the code editable (run from repo root)
**You must `cd` into the repo root** (the folder containing `pyproject.toml`) before running these commands:

```bash
cd /path/to/spectra-db
python -m pip install -U pip
pip install -e ".[dev,scrape,docs]"
```

---

## Build all three wheels locally (maintainers/contributors)

Yes — you can build wheels from **any active environment** (venv or conda). The environment just needs `build` installed.

### Step 1 — Activate your environment (any directory)
Then install build tooling:

```bash
python -m pip install -U pip build
```

### Step 2 — Build the wheels (run in each project directory)

**(A) Code wheel: `spectra-db`**
You must run this from the repo root (or pass the path to `python -m build`):

```bash
cd /path/to/spectra-db
python -m build
```

**(B) Assets wheel: `spectra-db-assets`**

```bash
cd /path/to/spectra-db/packages/spectra-db-assets
python -m build
```

**(C) Sources wheel: `spectra-db-sources`**

```bash
cd /path/to/spectra-db/packages/spectra-db-sources
python -m build
```

After building, you will have:

- `spectra-db/dist/*.whl`
- `spectra-db/packages/spectra-db-assets/dist/*.whl`
- `spectra-db/packages/spectra-db-sources/dist/*.whl`

### Step 3 — Install the built wheels into your active environment

You can run this from **any directory**, as long as you point at the wheel paths:

```bash
pip install /path/to/spectra-db/packages/spectra-db-assets/dist/*.whl
pip install /path/to/spectra-db/packages/spectra-db-sources/dist/*.whl   # optional
pip install /path/to/spectra-db/dist/*.whl
```

> Important: the assets/sources wheels must have their large files present locally at build time (DuckDB and NDJSON). If you use Git LFS, make sure you have the real files checked out (not just pointers).

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

## Molecular (WebBook) semantics and storage

WebBook pages have two “reference-like” systems:

- **DiaNN anchors**: footnotes/annotations referenced from table cells
- **ref-N anchors**: bibliographic citations in the “References” section (often with DOI)

We preserve both:

- Footnotes stored under `species.extra_json["webbook_footnotes_by_id"]`
- Bibliographic references normalized into `refs` with IDs like `WB:<webbook_id>:ref-1`
- Cell markers stored separately in `context_json["cell_note_targets"]`

---

## Repository strategy (Git vs data artifacts)

**Keep in Git**
- source code (`src/`)
- schemas (`src/spectra_db/db/*.sql`)
- tests (`tests/`)
- docs (`docs/`, `mkdocs.yml`)
- examples (`examples/`)

**Do not commit by default**
- `data/raw/**` (raw caches)
- build artifacts: `dist/`, `build/`, `*.egg-info/`
- `site/` (mkdocs output)

If you need to share large artifacts in-repo, prefer **Git LFS**. For end users, prefer the Release wheels described above.

---

## License

MIT. See `LICENSE`.
