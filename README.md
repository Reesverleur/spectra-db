# Spectra DB

**Spectra DB** is a **local-first spectroscopic data mirror + query API** backed by **DuckDB**.

The current focus is a faithful mirror of **NIST ASD (Atomic Spectra Database)**:

- scrape and cache ASD HTML responses (provenance / reproducibility)
- normalize to canonical NDJSON (diffable, resumable ingestion)
- load into DuckDB for fast local queries
- provide a CLI (`scripts/query.py`) and a small Python API (`spectra_db.query`)

> This project is not affiliated with NIST. Use at your own risk and subject to upstream terms/disclaimers.

---

## What this repository provides today

### Atomic (NIST ASD)

#### Energy levels (`energy1.pl`)
- Configuration, Term, J
- Level energy and uncertainty
- Landé g-factor and leading percentages when present
- **References**: supports **multiple references per level** (comma-separated in ASD tables)

#### Spectral lines (`lines1.pl`)
- Observed and Ritz wavelengths + uncertainties
- Relative intensity, Aki, accuracy (when present)
- Endpoint energies (Ei/Ek) and lower/upper level designations
- **References**: supports **multiple refs per line** for both:
  - **TP Ref** (transition probabilities; `type=T`)
  - **Line Ref** (line classification; `type=L`)

#### Bibliographic reference pipeline
- ASD table cells often contain **comma-separated** reference codes.
- We preserve:
  - the raw **reference codes** (as shown in ASD tables)
  - **reference keys** with an explicit kind prefix: `E:<code>`, `L:<code>`, `T:<code>`
  - **reference URLs** when present in HTML (`onclick="popded('...get_ASBib_ref.cgi?...')"`), or reconstructed
- `enrich_refs.py` (optional) fetches each unique ASBib page and extracts DOI + citation metadata.

### Future (planned)
- broader sources (molecules, HITRAN, ExoMol, etc.)
- better indexing and higher-level query primitives
- richer exports (BibTeX, citation bundles, etc.)

---

## Repository layout (high level)

- `tools/scrapers/nist_asd/` — ASD scrapers + parsers + enrichment
- `data/raw/` — cached HTML responses (traceability)
- `data/normalized/` — canonical NDJSON (resumable, diffable)
- `src/spectra_db/` — DuckDB schema + query API
- `scripts/query.py` — CLI querying / verification
- `examples/` — small Python examples

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

## NIST traceability and provenance

### How traceability is preserved
- Each fetch stores raw response bytes in `data/raw/nist_asd/.../*.body` plus request metadata in `*.meta`.
- Normalized rows preserve:
  - requested wavelength medium and header strings (lines)
  - original table fields not explicitly mapped are retained in `extra_json` / `payload`

### Vacuum vs air wavelengths
When scraping lines you specify the requested wavelength type:

- `--wavelength-type vacuum` (vacuum)
- `--wavelength-type vac+air` (ASD’s mixed mode where ASD may return air wavelengths where appropriate)

We store the requested medium and header strings for later verification.

---

## Data formats

### Canonical normalized NDJSON
Written to `data/normalized/`:

- `species.ndjson`
- `isotopologues.ndjson`
- `states.ndjson` (atomic levels)
- `transitions.ndjson` (spectral lines)
- `refs.ndjson` (bibliography)

Notes:
- `states.extra_json` may include:
  - `ref_codes`, `ref_keys`, `ref_urls` (lists)
- `transitions.intensity_json` is a JSON-serialized payload that may include:
  - `tp_ref_codes`, `line_ref_codes` (lists)
  - `tp_ref_keys`, `line_ref_keys` (lists)
  - `tp_ref_urls`, `line_ref_urls` (lists)

### DuckDB (fast query layer)
After loading NDJSON into DuckDB, query via:
- CLI (`scripts/query.py`)
- Python API (`spectra_db.query`)

---

## End-to-end ingestion workflow

### 0) (Optional) Start fresh
```bash
rm -f data/normalized/*.ndjson
```

### 1) Ingest levels for a spectrum
```bash
python -m tools.scrapers.nist_asd.fetch_levels --spectrum "Fe II"
```

### 2) Ingest lines for a spectrum (small window)
```bash
python -m tools.scrapers.nist_asd.fetch_lines --spectrum "Fe II" --min-wav 380 --max-wav 381 --unit nm --wavelength-type vacuum
```

Notes:
- The fetchers are **resumable** and prefer cached responses in `data/raw/`.
- Add `--force` to re-download instead of using cache.

### 3) Build the DB
```bash
python scripts/bootstrap_db.py --truncate-all
```

---

## Reference enrichment (optional, recommended)

Once you have `refs.ndjson`, enrich DOI + citation metadata:

```bash
python -m tools.scrapers.nist_asd.enrich_refs
```

For a quick test run (if supported by your script):
```bash
python -m tools.scrapers.nist_asd.enrich_refs --max 200
```

---

## Query from the command line

All CLI commands are run from repo root:

```bash
python scripts/query.py --help
```

### Search species
```bash
python scripts/query.py species He
python scripts/query.py species Iron
```

### Levels
Default columns include **degeneracy** `g = 2J + 1` next to `J`.

```bash
python scripts/query.py levels "Fe II" --limit 30
python scripts/query.py levels "Fe II" --max-energy 90000 --limit 50
```

Hide reference URLs:
```bash
python scripts/query.py levels "Fe II" --no-refs
```

Compact view (hides Unit/Unc/Landé g/Refs):
```bash
python scripts/query.py levels "Fe II" --compact
```

Explicit columns (overrides compact/no-refs):
```bash
python scripts/query.py levels "Fe II" --columns "Energy,J,g,Configuration,Term"
```

### Lines
```bash
python scripts/query.py lines "Fe II" --min-wav 380 --max-wav 381 --unit nm --limit 30
python scripts/query.py lines "H I" --min-wav 400 --max-wav 700 --unit nm --limit 30
```

Hide reference URLs:
```bash
python scripts/query.py lines "Fe II" --min-wav 380 --max-wav 381 --unit nm --no-refs
```

Compact view (hides uncertainties/Acc/Refs):
```bash
python scripts/query.py lines "Fe II" --min-wav 380 --max-wav 381 --unit nm --compact
```

Explicit columns:
```bash
python scripts/query.py lines "Fe II" --min-wav 380 --max-wav 381 --unit nm --columns "Obs,Ritz,Ei,Ek,Lower,Upper,Type"
```

### Export a machine-friendly JSON bundle
```bash
python scripts/query.py export "H I" --levels-max-energy 90000 --lines-min-wav 400 --lines-max-wav 700 --lines-unit nm --out examples/h_i_bundle.json
```

---

## Query from Python

```python
from spectra_db.query import open_default_api
from spectra_db.query.export import export_species_bundle
from spectra_db.util.asd_spectrum import parse_spectrum_label

api = open_default_api()

# Resolve a human-friendly ASD label (e.g., "H I") to species_id
ps = parse_spectrum_label("H I")
species_id = f"ASD:{ps.element}:{ps.charge:+d}"

# Pick an isotopologue (usually one per species for atomic ASD)
isos = api.isotopologues_for_species(species_id)
iso_id = isos[0]["iso_id"]

# Levels
levels = api.atomic_levels(iso_id=iso_id, limit=20, max_energy=100000.0)

# Lines (payload parsed)
lines = api.lines(
    iso_id=iso_id,
    unit="nm",
    min_wav=400.0,
    max_wav=700.0,
    limit=20,
    parse_payload=True,
)

# Export a bundle (JSON-serializable)
bundle = export_species_bundle(
    query="H I",
    levels_max_energy=90000,
    lines_min_wav=400,
    lines_max_wav=700,
    lines_unit="nm",
    levels_limit=2000,
    lines_limit=2000,
)
```

---

## Examples

- `examples/asd_demo.py` — programmatic usage walkthrough (levels, lines, export)

---

## Data size & Git strategy

Full ASD mirrors are large. Consider:
- ignoring `data/raw/` and `data/normalized/*.ndjson` in Git, or
- tracking large artifacts using Git LFS, or
- publishing DB snapshots as release assets.

DuckDB is rebuildable from normalized NDJSON.

---

## License

MIT. See `LICENSE`.
