# Spectra DB

Local-first spectroscopic data mirror + query API (DuckDB-backed), currently focused on **NIST ASD (Atomic Spectra Database)** levels and lines. The project is designed to **preserve everything available** (including optional/ion-specific columns) and expose a clean, fast interface from both the **command line** and **Python**.

> This project is not affiliated with NIST. Use is at your own risk and subject to upstream terms/disclaimers.

---

## What this repository provides today

### Atomic (NIST ASD)
- **Energy levels** per spectrum (e.g. `Fe II`): Configuration / Term / J, level energy, uncertainty, and optional fields such as **Landé g-factor** and **leading percentages** when present.
- **Spectral lines** per spectrum: Observed/Ritz wavelengths + uncertainties, intensity/probability fields when available (Aki, f, log(gf), accuracy codes), endpoint energies (Ei/Ek), and lower/upper level designations.
- **Bibliographic reference handling**
  - Store the *reference codes* (e.g. `L8672c99`, `T6892c83`)
  - Capture **reference URLs** when present in ASD HTML (`onclick="popded('...get_ASBib_ref.cgi?...')"`).
  - Optional **enrichment pass** to fetch citation/DOI from those URLs.

### Future (planned)
- **Molecular** spectroscopy ingestion (NIST molecular SRDs and/or other sources): rotational/vibrational constants, rovibrational coupling, hyperfine, electronic levels.
- Statistical mechanics / thermo calculations from exact level sums (partition functions, Cp/Cv, etc.).
- Better lineage: link transitions to level IDs where possible.

---

## Repository layout

```
src/spectra_db/          # installable package: schema + query + export helpers
tools/scrapers/          # ingestion tools (NOT part of installable package)
scripts/                 # CLI utilities (bootstrap + query)
data/raw/                # cached raw HTTP responses (HTML/text)
data/normalized/         # canonical NDJSON (levels, lines, refs, etc.)
data/db/                 # generated DuckDB database file(s)
examples/                # example Python scripts
tests/                   # unit tests (offline; no network)
```

---

## Install

Create and activate a venv:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
```

Install editable + extras:

```bash
pip install -e ".[dev,scrape,docs]"
```

Extras:
- `dev`: pytest, ruff, mypy, pre-commit
- `scrape`: requests, beautifulsoup4, lxml (scrapers + tests that import scrapers)
- `docs`: mkdocs + mkdocstrings

---

## NIST traceability and provenance

The guiding rule: **all reported values should be traceable back to NIST outputs**.

### How traceability is preserved
- Raw responses are cached under `data/raw/` with request metadata and hashes.
- Normalized records include:
  - `source` tags (e.g., `NIST_ASD_LINES`)
  - reference codes (`TP Ref`, `Line Ref`) and (when available) a NIST ASBib URL
  - for lines, the scraper stores the **exact observed/ritz column header** strings and the **requested** wavelength convention.

### Vacuum vs air wavelengths
- ASD may report **vacuum** or **air** wavelengths depending on the option used and/or wavelength region.
- The scraper records:
  - `wavelength_medium_requested` (`vacuum` or `vac+air`)
  - `wavelength_medium_inferred` (from header text like “Vac”/“Air”)
  - `observed_wavelength_header` / `ritz_wavelength_header`
- **Default recommendation for physics work:** scrape and store wavelengths in **vacuum** (`--wavelength-type vacuum`), and use `wavenumber_cm-1` when available for conversions.
- **Database Convention:** the included database files (pre-scraped) report all transition wavelengths in vacuum.

> The pipeline does **not** silently convert air↔vacuum during scraping. Any conversions you do later should be explicit and documented.

---

## Data formats

### Canonical normalized NDJSON (human-readable, diffable)
Files live in `data/normalized/`:

- `species.ndjson`
- `isotopologues.ndjson`
- `states.ndjson` (levels)
- `transitions.ndjson` (lines)
- `refs.ndjson`
- `parameters.ndjson` (reserved for molecular/constants work later)

Each line is a JSON object. This layer is the long-term archive.

### DuckDB (fast query layer)
`data/db/spectra.duckdb` is generated from `data/normalized/` using:

```bash
python scripts/bootstrap_db.py --truncate-all
```

You can regenerate at any time; DuckDB is considered a build artifact.

---

## Levels: what we store

In `states.ndjson` / `states` table:
- `configuration`, `term`, `j_value`, `g_value` (degeneracy = 2J+1 for ASD atomic levels)
- `energy_value`, `energy_unit` (typically `cm-1`), `energy_uncertainty`
- Optional when present: `lande_g`, `leading_percentages`
- `ref_id` and `ref_url` (via join to `refs.url`)
- `extra_json`: **all other columns** the ASD table provides for that spectrum (so nothing is lost)

---

## Lines: what we store

In `transitions.ndjson` / `transitions` table:
- `quantity_value` + `quantity_unit` (the chosen wavelength for sorting/filtering)
- `quantity_uncertainty` (best-available uncertainty corresponding to chosen wavelength)
- `selection_rules` (Type when available)
- `ref_id` chosen preferentially from **Line Ref** (else TP Ref)

Additionally:
- `intensity_json` (structured physics payload):
  - observed/ritz wavelengths + uncertainties
  - Ei/Ek (parsed even if shown as a combined “Ei - Ek” cell)
  - lower/upper level triplets: configuration / term / J
  - Aki, accuracy code, relative intensity
  - TP Ref / Line Ref codes
  - medium traceability fields (`wavelength_medium_requested`, inferred medium, header strings)
- `extra_json`: **all other columns** returned by ASD for that spectrum/range.

---

## End-to-end ingestion workflow (fresh start)

If you want a clean run:

```bash
rm -rf data/db/*
rm -f data/normalized/*.ndjson
# Optional: clear cache if you want to re-download everything
# rm -rf data/raw/nist_asd
```

### Spectrum lists
If you already have the spectrum lists, you can reuse them:
- `data/normalized/asd_spectra_levels.txt`
- `data/normalized/asd_spectra_lines.txt`

If you need to regenerate from holdings pages:

```bash
python -m tools.scrapers.nist_asd.list_spectra --kind levels --out data/normalized/asd_spectra_levels.txt
python -m tools.scrapers.nist_asd.list_spectra --kind lines  --out data/normalized/asd_spectra_lines.txt
```

### Ingest all levels
```bash
python -m tools.scrapers.nist_asd.bulk_ingest \
  --mode levels \
  --spectra-file data/normalized/asd_spectra_levels.txt \
  --units-levels cm-1
```

### Ingest all lines (adaptive splitting + resume)
Recommended defaults for “all lines”:
- wavelength range: `0 → 200000 nm` (ASD coverage)
- start with a large `--initial-bin` and let adaptive splitting handle dense spectra

```bash
python -m tools.scrapers.nist_asd.bulk_ingest \
  --mode lines \
  --spectra-file data/normalized/asd_spectra_lines.txt \
  --wav-min 0 \
  --wav-max 200000 \
  --initial-bin 200000 \
  --min-bin 0.5 \
  --line-unit nm \
  --wavelength-type vacuum
```

### Build the DB
```bash
python scripts/bootstrap_db.py --truncate-all
```

---

## Reference enrichment (optional, recommended)

After ingest, you can enrich `refs.ndjson` by fetching ASBib pages for refs that have URLs:

```bash
python -m tools.scrapers.nist_asd.enrich_refs
python scripts/bootstrap_db.py --truncate-all
```

---

## Query from the command line

### Search species
```bash
python scripts/query.py species Fe
```

### Levels
```bash
python scripts/query.py levels "Fe II" --limit 20
```

### Lines (visible)
```bash
python scripts/query.py lines "Fe II" --min-wav 380 --max-wav 780 --unit nm --limit 40
```

### Export a machine-friendly JSON bundle
```bash
python scripts/query.py export "H I" \
  --levels-max-energy 90000 \
  --lines-min-wav 400 --lines-max-wav 700 --lines-unit nm \
  --out h_i_bundle.json
```

---

## Query from Python

```python
from spectra_db.query import open_default_api
from spectra_db.query.export import export_species_bundle

api = open_default_api()

# Find species:
print(api.find_species("Fe"))

# Get isotopologue id:
iso_id = api.isotopologues_for_species("ASD:Fe:+1")[0]["iso_id"]

# Levels:
levels = api.atomic_levels(iso_id, limit=50, max_energy=100000)
print(levels[0])

# Lines:
lines = api.lines(iso_id, unit="nm", min_wav=380, max_wav=780, limit=100, parse_payload=True)
print(lines[0]["payload"])

# Export:
bundle = export_species_bundle(query="Fe II", lines_min_wav=380, lines_max_wav=780, lines_unit="nm")
```

---

## Examples

See:
- `examples/asd_demo.py` — end-to-end usage after you have ingested + bootstrapped.

Run:
```bash
python examples/asd_demo.py
```

---

## Development

Format / lint:
```bash
ruff check .
ruff format .
```

Tests:
```bash
python -m pytest -q
```

Docs preview:
```bash
mkdocs serve
```

---

## Data size & Git strategy

Full ASD mirrors are large. Consider:
- ignoring `data/raw/` and `data/normalized/*.ndjson` in Git, or
- tracking large artifacts using Git LFS, or
- publishing DB snapshots as release assets.

DuckDB is always rebuildable from normalized NDJSON.

---

## License

MIT. See `LICENSE`.
