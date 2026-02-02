# Spectra-DB

Spectra-DB is a **local-first spectroscopy database + query API** backed by **DuckDB**, built around a reproducible pipeline:

**fetch/cache → normalize to NDJSON → bootstrap into DuckDB → query via CLI + Python API**

## Quickstart

Once installed, you can query from anywhere:

```bash
spectra-db species HF
spectra-db levels "Fe II" --limit 10
spectra-db diatomic CO --limit 20
```

## Where to start

- **User Guide → README**: installation, profiles, CLI, and Python usage
- **API Reference**: documentation for the public Python modules and key runtime components
- **Developer → Contributing / Data layout**: dev setup and file layout conventions
