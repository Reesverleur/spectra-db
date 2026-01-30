"""
examples/asd_demo.py

Demonstrates using Spectra DB programmatically after you have ingested NIST ASD data
and built the local DuckDB.

Run from repo root (venv active):

    python examples/asd_demo.py

You should have already done:

    python scripts/bootstrap_db.py --truncate-all

This script demonstrates:
- fuzzy species search (e.g., "He")
- using a human-friendly ASD label (e.g., "H I") to resolve to the internal species_id
- printing levels and lines
- exporting a JSON bundle
"""

from __future__ import annotations

import json
from pathlib import Path

from spectra_db.query import open_default_api
from spectra_db.query.export import export_species_bundle
from spectra_db.util.asd_spectrum import parse_spectrum_label


def resolve_to_species_id(api, query: str) -> str:
    """Resolve a human-friendly query (e.g., 'H I' or 'He') to a single species_id.

    Strategy:
    - If query parses like an ASD spectrum label ('Fe II', 'Po LXVII', 'Ar 15+'), return that exact ion stage.
    - Otherwise, do a fuzzy species search and return the first match.
    """
    try:
        ps = parse_spectrum_label(query)
        return f"ASD:{ps.element}:{ps.charge:+d}"
    except Exception:
        matches = api.find_species(query, limit=10)
        if not matches:
            raise ValueError(f"No species found for query={query!r}") from None
        # You can pick a different match here depending on what you want.
        return matches[0]["species_id"]


def main() -> None:
    api = open_default_api()

    # 1) Fuzzy search example: "He" returns multiple possible ionization stages
    print("\n=== fuzzy species search: 'He' ===")
    he_matches = api.find_species("He", limit=10)
    for r in he_matches:
        print(r)

    # 2) Use a human-friendly ASD label ("H I") instead of internal "ASD:H:+0"
    # This resolves to "ASD:H:+0" automatically.
    user_friendly = "H I"
    species_id = resolve_to_species_id(api, user_friendly)
    print(f"\nResolved {user_friendly!r} -> species_id = {species_id}")

    isos = api.isotopologues_for_species(species_id)
    if not isos:
        raise RuntimeError(f"No isotopologues found for {species_id}; did you ingest + bootstrap?")

    iso_id = isos[0]["iso_id"]
    print(f"Using iso_id: {iso_id}")

    # 3) Levels
    levels = api.atomic_levels(iso_id, limit=15, max_energy=100000.0)
    print("\n=== first 15 levels (<= 100000 cm-1) ===")
    for lv in levels:
        print(
            {
                "E": lv["energy_value"],
                "unit": lv["energy_unit"],
                "unc": lv["energy_uncertainty"],
                "cfg": lv["configuration"],
                "term": lv["term"],
                "J": lv["j_value"],
                "g": lv["g_value"],
                "lande_g": lv.get("lande_g"),
                "ref_url": lv.get("ref_url"),
            }
        )

    # 4) Lines (visible range)
    lines = api.lines(
        iso_id,
        unit="nm",
        min_wav=400.0,
        max_wav=700.0,
        limit=10,
        parse_payload=True,
    )
    print("\n=== first 10 lines 400-700 nm ===")
    for ln in lines:
        payload = ln.get("payload") or {}
        print(
            {
                "lambda_nm": ln["wavelength"],
                "unc_nm": ln.get("unc"),
                "wavenumber_cm-1": payload.get("wavenumber_cm-1"),
                "Ei_cm-1": payload.get("Ei_cm-1"),
                "Ek_cm-1": payload.get("Ek_cm-1"),
                "Aki": payload.get("Aki_s-1"),
                "type": payload.get("type") or ln.get("selection_rules"),
                "ref_url": ln.get("ref_url"),
            }
        )

    # 5) Export bundle (JSON-serializable)
    bundle = export_species_bundle(
        query=user_friendly,  # can be "H I" OR a fuzzy string like "H"
        levels_max_energy=90000,
        lines_min_wav=400,
        lines_max_wav=700,
        lines_unit="nm",
        levels_limit=2000,
        lines_limit=2000,
    )
    print("\n=== export bundle keys ===")
    print(bundle.keys())

    out_path = Path("examples/h_i_bundle.json")
    out_path.write_text(json.dumps(bundle, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"\nWrote {out_path}")


if __name__ == "__main__":
    main()
