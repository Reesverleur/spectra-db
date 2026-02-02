from __future__ import annotations

import json
from typing import Any

from spectra_db.query.api import open_default_api
from spectra_db.util.asd_spectrum import parse_spectrum_label


def _resolve_species_ids(api, query: str) -> list[str]:
    """Resolve query like 'He I' or 'He' to one or more species_ids."""
    try:
        ps = parse_spectrum_label(query)
        return [f"ASD:{ps.element}:{ps.charge:+d}"]
    except Exception:
        matches = api.find_species(query, limit=500)
        return [m["species_id"] for m in matches]


def export_species_bundle(
    *,
    query: str,
    levels_max_energy: float | None = None,
    levels_limit: int = 5000,
    lines_min_wav: float | None = None,
    lines_max_wav: float | None = None,
    lines_unit: str = "nm",
    lines_limit: int = 10000,
    include_levels: bool = True,
    include_lines: bool = True,
    parse_line_payload: bool = True,
) -> dict[str, Any]:
    """
    Export a machine-friendly JSON bundle for one or more ASD species.

    This helper is designed for downstream programmatic use. The returned object is
    fully JSON-serializable.

    Args:
        query: A query like "He I" (exact ASD spectrum label) or "He" (fuzzy species search).
        levels_max_energy: Optional upper bound on level energy (<=).
        levels_limit: Maximum number of levels per isotopologue.
        lines_min_wav: Optional minimum wavelength (inclusive) for lines.
        lines_max_wav: Optional maximum wavelength (inclusive) for lines.
        lines_unit: Wavelength unit filter (must match stored `transitions.quantity_unit`), default "nm".
        lines_limit: Maximum number of lines per isotopologue.
        include_levels: If True, include the `levels` block.
        include_lines: If True, include the `lines` block.
        parse_line_payload: If True, parse `transitions.intensity_json` into a dict under each line entry.

    Returns:
        A dict containing:
            - `species`: resolved species rows
            - `isotopologues`: isotopologue rows for each species
            - optionally `levels` and `lines` blocks, depending on flags
    """
    api = open_default_api()
    species_ids = _resolve_species_ids(api, query)

    out: dict[str, Any] = {
        "query": query,
        "species_ids": species_ids,
        "species": [],
        "isotopologues": {},
    }

    # Species metadata
    for sid in species_ids:
        rows = api.find_species(sid, limit=50)
        # find exact match if present; else keep all hits
        exact = [r for r in rows if r.get("species_id") == sid]
        out["species"].extend(exact if exact else rows)

        iso = api.isotopologues_for_species(sid)
        out["isotopologues"][sid] = iso

    if include_levels:
        levels_block: dict[str, Any] = {}
        for sid in species_ids:
            for iso in out["isotopologues"].get(sid, []):
                iso_id = iso["iso_id"]
                levels_block[iso_id] = api.atomic_levels(
                    iso_id=iso_id,
                    limit=levels_limit,
                    max_energy=levels_max_energy,
                )
        out["levels"] = levels_block

    if include_lines:
        lines_block: dict[str, Any] = {}
        for sid in species_ids:
            for iso in out["isotopologues"].get(sid, []):
                iso_id = iso["iso_id"]
                lines_block[iso_id] = api.lines(
                    iso_id=iso_id,
                    unit=lines_unit,
                    min_wav=lines_min_wav,
                    max_wav=lines_max_wav,
                    limit=lines_limit,
                    parse_payload=parse_line_payload,
                )
        out["lines"] = lines_block

    return out


if __name__ == "__main__":
    # Small demo
    bundle = export_species_bundle(query="H I", levels_limit=10, lines_min_wav=400, lines_max_wav=700, lines_limit=5)
    print(json.dumps(bundle, indent=2, ensure_ascii=False))
