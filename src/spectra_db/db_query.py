# src/spectra_db/db_query.py
from __future__ import annotations

import json
from typing import Any

from spectra_db.query import open_default_api
from spectra_db.util.asd_spectrum import parse_spectrum_label


def _json_load_maybe(s: str | None) -> dict[str, Any]:
    if not s:
        return {}
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _resolve_atomic_species_id(api, query: str) -> str:
    """
    Resolve atomic queries:
    - Prefer ASD label parsing ("H I", "Fe II", etc.)
    - Fallback to fuzzy search on species table (first hit)
    """
    q = (query or "").strip()
    if not q:
        raise ValueError("Empty species query")

    try:
        ps = parse_spectrum_label(q)
        return f"ASD:{ps.element}:{ps.charge:+d}"
    except Exception:
        matches = api.find_species_smart(q, limit=50, include_formula_reversal=False)
        if not matches:
            raise ValueError(f"No atomic species found for query={query!r}") from None
        return matches[0]["species_id"]


def _resolve_molecular_species_id(api, query: str, *, exact_first: bool) -> str:
    """
    Resolve molecular queries using your existing exact + smart behavior (includes formula reversal).
    """
    q = (query or "").strip()
    if not q:
        raise ValueError("Empty species query")

    sid = api.resolve_species_id(
        q,
        exact_first=exact_first,
        fuzzy_fallback=True,
        fuzzy_limit=50,
        include_formula_reversal=True,
    )
    if sid is None:
        raise ValueError(f"No molecular species found for query={query!r}")
    return sid


def _pick_primary_iso_id(api, species_id: str) -> str:
    iso = api.isotopologues_for_species(species_id)
    if not iso:
        raise ValueError(f"{species_id}: no isotopologues")
    return iso[0]["iso_id"]


def _select_first_n_by_energy(rows: list[dict[str, Any]], *, n_total: int) -> list[dict[str, Any]]:
    """
    Select the first n_total rows by ascending energy_value (None sorted last).
    """
    if n_total <= 0:
        return []

    def key(r: dict[str, Any]) -> tuple[int, float]:
        ev = r.get("energy_value")
        if ev is None:
            return (1, float("inf"))
        try:
            return (0, float(ev))
        except Exception:
            return (1, float("inf"))

    rows_sorted = sorted(rows, key=key)
    return rows_sorted[:n_total]


def get_atomic_levels(
    species: str,
    *,
    n_excited: int = 5,
    max_energy: float | None = None,
    include_species_row: bool = True,
    include_isotopologues: bool = True,
) -> dict[str, Any]:
    """
    Convenience: resolve an atomic species and return ground + N excited levels.

    n_excited=0 => ground level only
    n_excited=1 => ground + 1 excited (2 levels total)
    """
    if n_excited < 0:
        raise ValueError("n_excited must be >= 0")

    api = open_default_api(profile="atomic", read_only=True, ensure_schema=False)
    sid = _resolve_atomic_species_id(api, species)
    iso_id = _pick_primary_iso_id(api, sid)

    # Ask for a bit more than needed so sorting works well.
    limit = max(50, n_excited + 5)
    levels_all = api.atomic_levels(iso_id=iso_id, limit=limit, max_energy=max_energy)
    levels_sel = _select_first_n_by_energy(levels_all, n_total=n_excited + 1)

    out: dict[str, Any] = {
        "profile": "atomic",
        "query": species,
        "species_id": sid,
        "iso_id": iso_id,
        "n_excited": n_excited,
        "levels": levels_sel,
    }

    if include_species_row:
        # QueryAPI.find_species is fuzzy; still useful as a metadata block.
        out["species_rows"] = api.find_species(species, limit=20)

    if include_isotopologues:
        out["isotopologues"] = api.isotopologues_for_species(sid)

    return out


def get_atomic_lines(
    species: str,
    *,
    n_excited: int = 5,
    unit: str = "nm",
    max_lines: int = 2000,
    min_wav: float | None = None,
    max_wav: float | None = None,
    include_species_row: bool = True,
    include_isotopologues: bool = True,
) -> dict[str, Any]:
    """
    Convenience: resolve an atomic species and return lines “reachable” from the first (ground + N excited) levels.

    Selection rule:
    - Determine an energy threshold from the (ground + N excited)-th level energy.
    - Keep lines whose payload Ei_cm-1 (lower state energy) is <= that threshold when available.
      If Ei_cm-1 is missing/unparseable, the line is kept.

    This gives an intuitive “give me lines originating from low-lying levels” behavior.

    n_excited=0 => use ground level threshold
    """
    if n_excited < 0:
        raise ValueError("n_excited must be >= 0")

    api = open_default_api(profile="atomic", read_only=True, ensure_schema=False)
    sid = _resolve_atomic_species_id(api, species)
    iso_id = _pick_primary_iso_id(api, sid)

    # Fetch levels to compute threshold
    lvl_limit = max(200, n_excited + 20)
    levels_all = api.atomic_levels(iso_id=iso_id, limit=lvl_limit, max_energy=None)
    levels_sel = _select_first_n_by_energy(levels_all, n_total=n_excited + 1)

    threshold: float | None = None
    if levels_sel:
        ev = levels_sel[-1].get("energy_value")
        try:
            threshold = float(ev) if ev is not None else None
        except Exception:
            threshold = None

    # Pull a lot of lines then filter down; still bounded.
    raw_lines = api.lines(
        iso_id=iso_id,
        unit=unit,
        min_wav=min_wav,
        max_wav=max_wav,
        limit=max_lines,
        parse_payload=True,
    )

    filtered: list[dict[str, Any]] = []
    for r in raw_lines:
        payload = r.get("payload") or {}
        ei = payload.get("Ei_cm-1")
        keep = True
        if threshold is not None and ei is not None:
            try:
                keep = float(ei) <= threshold
            except Exception:
                keep = True
        if keep:
            filtered.append(r)

    out: dict[str, Any] = {
        "profile": "atomic",
        "query": species,
        "species_id": sid,
        "iso_id": iso_id,
        "n_excited": n_excited,
        "level_energy_threshold_cm-1": threshold,
        "levels_used_for_threshold": levels_sel,
        "lines": filtered,
    }

    if include_species_row:
        out["species_rows"] = api.find_species(species, limit=20)

    if include_isotopologues:
        out["isotopologues"] = api.isotopologues_for_species(sid)

    return out


def get_diatomic_constants(
    species: str,
    *,
    n_excited: int = 0,
    exact_first: bool = True,
    model: str = "webbook_diatomic_constants",
    include_notes: bool = True,
    include_citations: bool = False,
) -> dict[str, Any]:
    """
    Convenience: resolve a molecular species and return diatomic constants for ground + N excited electronic states.

    n_excited=0 => ground electronic state only (lowest Te)
    """
    if n_excited < 0:
        raise ValueError("n_excited must be >= 0")

    api = open_default_api(profile="molecular", read_only=True, ensure_schema=False)
    sid = _resolve_molecular_species_id(api, species, exact_first=exact_first)
    iso_id = _pick_primary_iso_id(api, sid)

    # Species extra_json may contain WebBook metadata/footnotes
    sx_row = api.con.execute("SELECT extra_json FROM species WHERE species_id = ?", [sid]).fetchone()
    sx = _json_load_maybe(sx_row[0] if sx_row else None)

    webbook_id = sx.get("webbook_id")
    footnotes_by_id = sx.get("webbook_footnotes_by_id") if include_notes else None

    # States table: Te stored in states.energy_value for molecular
    state_rows = api.con.execute(
        "SELECT electronic_label, energy_value, extra_json FROM states WHERE iso_id = ? AND state_type = 'molecular'",
        [iso_id],
    ).fetchall()

    # Build state list sorted by Te
    states: list[dict[str, Any]] = []
    for label, te, extra_json in state_rows:
        states.append(
            {
                "state_label": (label or "").strip() or "(unknown)",
                "Te_cm-1": te,
                "extra": _json_load_maybe(extra_json),
            }
        )

    def state_key(s: dict[str, Any]) -> tuple[int, float, str]:
        te = s.get("Te_cm-1")
        if te is None:
            return (1, float("inf"), str(s.get("state_label", "")).lower())
        try:
            return (0, float(te), str(s.get("state_label", "")).lower())
        except Exception:
            return (1, float("inf"), str(s.get("state_label", "")).lower())

    states_sorted = sorted(states, key=state_key)
    states_sel = states_sorted[: n_excited + 1]

    # Pull parameters and attach to states using context_json["state_label"]
    params = api.parameters(iso_id=iso_id, model=model, limit=5000)
    by_state: dict[str, dict[str, Any]] = {s["state_label"]: {"state_label": s["state_label"], "Te_cm-1": s["Te_cm-1"], "constants": {}, "state_extra": s["extra"]} for s in states_sel}

    for p in params:
        ctx = _json_load_maybe(p.get("context_json"))
        st = (ctx.get("state_label") or "").strip() or "(unknown)"
        if st not in by_state:
            continue
        name = p.get("name")
        if not isinstance(name, str) or not name:
            continue
        # Keep both numeric and any text/suffix markers available
        by_state[st]["constants"][name] = {
            "value": p.get("value"),
            "unit": p.get("unit"),
            "uncertainty": p.get("uncertainty"),
            "text_value": p.get("text_value"),
            "value_suffix": p.get("value_suffix"),
            "context_json": p.get("context_json"),
            "raw_text": p.get("raw_text"),
        }

    citations: list[dict[str, Any]] | None = None
    if include_citations and webbook_id:
        ref_rows = api.con.execute(
            "SELECT ref_id, doi, citation, url FROM refs WHERE ref_id LIKE ? ORDER BY ref_id",
            [f"WB:{webbook_id}:ref-%"],
        ).fetchall()
        citations = [{"ref_id": rid, "doi": doi, "citation": cit, "url": url} for (rid, doi, cit, url) in ref_rows]

    out: dict[str, Any] = {
        "profile": "molecular",
        "query": species,
        "species_id": sid,
        "iso_id": iso_id,
        "n_excited": n_excited,
        "model": model,
        "webbook_id": webbook_id,
        "states": list(by_state.values()),
    }

    if include_notes:
        out["footnotes_by_id"] = footnotes_by_id

    if include_citations:
        out["citations"] = citations or []

    return out


__all__ = [
    "get_atomic_levels",
    "get_atomic_lines",
    "get_diatomic_constants",
]
