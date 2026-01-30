from __future__ import annotations

import argparse
import json
from collections import defaultdict
from typing import Any

from spectra_db.query import open_default_api
from spectra_db.util.asd_spectrum import parse_spectrum_label


def resolve_species_ids(api, query: str) -> list[str]:
    """Resolve a user query to species_ids.

    - If query looks like 'He I' or 'Fe II', resolve to exact ASD stage.
    - Otherwise do a substring search over species and return matches.
    """
    try:
        ps = parse_spectrum_label(query)
        return [f"ASD:{ps.element}:{ps.charge:+d}"]
    except Exception:
        matches = api.find_species(query, limit=200)
        return [m["species_id"] for m in matches]


def _format_table(rows: list[dict[str, Any]], columns: list[tuple[str, str]]) -> str:
    """Format rows into an aligned table with | separators."""
    table = []
    for r in rows:
        table.append([("" if r.get(k) is None else str(r.get(k))) for k, _ in columns])

    headers = [h for _, h in columns]

    widths = []
    for j in range(len(columns)):
        col_vals = [headers[j], *[row[j] for row in table]]
        widths.append(max(len(v) for v in col_vals))

    def fmt_row(vals: list[str]) -> str:
        return " | ".join(v.ljust(widths[i]) for i, v in enumerate(vals))

    sep = "-+-".join("-" * w for w in widths)

    out_lines = [fmt_row(headers), sep]
    out_lines.extend(fmt_row(r) for r in table)
    return "\n".join(out_lines)


def _group_sticky_levels(disp: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group levels by (configuration, term) but keep groups ordered by min energy."""
    groups = defaultdict(list)
    group_min: dict[tuple[str, str], float] = {}

    for d in disp:
        key = (d.get("config") or "", d.get("term") or "")
        groups[key].append(d)

    for key, items in groups.items():
        vals = [x["energy_value"] for x in items if x["energy_value"] is not None]
        group_min[key] = min(vals) if vals else float("inf")

    sorted_keys = sorted(groups.keys(), key=lambda k: (group_min[k], k[0], k[1]))

    out = []
    for key in sorted_keys:
        items = groups[key]
        items.sort(
            key=lambda x: (
                float("inf") if x["energy_value"] is None else x["energy_value"],
                float("inf") if x["J"] is None else x["J"],
            )
        )
        out.extend(items)
    return out


def _label_level_side(side: dict[str, Any] | None) -> str:
    """Make a compact label like '2p 2P° J=3/2'."""
    if not side:
        return ""
    cfg = (side.get("configuration") or "").strip()
    term = (side.get("term") or "").strip()
    j = (side.get("J") or "").strip()
    parts = [p for p in [cfg, term, (f"J={j}" if j else "")] if p]
    return " ".join(parts)


def main() -> None:
    ap = argparse.ArgumentParser(description="Query local Spectra DB.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("species", help="Search species by text.")
    sp.add_argument("q")

    lv = sub.add_parser("levels", help="List energy levels for a species/spectrum.")
    lv.add_argument("q", help='e.g. "He I" or "He"')
    lv.add_argument("--limit", type=int, default=20)
    lv.add_argument("--max-energy", type=float, default=None)

    ln = sub.add_parser("lines", help="List spectral lines for a species/spectrum.")
    ln.add_argument("q", help='e.g. "H I" or "H"')
    ln.add_argument(
        "--min-wav",
        type=float,
        default=None,
        help="Minimum wavelength for filter (same unit as stored).",
    )
    ln.add_argument(
        "--max-wav",
        type=float,
        default=None,
        help="Maximum wavelength for filter (same unit as stored).",
    )
    ln.add_argument("--unit", default="nm", help="Filter by quantity_unit (default: nm).")
    ln.add_argument("--limit", type=int, default=30)

    args = ap.parse_args()
    api = open_default_api()

    if args.cmd == "species":
        rows = api.find_species(args.q, limit=50)
        for r in rows:
            print(f"{r['species_id']:15}  {r['formula']:4}  {r.get('name')}")
        return

    if args.cmd == "levels":
        sids = resolve_species_ids(api, args.q)
        if not sids:
            print("No species found.")
            return

        for sid in sids:
            iso = api.isotopologues_for_species(sid)
            if not iso:
                print(f"{sid}: no isotopologues")
                continue
            iso_id = iso[0]["iso_id"]

            rows = api.atomic_levels(iso_id=iso_id, limit=args.limit, max_energy=args.max_energy)

            disp = []
            for r in rows:
                disp.append(
                    {
                        "energy_value": r["energy_value"],  # helper for grouping; removed later
                        "Energy": r["energy_value"],
                        "Unit": r["energy_unit"],
                        "Unc": r["energy_uncertainty"],
                        "J": r["j_value"],
                        "g": r["g_value"],
                        "config": r["configuration"],
                        "term": r["term"],
                        "Ref_URL": r.get("ref_url"),
                    }
                )

            disp = _group_sticky_levels(disp)
            for d in disp:
                d.pop("energy_value", None)

            print(f"\n== {sid} (iso: {iso_id}) ==")
            print(
                _format_table(
                    disp,
                    [
                        ("config", "Configuration"),
                        ("term", "Term"),
                        ("J", "J"),
                        ("g", "g"),
                        ("Energy", "Energy"),
                        ("Unc", "Unc"),
                        ("Unit", "Unit"),
                        ("Ref", "Ref_URL"),
                    ],
                )
            )
        return

    if args.cmd == "lines":
        sids = resolve_species_ids(api, args.q)
        if not sids:
            print("No species found.")
            return

        for sid in sids:
            iso = api.isotopologues_for_species(sid)
            if not iso:
                print(f"{sid}: no isotopologues")
                continue
            iso_id = iso[0]["iso_id"]

            # Build SQL with optional bounds
            clauses = ["iso_id = ?", "quantity_unit = ?"]
            params: list[Any] = [iso_id, args.unit]

            if args.min_wav is not None:
                clauses.append("quantity_value >= ?")
                params.append(args.min_wav)
            if args.max_wav is not None:
                clauses.append("quantity_value <= ?")
                params.append(args.max_wav)

            where = " AND ".join(clauses)
            q = f"""
            SELECT quantity_value, quantity_unit, quantity_uncertainty,
                   intensity_json, selection_rules, ref_id
            FROM transitions
            WHERE {where}
            ORDER BY quantity_value
            LIMIT ?
            """
            params.append(args.limit)

            rows = api.con.execute(q, params).fetchall()
            cols = ["wav", "unit", "unc", "payload", "type", "ref"]
            recs = [dict(zip(cols, r, strict=True)) for r in rows]

            disp = []
            for r in recs:
                payload = {}
                if r["payload"]:
                    try:
                        payload = json.loads(r["payload"])
                    except Exception:
                        payload = {}

                lower = _label_level_side(payload.get("lower"))
                upper = _label_level_side(payload.get("upper"))

                disp.append(
                    {
                        "Lambda": r["wav"],
                        "Unit": r["unit"],
                        "Unc": r["unc"],
                        "WN(cm-1)": payload.get("wavenumber_cm-1"),
                        "Ei(cm-1)": payload.get("Ei_cm-1"),
                        "Ek(cm-1)": payload.get("Ek_cm-1"),
                        "Type": payload.get("type") or r.get("type"),
                        "Aki": payload.get("Aki_s-1"),
                        "f": payload.get("f"),
                        "log(gf)": payload.get("log_gf"),
                        "Lower": lower,
                        "Upper": upper,
                        "Ref_URL": r.get("ref_url"),
                    }
                )

            print(f"\n== {sid} (iso: {iso_id}) ==")
            print(
                _format_table(
                    disp,
                    [
                        ("Lambda", "Lambda"),
                        ("Unc", "Unc"),
                        ("Unit", "Unit"),
                        ("WN(cm-1)", "WN(cm-1)"),
                        ("Ei(cm-1)", "Ei(cm-1)"),
                        ("Ek(cm-1)", "Ek(cm-1)"),
                        ("Type", "Type"),
                        ("Aki", "Aki"),
                        ("f", "f"),
                        ("log(gf)", "log(gf)"),
                        ("Lower", "Lower"),
                        ("Upper", "Upper"),
                        ("Ref", "Ref_URL"),
                    ],
                )
            )
        return


if __name__ == "__main__":
    main()
