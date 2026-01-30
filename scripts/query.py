from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

from spectra_db.query import open_default_api
from spectra_db.query.export import export_species_bundle
from spectra_db.util.asd_spectrum import parse_spectrum_label

"""
Spectra DB Query CLI
====================

This script is for interactive/verification querying of the local Spectra DB.
It supports both human-readable tables and machine-friendly JSON export.

How queries are resolved
------------------------
Many commands accept a "query" string which can be either:

1) An ASD spectrum label (exact ion stage), e.g.
   - "H I"
   - "He I"
   - "Fe II"
   - "Po LXVII"
   - "Ar 15+"

   In this case we resolve directly to a single internal species_id:
     ASD:<Element>:<+charge>
   Example:
     "He I"  -> ASD:He:+0
     "He II" -> ASD:He:+1

2) A fuzzy search string, e.g. "He" or "Iron"
   In this case we search the species table (formula/name) and return all matches.

Commands
--------
species <q>
    Search species by substring.

levels <q> [--limit N] [--max-energy E]
    Print atomic energy levels as a readable table.
    Output is "group-sticky": levels are grouped by (Configuration, Term) while groups
    are ordered by the minimum energy in that group.

lines <q> [--min-wav A] [--max-wav B] [--unit nm] [--limit N]
    Print line data in the chosen wavelength unit with a readable table.
    The line table is populated from transitions.intensity_json, which contains
    wavenumber and other physics fields when available.

export <q> [--levels-max-energy E] [--levels-limit N]
          [--lines-min-wav A] [--lines-max-wav B] [--lines-unit nm] [--lines-limit N]
          [--out PATH]
    Emit a machine-friendly JSON bundle containing species + isotopologues and optional
    levels/lines. If --out is omitted, prints JSON to stdout.

Examples
--------
# List species matching an element symbol:
python scripts/query.py species He

# Print first 20 He I levels (grouped):
python scripts/query.py levels "He I" --limit 20

# Print visible H I lines (nm):
python scripts/query.py lines "H I" --min-wav 400 --max-wav 700 --unit nm --limit 30

# Export a JSON bundle for downstream code:
python scripts/query.py export "H I" --levels-max-energy 90000 --lines-min-wav 400 --lines-max-wav 700 --out h_i.json
"""


def resolve_species_ids(api, query: str) -> list[str]:
    """Resolve a user query to internal species_ids.

    - If query looks like 'He I'/'Fe II'/'Ar 15+' resolve to exactly one ASD ion stage.
    - Otherwise search the species table by substring and return all matches.
    """
    try:
        ps = parse_spectrum_label(query)
        return [f"ASD:{ps.element}:{ps.charge:+d}"]
    except Exception:
        matches = api.find_species(query, limit=200)
        return [m["species_id"] for m in matches]


_FLOAT_RE = re.compile(r"[-+]?\d+(?:\.\d+)?")


def _group_thousands_space(n: int) -> str:
    s = str(abs(n))
    parts = []
    while s:
        parts.append(s[-3:])
        s = s[:-3]
    out = " ".join(reversed(parts))
    return ("-" if n < 0 else "") + out


def _fmt_cm1(x: float | None) -> str:
    if x is None:
        return ""
    # NIST shows e.g. "1 872.5998"
    s = f"{float(x):.4f}"
    if "." in s:
        ip, fp = s.split(".", 1)
        return f"{_group_thousands_space(int(ip))}.{fp}"
    return _group_thousands_space(int(s))


def _fmt_unc(x: float | None) -> str:
    if x is None:
        return ""
    x = float(x)
    # keep at least 4 decimals; allow up to 6 if needed
    s6 = f"{x:.6f}".rstrip("0").rstrip(".")
    if "." in s6 and len(s6.split(".", 1)[1]) >= 4:
        return s6
    return f"{x:.4f}"


def _fmt_trim(x: float | None, decimals: int = 3) -> str:
    if x is None:
        return ""
    s = f"{float(x):.{decimals}f}".rstrip("0").rstrip(".")
    return s


def _fmt_j(j: float | None) -> str:
    if j is None:
        return ""
    j = float(j)
    # Prefer halves like ASD (9/2, 7/2, ...)
    two_j = round(j * 2)
    if abs(j * 2 - two_j) < 1e-8:
        if two_j % 2 == 0:
            return str(two_j // 2)
        return f"{two_j}/2"
    # fallback
    return _fmt_trim(j, 6)


def _first_url_ellipsis(urls: object) -> str:
    if not urls:
        return ""
    if isinstance(urls, str):
        return urls
    if isinstance(urls, list):
        cleaned = [u for u in urls if u]
        if not cleaned:
            return ""
        return cleaned[0] + (" …" if len(cleaned) > 1 else "")
    return str(urls)


def _json_load_maybe(s: str | None) -> dict:
    if not s:
        return {}
    try:
        import json

        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _extract_level_reference(r: dict) -> str:
    """
    Prefer the ASD-style reference codes if present in extra_json;
    fall back to ref_url if that's all we have.
    """
    ex = _json_load_maybe(r.get("extra_json"))
    for key in ("Reference", "References", "Ref", "Refs", "ref", "refs"):
        if key in ex and ex[key]:
            v = ex[key]
            if isinstance(v, list):
                return ", ".join(str(x) for x in v if x is not None and str(x).strip())
            return str(v)
    return r.get("ref_url") or ""


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


def _format_table_adv(rows: list[dict[str, Any]], columns: list[tuple[str, str, str]]) -> str:
    """
    columns: (key, header, align) where align is 'l' or 'r'
    """
    table = []
    for r in rows:
        table.append([("" if r.get(k) is None else str(r.get(k))) for k, _, _ in columns])

    headers = [h for _, h, _ in columns]

    widths = []
    for j in range(len(columns)):
        col_vals = [headers[j], *[row[j] for row in table]]
        widths.append(max(len(v) for v in col_vals))

    def fmt_row(vals: list[str], is_header: bool = False) -> str:
        out = []
        for i, v in enumerate(vals):
            align = columns[i][2]
            if is_header:
                out.append(v.ljust(widths[i]))
            else:
                out.append(v.rjust(widths[i]) if align == "r" else v.ljust(widths[i]))
        return " | ".join(out)

    sep = "-+-".join("-" * w for w in widths)

    out_lines = [fmt_row(headers, is_header=True), sep]
    out_lines.extend(fmt_row(r) for r in table)
    return "\n".join(out_lines)


def _group_sticky_levels(disp: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group levels by (Configuration, Term) while ordering groups by min energy."""
    groups = defaultdict(list)
    group_min: dict[tuple[str, str], float] = {}

    for d in disp:
        key = (d.get("Configuration") or "", d.get("Term") or "")
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
                float("-inf") if x["J"] is None else -float(x["J"]),
            )
        )
        out.extend(items)
    return out


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
    ln.add_argument("--min-wav", type=float, default=None)
    ln.add_argument("--max-wav", type=float, default=None)
    ln.add_argument("--unit", default="nm", help="Filter by wavelength unit stored in DB (default: nm).")
    ln.add_argument("--limit", type=int, default=30)

    ex = sub.add_parser("export", help="Export a machine-friendly JSON bundle.")
    ex.add_argument("q", help='e.g. "H I" or "H"')
    ex.add_argument("--levels-max-energy", type=float, default=None)
    ex.add_argument("--levels-limit", type=int, default=5000)
    ex.add_argument("--lines-min-wav", type=float, default=None)
    ex.add_argument("--lines-max-wav", type=float, default=None)
    ex.add_argument("--lines-unit", default="nm")
    ex.add_argument("--lines-limit", type=int, default=10000)
    ex.add_argument("--out", type=Path, default=None)

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
                        "energy_value": r["energy_value"],  # helper for sorting only
                        "Configuration": r.get("configuration"),
                        "Term": r.get("term"),
                        "J": r.get("j_value"),
                        "Level": r.get("energy_value"),
                        "Uncertainty": r.get("energy_uncertainty"),
                        "Landé-g": r.get("lande_g"),
                        "Leading percentages": r.get("leading_percentages"),
                        "Ref URL": _first_url_ellipsis(_json_load_maybe(r.get("extra_json")).get("ref_urls") or r.get("ref_url")),
                        "extra_json": r.get("extra_json"),
                    }
                )

            # Keep ASD-like grouping order, but do NOT blank repeated config/term
            disp = _group_sticky_levels(disp)

            # Forward-fill Configuration / Term so implied values are printed
            last_conf = ""
            last_term = ""
            for d in disp:
                c = (d.get("Configuration") or "").strip()
                t = (d.get("Term") or "").strip()
                if c:
                    last_conf = c
                else:
                    d["Configuration"] = last_conf
                if t:
                    last_term = t
                else:
                    d["Term"] = last_term

            # Convert to display strings
            out_rows = []
            for d in disp:
                out_rows.append(
                    {
                        "Configuration": d.get("Configuration") or "",
                        "Term": d.get("Term") or "",
                        "J": _fmt_j(d.get("J")),
                        "Level": _fmt_cm1(d.get("Level")),
                        "Uncertainty": _fmt_unc(d.get("Uncertainty")),
                        "Landé-g": _fmt_trim(d.get("Landé-g"), 3),
                        "Leading percentages": (d.get("Leading percentages") or ""),
                        "Reference": (d.get("Reference") or ""),
                    }
                )

            print(f"\n== {sid} (iso: {iso_id}) ==")
            print(
                _format_table_adv(
                    out_rows,
                    [
                        ("Configuration", "Configuration", "l"),
                        ("Term", "Term", "l"),
                        ("J", "J", "r"),
                        ("Level", "Level (cm^-1)", "r"),
                        ("Uncertainty", "Uncertainty (cm^-1)", "r"),
                        ("Landé-g", "Landé-g", "r"),
                        ("Leading percentages", "Leading percentages", "r"),
                        ("Ref URL", "Ref URL", "l"),
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

            rows = api.lines(
                iso_id=iso_id,
                unit=args.unit,
                min_wav=args.min_wav,
                max_wav=args.max_wav,
                limit=args.limit,
                parse_payload=True,
            )

            disp = []
            for r in rows:
                payload = r.get("payload") or {}

                # Pull values in the same structure as NIST table
                obs = payload.get("observed_wavelength")
                obs_unc = payload.get("observed_wavelength_unc")
                ritz = payload.get("ritz_wavelength")
                ritz_unc = payload.get("ritz_wavelength_unc")

                relint = payload.get("relative_intensity")
                aki = payload.get("Aki_s-1")
                acc = payload.get("accuracy_code")

                ei = payload.get("Ei_cm-1")
                ek = payload.get("Ek_cm-1")
                # If Ek missing but Ei and wavenumber present, compute Ek (traceable derivation)
                wn = payload.get("wavenumber_cm-1")
                if ek is None and ei is not None and wn is not None:
                    try:
                        ek = float(ei) + float(wn)
                    except Exception:
                        pass

                lower = payload.get("lower") or {}
                upper = payload.get("upper") or {}

                # NIST shows conf, term, J in separate subfields; we print as one cell
                lower_cell = " ".join([x for x in [lower.get("configuration"), lower.get("term"), lower.get("J")] if x])
                upper_cell = " ".join([x for x in [upper.get("configuration"), upper.get("term"), upper.get("J")] if x])

                ttype = payload.get("type") or r.get("selection_rules")
                tp_urls = payload.get("tp_ref_urls") or []
                line_urls = payload.get("line_ref_urls") or []

                disp.append(
                    {
                        "Obs": obs,
                        "Unc": obs_unc,
                        "Ritz": ritz,
                        "Unc2": ritz_unc,
                        "RelInt": relint,
                        "Aki": aki,
                        "Acc": acc,
                        "Ei": ei,
                        "Ek": ek,
                        "Lower": lower_cell,
                        "Upper": upper_cell,
                        "Type": ttype,
                        "TP Ref URL": _first_url_ellipsis(tp_urls),
                        "Line Ref URL": _first_url_ellipsis(line_urls),
                    }
                )

            print(f"\n== {sid} (iso: {iso_id}) ==")
            print(
                _format_table(
                    disp,
                    [
                        ("Obs", "Observed λ"),
                        ("Unc", "Unc"),
                        ("Ritz", "Ritz λ"),
                        ("Unc2", "Unc"),
                        ("RelInt", "Rel. Int."),
                        ("Aki", "Aki (s^-1)"),
                        ("Acc", "Acc"),
                        ("Ei", "Ei (cm^-1)"),
                        ("Ek", "Ek (cm^-1)"),
                        ("Lower", "Lower Level Conf., Term, J"),
                        ("Upper", "Upper Level Conf., Term, J"),
                        ("Type", "Type"),
                        ("TP Ref URL", "TP Ref URL"),
                        ("Line Ref URL", "Line Ref URL"),
                    ],
                )
            )
        return

    if args.cmd == "export":
        bundle = export_species_bundle(
            query=args.q,
            levels_max_energy=args.levels_max_energy,
            levels_limit=args.levels_limit,
            lines_min_wav=args.lines_min_wav,
            lines_max_wav=args.lines_max_wav,
            lines_unit=args.lines_unit,
            lines_limit=args.lines_limit,
        )
        text = json.dumps(bundle, indent=2, ensure_ascii=False)
        if args.out:
            args.out.parent.mkdir(parents=True, exist_ok=True)
            args.out.write_text(text + "\n", encoding="utf-8")
            print(f"Wrote {args.out}")
        else:
            print(text)
        return


if __name__ == "__main__":
    main()
