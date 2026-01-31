from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from spectra_db.query import open_default_api
from spectra_db.query.export import export_species_bundle
from spectra_db.util.asd_spectrum import parse_spectrum_label


def resolve_species_ids(api, query: str) -> list[str]:
    try:
        ps = parse_spectrum_label(query)
        return [f"ASD:{ps.element}:{ps.charge:+d}"]
    except Exception:
        matches = api.find_species(query, limit=200)
        return [m["species_id"] for m in matches]


def _json_load_maybe(s: str | None) -> dict[str, Any]:
    if not s:
        return {}
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _first_url_ellipsis(urls: object) -> str:
    if not urls:
        return ""
    if isinstance(urls, str):
        return urls.strip()
    if isinstance(urls, list):
        cleaned = [str(u).strip() for u in urls if u and str(u).strip()]
        if not cleaned:
            return ""
        return cleaned[0] + (" …" if len(cleaned) > 1 else "")
    return str(urls)


def _format_table(rows: list[dict[str, Any]], columns: list[tuple[str, str]]) -> str:
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
                float("inf") if x["J"] is None else x["J"],
            )
        )
        out.extend(items)
    return out


def _parse_columns_arg(s: str | None) -> list[str] | None:
    if not s:
        return None
    cols = [c.strip() for c in s.split(",") if c.strip()]
    return cols or None


def _apply_column_filter(
    columns_full: list[tuple[str, str]],
    *,
    include_keys: list[str] | None,
    exclude_keys: set[str],
) -> list[tuple[str, str]]:
    """
    - If include_keys is provided, it overrides exclude_keys and returns columns in that order.
    - Otherwise returns columns_full minus excluded keys.
    """
    col_map = {k: h for k, h in columns_full}
    if include_keys:
        out: list[tuple[str, str]] = []
        for k in include_keys:
            if k in col_map:
                out.append((k, col_map[k]))
        return out
    return [(k, h) for (k, h) in columns_full if k not in exclude_keys]


def _degeneracy_g_from_j(j: object) -> float | None:
    """
    Degeneracy (2J+1). Works for integer/half-integer J.
    """
    if j is None:
        return None
    try:
        return 2.0 * float(j) + 1.0
    except Exception:
        return None


def main() -> None:
    ap = argparse.ArgumentParser(description="Query local Spectra DB.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("species", help="Search species by text.")
    sp.add_argument("q")

    lv = sub.add_parser("levels", help="List energy levels for a species/spectrum.")
    lv.add_argument("q", help='e.g. "He I" or "He"')
    lv.add_argument("--limit", type=int, default=20)
    lv.add_argument("--max-energy", type=float, default=None)
    lv.add_argument("--no-refs", action="store_true", help="Hide reference URL column.")
    lv.add_argument("--compact", action="store_true", help="Hide commonly irrelevant columns.")
    lv.add_argument(
        "--columns",
        default=None,
        help=("Comma-separated column keys to show (overrides --no-refs/--compact). Levels keys: Energy,Unit,Unc,J,g,LandeG,Configuration,Term,RefURL"),
    )

    ln = sub.add_parser("lines", help="List spectral lines for a species/spectrum.")
    ln.add_argument("q", help='e.g. "H I" or "H"')
    ln.add_argument("--min-wav", type=float, default=None)
    ln.add_argument("--max-wav", type=float, default=None)
    ln.add_argument("--unit", default="nm", help="Filter by wavelength unit stored in DB (default: nm).")
    ln.add_argument("--limit", type=int, default=30)
    ln.add_argument("--no-refs", action="store_true", help="Hide TP/Line reference URL columns.")
    ln.add_argument("--compact", action="store_true", help="Hide commonly irrelevant columns.")
    ln.add_argument(
        "--columns",
        default=None,
        help=("Comma-separated column keys to show (overrides --no-refs/--compact). Lines keys: Obs,ObsUnc,Ritz,RitzUnc,RelInt,Aki,Acc,Ei,Ek,Lower,Upper,Type,TPRefURL,LineRefURL"),
    )

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

        include_keys = _parse_columns_arg(args.columns)

        for sid in sids:
            iso = api.isotopologues_for_species(sid)
            if not iso:
                print(f"{sid}: no isotopologues")
                continue
            iso_id = iso[0]["iso_id"]

            rows = api.atomic_levels(iso_id=iso_id, limit=args.limit, max_energy=args.max_energy)

            disp = []
            for r in rows:
                exj = _json_load_maybe(r.get("extra_json"))
                ref_urls = exj.get("ref_urls") or r.get("ref_url")

                jv = r.get("j_value")
                gdeg = _degeneracy_g_from_j(jv)

                disp.append(
                    {
                        "energy_value": r["energy_value"],  # helper for sorting only
                        # display keys (stable identifiers for column selection)
                        "Energy": r["energy_value"],
                        "Unit": r["energy_unit"],
                        "Unc": r["energy_uncertainty"],
                        "J": jv,
                        "g": gdeg,  # degeneracy 2J+1
                        "LandeG": r.get("lande_g"),
                        "Configuration": r["configuration"],
                        "Term": r["term"],
                        "RefURL": _first_url_ellipsis(ref_urls),
                    }
                )

            disp = _group_sticky_levels(disp)
            for d in disp:
                d.pop("energy_value", None)

            columns_full = [
                ("Energy", "Energy"),
                ("Unit", "Unit"),
                ("Unc", "Unc"),
                ("J", "J"),
                ("g", "g"),  # immediately to the right of J
                ("LandeG", "Landé g"),
                ("Configuration", "Configuration"),
                ("Term", "Term"),
                ("RefURL", "Ref URL"),
            ]

            exclude: set[str] = set()
            if args.compact:
                # Keep the most-used columns for quick inspection
                # (Energy, J, g, Configuration, Term)
                exclude |= {"Unit", "Unc", "LandeG", "RefURL"}
            if args.no_refs:
                exclude |= {"RefURL"}

            columns = _apply_column_filter(columns_full, include_keys=include_keys, exclude_keys=exclude)

            print(f"\n== {sid} (iso: {iso_id}) ==")
            print(_format_table(disp, columns))
        return

    if args.cmd == "lines":
        sids = resolve_species_ids(api, args.q)
        if not sids:
            print("No species found.")
            return

        include_keys = _parse_columns_arg(args.columns)

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

                obs = payload.get("observed_wavelength")
                obs_unc = payload.get("observed_wavelength_unc")
                ritz = payload.get("ritz_wavelength")
                ritz_unc = payload.get("ritz_wavelength_unc")

                relint = payload.get("relative_intensity")
                aki = payload.get("Aki_s-1")
                acc = payload.get("accuracy_code")

                ei = payload.get("Ei_cm-1")
                ek = payload.get("Ek_cm-1")
                wn = payload.get("wavenumber_cm-1")
                if ek is None and ei is not None and wn is not None:
                    try:
                        ek = float(ei) + float(wn)
                    except Exception:
                        pass

                lower = payload.get("lower") or {}
                upper = payload.get("upper") or {}
                lower_cell = ";  ".join([x for x in [lower.get("configuration"), lower.get("term"), lower.get("J")] if x])
                upper_cell = ";  ".join([x for x in [upper.get("configuration"), upper.get("term"), upper.get("J")] if x])

                ttype = payload.get("type") or r.get("selection_rules")

                tp_urls = payload.get("tp_ref_urls") or []
                line_urls = payload.get("line_ref_urls") or []

                disp.append(
                    {
                        # stable keys for column selection
                        "Obs": obs,
                        "ObsUnc": obs_unc,
                        "Ritz": ritz,
                        "RitzUnc": ritz_unc,
                        "RelInt": relint,
                        "Aki": aki,
                        "Acc": acc,
                        "Ei": ei,
                        "Ek": ek,
                        "Lower": lower_cell,
                        "Upper": upper_cell,
                        "Type": ttype,
                        "TPRefURL": _first_url_ellipsis(tp_urls),
                        "LineRefURL": _first_url_ellipsis(line_urls),
                    }
                )

            columns_full = [
                ("Obs", "Observed λ"),
                ("ObsUnc", "Unc"),
                ("Ritz", "Ritz λ"),
                ("RitzUnc", "Unc"),
                ("RelInt", "Rel. Int."),
                ("Aki", "Aki (s^-1)"),
                ("Acc", "Acc"),
                ("Ei", "Ei (cm^-1)"),
                ("Ek", "Ek (cm^-1)"),
                ("Lower", "Lower Level Conf.; Term; J"),
                ("Upper", "Upper Level Conf.; Term; J"),
                ("Type", "Type"),
                ("TPRefURL", "TP Ref URL"),
                ("LineRefURL", "Line Ref URL"),
            ]

            exclude: set[str] = set()
            if args.compact:
                # typical "fast scan" view
                exclude |= {"ObsUnc", "RitzUnc", "Acc", "TPRefURL", "LineRefURL"}
            if args.no_refs:
                exclude |= {"TPRefURL", "LineRefURL"}

            columns = _apply_column_filter(columns_full, include_keys=include_keys, exclude_keys=exclude)

            print(f"\n== {sid} (iso: {iso_id}) ==")
            print(_format_table(disp, columns))
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
