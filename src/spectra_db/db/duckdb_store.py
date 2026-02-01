# src/spectra_db/cli.py
from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any

from spectra_db.db.duckdb_store import DuckDBStore
from spectra_db.query import open_default_api
from spectra_db.query.export import export_species_bundle
from spectra_db.util.asd_spectrum import parse_spectrum_label
from spectra_db.util.paths import get_paths


def resolve_species_ids_atomic(api, query: str) -> list[str]:
    """
    Atomic profile:
    - Prefer ASD spectrum label parsing ("H I" etc.)
    - Fallback to fuzzy search for convenience
    """
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


def _fmt_number(x: float) -> str:
    if x is None:
        return ""

    try:
        if math.isnan(x):
            return "nan"
        if math.isinf(x):
            return "inf" if x > 0 else "-inf"
    except Exception:
        return str(x)

    y = round(float(x), 6)
    ay = abs(y)

    if ay != 0.0 and ay < 1e-3:
        s = f"{y:.6e}"
        mantissa, exp = s.split("e", 1)

        mantissa = mantissa.rstrip("0").rstrip(".")
        if mantissa in {"-0", ""}:
            mantissa = "0"

        sign = exp[0]
        digits = exp[1:].lstrip("0") or "0"
        exp_compact = (sign + digits) if sign == "-" else digits

        return f"{mantissa}e{exp_compact}"

    s = f"{y:.6f}".rstrip("0").rstrip(".")
    if s == "-0":
        s = "0"
    return s if s else "0"


def _fmt_cell(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        return _fmt_number(v)
    return str(v)


def _format_table(rows: list[dict[str, Any]], columns: list[tuple[str, str]]) -> str:
    table: list[list[str]] = []
    for r in rows:
        table.append([_fmt_cell(r.get(k)) for k, _ in columns])

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
    col_map = {k: h for k, h in columns_full}
    if include_keys:
        out: list[tuple[str, str]] = []
        for k in include_keys:
            if k in col_map:
                out.append((k, col_map[k]))
        return out
    return [(k, h) for (k, h) in columns_full if k not in exclude_keys]


def _degeneracy_g_from_j(j: object) -> float | None:
    if j is None:
        return None
    try:
        return 2.0 * float(j) + 1.0
    except Exception:
        return None


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Query local Spectra DB.")
    ap.add_argument(
        "--profile",
        choices=["atomic", "molecular"],
        default="atomic",
        help="Which database profile to query (atomic default; molecular is separate DB).",
    )

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

    dc = sub.add_parser("diatomic", help="Show WebBook diatomic constants (pivoted by electronic state).")
    dc.add_argument("q", help='e.g. "CO" or "Hydrogen fluoride" or "HF"')
    dc.add_argument("--limit", type=int, default=5000)
    dc.add_argument("--model", default="webbook_diatomic_constants")
    dc.add_argument("--footnotes", action="store_true", help="Print referenced DiaNN footnotes for the displayed table.")
    dc.add_argument("--citations", action="store_true", help="Print bibliographic references (WebBook 'References' section).")
    dc.add_argument("--exact", action="store_true", help="Try exact formula/name/species_id match first; fallback to fuzzy if not found.")
    dc.add_argument("--species-id", default=None, help="Query a specific species_id directly (bypasses search).")

    ex = sub.add_parser("export", help="Export a machine-friendly JSON bundle.")
    ex.add_argument("q", help='e.g. "H I" or "H"')
    ex.add_argument("--levels-max-energy", type=float, default=None)
    ex.add_argument("--levels-limit", type=int, default=5000)
    ex.add_argument("--lines-min-wav", type=float, default=None)
    ex.add_argument("--lines-max-wav", type=float, default=None)
    ex.add_argument("--lines-unit", default="nm")
    ex.add_argument("--lines-limit", type=int, default=10000)
    ex.add_argument("--out", type=Path, default=None)

    bs = sub.add_parser("bootstrap", help="Bootstrap DuckDB from normalized NDJSON.")
    bs.add_argument("--normalized", type=Path, default=None, help="Override normalized NDJSON directory. Defaults depend on profile.")
    bs.add_argument("--db-path", type=Path, default=None, help="Override output DuckDB path. Defaults depend on profile.")
    bs.add_argument("--truncate-all", action="store_true", help="Delete existing rows before loading.")

    args = ap.parse_args(argv)

    if args.cmd == "bootstrap":
        paths = get_paths()
        norm_dir = args.normalized
        if norm_dir is None:
            norm_dir = paths.normalized_dir if args.profile == "atomic" else paths.normalized_molecular_dir

        db_path = args.db_path
        if db_path is None:
            db_path = paths.default_duckdb_path if args.profile == "atomic" else paths.default_molecular_duckdb_path

        store = DuckDBStore(db_path=db_path)
        counts = store.bootstrap_from_normalized_dir(norm_dir, truncate_all=args.truncate_all, profile=args.profile)

        print(f"Bootstrapped profile={args.profile}")
        for k, v in counts.items():
            print(f"{k:26} {v:8}")
        return

    # diatomic is always molecular profile
    profile = args.profile
    if args.cmd == "diatomic":
        profile = "molecular"

    # IMPORTANT: read-only query connection; no schema init.
    api = open_default_api(profile=profile, read_only=True, ensure_schema=False)

    if args.cmd == "species":
        rows = api.find_species_smart(args.q, limit=50, include_formula_reversal=True)
        for r in rows:
            print(f"{r['species_id']:18}  {(r.get('formula') or ''):8}  {r.get('name')}")
        return

    if args.cmd == "levels":
        sids = resolve_species_ids_atomic(api, args.q)
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
                        "energy_value": r["energy_value"],
                        "Energy": r["energy_value"],
                        "Unit": r["energy_unit"],
                        "Unc": r["energy_uncertainty"],
                        "J": jv,
                        "g": gdeg,
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
                ("g", "g"),
                ("LandeG", "Landé g"),
                ("Configuration", "Configuration"),
                ("Term", "Term"),
                ("RefURL", "Ref URL"),
            ]

            exclude: set[str] = set()
            if args.compact:
                exclude |= {"Unit", "Unc", "LandeG", "RefURL"}
            if args.no_refs:
                exclude |= {"RefURL"}

            columns = _apply_column_filter(columns_full, include_keys=include_keys, exclude_keys=exclude)

            print(f"\n== {sid} (iso: {iso_id}) ==")
            print(_format_table(disp, columns))
        return

    if args.cmd == "lines":
        sids = resolve_species_ids_atomic(api, args.q)
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
                exclude |= {"ObsUnc", "RitzUnc", "Acc", "TPRefURL", "LineRefURL"}
            if args.no_refs:
                exclude |= {"TPRefURL", "LineRefURL"}

            columns = _apply_column_filter(columns_full, include_keys=include_keys, exclude_keys=exclude)

            print(f"\n== {sid} (iso: {iso_id}) ==")
            print(_format_table(disp, columns))
        return

    if args.cmd == "diatomic":
        # Resolve species_id
        sid = None

        if args.species_id:
            sid = args.species_id.strip()

        if sid is None and args.exact:
            sid = api.resolve_species_id(args.q, exact_first=True, fuzzy_fallback=True, fuzzy_limit=50)
            if sid is None:
                print(f"[diatomic] Exact match failed for {args.q!r}; falling back to fuzzy search…")

        if sid is None:
            sid = api.resolve_species_id(args.q, exact_first=True, fuzzy_fallback=True, fuzzy_limit=50)

        if sid is None:
            print("No species found.")
            return

        iso = api.isotopologues_for_species(sid)
        if not iso:
            print(f"{sid}: no isotopologues")
            return
        iso_id = iso[0]["iso_id"]

        sx_row = api.con.execute("SELECT extra_json FROM species WHERE species_id = ?", [sid]).fetchone()
        sx = _json_load_maybe(sx_row[0] if sx_row else None)
        webbook_id = sx.get("webbook_id")

        raw_foot = sx.get("webbook_footnotes_by_id") or {}

        def _foot_entry(v):
            if v is None:
                return {"text": "", "ref_targets": [], "dia_targets": []}
            if isinstance(v, str):
                return {"text": v, "ref_targets": [], "dia_targets": []}
            if isinstance(v, dict):
                return {"text": v.get("text") or "", "ref_targets": v.get("ref_targets") or [], "dia_targets": v.get("dia_targets") or []}
            return {"text": str(v), "ref_targets": [], "dia_targets": []}

        footnotes_by_id = {k: _foot_entry(v) for k, v in raw_foot.items()}

        referenced_note_targets: set[str] = set()

        def _markers(targets: list[str] | None) -> str:
            if not targets:
                return ""
            uniq = list(dict.fromkeys([str(t) for t in targets]))
            for t in uniq:
                referenced_note_targets.add(t)
            return " " + " ".join([f"[{t}]" for t in uniq])

        params = api.parameters(iso_id=iso_id, model=args.model, limit=args.limit)

        state_rows = api.con.execute(
            "SELECT electronic_label, energy_value, extra_json FROM states WHERE iso_id = ? AND state_type = 'molecular'",
            [iso_id],
        ).fetchall()

        by_state: dict[str, dict[str, Any]] = {}

        for st_label, te, extra_json in state_rows:
            st = (st_label or "").strip() or "(unknown)"
            extra = _json_load_maybe(extra_json)
            trans = (extra.get("Trans_clean") or "").strip()
            trans_marks = _markers(extra.get("Trans_note_targets") or [])
            te_marks = _markers(extra.get("Te_note_targets") or [])

            by_state.setdefault(st, {"State": st})
            by_state[st]["Te"] = te
            by_state[st]["Te_disp"] = f"{_fmt_cell(te)}{te_marks}" if te is not None else ""
            by_state[st]["Trans"] = (trans + trans_marks).strip()

        for p in params:
            ctx = _json_load_maybe(p.get("context_json"))
            st = (ctx.get("state_label") or "").strip() or "(unknown)"
            rec = by_state.setdefault(st, {"State": st, "Te": None, "Te_disp": "", "Trans": ""})

            marks = _markers(ctx.get("cell_note_targets") or [])

            if p["name"] == "nu00":
                suf = (ctx.get("value_suffix") or "").strip()
                base = f"{_fmt_cell(p['value'])} {suf}".strip()
                rec["nu00"] = f"{base}{marks}".strip()
            elif p["name"] == "Te":
                rec["Te"] = p.get("value")
                rec["Te_disp"] = f"{_fmt_cell(p['value'])}{marks}".strip()
            else:
                rec[p["name"]] = f"{_fmt_cell(p['value'])}{marks}".strip()

        columns_full = [
            ("State", "State"),
            ("Te_disp", "Te"),
            ("we", "ωe"),
            ("wexe", "ωexe"),
            ("weye", "ωeye"),
            ("Be", "Be"),
            ("ae", "αe"),
            ("ge", "γe"),
            ("De", "De"),
            ("be", "βe"),
            ("re", "re"),
            ("Trans", "Trans."),
            ("nu00", "ν00"),
        ]

        def _sort_key(rec: dict[str, Any]) -> tuple[int, float, str]:
            te = rec.get("Te", None)
            state = str(rec.get("State", "") or "")
            if te is None:
                return (1, float("inf"), state.lower())
            try:
                return (0, float(te), state.lower())
            except Exception:
                return (1, float("inf"), state.lower())

        out_rows = sorted(by_state.values(), key=_sort_key)

        print(f"\n== {sid} (iso: {iso_id}) ==")
        print(_format_table(out_rows, columns_full))

        if args.footnotes:
            targets = sorted(
                referenced_note_targets,
                key=lambda x: (int(x[3:]) if x.startswith("Dia") and x[3:].isdigit() else 10**9, x),
            )
            print("\n--- Footnotes referenced by table markers ---")
            if not targets:
                print("(none)")
            else:
                for t in targets:
                    ent = footnotes_by_id.get(t)
                    if not ent or not ent.get("text"):
                        print(f"[{t}] (missing)")
                        continue
                    text = ent["text"]
                    preview = text if len(text) <= 500 else text[:500] + "..."
                    line = f"[{t}] {preview}"
                    refs = ent.get("ref_targets") or []
                    if refs:
                        line += "  cites: " + " ".join([f"[{r}]" for r in refs])
                    print(line)

        if args.citations:
            print("\n--- Citations (WebBook References section) ---")
            if not webbook_id:
                print("(no webbook_id on species.extra_json)")
            else:
                ref_rows = api.con.execute(
                    "SELECT ref_id, doi, citation, url FROM refs WHERE ref_id LIKE ? ORDER BY ref_id",
                    [f"WB:{webbook_id}:ref-%"],
                ).fetchall()
                if not ref_rows:
                    print("(none)")
                else:
                    for ref_id, doi, citation, url in ref_rows:
                        short = ref_id.split(":")[-1]
                        print({"tag": f"[{short}]", "doi": doi, "citation": citation, "url": url})

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
