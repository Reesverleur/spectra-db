"""
examples/molecular_demo.py

Molecular demo + validation script for Spectra-DB (WebBook diatomic constants).

Run from repo root:

    python examples/molecular_demo.py --q CO --strict

This example shows how to retrieve:
- diatomic constants (Te, we, Be, De, nu00 + suffix, Trans)
- table footnotes (DiaNN) and link them to individual cells via cell_note_targets
- bibliographic references (ref-N) and how footnotes can cite refs via ref_targets

It also performs validation checks (--strict).
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from spectra_db.query import open_default_api


def _json_load_maybe(s: str | None) -> dict[str, Any]:
    if not s:
        return {}
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _sorted_state_labels_by_te(state_map: dict[str, dict[str, Any]]) -> list[str]:
    def key(label: str) -> tuple[int, float, str]:
        rec = state_map[label]
        te = rec.get("Te")
        if te is None:
            return (1, float("inf"), label.lower())
        try:
            return (0, float(te), label.lower())
        except Exception:
            return (1, float("inf"), label.lower())

    return sorted(state_map.keys(), key=key)


def _normalize_footnote_entry(entry: Any) -> dict[str, Any]:
    """
    Backwards compatible:
      old: "Dia53": "some text"
      new: "Dia53": {"text": "...", "ref_targets": [...], "dia_targets": [...]}
    """
    if entry is None:
        return {"text": "", "ref_targets": [], "dia_targets": []}
    if isinstance(entry, str):
        return {"text": entry, "ref_targets": [], "dia_targets": []}
    if isinstance(entry, dict):
        return {
            "text": entry.get("text") or "",
            "ref_targets": entry.get("ref_targets") or [],
            "dia_targets": entry.get("dia_targets") or [],
        }
    return {"text": str(entry), "ref_targets": [], "dia_targets": []}


def fetch_webbook_metadata(api, species_id: str) -> dict[str, Any]:
    row = api.con.execute("SELECT extra_json FROM species WHERE species_id = ?", [species_id]).fetchone()
    extra = _json_load_maybe(row[0] if row else None)

    webbook_id = extra.get("webbook_id")
    notes_text = extra.get("webbook_notes_text")

    raw_foot = extra.get("webbook_footnotes_by_id") or {}
    footnotes_by_id = {k: _normalize_footnote_entry(v) for k, v in raw_foot.items()}

    embedded_refs = extra.get("webbook_references") or []

    refs_table: list[dict[str, Any]] = []
    if webbook_id:
        rows = api.con.execute(
            "SELECT ref_id, citation, doi, url FROM refs WHERE ref_id LIKE ? ORDER BY ref_id",
            [f"WB:{webbook_id}:ref-%"],
        ).fetchall()
        refs_table = [{"ref_id": r, "citation": c, "doi": d, "url": u} for (r, c, d, u) in rows]

    return {
        "webbook_id": webbook_id,
        "notes_text": notes_text,
        "footnotes_by_id": footnotes_by_id,
        "embedded_references": embedded_refs,
        "refs_table": refs_table,
    }


def fetch_states(api, iso_id: str) -> dict[str, dict[str, Any]]:
    rows = api.con.execute(
        "SELECT electronic_label, energy_value, energy_unit, extra_json FROM states WHERE iso_id = ? AND state_type = 'molecular'",
        [iso_id],
    ).fetchall()

    out: dict[str, dict[str, Any]] = {}
    for label, te, unit, extra_json in rows:
        st = (label or "").strip() or "(unknown)"
        extra = _json_load_maybe(extra_json)
        out[st] = {
            "State": st,
            "Te": te,
            "Te_unit": unit,
            "Te_note_targets": extra.get("Te_note_targets") or [],
            "Trans": (extra.get("Trans_clean") or "").strip(),
            "Trans_note_targets": extra.get("Trans_note_targets") or [],
            "params": {},
        }
    return out


def pivot_params_by_state(state_map: dict[str, dict[str, Any]], params: list[dict[str, Any]]) -> None:
    for p in params:
        name = p.get("name")
        if not name:
            continue
        ctx = _json_load_maybe(p.get("context_json"))
        st = (ctx.get("state_label") or "").strip() or "(unknown)"
        rec = state_map.setdefault(
            st,
            {"State": st, "Te": None, "Te_unit": None, "Te_note_targets": [], "Trans": "", "Trans_note_targets": [], "params": {}},
        )
        rec["params"][name] = {
            "value": p.get("value"),
            "unit": p.get("unit"),
            "suffix": (ctx.get("value_suffix") or "").strip() or None if name == "nu00" else None,
            "note_targets": ctx.get("cell_note_targets") or [],
            "context": ctx,
        }


def main() -> None:
    ap = argparse.ArgumentParser(description="Molecular demo + validation")
    ap.add_argument("--q", default="CO", help="Molecule query (default: CO)")
    ap.add_argument("--model", default="webbook_diatomic_constants")
    ap.add_argument("--limit", type=int, default=5000)
    ap.add_argument("--strict", action="store_true", help="Fail if expected features are missing.")
    ap.add_argument("--write-json", action="store_true", help="Write a JSON snapshot under examples/")
    args = ap.parse_args()

    api = open_default_api(profile="molecular")

    print("\n=== fuzzy species search (for exploration) ===")
    for r in api.find_species(args.q, limit=10):
        print(r)

    # Resolve species_id with exact-first, with a clear fallback message
    sid = api.resolve_species_id(args.q, exact_first=True, fuzzy_fallback=False)
    if sid is None:
        print(f"[molecular_demo] Exact match failed for {args.q!r}; falling back to fuzzy search…")
        sid = api.resolve_species_id(args.q, exact_first=False, fuzzy_fallback=True, fuzzy_limit=10)

    if sid is None:
        raise RuntimeError(f"No species found for {args.q!r}")

    isos = api.isotopologues_for_species(sid)
    if not isos:
        raise RuntimeError(f"No isotopologues found for {sid}; did you bootstrap molecular DB?")
    iso_id = isos[0]["iso_id"]

    wb = fetch_webbook_metadata(api, sid)
    states = fetch_states(api, iso_id)
    params = api.parameters(iso_id=iso_id, model=args.model, limit=args.limit)
    pivot_params_by_state(states, params)

    order = _sorted_state_labels_by_te(states)

    print(f"\nResolved {args.q!r} -> {sid}  iso_id={iso_id}")
    print("webbook_id:", wb.get("webbook_id"))
    print("footnotes:", len(wb["footnotes_by_id"]))
    print("references (embedded):", len(wb["embedded_references"]))
    print("references (refs table):", len(wb["refs_table"]))

    if args.strict:
        if (wb.get("notes_text") or "").strip() == "":
            raise AssertionError("STRICT: species.extra_json['webbook_notes_text'] is empty/missing.")
        if not wb.get("webbook_id"):
            raise AssertionError("STRICT: webbook_id missing from species.extra_json.")
        if len(wb["footnotes_by_id"]) == 0:
            raise AssertionError("STRICT: no footnotes scraped (webbook_footnotes_by_id empty).")
        if len(wb["embedded_references"]) == 0:
            raise AssertionError("STRICT: embedded webbook_references empty.")
        if len(wb["refs_table"]) == 0:
            raise AssertionError("STRICT: refs table has no entries for this webbook_id.")

    foot = wb["footnotes_by_id"]
    refs_table_by_short = {r["ref_id"].split(":")[-1]: r for r in wb["refs_table"]}

    found_any_targets = False
    resolved_targets = 0
    found_footnote_with_ref = False
    footnote_ref_links_found = 0

    for rec in states.values():
        for t in rec.get("Te_note_targets") or []:
            found_any_targets = True
            ent = foot.get(t)
            if ent and ent["text"].strip():
                resolved_targets += 1

        for t in rec.get("Trans_note_targets") or []:
            found_any_targets = True
            ent = foot.get(t)
            if ent and ent["text"].strip():
                resolved_targets += 1

        for p in (rec.get("params") or {}).values():
            targets = p.get("note_targets") or []
            if targets:
                found_any_targets = True
            for t in targets:
                ent = foot.get(t)
                if ent and ent["text"].strip():
                    resolved_targets += 1
                    for rt in ent.get("ref_targets") or []:
                        found_footnote_with_ref = True
                        if rt in refs_table_by_short:
                            footnote_ref_links_found += 1

    if args.strict:
        if not found_any_targets:
            raise AssertionError("STRICT: no cell/state had note_targets; expected at least one.")
        if resolved_targets == 0:
            raise AssertionError("STRICT: note_targets existed but none resolved to non-empty footnote text.")
        if not found_footnote_with_ref:
            raise AssertionError("STRICT: no footnote cited any ref-N via ref_targets.")
        if footnote_ref_links_found == 0:
            raise AssertionError("STRICT: footnotes cite refs but none of those refs exist in refs table.")

    # Show one illustrative example (ground state if available)
    if order:
        ground = order[0]
        g = states[ground]
        print("\n=== Example: ground state Te + markers ===")
        te_marks = " ".join([f"[{t}]" for t in (g.get("Te_note_targets") or [])])
        print({"State": ground, "Te": g.get("Te"), "Te_unit": g.get("Te_unit"), "markers": te_marks})

        # Prefer nu00 if it has markers; otherwise find first parameter with markers
        best = None
        if "nu00" in g.get("params", {}) and (g["params"]["nu00"].get("note_targets") or []):
            best = ("nu00", g["params"]["nu00"])
        else:
            for pname, p in (g.get("params") or {}).items():
                if p.get("note_targets"):
                    best = (pname, p)
                    break

        if best:
            pname, p = best
            marks = " ".join([f"[{t}]" for t in (p.get("note_targets") or [])])
            print("\n=== Example: parameter with footnote markers ===")
            print({"State": ground, "param": pname, "value": p.get("value"), "unit": p.get("unit"), "suffix": p.get("suffix"), "markers": marks})

            targets = p.get("note_targets") or []
            if targets:
                t0 = targets[0]
                ent = foot.get(t0)
                if ent:
                    preview = ent["text"][:300] + ("..." if len(ent["text"]) > 300 else "")
                    cited = ent.get("ref_targets") or []
                    print("\n=== Resolve first marker -> footnote (and cited refs) ===")
                    print({"marker": f"[{t0}]", "footnote_text_preview": preview, "cites": [f"[{x}]" for x in cited[:8]]})

    if args.write_json:
        out = {
            "query": args.q,
            "species_id": sid,
            "iso_id": iso_id,
            "webbook": wb,
            "states": states,
            "state_order_by_Te": order,
        }
        out_path = Path("examples") / f"{args.q.lower()}_molecular_demo_snapshot.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(out, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"\nWrote {out_path.resolve()}")

    print("\n=== Validation summary ===")
    print(
        {
            "states": len(states),
            "params_total": sum(len(s.get("params") or {}) for s in states.values()),
            "found_any_note_targets": found_any_targets,
            "resolved_note_targets_count": resolved_targets,
            "found_footnote_citing_ref": found_footnote_with_ref,
            "footnote_ref_links_found_in_refs_table": footnote_ref_links_found,
        }
    )


if __name__ == "__main__":
    main()
