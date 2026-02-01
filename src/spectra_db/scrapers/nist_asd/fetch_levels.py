from __future__ import annotations

import argparse
import html as _html
import json
import re
from dataclasses import dataclass
from typing import Any

import pandas as pd
from bs4 import BeautifulSoup

from spectra_db.scrapers.common.http import fetch_cached
from spectra_db.scrapers.common.ids import make_id
from spectra_db.scrapers.common.ndjson import append_ndjson_dedupe
from spectra_db.scrapers.nist_asd.asd_client import LEVELS_URL, LevelsQuery, build_levels_params
from spectra_db.scrapers.nist_asd.normalize_atomic import (
    iso_id_for,
    make_isotopologue_record,
    make_species_record,
    parse_spectrum_label,
    species_id_for,
)
from spectra_db.scrapers.nist_asd.parse_levels import parse_levels_response
from spectra_db.util.paths import get_paths

_FLOAT_RE = re.compile(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?")
_POPDED_RE = re.compile(r"popded\('([^']+)'\)")
_REF_SPLIT_RE = re.compile(r"\s*,\s*")


@dataclass(frozen=True)
class FetchRunResult:
    ok: bool
    written: int
    status_code: int | None
    message: str
    raw_path: str | None = None


def extract_ref_urls_from_html(raw_html: str) -> dict[str, str]:
    """
    Build a mapping from visible reference code text -> popded URL.
    Works even when a table cell has multiple <a> tags (comma-separated).
    """
    soup = BeautifulSoup(raw_html, "html.parser")
    out: dict[str, str] = {}
    for a in soup.find_all("a"):
        txt = (a.get_text() or "").strip()
        onclick = (a.get("onclick") or "").strip()
        if not txt or not onclick:
            continue
        m = _POPDED_RE.search(onclick)
        if not m:
            continue
        url = _html.unescape(m.group(1)).strip()
        # Keep last-seen; usually identical anyway.
        out[txt] = url
    return out


def split_ref_codes(cell: object) -> list[str]:
    """
    Split a reference cell like:
        "L18349,L18361c138"
        "L18349, L18361c138"
    into a stable ordered unique list.
    """
    if cell is None:
        return []
    s = str(cell).strip()
    if not s or s.lower() == "nan":
        return []
    parts = [p.strip() for p in _REF_SPLIT_RE.split(s) if p.strip()]
    # preserve order, drop duplicates
    return list(dict.fromkeys(parts))


def make_ref_key(kind: str, code: str) -> str:
    """
    kind:
      - "E" for energy level references (ASBib type=E)
    code is the visible ASD code (often starts with L..., but that is fine).
    """
    return f"{kind}:{code}"


def _safe_float(x: object) -> float | None:
    s = str(x).strip().strip("[]()")
    if not s or s.lower() == "nan":
        return None
    s = s.replace(" ", "").replace(",", "")
    m = _FLOAT_RE.search(s)
    return float(m.group(0)) if m else None


def _parse_j(x: object) -> float | None:
    s = str(x).strip()
    if not s or s.lower() == "nan":
        return None
    if "/" in s:
        a, b = s.split("/", 1)
        try:
            return float(a) / float(b)
        except Exception:
            return None
    try:
        return float(s)
    except Exception:
        return None


def _col_exact(df: pd.DataFrame, name: str) -> str:
    target = name.strip().lower()
    for c in df.columns:
        if str(c).strip().lower() == target:
            return c
    raise KeyError(f"Missing required column: {name}. Columns={list(df.columns)}")


def _find_col_contains(df: pd.DataFrame, *needles: str) -> str | None:
    needles_l = [n.lower() for n in needles]
    for c in df.columns:
        name = str(c).strip().lower()
        if all(n in name for n in needles_l):
            return c
    return None


def _find_level_col(df: pd.DataFrame) -> str:
    for c in df.columns:
        if "level" in str(c).strip().lower():
            return c
    raise KeyError(f"Missing Level column. Columns={list(df.columns)}")


def _normalize_missing_series(s: pd.Series) -> pd.Series:
    return s.replace({"": pd.NA, "nan": pd.NA, "NaN": pd.NA, "None": pd.NA})


def run(*, spectrum: str, units: str = "cm-1", force: bool = False) -> FetchRunResult:
    try:
        paths = get_paths()
        ps = parse_spectrum_label(spectrum)
        sid = species_id_for(ps)
        iso_id = iso_id_for(sid)

        units_code = 0 if units == "cm-1" else 1
        q = LevelsQuery(spectrum=ps.asd_label, units=units_code, format_code=0)
        params = build_levels_params(q)

        raw_dir = paths.raw_dir / "nist_asd" / "levels"
        fr = fetch_cached(url=LEVELS_URL, params=params, cache_dir=raw_dir, force=force)

        raw_bytes = fr.content_path.read_bytes()
        raw_html = raw_bytes.decode("utf-8", errors="replace")
        ref_url_map = extract_ref_urls_from_html(raw_html)

        if fr.status_code != 200:
            return FetchRunResult(
                False,
                0,
                fr.status_code,
                f"HTTP {fr.status_code} fetching levels for {ps.asd_label}",
                str(fr.content_path),
            )

        df = parse_levels_response(raw_bytes)
        if df.empty:
            return FetchRunResult(
                False,
                0,
                fr.status_code,
                f"Parsed 0 rows for levels: {ps.asd_label}",
                str(fr.content_path),
            )

        cfg_col = _col_exact(df, "Configuration")
        term_col = _col_exact(df, "Term")
        j_col = _col_exact(df, "J")
        level_col = _find_level_col(df)
        unc_col = _find_col_contains(df, "unc")
        ref_col = _find_col_contains(df, "ref")

        lande_col = _find_col_contains(df, "land", "g")  # "Landé g-factor" etc.
        perc_col = _find_col_contains(df, "percent")  # "Leading Percentages"

        # Forward-fill for continuation rows
        df[cfg_col] = _normalize_missing_series(df[cfg_col]).ffill()
        df[term_col] = _normalize_missing_series(df[term_col]).ffill()

        normalized_dir = paths.normalized_dir
        species_path = normalized_dir / "species.ndjson"
        iso_path = normalized_dir / "isotopologues.ndjson"
        refs_path = normalized_dir / "refs.ndjson"
        states_path = normalized_dir / "states.ndjson"

        append_ndjson_dedupe(species_path, [make_species_record(ps)], "species_id")
        append_ndjson_dedupe(iso_path, [make_isotopologue_record(sid)], "iso_id")

        ref_records: list[dict] = []
        state_records: list[dict] = []

        handled_cols = {cfg_col, term_col, j_col, level_col}
        if unc_col:
            handled_cols.add(unc_col)
        if ref_col:
            handled_cols.add(ref_col)
        if lande_col:
            handled_cols.add(lande_col)
        if perc_col:
            handled_cols.add(perc_col)

        for _, row in df.iterrows():
            cfg = str(row.get(cfg_col, "")).strip()
            term = str(row.get(term_col, "")).strip()
            j_raw = str(row.get(j_col, "")).strip()
            if not cfg or not term or not j_raw:
                continue

            jv = _parse_j(j_raw)
            g = (2.0 * jv + 1.0) if jv is not None else None

            energy = _safe_float(row.get(level_col))
            if energy is None:
                continue

            unc = _safe_float(row.get(unc_col)) if unc_col else None

            # ---- References (multi-ref aware) ----
            ref_cell = str(row.get(ref_col, "")).strip() if ref_col else ""
            ref_codes = split_ref_codes(ref_cell)
            ref_keys = [make_ref_key("E", c) for c in ref_codes]
            ref_urls = [ref_url_map.get(c) for c in ref_codes if ref_url_map.get(c)]

            # Back-compat singleton
            primary_ref_id = ref_keys[0] if ref_keys else None

            # Emit one ref record per code/key (dedupe happens later)
            for code in ref_codes:
                rk = make_ref_key("E", code)
                ref_records.append(
                    {
                        "ref_id": rk,
                        "citation": None,
                        "doi": None,
                        "url": ref_url_map.get(code),
                        "notes": f"ASD Energy Level ref code={code}; url extracted from popded(...) when available.",
                    }
                )

            lande_g = _safe_float(row.get(lande_col)) if lande_col else None
            leading_pct = None
            if perc_col:
                val = row.get(perc_col)
                if val is not None and str(val).strip().lower() != "nan":
                    leading_pct = str(val).strip()

            # Capture ALL remaining columns for future-proofing
            extras: dict[str, Any] = {}
            for c in df.columns:
                if c in handled_cols:
                    continue
                v = row.get(c)
                if v is None:
                    continue
                sv = str(v).strip()
                if not sv or sv.lower() == "nan":
                    continue
                extras[str(c)] = sv

            # Store multi-ref info in extra_json so query/export can show it
            if ref_codes:
                extras["ref_codes"] = ref_codes
                extras["ref_keys"] = ref_keys
            if ref_urls:
                extras["ref_urls"] = ref_urls

            extra_json = json.dumps(extras, ensure_ascii=False) if extras else None

            # Make ID stable and sensitive to the full ref list (prevents corruption collisions)
            refs_for_id = ",".join(ref_keys) if ref_keys else ""
            state_id = make_id("state", iso_id, cfg, term, j_raw, str(energy), refs_for_id)

            state_records.append(
                {
                    "state_id": state_id,
                    "iso_id": iso_id,
                    "state_type": "atomic",
                    "electronic_label": f"{cfg} {term} J={j_raw}",
                    "vibrational_json": None,
                    "rotational_json": None,
                    "parity": None,
                    "configuration": cfg,
                    "term": term,
                    "j_value": jv,
                    "f_value": None,
                    "g_value": g,
                    "lande_g": lande_g,
                    "leading_percentages": leading_pct,
                    "extra_json": extra_json,
                    "energy_value": energy,
                    "energy_unit": units,
                    "energy_uncertainty": unc,
                    # Back-compat singleton ref_id; full list is in extra_json
                    "ref_id": primary_ref_id,
                    "notes": f"NIST ASD energy levels for {ps.asd_label}",
                }
            )

        append_ndjson_dedupe(refs_path, ref_records, "ref_id")
        n = append_ndjson_dedupe(states_path, state_records, "state_id")

        return FetchRunResult(True, n, fr.status_code, "OK", str(fr.content_path))

    except Exception as e:
        return FetchRunResult(False, 0, None, f"Exception: {type(e).__name__}: {e}", None)


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch NIST ASD energy levels (energy1.pl).")
    ap.add_argument("--spectrum", required=True)
    ap.add_argument("--units", default="cm-1", choices=["cm-1", "eV"])
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    res = run(spectrum=args.spectrum, units=args.units, force=args.force)
    if res.ok:
        print(f"Wrote {res.written} state records for {args.spectrum}. Raw: {res.raw_path}")
    else:
        print(f"ERROR: {res.message} Raw: {res.raw_path}")


if __name__ == "__main__":
    main()
