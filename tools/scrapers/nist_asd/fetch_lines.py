from __future__ import annotations

import argparse
import html as _html
import json
import re
from dataclasses import dataclass

from bs4 import BeautifulSoup

from spectra_db.util.paths import get_paths
from tools.scrapers.common.http import fetch_cached
from tools.scrapers.common.ndjson import append_ndjson_dedupe
from tools.scrapers.nist_asd.asd_client import LINES_URL, LinesQuery, build_lines_params
from tools.scrapers.nist_asd.normalize_atomic import (
    iso_id_for,
    make_isotopologue_record,
    make_species_record,
    make_transition_record,
    parse_spectrum_label,
    species_id_for,
)
from tools.scrapers.nist_asd.parse_lines import parse_lines_response

_FLOAT_RE = re.compile(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?")
_POPDED_RE = re.compile(r"popded\('([^']+)'\)")


@dataclass(frozen=True)
class FetchRunResult:
    ok: bool
    written: int
    status_code: int | None
    message: str
    raw_path: str | None = None


def extract_ref_urls_from_html(raw_html: str) -> dict[str, str]:
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
        out[txt] = _html.unescape(m.group(1)).strip()
    return out


def _safe_float(x: object) -> float | None:
    s = str(x).strip()
    if not s or s.lower() == "nan":
        return None
    s = s.replace(" ", "").replace(",", "")
    m = _FLOAT_RE.search(s)
    return float(m.group(0)) if m else None


def _find_cols(df, *needles: str) -> list[str]:
    needles_l = [n.lower() for n in needles]
    out: list[str] = []
    for c in df.columns:
        name = str(c).lower()
        if all(n in name for n in needles_l):
            out.append(c)
    return out


def _find_col(df, *needles: str) -> str | None:
    cols = _find_cols(df, *needles)
    return cols[0] if cols else None


def _infer_medium_from_header(header: str | None) -> str | None:
    if not header:
        return None
    h = header.lower()
    if "vac" in h:
        return "vacuum"
    if "air" in h:
        return "air"
    return None


def _parse_energy_range(val: object) -> tuple[float | None, float | None]:
    """Parse a cell like '1872.5998 - 112 994.097' into (Ei, Ek)."""
    s = str(val).strip()
    if not s or s.lower() == "nan":
        return (None, None)
    s = s.replace(" ", "").replace(",", "")
    parts = re.split(r"\s*-\s*", s)
    nums: list[float] = []
    for p in parts:
        m = _FLOAT_RE.search(p)
        if m:
            nums.append(float(m.group(0)))
    if len(nums) >= 2:
        return nums[0], nums[1]
    if len(nums) == 1:
        return nums[0], None
    return (None, None)


def _parse_level_triplet(val: object) -> dict[str, str | None]:
    """Parse a combined 'Conf  Term...  J' cell.

    Assumptions (per ASD lines tables):
      - Configuration has no spaces (first token)
      - J has no spaces (last token, e.g. 9/2)
      - Term may contain internal spaces (middle tokens)

    Handles multiple spaces/tabs and ignores empty/'nan' cells.
    """
    s = str(val).strip()
    if not s or s.lower() == "nan":
        return {"configuration": None, "term": None, "J": None}

    # split on any whitespace (spaces or tabs)
    toks = re.split(r"\s+", s)
    if len(toks) < 2:
        return {"configuration": s, "term": None, "J": None}

    config = toks[0]
    j = toks[-1]
    term = " ".join(toks[1:-1]).strip() or None

    return {"configuration": config, "term": term, "J": j}


def _prune(obj: object) -> object:
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            v2 = _prune(v)
            if v2 is None:
                continue
            if isinstance(v2, dict) and not v2:
                continue
            out[k] = v2
        return out
    return obj


def run(
    *,
    spectrum: str,
    min_wav: float,
    max_wav: float,
    unit: str = "nm",
    wavelength_type: str = "vacuum",
    force: bool = False,
) -> FetchRunResult:
    try:
        paths = get_paths()
        ps = parse_spectrum_label(spectrum)
        sid = species_id_for(ps)
        iso_id = iso_id_for(sid)

        q = LinesQuery(
            spectra=ps.asd_label,
            low_w=min_wav,
            upp_w=max_wav,
            unit=unit,
            wavelength_type=wavelength_type,
            energy_level_unit="cm-1",
            format_code=1,
        )
        params = build_lines_params(q)

        raw_dir = paths.raw_dir / "nist_asd" / "lines"
        fr = fetch_cached(url=LINES_URL, params=params, cache_dir=raw_dir, force=force)

        raw_bytes = fr.content_path.read_bytes()
        raw_html = raw_bytes.decode("utf-8", errors="replace")
        ref_url_map = extract_ref_urls_from_html(raw_html)

        if fr.status_code != 200:
            return FetchRunResult(False, 0, fr.status_code, f"HTTP {fr.status_code} fetching lines for {ps.asd_label}", str(fr.content_path))

        df = parse_lines_response(raw_bytes)
        if df.empty:
            return FetchRunResult(True, 0, fr.status_code, "OK (0 rows)", str(fr.content_path))

        # Wavelengths
        obs_wl_col = _find_col(df, "observed", "wavelength")
        ritz_wl_col = _find_col(df, "ritz", "wavelength")

        # Two uncertainty cols by order
        unc_cols = _find_cols(df, "unc")
        obs_unc_col = _find_col(df, "unc", "observed") or (unc_cols[0] if len(unc_cols) >= 1 else None)
        ritz_unc_col = _find_col(df, "unc", "ritz") or (unc_cols[1] if len(unc_cols) >= 2 else None)

        # Rel int / Aki / Acc
        relint_col = _find_col(df, "rel", "int")
        aki_col = _find_col(df, "aki")
        acc_col = _find_col(df, "acc")

        # Ei/Ek: may be separate or packed
        ei_col = _find_col(df, "ei")
        ek_col = _find_col(df, "ek")

        # Lower/Upper: prefer split cols, else combined
        lo_conf_col = _find_col(df, "lower", "conf")
        lo_term_col = _find_col(df, "lower", "term")
        lo_j_col = _find_col(df, "lower", "j")
        up_conf_col = _find_col(df, "upper", "conf")
        up_term_col = _find_col(df, "upper", "term")
        up_j_col = _find_col(df, "upper", "j")

        lower_combined = _find_col(df, "lower", "level")
        upper_combined = _find_col(df, "upper", "level")

        # Type + refs
        type_col = None
        for c in df.columns:
            if str(c).strip().lower() == "type":
                type_col = c
                break
        if type_col is None:
            type_col = _find_col(df, "type")

        tp_col = _find_col(df, "tp", "ref")
        line_ref_col = _find_col(df, "line", "ref")

        # Optional f/log(gf)
        loggf_col = _find_col(df, "log", "gf") or _find_col(df, "log(gf)")
        f_col = _find_col(df, "f")

        normalized_dir = paths.normalized_dir
        species_path = normalized_dir / "species.ndjson"
        iso_path = normalized_dir / "isotopologues.ndjson"
        refs_path = normalized_dir / "refs.ndjson"
        trans_path = normalized_dir / "transitions.ndjson"

        append_ndjson_dedupe(species_path, [make_species_record(ps)], "species_id")
        append_ndjson_dedupe(iso_path, [make_isotopologue_record(sid)], "iso_id")

        handled_cols = set(
            c
            for c in [
                obs_wl_col,
                ritz_wl_col,
                obs_unc_col,
                ritz_unc_col,
                relint_col,
                aki_col,
                acc_col,
                ei_col,
                ek_col,
                lo_conf_col,
                lo_term_col,
                lo_j_col,
                up_conf_col,
                up_term_col,
                up_j_col,
                lower_combined,
                upper_combined,
                type_col,
                tp_col,
                line_ref_col,
                loggf_col,
                f_col,
            ]
            if c
        )

        ref_records: list[dict] = []
        trans_records: list[dict] = []

        for _, row in df.iterrows():
            obs_wl = _safe_float(row.get(obs_wl_col)) if obs_wl_col else None
            ritz_wl = _safe_float(row.get(ritz_wl_col)) if ritz_wl_col else None
            wav = obs_wl if obs_wl is not None else ritz_wl
            if wav is None:
                continue

            obs_unc = _safe_float(row.get(obs_unc_col)) if obs_unc_col else None
            ritz_unc = _safe_float(row.get(ritz_unc_col)) if ritz_unc_col else None
            chosen_unc = obs_unc if (obs_wl is not None) else ritz_unc

            # refs
            tp_ref = str(row.get(tp_col)).strip() if tp_col else ""
            line_ref = str(row.get(line_ref_col)).strip() if line_ref_col else ""
            tp_ref_id = tp_ref if tp_ref and tp_ref.lower() != "nan" else None
            line_ref_id = line_ref if line_ref and line_ref.lower() != "nan" else None
            ref_id = line_ref_id or tp_ref_id

            for rid in [tp_ref_id, line_ref_id]:
                if not rid:
                    continue
                ref_records.append(
                    {
                        "ref_id": rid,
                        "citation": None,
                        "doi": None,
                        "url": ref_url_map.get(rid),
                        "notes": "ASD ref id; url extracted from popded(...) when available.",
                    }
                )

            # Ei/Ek robust
            ei = _safe_float(row.get(ei_col)) if ei_col else None
            ek = _safe_float(row.get(ek_col)) if ek_col else None

            # If the cell contains a hyphen, parse range and trust it
            if ei_col:
                cell = str(row.get(ei_col))
                if "-" in cell or "–" in cell or "—" in cell:
                    ei2, ek2 = _parse_energy_range(row.get(ei_col))
                    if ei2 is not None:
                        ei = ei2
                    if ek2 is not None:
                        ek = ek2

            if ek is None and ek_col:
                cell = str(row.get(ek_col))
                if "-" in cell or "–" in cell or "—" in cell:
                    ei2, ek2 = _parse_energy_range(row.get(ek_col))
                    if ei is None and ei2 is not None:
                        ei = ei2
                    if ek2 is not None:
                        ek = ek2

            # Lower/Upper parsing:
            # Prefer the combined cells "Lower Level Conf., Term, J" and "Upper Level Conf., Term, J"
            # because they reliably contain all three pieces.
            if lower_combined:
                lower = _parse_level_triplet(row.get(lower_combined))
            else:
                lower = {
                    "configuration": str(row.get(lo_conf_col)).strip() if lo_conf_col else None,
                    "term": str(row.get(lo_term_col)).strip() if lo_term_col else None,
                    "J": str(row.get(lo_j_col)).strip() if lo_j_col else None,
                }

            if upper_combined:
                upper = _parse_level_triplet(row.get(upper_combined))
            else:
                upper = {
                    "configuration": str(row.get(up_conf_col)).strip() if up_conf_col else None,
                    "term": str(row.get(up_term_col)).strip() if up_term_col else None,
                    "J": str(row.get(up_j_col)).strip() if up_j_col else None,
                }

            # Normalize empties / "nan" to None
            for side in (lower, upper):
                for k in ("configuration", "term", "J"):
                    v = side.get(k)
                    if v is None:
                        continue
                    vv = str(v).strip()
                    side[k] = None if (vv == "" or vv.lower() == "nan") else vv

            ttype = str(row.get(type_col)).strip() if type_col else None
            if ttype and ttype.lower() == "nan":
                ttype = None

            payload: dict[str, object] = {
                "observed_wavelength": obs_wl,
                "observed_wavelength_unc": obs_unc,
                "ritz_wavelength": ritz_wl,
                "ritz_wavelength_unc": ritz_unc,
                "wavelength_unit": unit,
                "wavelength_medium_requested": wavelength_type,
                "wavelength_medium_inferred": _infer_medium_from_header(obs_wl_col),
                "observed_wavelength_header": obs_wl_col,
                "ritz_wavelength_header": ritz_wl_col,
                "relative_intensity": _safe_float(row.get(relint_col)) if relint_col else None,
                "Aki_s-1": _safe_float(row.get(aki_col)) if aki_col else None,
                "accuracy_code": str(row.get(acc_col)).strip() if acc_col else None,
                "Ei_cm-1": ei,
                "Ek_cm-1": ek,
                "lower": lower,
                "upper": upper,
                "type": ttype,
                "tp_ref_id": tp_ref_id,
                "line_ref_id": line_ref_id,
                "log_gf": _safe_float(row.get(loggf_col)) if loggf_col else None,
                "f": _safe_float(row.get(f_col)) if f_col else None,
            }

            payload = _prune(payload)  # type: ignore[assignment]
            intensity_json = json.dumps(payload, ensure_ascii=False)

            # extras
            extras: dict[str, object] = {}
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
            extra_json = json.dumps(extras, ensure_ascii=False) if extras else None

            trans_records.append(
                make_transition_record(
                    iso_id=iso_id,
                    quantity_value=float(wav),
                    quantity_unit=unit,
                    quantity_uncertainty=chosen_unc,
                    intensity_json=intensity_json,
                    extra_json=extra_json,
                    selection_rules=ttype,
                    ref_id=ref_id,
                    source="NIST_ASD_LINES",
                    notes=f"NIST ASD lines for {ps.asd_label} [{min_wav}, {max_wav}] {unit}",
                )
            )

        append_ndjson_dedupe(refs_path, ref_records, "ref_id")
        n = append_ndjson_dedupe(trans_path, trans_records, "transition_id")

        return FetchRunResult(True, n, fr.status_code, "OK", str(fr.content_path))

    except Exception as e:
        return FetchRunResult(False, 0, None, f"Exception: {type(e).__name__}: {e}", None)


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch NIST ASD lines (lines1.pl).")
    ap.add_argument("--spectrum", required=True)
    ap.add_argument("--min-wav", type=float, required=True)
    ap.add_argument("--max-wav", type=float, required=True)
    ap.add_argument("--unit", default="nm", choices=["nm", "Angstrom", "um"])
    ap.add_argument("--wavelength-type", default="vacuum", choices=["vacuum", "vac+air"])
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    res = run(
        spectrum=args.spectrum,
        min_wav=args.min_wav,
        max_wav=args.max_wav,
        unit=args.unit,
        wavelength_type=args.wavelength_type,
        force=args.force,
    )
    if res.ok:
        print(f"Wrote {res.written} transitions for {args.spectrum}. Raw: {res.raw_path}")
    else:
        print(f"ERROR: {res.message} Raw: {res.raw_path}")


if __name__ == "__main__":
    main()
