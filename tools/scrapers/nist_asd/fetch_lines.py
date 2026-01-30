from __future__ import annotations

import argparse
import html as _html
import json
import re
from dataclasses import dataclass
from typing import Any

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
    """Map ref_id text (e.g. 'L8672c99') -> popup URL extracted from popded('...')."""
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
        out[txt] = url
    return out


def _safe_float(x: object) -> float | None:
    s = str(x).strip()
    if not s or s.lower() == "nan":
        return None
    s = s.replace(" ", "").replace(",", "")
    m = _FLOAT_RE.search(s)
    return float(m.group(0)) if m else None


def _get_col(df, *needles: str) -> str | None:
    needles_l = [n.lower() for n in needles]
    for c in df.columns:
        name = str(c).lower()
        if all(n in name for n in needles_l):
            return c
    return None


def _parse_gi_gk(x: object) -> tuple[float | None, float | None]:
    s = str(x).strip()
    if not s or s.lower() == "nan":
        return (None, None)
    s = s.replace("–", "-").replace("—", "-")
    parts = [p.strip() for p in s.split("-") if p.strip()]
    if len(parts) == 2:
        return (_safe_float(parts[0]), _safe_float(parts[1]))
    return (None, None)


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


def _infer_medium_from_header(header: str | None) -> str | None:
    """Infer wavelength medium from a column header string."""
    if not header:
        return None
    h = header.lower()
    if "vac" in h:
        return "vacuum"
    if "air" in h:
        return "air"
    return None


def run(
    *,
    spectrum: str,
    min_wav: float,
    max_wav: float,
    unit: str = "nm",
    wavelength_type: str = "vacuum",
    force: bool = False,
) -> FetchRunResult:
    """Fetch ASD lines for one spectrum window and write normalized NDJSON (with ref URLs + extra_json)."""
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

        # Discover commonly-used columns (best-effort)
        obs_wl_col = _get_col(df, "Observed", "Wavelength")
        ritz_wl_col = _get_col(df, "Ritz", "Wavelength")
        obs_unc_col = _get_col(df, "Unc", "Observed") or _get_col(df, "Unc.")
        ritz_unc_col = _get_col(df, "Unc", "Ritz")
        wn_col = _get_col(df, "Wavenumber")
        wn_unc_col = _get_col(df, "Unc", "Wavenumber") or _get_col(df, "Unc.", "Wavenumber")
        ei_col = next((c for c in df.columns if str(c).strip().startswith("Ei")), None)
        ek_col = next((c for c in df.columns if str(c).strip().startswith("Ek")), None)

        cfi_col = _get_col(df, "Conf", "i") or _get_col(df, "Configuration", "i")
        tfi_col = _get_col(df, "Term", "i")
        ji_col = _get_col(df, "J", "i")
        cfk_col = _get_col(df, "Conf", "k") or _get_col(df, "Configuration", "k")
        tfk_col = _get_col(df, "Term", "k")
        jk_col = _get_col(df, "J", "k")

        gi_col = _get_col(df, "gi")
        gk_col = _get_col(df, "gk")
        gigk_col = _get_col(df, "gi", "gk")

        type_col = _get_col(df, "Type")
        aki_col = _get_col(df, "Aki")
        loggf_col = _get_col(df, "log", "gf") or _get_col(df, "log(gf)")
        f_col = _get_col(df, "f")
        ref_col = _get_col(df, "Ref") or _get_col(df, "Reference")

        normalized_dir = paths.normalized_dir
        species_path = normalized_dir / "species.ndjson"
        iso_path = normalized_dir / "isotopologues.ndjson"
        refs_path = normalized_dir / "refs.ndjson"
        trans_path = normalized_dir / "transitions.ndjson"

        append_ndjson_dedupe(species_path, [make_species_record(ps)], "species_id")
        append_ndjson_dedupe(iso_path, [make_isotopologue_record(sid)], "iso_id")

        # Columns we explicitly map into intensity_json
        handled_cols = set()
        for c in [
            obs_wl_col,
            ritz_wl_col,
            obs_unc_col,
            ritz_unc_col,
            wn_col,
            wn_unc_col,
            ei_col,
            ek_col,
            cfi_col,
            tfi_col,
            ji_col,
            cfk_col,
            tfk_col,
            jk_col,
            gi_col,
            gk_col,
            gigk_col,
            type_col,
            aki_col,
            loggf_col,
            f_col,
            ref_col,
        ]:
            if c:
                handled_cols.add(c)

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

            ref = str(row.get(ref_col)).strip() if ref_col else ""
            ref_id = ref if ref and ref.lower() != "nan" else None
            if ref_id:
                ref_records.append(
                    {
                        "ref_id": ref_id,
                        "citation": None,
                        "doi": None,
                        "url": ref_url_map.get(ref_id),
                        "notes": "ASD ref id; url extracted from popded(...) when available.",
                    }
                )

            payload: dict[str, object] = {
                "observed_wavelength": obs_wl,
                "ritz_wavelength": ritz_wl,
                "wavelength_unit": unit,
                "observed_wavelength_unc": obs_unc,
                "ritz_wavelength_unc": ritz_unc,
                "wavenumber_cm-1": _safe_float(row.get(wn_col)) if wn_col else None,
                "wavenumber_unc_cm-1": _safe_float(row.get(wn_unc_col)) if wn_unc_col else None,
                "Ei_cm-1": _safe_float(row.get(ei_col)) if ei_col else None,
                "Ek_cm-1": _safe_float(row.get(ek_col)) if ek_col else None,
                "type": str(row.get(type_col)).strip() if type_col else None,
                "Aki_s-1": _safe_float(row.get(aki_col)) if aki_col else None,
                "log_gf": _safe_float(row.get(loggf_col)) if loggf_col else None,
                "f": _safe_float(row.get(f_col)) if f_col else None,
                "lower": {
                    "configuration": str(row.get(cfi_col)).strip() if cfi_col else None,
                    "term": str(row.get(tfi_col)).strip() if tfi_col else None,
                    "J": str(row.get(ji_col)).strip() if ji_col else None,
                },
                "upper": {
                    "configuration": str(row.get(cfk_col)).strip() if cfk_col else None,
                    "term": str(row.get(tfk_col)).strip() if tfk_col else None,
                    "J": str(row.get(jk_col)).strip() if jk_col else None,
                },
                "wavelength_medium_requested": wavelength_type,
                "observed_wavelength_header": obs_wl_col,
                "ritz_wavelength_header": ritz_wl_col,
                "wavelength_medium_inferred": _infer_medium_from_header(obs_wl_col),
            }

            # Record wavelength convention information

            gi = _safe_float(row.get(gi_col)) if gi_col else None
            gk = _safe_float(row.get(gk_col)) if gk_col else None
            if (gi is None or gk is None) and gigk_col:
                gi2, gk2 = _parse_gi_gk(row.get(gigk_col))
                gi = gi if gi is not None else gi2
                gk = gk if gk is not None else gk2
            payload["gi"] = gi
            payload["gk"] = gk

            payload = _prune(payload)  # type: ignore[assignment]
            intensity_json = json.dumps(payload, ensure_ascii=False)

            # Capture EVERYTHING else from the table
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
            extra_json = json.dumps(extras, ensure_ascii=False) if extras else None

            trans_records.append(
                make_transition_record(
                    iso_id=iso_id,
                    quantity_value=float(wav),
                    quantity_unit=unit,
                    quantity_uncertainty=chosen_unc,
                    intensity_json=intensity_json,
                    extra_json=extra_json,
                    selection_rules=str(row.get(type_col)).strip() if type_col else None,
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
