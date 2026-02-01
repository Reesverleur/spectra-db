from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import unquote

from bs4 import BeautifulSoup
from bs4.element import Tag

from spectra_db.util.paths import get_paths
from tools.scrapers.common.ids import make_id
from tools.scrapers.common.ndjson import append_ndjson_dedupe

WEBBOOK_CBOOK_URL = "https://webbook.nist.gov/cgi/cbook.cgi"

_FLOAT_RE = re.compile(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?")
_NOTE_TARGET_RE = re.compile(r"^dia\s*\d+$", flags=re.IGNORECASE)  # DiaNN
_REF_TARGET_RE = re.compile(r"^ref-\d+$")  # ref-N


@dataclass(frozen=True)
class NormalizeResult:
    ok: bool
    written: dict[str, int]
    message: str


def _clean_text(s: str) -> str:
    s = s.replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _out_dir(paths) -> Path:
    return getattr(paths, "normalized_molecular_dir", paths.normalized_dir)


def _extract_webbook_formula(soup: BeautifulSoup) -> str | None:
    # Prefer JSON-LD: {"molecularFormula": "CO"}
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            obj = json.loads(script.get_text() or "")
            if isinstance(obj, dict) and obj.get("molecularFormula"):
                return str(obj["molecularFormula"]).strip()
        except Exception:
            continue
    return None


def _find_diatomic_tables(soup: BeautifulSoup) -> list[Any]:
    out: list[Any] = []
    for table in soup.find_all("table"):
        cap = table.find("caption")
        if not cap:
            continue
        cap_text = _clean_text(cap.get_text(" ", strip=True))
        if cap_text.lower().startswith("diatomic constants for"):
            out.append(table)
    return out


def _normalize_dia_id(raw: str) -> str | None:
    """Convert variations like 'Dia53', 'dia 53' -> 'Dia53'."""
    if not raw:
        return None
    m = re.match(r"^dia\s*(\d+)$", raw.strip(), flags=re.IGNORECASE)
    if not m:
        return None
    return f"Dia{int(m.group(1))}"


def _extract_note_targets_from_cell(td) -> list[str]:
    """
    Return WebBook table footnote targets from <a href="#Dia53"> etc.
    De-duplicates while preserving order. Normalizes to "DiaN".
    """
    targets: list[str] = []
    for a in td.find_all("a"):
        href = (a.get("href") or "").strip()
        if href.startswith("#"):
            dia = _normalize_dia_id(href[1:])
            if dia:
                targets.append(dia)
    return list(dict.fromkeys(targets))


def _extract_sub_markers_from_cell(td) -> list[str]:
    """WebBook uses <sub>…</sub> inline markers in numeric cells."""
    markers: list[str] = []
    for sub in td.find_all("sub"):
        t = _clean_text(sub.get_text(" ", strip=True))
        if t:
            markers.append(t)
    return list(dict.fromkeys(markers))


def _cell_text_without_links_or_footnotes(td, *, keep_sup: bool) -> str:
    """
    - Removes <a> link text (footnote numbers)
    - Removes <sub> markers
    - Optionally removes <sup>
    """
    html = str(td)
    soup = BeautifulSoup(html, "lxml")
    td2 = soup.find(["td", "th"])
    if td2 is None:
        return _clean_text(td.get_text(" ", strip=True))

    for a in td2.find_all("a"):
        a.decompose()
    for sub in td2.find_all("sub"):
        sub.decompose()
    if not keep_sup:
        for sup in td2.find_all("sup"):
            sup.decompose()

    return _clean_text(td2.get_text(" ", strip=True))


def _parse_number_with_source_rounding(clean_cell: str) -> tuple[float | None, dict[str, Any], str | None]:
    """
    Parse a numeric cell, returning (value, flags, suffix).
    suffix is a trailing letter token like "Z" for nu00.
    """
    raw = _clean_text(clean_cell)
    if not raw:
        return None, {"raw": ""}, None

    flags: dict[str, Any] = {"raw": raw}
    s = raw

    if s.startswith("(") and s.endswith(")"):
        flags["brackets"] = "()"
        s = s[1:-1].strip()
    elif s.startswith("[") and s.endswith("]"):
        flags["brackets"] = "[]"
        s = s[1:-1].strip()

    s_compact = s.replace(",", "").replace(" ", "")
    m = _FLOAT_RE.search(s_compact)
    if not m:
        return None, flags, None

    token = m.group(0)
    flags["token"] = token

    decimals = None
    if "e" not in token.lower() and "." in token:
        decimals = len(token.split(".", 1)[1])
        flags["decimals"] = decimals

    suffix = None
    remainder = raw.replace(token, "", 1).strip()
    if remainder and re.fullmatch(r"[A-Za-z]+", remainder):
        suffix = remainder

    try:
        d = Decimal(token)
    except InvalidOperation:
        return None, flags, suffix

    val = float(d)
    if decimals is not None:
        val = round(val, decimals)

    return val, flags, suffix


def _split_trans_text_and_suffix(clean_trans: str) -> tuple[str | None, str | None]:
    """Trans cells like 'A ↔ X R' often end with a letter suffix."""
    s = _clean_text(clean_trans)
    if not s:
        return None, None
    parts = s.split()
    if len(parts) >= 2 and re.fullmatch(r"[A-Za-z]+", parts[-1] or ""):
        return _clean_text(" ".join(parts[:-1])), parts[-1]
    return s, None


def _is_in_footerish(tag: Tag) -> bool:
    """
    Heuristic: ignore obvious footer/nav containers.
    NOTE: we do NOT use this to filter the notes or references themselves.
    """
    for parent in tag.parents:
        if not getattr(parent, "name", None):
            continue
        pid = (parent.get("id") or "").lower()
        cls = " ".join(parent.get("class") or []).lower()
        if pid in {"footer", "nav"}:
            return True
        if "footer" in cls or "nav" in cls:
            return True
    return False


def _extract_upper_notes_text(soup: BeautifulSoup) -> str | None:
    """
    WebBook often has two "Notes" areas; we want the *upper* Notes section.
    Approach:
      - find first heading whose text is exactly "Notes" and not footer/nav
      - collect text until next major heading (References / another H1/H2)
    """
    headings = soup.find_all(["h1", "h2", "h3", "h4"])
    for h in headings:
        if _is_in_footerish(h):
            continue
        if _clean_text(h.get_text(" ", strip=True)).lower() != "notes":
            continue

        parts: list[str] = []
        for sib in h.find_all_next():
            if sib is h:
                continue
            if getattr(sib, "name", None) in {"h1", "h2", "h3", "h4"}:
                t = _clean_text(sib.get_text(" ", strip=True)).lower()
                if t in {"references", "notes"}:
                    break
                if sib.name in {"h1", "h2"}:
                    break
            if isinstance(sib, Tag) and _is_in_footerish(sib):
                break
            if getattr(sib, "name", None) in {"script", "style"}:
                continue
            txt = _clean_text(sib.get_text(" ", strip=True))
            if txt:
                parts.append(txt)

        text = _clean_text(" ".join(parts))
        return text or None

    return None


def _looks_like_nav_or_footer_text(text: str) -> bool:
    t = _clean_text(text).lower()
    if not t:
        return True
    bad_phrases = [
        "go to:",
        "constants of diatomic molecules",
        "data compilation copyright",
        "u.s. secretary of commerce",
        "all rights reserved",
        "nist standard reference database",
    ]
    return any(p in t for p in bad_phrases)


def _extract_footnote_blocks(soup: BeautifulSoup) -> dict[str, dict[str, Any]]:
    """
    Footnotes ("List of Notes") are typically a table like:
      <tr><td><a id="Dia1">1</a></td><td>...text... <a href="#ref-1">...</a></td></tr>

    Returns:
      {
        "Dia1": {"text": "...", "ref_targets": ["ref-1", ...], "dia_targets": ["Dia88", ...]},
        ...
      }
    """
    out: dict[str, dict[str, Any]] = {}

    # find all tags with id/name DiaNN
    anchors: list[Tag] = []
    for tag in soup.find_all(True):
        dia = _normalize_dia_id((tag.get("id") or tag.get("name") or "").strip())
        if dia:
            anchors.append(tag)

    def _extract_targets_from_td(td: Tag) -> tuple[list[str], list[str]]:
        ref_targets: list[str] = []
        dia_targets: list[str] = []
        for a in td.find_all("a"):
            href = (a.get("href") or "").strip()
            if not href.startswith("#"):
                continue
            frag = href[1:].strip()
            if _REF_TARGET_RE.match(frag):
                ref_targets.append(frag)
            else:
                dia = _normalize_dia_id(frag)
                if dia:
                    dia_targets.append(dia)
        return list(dict.fromkeys(ref_targets)), list(dict.fromkeys(dia_targets))

    for a in anchors:
        dia = _normalize_dia_id((a.get("id") or a.get("name") or "").strip())
        if not dia:
            continue
        tr = a.find_parent("tr")
        if tr is None:
            continue
        tds = tr.find_all("td", recursive=False)
        if len(tds) < 2:
            continue

        # choose the td that does NOT contain the anchor
        content_td = None
        for td in tds:
            # use find_all instead of a lambda capturing loop variable
            if a in td.find_all(True):
                continue
            content_td = td
            break
        if content_td is None:
            content_td = tds[-1]

        text = _clean_text(content_td.get_text(" ", strip=True))
        if not text:
            continue

        ref_targets, dia_targets = _extract_targets_from_td(content_td)
        out[dia] = {"text": text, "ref_targets": ref_targets, "dia_targets": dia_targets}

    return out


def _extract_references_from_ref_spans(soup: BeautifulSoup, *, webbook_id: str) -> list[dict[str, Any]]:
    """
    Extract references from CO-style layout:

      <h2 id="Refs">References</h2>
      <p class="section-head">Go To: ...</p>
      <p class="section-head">Data compilation copyright ...</p>

      <p>
        <span id="ref-1"><strong>Short label</strong></span><br/>
        <span class="Z3988" title="...OpenURL..."></span>
        ...full citation text... [all data]
      </p>

    We take parent <p> blocks of each <span id="ref-N"> and extract full text,
    skipping section-head junk.
    """
    refs: list[dict[str, Any]] = []

    ref_spans = soup.find_all("span", id=re.compile(r"^ref-\d+$"))
    if not ref_spans:
        return refs

    def _doi_from_block(p: Tag, citation_text: str) -> str | None:
        # 1) direct doi.org link
        for a in p.find_all("a"):
            href = (a.get("href") or "").strip()
            if "doi.org/" in href:
                return href.split("doi.org/", 1)[1].strip().strip("/")

        # 2) Z3988 OpenURL metadata
        z = p.find("span", class_="Z3988")
        if z is not None:
            title = z.get("title") or ""
            if title:
                decoded = unquote(title)
                m = re.search(r"info:doi/([^&\s]+)", decoded, flags=re.IGNORECASE)
                if m:
                    return m.group(1).strip().strip(".")
                m = re.search(r"rft\.doi=([^&\s]+)", decoded, flags=re.IGNORECASE)
                if m:
                    return m.group(1).strip().strip(".")

        # 3) text "doi:"
        m = re.search(r"\bdoi\s*:\s*([^\s;,]+)", citation_text, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip().strip(".")
        return None

    seen: set[str] = set()
    for span in ref_spans:
        rid = (span.get("id") or "").strip()
        if not _REF_TARGET_RE.match(rid):
            continue
        if rid in seen:
            continue
        seen.add(rid)

        p = span.find_parent("p")
        if p is None:
            continue

        # Skip the junk header paragraphs
        cls = p.get("class") or []
        if "section-head" in cls:
            continue

        citation = _clean_text(p.get_text(" ", strip=True))
        if not citation or _looks_like_nav_or_footer_text(citation):
            continue

        doi = _doi_from_block(p, citation)
        ref_id = f"WB:{webbook_id}:{rid}"
        url = f"{WEBBOOK_CBOOK_URL}?ID={webbook_id}&Mask=1000#{rid}"
        refs.append(
            {
                "ref_id": ref_id,
                "ref_type": "webbook_reference",
                "citation": citation,
                "doi": doi,
                "url": url,
                "notes": None,
            }
        )

    return refs


def run(*, webbook_id: str, body_path: Path | None = None) -> NormalizeResult:
    paths = get_paths()
    outdir = _out_dir(paths)

    if body_path is None:
        return NormalizeResult(ok=False, written={}, message="body_path is required (point to the cached .body file)")

    html = body_path.read_text(encoding="utf-8", errors="replace")
    soup = BeautifulSoup(html, "lxml")

    formula = _extract_webbook_formula(soup) or "UNKNOWN"
    species_id = f"MOL:{formula}:{0:+d}"

    notes_text = _extract_upper_notes_text(soup)
    footnotes_by_id = _extract_footnote_blocks(soup)

    # CO-style references extraction
    refs_recs = _extract_references_from_ref_spans(soup, webbook_id=webbook_id)

    species_extra = {
        "source": f"webbook:{webbook_id}",
        "webbook_id": webbook_id,
        "webbook_notes_text": notes_text,
        "webbook_footnotes_by_id": footnotes_by_id,
        "webbook_references": [{"ref_id": r["ref_id"], "citation": r["citation"], "doi": r.get("doi"), "url": r.get("url")} for r in refs_recs],
    }

    species_rec = {
        "species_id": species_id,
        "formula": formula,
        "name": None,
        "charge": 0,
        "multiplicity": None,
        "inchi_key": None,
        "tags": "webbook",
        "notes": None,
        "extra_json": json.dumps(species_extra, ensure_ascii=False),
    }

    iso_recs: list[dict[str, Any]] = []
    state_recs: list[dict[str, Any]] = []
    param_recs: list[dict[str, Any]] = []

    diatomic_tables = _find_diatomic_tables(soup)
    if not diatomic_tables:
        return NormalizeResult(ok=False, written={}, message="No 'Diatomic constants for ...' tables found in HTML.")

    names = ["Te", "we", "wexe", "weye", "Be", "ae", "ge", "De", "be", "re", "Trans", "nu00"]

    unit_map = {
        "Te": "cm-1",
        "we": "cm-1",
        "wexe": "cm-1",
        "weye": "cm-1",
        "Be": "cm-1",
        "ae": "cm-1",
        "ge": "cm-1",
        "De": "cm-1",
        "be": "cm-1",
        "re": "A",
        "nu00": "cm-1",
    }

    model = "webbook_diatomic_constants"
    source = f"webbook:{webbook_id}"

    for table in diatomic_tables:
        cap = table.find("caption")
        cap_text = _clean_text(cap.get_text(" ", strip=True)) if cap else ""

        iso_label = cap_text.split("for", 1)[-1].strip() if "for" in cap_text else cap_text.strip()
        iso_label = iso_label or formula
        iso_id = make_id("iso", species_id, iso_label)

        iso_recs.append(
            {
                "iso_id": iso_id,
                "species_id": species_id,
                "label": iso_label,
                "composition_json": json.dumps({"raw_caption": cap_text}, ensure_ascii=False),
                "nuclear_spins_json": None,
                "mass_amu": None,
                "abundance": None,
                "notes": None,
            }
        )

        for tr in table.find_all("tr"):
            if tr.find_all("th"):
                continue

            tds = tr.find_all("td")
            if not tds:
                continue
            if len(tds) < 1 + len(names):
                continue

            state_td = tds[0]
            state_label_raw = _cell_text_without_links_or_footnotes(state_td, keep_sup=True)
            state_label = re.sub(r"\s+", "", state_label_raw)
            if not state_label:
                continue

            cells = tds[1 : 1 + len(names)]
            state_id = make_id("state", iso_id, "webbook", state_label)

            # Trans stored on state extra_json
            trans_cell = cells[names.index("Trans")]
            trans_note_targets = _extract_note_targets_from_cell(trans_cell)
            trans_markers = _extract_sub_markers_from_cell(trans_cell)
            trans_clean_cell = _cell_text_without_links_or_footnotes(trans_cell, keep_sup=False)
            trans_text, trans_suffix = _split_trans_text_and_suffix(trans_clean_cell)

            # Te stored also as state.energy_value
            te_cell = cells[names.index("Te")]
            te_note_targets = _extract_note_targets_from_cell(te_cell)
            te_markers = _extract_sub_markers_from_cell(te_cell)
            te_clean_cell = _cell_text_without_links_or_footnotes(te_cell, keep_sup=False)
            te_val, te_flags, _ = _parse_number_with_source_rounding(te_clean_cell)

            state_recs.append(
                {
                    "state_id": state_id,
                    "iso_id": iso_id,
                    "state_type": "molecular",
                    "electronic_label": state_label,
                    "vibrational_json": None,
                    "rotational_json": None,
                    "parity": None,
                    "configuration": None,
                    "term": None,
                    "j_value": None,
                    "f_value": None,
                    "g_value": None,
                    "lande_g": None,
                    "leading_percentages": None,
                    "extra_json": json.dumps(
                        {
                            "source": source,
                            "webbook_id": webbook_id,
                            "table_caption": cap_text,
                            "Te_flags": te_flags,
                            "Te_markers": te_markers,
                            "Te_note_targets": te_note_targets,
                            "Trans_text": trans_text,
                            "Trans_suffix": trans_suffix,
                            "Trans_clean": (f"{trans_text} {trans_suffix}".strip() if trans_text else None),
                            "Trans_note_targets": trans_note_targets,
                            "Trans_markers": trans_markers,
                        },
                        ensure_ascii=False,
                    ),
                    "energy_value": te_val,
                    "energy_unit": "cm-1" if te_val is not None else None,
                    "energy_uncertainty": None,
                    "ref_id": None,  # DiaNN are footnotes, not bib refs
                    "notes": None,
                }
            )

            # Numeric parameters (skip Trans)
            for idx, pname in enumerate(names):
                if pname == "Trans":
                    continue

                cell = cells[idx]
                note_targets = _extract_note_targets_from_cell(cell)
                markers = _extract_sub_markers_from_cell(cell)

                raw_cell = _clean_text(cell.get_text(" ", strip=True))
                clean_cell = _cell_text_without_links_or_footnotes(cell, keep_sup=False)

                val, flags, suffix = _parse_number_with_source_rounding(clean_cell)
                if val is None:
                    continue

                context: dict[str, Any] = {
                    "state_id": state_id,
                    "state_label": state_label,
                    "table_caption": cap_text,
                    "raw_cell": raw_cell,
                    "clean_cell": clean_cell,
                    "cell_flags": flags,
                    "cell_markers": markers,
                    "cell_note_targets": note_targets,
                }

                value_suffix = None
                if pname == "nu00" and suffix:
                    value_suffix = suffix
                    context["value_suffix"] = suffix
                    context.setdefault("cell_flags", {})["suffix"] = suffix

                param_recs.append(
                    {
                        "param_id": make_id("param", iso_id, model, state_id, pname),
                        "iso_id": iso_id,
                        "model": model,
                        "name": pname,
                        "value": float(val),
                        "unit": unit_map.get(pname, "cm-1"),
                        "uncertainty": None,
                        "text_value": None,
                        "value_suffix": value_suffix,
                        "markers_json": json.dumps({"sub": markers}, ensure_ascii=False) if markers else None,
                        "ref_ids_json": None,
                        "context_json": json.dumps(context, ensure_ascii=False),
                        "raw_text": clean_cell,
                        "convention": None,
                        "ref_id": None,
                        "source": source,
                        "notes": None,
                    }
                )

    outdir.mkdir(parents=True, exist_ok=True)

    written: dict[str, int] = {}
    written["species"] = append_ndjson_dedupe(outdir / "species.ndjson", [species_rec], "species_id")
    written["isotopologues"] = append_ndjson_dedupe(outdir / "isotopologues.ndjson", iso_recs, "iso_id")
    written["refs"] = append_ndjson_dedupe(outdir / "refs.ndjson", refs_recs, "ref_id")
    written["states"] = append_ndjson_dedupe(outdir / "states.ndjson", state_recs, "state_id")
    written["parameters"] = append_ndjson_dedupe(outdir / "parameters.ndjson", param_recs, "param_id")

    return NormalizeResult(ok=True, written=written, message="ok")


def main() -> None:
    ap = argparse.ArgumentParser(description="Normalize NIST WebBook diatomic constants into canonical NDJSON.")
    ap.add_argument("--webbook-id", required=True, help="e.g. C630080 for CO. Used to build stable URLs.")
    ap.add_argument("--body-path", type=Path, required=True, help="Path to cached .body HTML file.")
    args = ap.parse_args()

    rr = run(webbook_id=args.webbook_id, body_path=args.body_path)
    print(rr)


if __name__ == "__main__":
    main()
