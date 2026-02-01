from __future__ import annotations

import argparse
import re
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

LEVELS_PT = "https://physics.nist.gov/cgi-bin/ASD/levels_pt.pl"
LINES_PT = "https://physics.nist.gov/cgi-bin/ASD/lines_pt.pl"

# Holds pages look like:
#   https://physics.nist.gov/cgi-bin/ASD/levels_hold.pl?el=Ca
#   https://physics.nist.gov/cgi-bin/ASD/lines_hold.pl?el=Ca
HOLD_RE = re.compile(r"/cgi-bin/ASD/(levels_hold|lines_hold)\.pl\?el=([A-Za-z]{1,2})")

# Spectrum labels we want:
#   "Fe I", "Fe II", "Ar 15+", "Po LXVII"
SPEC_RE = re.compile(r"\b([A-Z][a-z]?)\s+([IVXLCDM]{1,12}|\d+\+)\b")


def _get(url: str, timeout_s: float = 60.0) -> str:
    r = requests.get(url, timeout=timeout_s, headers={"User-Agent": "spectra-db/0.0.1"})
    r.raise_for_status()
    return r.text


def _extract_hold_links(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[str] = []
    for a in soup.find_all("a"):
        href = a.get("href") or ""
        if HOLD_RE.search(href):
            links.append(urljoin(base_url, href))
    # de-dupe preserve order
    seen = set()
    out = []
    for u in links:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def _extract_spectra_from_hold_page(html: str) -> list[str]:
    """Extract spectrum labels like 'Ca I' from a holdings page."""
    # We intentionally regex-scan the text to avoid depending on table structure.
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n")
    found = []
    for m in SPEC_RE.finditer(text):
        found.append(f"{m.group(1)} {m.group(2)}")
    # de-dupe preserve order
    seen = set()
    out = []
    for s in found:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def fetch_spectra_list(kind: str) -> list[str]:
    """Fetch ASD spectra list using holdings pages.

    kind:
      - "levels": spectra with level holdings
      - "lines": spectra with line holdings
      - "both": union
    """
    kind = kind.lower()
    if kind not in {"levels", "lines", "both"}:
        raise ValueError("kind must be one of: levels, lines, both")

    urls = []
    if kind in {"levels", "both"}:
        urls.append(LEVELS_PT)
    if kind in {"lines", "both"}:
        urls.append(LINES_PT)

    all_spectra: list[str] = []
    seen = set()

    for pt_url in urls:
        pt_html = _get(pt_url)
        hold_links = _extract_hold_links(pt_html, pt_url)

        # If the periodic table page doesn’t expose hold links directly,
        # fall back to constructing them by element symbols found in the page text.
        if not hold_links:
            soup = BeautifulSoup(pt_html, "html.parser")
            text = soup.get_text(" ")
            elems = sorted(set(re.findall(r"\b([A-Z][a-z]?)\b", text)))
            # filter to plausible element symbols (1-2 chars)
            elems = [e for e in elems if 1 <= len(e) <= 2]
            # build hold URLs
            is_levels = "levels_pt.pl" in pt_url
            for el in elems:
                hold_links.append(f"https://physics.nist.gov/cgi-bin/ASD/{'levels_hold' if is_levels else 'lines_hold'}.pl?el={el}")

        for hold_url in hold_links:
            try:
                hold_html = _get(hold_url)
            except Exception:
                # some elements may legitimately have no holdings page in that category
                continue
            spectra = _extract_spectra_from_hold_page(hold_html)
            for s in spectra:
                if s not in seen:
                    seen.add(s)
                    all_spectra.append(s)

    return all_spectra


def main() -> None:
    ap = argparse.ArgumentParser(description="List ASD spectra from holdings periodic-table pages.")
    ap.add_argument("--kind", choices=["levels", "lines", "both"], default="levels")
    ap.add_argument("--out", type=Path, default=None, help="Write spectra to a file, one per line.")
    args = ap.parse_args()

    spectra = fetch_spectra_list(args.kind)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text("\n".join(spectra) + "\n", encoding="utf-8")
        print(f"Wrote {len(spectra)} spectra to {args.out}")
    else:
        print("\n".join(spectra))


if __name__ == "__main__":
    main()
