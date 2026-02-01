from __future__ import annotations

import argparse
import html as html_lib
import json
import re
import time
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup

from spectra_db.scrapers.nist_webbook.fetch_webbook import run as fetch_webbook_run
from spectra_db.util.paths import get_paths

WEBBOOK_BASE = "https://webbook.nist.gov"
CBOOK_URL = f"{WEBBOOK_BASE}/cgi/cbook.cgi"

# Union over element symbols to discover IDs.
ELEMENT_SYMBOLS = [
    "H",
    "He",
    "Li",
    "Be",
    "B",
    "C",
    "N",
    "O",
    "F",
    "Ne",
    "Na",
    "Mg",
    "Al",
    "Si",
    "P",
    "S",
    "Cl",
    "Ar",
    "K",
    "Ca",
    "Sc",
    "Ti",
    "V",
    "Cr",
    "Mn",
    "Fe",
    "Co",
    "Ni",
    "Cu",
    "Zn",
    "Ga",
    "Ge",
    "As",
    "Se",
    "Br",
    "Kr",
    "Rb",
    "Sr",
    "Y",
    "Zr",
    "Nb",
    "Mo",
    "Tc",
    "Ru",
    "Rh",
    "Pd",
    "Ag",
    "Cd",
    "In",
    "Sn",
    "Sb",
    "Te",
    "I",
    "Xe",
    "Cs",
    "Ba",
    "La",
    "Ce",
    "Pr",
    "Nd",
    "Pm",
    "Sm",
    "Eu",
    "Gd",
    "Tb",
    "Dy",
    "Ho",
    "Er",
    "Tm",
    "Yb",
    "Lu",
    "Hf",
    "Ta",
    "W",
    "Re",
    "Os",
    "Ir",
    "Pt",
    "Au",
    "Hg",
    "Tl",
    "Pb",
    "Bi",
    "Po",
    "At",
    "Rn",
    "Fr",
    "Ra",
    "Ac",
    "Th",
    "Pa",
    "U",
    "Np",
    "Pu",
    "Am",
    "Cm",
    "Bk",
    "Cf",
    "Es",
    "Fm",
    "Md",
    "No",
    "Lr",
    "Rf",
    "Db",
    "Sg",
    "Bh",
    "Hs",
    "Mt",
    "Ds",
    "Rg",
    "Cn",
    "Nh",
    "Fl",
    "Mc",
    "Lv",
    "Ts",
    "Og",
]

ID_PARAM_RE = re.compile(r"[?&]ID=([^&#]+)")
WEBBOOK_ID_RE = re.compile(r"^[A-Za-z]\d+$")  # matches IDs you pasted: C1333740, C13776700, ...


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def http_get(url: str, *, user_agent: str, timeout_s: float) -> tuple[str, bytes]:
    req = Request(
        url,
        headers={"User-Agent": user_agent, "Accept": "text/html,application/xhtml+xml"},
        method="GET",
    )
    with urlopen(req, timeout=timeout_s) as resp:
        return resp.geturl(), resp.read()


def parse_html(body: bytes) -> BeautifulSoup:
    return BeautifulSoup(body.decode("utf-8", errors="replace"), "lxml")


def build_search_url(*, element_symbol: str, include_ions: bool, units: str) -> str:
    """
    Uses the exact query form you verified:

      ?Formula=H&AllowOther=on&AllowExtra=on&Units=SI&cDI=on

    - AllowOther/AllowExtra broaden match to species containing the element
    - cDI=on restricts results to pages with diatomic constants
    - include_ions=True => do NOT set NoIon=on (default)
    """
    params: dict[str, str] = {
        "Formula": element_symbol,
        "AllowOther": "on",
        "AllowExtra": "on",
        "Units": units,
        "cDI": "on",
    }
    if not include_ions:
        params["NoIon"] = "on"
    return f"{CBOOK_URL}?{urlencode(params)}"


def extract_ids_from_results(soup: BeautifulSoup) -> set[str]:
    """
    Extract WebBook IDs from result links.

    Result links look like:
      <a href="/cgi/cbook.cgi?ID=C3315375&amp;Units=SI&amp;Mask=1000">...</a>
    """
    ids: set[str] = set()
    for a in soup.find_all("a"):
        href = a.get("href")
        if not href:
            continue
        href = html_lib.unescape(href)
        m = ID_PARAM_RE.search(href)
        if not m:
            continue
        candidate = m.group(1).strip()
        if WEBBOOK_ID_RE.match(candidate):
            ids.add(candidate)
    return ids


def append_jsonl(path: Path, obj: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def main() -> None:
    ap = argparse.ArgumentParser(description="Bulk ingest WebBook diatomic constants pages into the cache used by normalize_cache.py.")
    ap.add_argument("--mask", type=int, default=1000, help="Mask to fetch (1000 = diatomic constants).")
    ap.add_argument("--units", default="SI", help="Units parameter for discovery searches (default: SI).")
    ap.add_argument("--sleep", type=float, default=0.5, help="Delay between requests in seconds (default: 0.5).")
    ap.add_argument("--timeout", type=float, default=60.0, help="HTTP timeout in seconds (passed to fetch_webbook).")
    ap.add_argument("--user-agent", default="spectra-db-bulk-ingest/1.0 (local research)", help="User-Agent header for discovery requests.")
    ap.add_argument("--force", action="store_true", help="Force re-fetch even if cached (passed to fetch_webbook).")
    ap.add_argument("--max-elements", type=int, default=None, help="Limit element symbols for a quick test run.")
    ap.add_argument("--max-fetch", type=int, default=None, help="Limit number of species pages fetched for a quick test run.")
    ap.add_argument("--exclude-ions", action="store_true", help="Exclude ions (default: include ions).")

    args = ap.parse_args()

    include_ions = not args.exclude_ions
    elements = ELEMENT_SYMBOLS[: args.max_elements] if args.max_elements else ELEMENT_SYMBOLS

    paths = get_paths()
    cache_dir = paths.raw_dir / "nist_webbook" / "cbook"
    ensure_dir(cache_dir)

    manifest_path = cache_dir / "diatomic_ids.txt"
    log_path = cache_dir / "bulk_diatomic.log.jsonl"

    # 1) Discover IDs
    all_ids: set[str] = set()
    for el in elements:
        search_url = build_search_url(element_symbol=el, include_ions=include_ions, units=args.units)
        final_url, body = http_get(search_url, user_agent=args.user_agent, timeout_s=args.timeout)
        soup = parse_html(body)

        found = extract_ids_from_results(soup)

        # Sometimes a search can resolve directly to a species page (rare), capture final URL ID too
        m = ID_PARAM_RE.search(final_url)
        if m:
            cid = m.group(1).strip()
            if WEBBOOK_ID_RE.match(cid):
                found.add(cid)

        before = len(all_ids)
        all_ids |= found
        after = len(all_ids)

        append_jsonl(
            log_path,
            {
                "ts": utc_now_iso(),
                "event": "discover",
                "element": el,
                "search_url": search_url,
                "final_url": final_url,
                "found_ids": len(found),
                "total_ids": after,
                "delta": after - before,
            },
        )

        if args.sleep > 0:
            time.sleep(args.sleep)

    manifest_path.write_text("\n".join(sorted(all_ids)) + "\n", encoding="utf-8")
    print(f"Discovered {len(all_ids)} IDs. Wrote manifest: {manifest_path}")

    # 2) Fetch each discovered ID using the canonical fetch_webbook code (normalize-compatible cache files)
    fetched_ok = 0
    fetched_fail = 0
    attempted = 0

    for webbook_id in sorted(all_ids):
        if args.max_fetch is not None and attempted >= args.max_fetch:
            break
        attempted += 1

        rr = fetch_webbook_run(
            webbook_id=webbook_id,
            mask=args.mask,
            force=args.force,
            timeout_s=args.timeout,
        )

        append_jsonl(
            log_path,
            {
                "ts": utc_now_iso(),
                "event": "fetch",
                "webbook_id": webbook_id,
                "mask": args.mask,
                "force": bool(args.force),
                "result": asdict(rr),
            },
        )

        if rr.ok:
            fetched_ok += 1
        else:
            fetched_fail += 1

        if args.sleep > 0:
            time.sleep(args.sleep)

    print(f"Fetch complete. attempted={attempted} ok={fetched_ok} fail={fetched_fail}")
    print("Next steps:")
    print("  python -m spectra_db.scrapers.nist_webbook.normalize_cache")
    print("  python scripts/bootstrap_db.py --profile molecular --truncate-all")


if __name__ == "__main__":
    main()
