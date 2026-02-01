from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup

from spectra_db.util.paths import get_paths  # <-- FIXES NameError

WEBBOOK_BASE = "https://webbook.nist.gov"
FORMULA_SEARCH_URL = f"{WEBBOOK_BASE}/chemistry/form-ser/"
CBOOK_URL = f"{WEBBOOK_BASE}/cgi/cbook.cgi"

# Union over all element symbols to discover IDs of pages that contain
# "Constants of diatomic molecules" data.
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

ID_RE = re.compile(r"[?&]ID=([^&#]+)")


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def http_get(url: str, *, user_agent: str, timeout_s: float) -> tuple[str, bytes]:
    req = Request(
        url,
        headers={
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml",
        },
        method="GET",
    )
    with urlopen(req, timeout=timeout_s) as resp:
        final_url = resp.geturl()
        body = resp.read()
    return final_url, body


def parse_html(body: bytes) -> BeautifulSoup:
    return BeautifulSoup(body.decode("utf-8", errors="replace"), "lxml")


def extract_ids_from_html(soup: BeautifulSoup) -> set[str]:
    ids: set[str] = set()
    for a in soup.find_all("a"):
        href = a.get("href")
        if not href:
            continue
        m = ID_RE.search(href)
        if m:
            ids.add(m.group(1))
    return ids


def extract_id_from_url(url: str) -> str | None:
    m = ID_RE.search(url)
    return m.group(1) if m else None


@dataclass(frozen=True)
class FormulaSearchForm:
    action_url: str
    method: str
    formula_field: str
    allow_unspecified_field: str | None
    allow_more_atoms_field: str | None
    exclude_ions_field: str | None
    diatomic_constants_field: str
    diatomic_constants_value: str | None


def _nearest_text(tag) -> str:
    parent = tag.parent
    if parent is None:
        return ""
    return " ".join(parent.stripped_strings)


def load_formula_search_form(*, user_agent: str, timeout_s: float) -> FormulaSearchForm:
    """
    Downloads the formula search page and identifies:
      - form action/method
      - the formula input name
      - checkbox name for 'Constants of diatomic molecules'
      - checkbox names for allow-unspecified / allow-more-atoms / exclude-ions (if present)

    We do NOT set 'exact isotopes' or anything restrictive — we want all isotopologues.
    """
    final_url, body = http_get(FORMULA_SEARCH_URL, user_agent=user_agent, timeout_s=timeout_s)
    soup = parse_html(body)

    form = soup.find("form")
    if form is None:
        raise RuntimeError("Could not find <form> on the formula search page. NIST may have changed HTML.")

    action = form.get("action") or "/cgi/cbook.cgi"
    action_url = urljoin(final_url, action)
    method = (form.get("method") or "GET").upper()

    # formula text input
    formula_field = None
    for inp in form.find_all("input"):
        t = (inp.get("type") or "").lower()
        if t in {"text", "search"}:
            name = inp.get("name")
            if name:
                formula_field = name
                break
    if not formula_field:
        for inp in form.find_all("input"):
            name = (inp.get("name") or "").lower()
            if "formula" in name:
                formula_field = inp.get("name")
                break
    if not formula_field:
        raise RuntimeError("Could not determine the formula input field name from the formula search page.")

    allow_unspecified_field = None
    allow_more_atoms_field = None
    exclude_ions_field = None
    diatomic_constants_field = None
    diatomic_constants_value = None

    for inp in form.find_all("input"):
        t = (inp.get("type") or "").lower()
        if t != "checkbox":
            continue
        name = inp.get("name")
        if not name:
            continue
        context = _nearest_text(inp).lower()

        if "allow elements not specified" in context:
            allow_unspecified_field = name
        elif "allow more atoms" in context:
            allow_more_atoms_field = name
        elif "exclude ions" in context:
            exclude_ions_field = name
        elif "constants of diatomic molecules" in context:
            diatomic_constants_field = name
            diatomic_constants_value = inp.get("value")

    if not diatomic_constants_field:
        raise RuntimeError(
            "Could not find the checkbox field for 'Constants of diatomic molecules' on the formula search form.\nIf NIST changed the HTML, paste the form section and we’ll update the matcher."
        )

    return FormulaSearchForm(
        action_url=action_url,
        method=method,
        formula_field=formula_field,
        allow_unspecified_field=allow_unspecified_field,
        allow_more_atoms_field=allow_more_atoms_field,
        exclude_ions_field=exclude_ions_field,
        diatomic_constants_field=diatomic_constants_field,
        diatomic_constants_value=diatomic_constants_value,
    )


def build_search_url(form: FormulaSearchForm, *, formula_query: str, include_ions: bool) -> str:
    """
    Build a URL for the formula search restricted to 'Constants of diatomic molecules',
    using broad match options so each element search returns a wide subset.
    """
    params: dict[str, str] = {}
    params[form.formula_field] = formula_query

    # Broad matching
    if form.allow_unspecified_field:
        params[form.allow_unspecified_field] = "on"
    if form.allow_more_atoms_field:
        params[form.allow_more_atoms_field] = "on"

    # Include ions by default (so do NOT check 'exclude ions')
    if not include_ions and form.exclude_ions_field:
        params[form.exclude_ions_field] = "on"

    # Restrict to diatomic constants
    params[form.diatomic_constants_field] = form.diatomic_constants_value or "on"

    return form.action_url + "?" + urlencode(params)


def write_cache_pair(*, cache_dir: Path, webbook_id: str, url: str, body: bytes) -> tuple[Path, Path]:
    ensure_dir(cache_dir)
    body_path = cache_dir / f"{webbook_id}.body"
    meta_path = cache_dir / f"{webbook_id}.meta.json"

    body_path.write_bytes(body)

    meta = {
        "source": "nist_webbook",
        "kind": "cbook",
        "webbook_id": webbook_id,
        "url": url,
        "fetched_at": utc_now_iso(),
        "sha256": sha256_bytes(body),
        "bytes": len(body),
    }
    meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return body_path, meta_path


def discover_all_ids(
    *,
    form: FormulaSearchForm,
    include_ions: bool,
    sleep_s: float,
    user_agent: str,
    timeout_s: float,
    max_elements: int | None,
    log_jsonl: Path,
) -> set[str]:
    ids: set[str] = set()
    elements = ELEMENT_SYMBOLS[:max_elements] if max_elements else ELEMENT_SYMBOLS

    for el in elements:
        url = build_search_url(form, formula_query=el, include_ions=include_ions)
        final_url, body = http_get(url, user_agent=user_agent, timeout_s=timeout_s)
        soup = parse_html(body)

        new_ids = extract_ids_from_html(soup)

        # Sometimes a search resolves directly to a species page
        single_id = extract_id_from_url(final_url)
        if single_id:
            new_ids.add(single_id)

        before = len(ids)
        ids |= new_ids
        after = len(ids)

        ensure_dir(log_jsonl.parent)
        with log_jsonl.open("a", encoding="utf-8") as f:
            f.write(
                json.dumps(
                    {
                        "ts": utc_now_iso(),
                        "event": "discover",
                        "element": el,
                        "search_url": url,
                        "final_url": final_url,
                        "found_ids": len(new_ids),
                        "total_ids": after,
                        "delta": after - before,
                    }
                )
                + "\n"
            )

        if sleep_s > 0:
            time.sleep(sleep_s)

    return ids


def fetch_all_species_pages(
    *,
    ids: list[str],
    cache_dir: Path,
    mask: str,
    sleep_s: float,
    user_agent: str,
    timeout_s: float,
    force: bool,
    log_jsonl: Path,
    max_fetch: int | None,
) -> None:
    done = 0
    for webbook_id in ids:
        if max_fetch is not None and done >= max_fetch:
            break

        body_path = cache_dir / f"{webbook_id}.body"
        meta_path = cache_dir / f"{webbook_id}.meta.json"
        if not force and body_path.exists() and meta_path.exists():
            continue

        url = f"{CBOOK_URL}?{urlencode({'ID': webbook_id, 'Mask': mask})}"

        ok = False
        err = None
        final_url = url
        body = b""
        try:
            final_url, body = http_get(url, user_agent=user_agent, timeout_s=timeout_s)
            write_cache_pair(cache_dir=cache_dir, webbook_id=webbook_id, url=final_url, body=body)
            ok = True
        except Exception as e:
            err = repr(e)

        ensure_dir(log_jsonl.parent)
        with log_jsonl.open("a", encoding="utf-8") as f:
            f.write(
                json.dumps(
                    {
                        "ts": utc_now_iso(),
                        "event": "fetch",
                        "webbook_id": webbook_id,
                        "url": url,
                        "final_url": final_url,
                        "ok": ok,
                        "bytes": len(body) if ok else 0,
                        "error": err,
                    }
                )
                + "\n"
            )

        if ok:
            done += 1

        if sleep_s > 0:
            time.sleep(sleep_s)


def main() -> None:
    ap = argparse.ArgumentParser(description="Bulk ingest NIST WebBook diatomic-constants pages into local cache.")
    ap.add_argument("--mask", default="1000", help="Mask to fetch for each species page (default: 1000).")
    ap.add_argument("--sleep", type=float, default=0.5, help="Delay between requests in seconds (default: 0.5).")
    ap.add_argument("--timeout", type=float, default=30.0, help="HTTP timeout in seconds.")
    ap.add_argument("--user-agent", default="spectra-db-bulk-ingest/1.0 (local research)", help="User-Agent header.")
    ap.add_argument("--force", action="store_true", help="Re-fetch even if cache files exist.")
    ap.add_argument("--max-elements", type=int, default=None, help="Limit element symbols for a quick test run.")
    ap.add_argument("--max-fetch", type=int, default=None, help="Limit number of species pages fetched for a quick test run.")
    ap.add_argument("--include-ions", action="store_true", help="Include ions (default: included).")
    ap.add_argument("--exclude-ions", action="store_true", help="Exclude ions (overrides include).")

    ap.add_argument("--cache-dir", type=Path, default=None, help="Cache dir for .body/.meta.json")
    ap.add_argument("--manifest", type=Path, default=None, help="Write discovered IDs to this file")
    ap.add_argument("--log", type=Path, default=None, help="JSONL log path")

    args = ap.parse_args()

    # Default: include ions
    include_ions = True
    if args.exclude_ions:
        include_ions = False
    if args.include_ions:
        include_ions = True

    repo_root = get_paths().repo_root
    default_cache = repo_root / "data" / "raw" / "nist_webbook" / "cbook"
    cache_dir = args.cache_dir or default_cache

    default_manifest = repo_root / "data" / "raw" / "nist_webbook" / "diatomic_ids.txt"
    manifest_path = args.manifest or default_manifest

    default_log = repo_root / "data" / "raw" / "nist_webbook" / "bulk_diatomic.log.jsonl"
    log_path = args.log or default_log

    print("Loading NIST formula search form…")
    form = load_formula_search_form(user_agent=args.user_agent, timeout_s=args.timeout)
    print(
        "Form parsed:",
        {
            "action_url": form.action_url,
            "method": form.method,
            "formula_field": form.formula_field,
            "diatomic_field": form.diatomic_constants_field,
        },
    )

    print("Discovering IDs (union over element searches)…")
    ids = discover_all_ids(
        form=form,
        include_ions=include_ions,
        sleep_s=args.sleep,
        user_agent=args.user_agent,
        timeout_s=args.timeout,
        max_elements=args.max_elements,
        log_jsonl=log_path,
    )

    ensure_dir(manifest_path.parent)
    manifest_path.write_text("\n".join(sorted(ids)) + "\n", encoding="utf-8")
    print(f"Discovered {len(ids)} IDs. Wrote manifest: {manifest_path}")

    print("Fetching species pages into cache…")
    fetch_all_species_pages(
        ids=sorted(ids),
        cache_dir=cache_dir,
        mask=args.mask,
        sleep_s=args.sleep,
        user_agent=args.user_agent,
        timeout_s=args.timeout,
        force=args.force,
        log_jsonl=log_path,
        max_fetch=args.max_fetch,
    )

    print(f"Done. Cache dir: {cache_dir}")
    print("Next steps:")
    print("  python tools/scrapers/nist_webbook/normalize_cache.py")
    print("  python scripts/bootstrap_db.py --profile molecular --truncate-all")


if __name__ == "__main__":
    main()
