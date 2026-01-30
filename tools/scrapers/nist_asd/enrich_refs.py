from __future__ import annotations

import argparse
import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup

from spectra_db.util.paths import get_paths
from tools.scrapers.common.http import fetch_cached

DOI_RE = re.compile(r"\b10\.\d{4,9}/[-._;()/:A-Z0-9]+\b", re.IGNORECASE)

REF_KEY_RE = re.compile(r"^(?P<kind>[ELT]):(?P<code>.+)$")
CODE_RE = re.compile(r"^[A-Za-z]+(?P<db_id>\d+)(?P<comment>[A-Za-z]\d+)?$")


def reconstruct_asbib_url(
    ref_key: str,
    *,
    element: str | None = None,
    spectr_charge: int | None = None,
) -> str | None:
    """
    Reconstruct ASBib popup URL from a ref key like:
      - E:L18349
      - L:L18361c138
      - T:T6892c83
    """
    m = REF_KEY_RE.match(ref_key.strip())
    if not m:
        return None
    kind = m.group("kind")
    code = m.group("code").strip()

    m2 = CODE_RE.match(code)
    if not m2:
        return None

    db_id = m2.group("db_id")
    comment = m2.group("comment") or ""

    base = "https://physics.nist.gov/cgi-bin/ASBib1/get_ASBib_ref.cgi"
    params = [
        ("db", "el"),
        ("db_id", db_id),
        ("comment_code", comment),
        ("element", element or ""),
        ("spectr_charge", "" if spectr_charge is None else str(spectr_charge)),
        ("type", kind),
    ]

    # build query string manually (no requests dependency here)
    from urllib.parse import urlencode

    qs = urlencode([(k, v) for (k, v) in params if v != ""], doseq=True)
    return f"{base}?{qs}"


@dataclass
class EnrichResult:
    ok: bool
    status_code: int | None
    message: str
    citation: str | None = None
    doi: str | None = None


def _should_retry(code: int | None) -> bool:
    return code in {429, 502, 503, 504}


def _backoff_sleep(attempt: int, base_s: float) -> None:
    time.sleep(min(base_s * (2**attempt), 60.0))


def _extract_citation_and_doi(html: str) -> tuple[str | None, str | None]:
    """Extract a compact citation string and DOI (best-effort)."""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    doi = None
    m = DOI_RE.search(text)
    if m:
        doi = m.group(0)

    # Heuristic citation: take the first few non-empty content-y lines
    filtered = []
    for ln in lines:
        low = ln.lower()
        if "standard reference" in low:
            continue
        if low.startswith(("search", "back", "home")):
            continue
        filtered.append(ln)
        if len(filtered) >= 6:
            break

    citation = " ".join(filtered).strip() if filtered else None
    if citation and len(citation) > 600:
        citation = citation[:600].rstrip() + "…"

    return citation, doi


def _read_ndjson(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    out: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out


def _write_ndjson(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    tmp.replace(path)


def enrich_one(url: str, cache_dir: Path, max_retries: int, backoff_base: float, force: bool) -> EnrichResult:
    """Fetch and parse a single ASBib ref page."""
    for attempt in range(max_retries + 1):
        fr = fetch_cached(url=url, params={}, cache_dir=cache_dir, force=force, polite_delay_s=0.0)
        code = fr.status_code

        if code != 200:
            msg = f"HTTP {code} for {url}"
            if _should_retry(code) and attempt < max_retries:
                _backoff_sleep(attempt, backoff_base)
                continue
            return EnrichResult(False, code, msg)

        html = fr.content_path.read_text(encoding="utf-8", errors="replace")
        citation, doi = _extract_citation_and_doi(html)
        return EnrichResult(True, code, "OK", citation=citation, doi=doi)

    return EnrichResult(False, None, "Retries exceeded")


def main() -> None:
    paths = get_paths()
    ap = argparse.ArgumentParser(description="Enrich refs.ndjson by fetching ASBib popded URLs.")
    ap.add_argument("--force", action="store_true", help="Re-fetch even if cached.")
    ap.add_argument("--max", type=int, default=None, help="Max refs to enrich (for testing).")
    ap.add_argument("--max-retries", type=int, default=3)
    ap.add_argument("--backoff-base", type=float, default=2.0)
    ap.add_argument("--polite-sleep", type=float, default=0.2)
    args = ap.parse_args()

    refs_path = paths.normalized_dir / "refs.ndjson"
    refs = _read_ndjson(refs_path)
    if not refs:
        print(f"No refs found at {refs_path}")
        return

    cache_dir = paths.raw_dir / "nist_asd" / "refs"

    todo = []
    # Fill missing URL fields from ref_id encoding when possible
    for r in refs:
        if r.get("url"):
            continue
        rid = r.get("ref_id")
        if not rid:
            continue
        # We usually don't know element/spectr_charge at this stage; leave them out.
        # URLs typically still resolve; enrichment will test HTTP status.
        r["url"] = reconstruct_asbib_url(str(rid))
    for r in refs:
        if r.get("citation"):
            continue
        url = r.get("url")
        if url:
            todo.append(r)

    if args.max is not None:
        todo = todo[: args.max]

    print(f"Refs total: {len(refs)} | To enrich: {len(todo)}")

    updated = 0
    for i, r in enumerate(todo, start=1):
        ref_id = r.get("ref_id")
        url = r.get("url")
        if not url:
            continue

        print(f"[{i}/{len(todo)}] {ref_id}")
        res = enrich_one(url, cache_dir, args.max_retries, args.backoff_base, args.force)

        if res.ok:
            r["citation"] = res.citation
            if res.doi:
                r["doi"] = res.doi
            updated += 1
        else:
            r["notes"] = f"{(r.get('notes') or '').strip()} | enrich_error: {res.message}".strip(" |")

        time.sleep(args.polite_sleep)

    _write_ndjson(refs_path, refs)
    print(f"Done. Updated {updated} refs. Wrote {refs_path}")


if __name__ == "__main__":
    main()
