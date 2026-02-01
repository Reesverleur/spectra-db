# tools/scrapers/nist_webbook/fetch_webbook.py
from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Any

from spectra_db.scrapers.common.http import FetchResult, fetch_cached
from spectra_db.util.paths import get_paths

WEBBOOK_CBOOK_URL = "https://webbook.nist.gov/cgi/cbook.cgi"


@dataclass(frozen=True)
class FetchRunResult:
    ok: bool
    status_code: int | None
    message: str
    raw_path: str | None = None
    meta_path: str | None = None


def run(*, webbook_id: str, mask: int = 1000, force: bool = False, timeout_s: float = 60.0) -> FetchRunResult:
    paths = get_paths()
    cache_dir = paths.raw_dir / "nist_webbook" / "cbook"

    params: dict[str, Any] = {"ID": webbook_id, "Mask": str(mask)}
    res: FetchResult = fetch_cached(
        url=WEBBOOK_CBOOK_URL,
        params=params,
        cache_dir=cache_dir,
        timeout_s=timeout_s,
        force=force,
    )

    if res.status_code != 200:
        return FetchRunResult(ok=False, status_code=res.status_code, message=f"HTTP {res.status_code}", raw_path=str(res.content_path), meta_path=str(res.meta_path))

    return FetchRunResult(
        ok=True,
        status_code=res.status_code,
        message="ok",
        raw_path=str(res.content_path),
        meta_path=str(res.meta_path),
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch and cache a NIST WebBook cbook.cgi page.")
    ap.add_argument("--id", dest="webbook_id", required=True, help="WebBook ID, e.g. C630080 for CO.")
    ap.add_argument("--mask", type=int, default=1000, help="Mask (1000 = diatomic constants).")
    ap.add_argument("--force", action="store_true", help="Force re-fetch even if cached.")
    ap.add_argument("--timeout-s", type=float, default=60.0, help="HTTP timeout (seconds).")

    args = ap.parse_args()
    rr = run(webbook_id=args.webbook_id, mask=args.mask, force=args.force, timeout_s=args.timeout_s)
    print(rr)


if __name__ == "__main__":
    main()
