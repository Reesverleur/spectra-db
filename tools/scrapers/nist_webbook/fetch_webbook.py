from __future__ import annotations

import argparse
import json
from dataclasses import dataclass

from spectra_db.util.paths import get_paths
from tools.scrapers.common.http import fetch_cached


@dataclass(frozen=True)
class RunResult:
    ok: bool
    written: int
    body_path: str | None
    meta_path: str | None


def run(*, cas: str, mask: str, force: bool, timeout_s: float = 60.0) -> RunResult:
    paths = get_paths()
    cache_dir = paths.raw_dir / "nist_webbook" / f"ID_{cas}" / f"mask_{mask}"

    res = fetch_cached(
        url="https://webbook.nist.gov/cgi/cbook.cgi",
        params={"ID": cas, "Mask": mask},
        cache_dir=cache_dir,
        force=force,
        timeout_s=timeout_s,
    )

    return RunResult(
        ok=(res.status_code == 200),
        written=1 if res.status_code == 200 else 0,
        body_path=str(res.content_path),
        meta_path=str(res.meta_path),
    )


def main() -> None:
    p = argparse.ArgumentParser(description="Cache NIST WebBook pages into data/raw/ with metadata.")
    p.add_argument("--id", default="C630080", help="WebBook species ID (CO default).")
    p.add_argument("--mask", default="1000", help="Mask=1000 is diatomic constants for CO.")
    p.add_argument("--force", action="store_true")
    p.add_argument("--timeout-s", type=float, default=60.0)
    args = p.parse_args()

    rr = run(cas=args.id, mask=args.mask, force=args.force, timeout_s=args.timeout_s)
    print(json.dumps(rr.__dict__, indent=2))


if __name__ == "__main__":
    main()
