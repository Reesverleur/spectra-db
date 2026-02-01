# tools/scrapers/nist_webbook/normalize_cache.py
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from spectra_db.scrapers.common.ndjson import append_ndjson_dedupe
from spectra_db.scrapers.nist_webbook.normalize_diatomic_constants import run as normalize_diatomic
from spectra_db.util.paths import get_paths


@dataclass(frozen=True)
class NormalizeCacheResult:
    ok: bool
    scanned: int
    eligible: int
    processed: int
    skipped_already_ingested: int
    skipped_incomplete_pair: int
    skipped_non_200: int
    skipped_non_diatomic_mask: int
    skipped_no_diatomic_table: int
    errors: int
    message: str


def _read_ingest_log_keys(path: Path) -> set[str]:
    if not path.exists():
        return set()
    keys: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        k = obj.get("cache_key")
        if isinstance(k, str) and k:
            keys.add(k)
    return keys


def _load_meta(meta_path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _is_expected_no_data(message: str) -> bool:
    """
    Some pages discovered via cDI=on genuinely have no diatomic constants table.
    That is an expected "no data" case and should not be counted as an error.

    The underlying normalizer returns:
      "No 'Diatomic constants for ...' tables found in HTML."
    """
    return "no 'diatomic constants" in (message or "").lower()


def run(*, cache_dir: Path | None = None) -> NormalizeCacheResult:
    """
    Scan the WebBook cache directory and normalize all *new* diatomic-constants pages (Mask=1000)
    into molecular NDJSON.

    Dedupe:
      - Per-cache-entry: ingestion log keyed by cache_key (<basename> of <key>.meta.json)
      - Per-row: append_ndjson_dedupe() in the underlying normalizer

    Notes:
      - Some discovered pages are legitimate "no data" cases (no diatomic constants table).
        These are logged as ingested with no_data=true and do not count as errors.
    """
    paths = get_paths()
    cache_dir = cache_dir or (paths.raw_dir / "nist_webbook" / "cbook")
    out_norm = paths.normalized_molecular_dir
    out_norm.mkdir(parents=True, exist_ok=True)

    ingest_log = out_norm / "webbook_ingested.ndjson"
    already = _read_ingest_log_keys(ingest_log)

    scanned = 0
    eligible = 0
    processed = 0
    skipped_already = 0
    skipped_pair = 0
    skipped_non200 = 0
    skipped_mask = 0
    skipped_no_table = 0
    errors = 0

    if not cache_dir.exists():
        return NormalizeCacheResult(
            ok=False,
            scanned=0,
            eligible=0,
            processed=0,
            skipped_already_ingested=0,
            skipped_incomplete_pair=0,
            skipped_non_200=0,
            skipped_non_diatomic_mask=0,
            skipped_no_diatomic_table=0,
            errors=0,
            message=f"Cache directory not found: {cache_dir}",
        )

    metas = sorted(cache_dir.glob("*.meta.json"))
    for meta_path in metas:
        scanned += 1

        # meta files are named <key>.meta.json
        # cache_key should be <key>
        name = meta_path.name
        if not name.endswith(".meta.json"):
            continue
        cache_key = name[: -len(".meta.json")]

        if cache_key in already:
            skipped_already += 1
            continue

        # Pair correctly: <key>.body
        body_path = meta_path.with_name(f"{cache_key}.body")
        if not body_path.exists():
            skipped_pair += 1
            continue

        meta = _load_meta(meta_path)
        if not meta:
            errors += 1
            continue

        status_code = meta.get("status_code")
        if status_code != 200:
            skipped_non200 += 1
            continue

        params = meta.get("params") or {}
        webbook_id = params.get("ID")
        mask = params.get("Mask")

        # Only normalize diatomic constants here (Mask=1000)
        if str(mask) != "1000":
            skipped_mask += 1
            continue

        if not isinstance(webbook_id, str) or not webbook_id:
            errors += 1
            continue

        eligible += 1

        try:
            rr = normalize_diatomic(webbook_id=webbook_id, body_path=body_path)
        except Exception:
            errors += 1
            continue

        if rr.ok:
            processed += 1
            already.add(cache_key)
            log_row = {
                "cache_key": cache_key,
                "source": "nist_webbook",
                "webbook_id": webbook_id,
                "mask": str(mask),
                "retrieved_utc": meta.get("retrieved_utc"),
                "content_sha256": meta.get("content_sha256"),
                "body_filename": body_path.name,
                "meta_filename": meta_path.name,
                "normalize_ok": True,
                "no_data": False,
                "normalize_message": rr.message,
            }
            append_ndjson_dedupe(ingest_log, [log_row], "cache_key")
            continue

        # Expected: discovered page with no diatomic constants table
        if _is_expected_no_data(rr.message):
            skipped_no_table += 1
            already.add(cache_key)
            log_row = {
                "cache_key": cache_key,
                "source": "nist_webbook",
                "webbook_id": webbook_id,
                "mask": str(mask),
                "retrieved_utc": meta.get("retrieved_utc"),
                "content_sha256": meta.get("content_sha256"),
                "body_filename": body_path.name,
                "meta_filename": meta_path.name,
                "normalize_ok": False,
                "no_data": True,
                "normalize_message": rr.message,
            }
            append_ndjson_dedupe(ingest_log, [log_row], "cache_key")
            continue

        # Unexpected failure: count as error and do not mark ingested.
        errors += 1

    ok = errors == 0
    msg = "ok" if ok else f"completed with {errors} errors"
    return NormalizeCacheResult(
        ok=ok,
        scanned=scanned,
        eligible=eligible,
        processed=processed,
        skipped_already_ingested=skipped_already,
        skipped_incomplete_pair=skipped_pair,
        skipped_non_200=skipped_non200,
        skipped_non_diatomic_mask=skipped_mask,
        skipped_no_diatomic_table=skipped_no_table,
        errors=errors,
        message=msg,
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="Normalize all new cached NIST WebBook diatomic constants pages (Mask=1000) into molecular NDJSON.")
    ap.add_argument(
        "--cache-dir",
        type=Path,
        default=None,
        help="Override cache directory. Default: data/raw/nist_webbook/cbook",
    )
    args = ap.parse_args()
    rr = run(cache_dir=args.cache_dir)
    print(rr)


if __name__ == "__main__":
    main()
