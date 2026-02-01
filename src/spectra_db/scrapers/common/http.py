from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests


@dataclass
class FetchResult:
    """Result of a cached fetch."""

    url: str
    params: dict[str, Any]
    status_code: int
    retrieved_utc: str
    content_path: Path
    meta_path: Path
    from_cache: bool


def _utc_iso() -> str:
    """Return current UTC timestamp in ISO-8601 format (seconds precision)."""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def stable_request_key(url: str, params: dict[str, Any]) -> str:
    """Create a stable hash key for a URL+params request."""
    items = sorted((k, str(v)) for k, v in params.items())
    blob = url + "\n" + "\n".join(f"{k}={v}" for k, v in items)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def sha256_bytes(b: bytes) -> str:
    """SHA256 of bytes."""
    return hashlib.sha256(b).hexdigest()


def ensure_dir(p: Path) -> None:
    """Create directory if missing."""
    p.mkdir(parents=True, exist_ok=True)


def fetch_cached(
    *,
    url: str,
    params: dict[str, Any],
    cache_dir: Path,
    session: requests.Session | None = None,
    polite_delay_s: float = 0.4,
    timeout_s: float = 60.0,
    force: bool = False,
) -> FetchResult:
    """Fetch a URL with query params, caching response body and metadata.

    Cache layout:
      cache_dir/
        <key>.body
        <key>.meta.json

    Args:
        url: Base URL.
        params: Query parameters (will be sorted for stable caching).
        cache_dir: Cache directory.
        session: Optional requests session.
        polite_delay_s: Sleep duration after a network fetch (not used if cache hit).
        timeout_s: Requests timeout.
        force: If True, re-fetch even if cached.

    Returns:
        FetchResult describing the saved files.
    """
    ensure_dir(cache_dir)
    key = stable_request_key(url, params)
    body_path = cache_dir / f"{key}.body"
    meta_path = cache_dir / f"{key}.meta.json"

    if body_path.exists() and meta_path.exists() and not force:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        return FetchResult(
            url=url,
            params=params,
            status_code=int(meta.get("status_code", 200)),
            retrieved_utc=str(meta.get("retrieved_utc", "")),
            content_path=body_path,
            meta_path=meta_path,
            from_cache=True,
        )

    sess = session or requests.Session()
    headers = {
        "User-Agent": "spectra-db/0.0.1 (research; contact via repo issues)",
    }
    resp = sess.get(url, params=params, headers=headers, timeout=timeout_s)
    retrieved = _utc_iso()

    body_path.write_bytes(resp.content)
    meta = {
        "url": url,
        "params": {k: str(v) for k, v in params.items()},
        "status_code": resp.status_code,
        "retrieved_utc": retrieved,
        "content_sha256": sha256_bytes(resp.content),
        "content_type": resp.headers.get("Content-Type", ""),
    }
    meta_path.write_text(json.dumps(meta, indent=2, sort_keys=True), encoding="utf-8")

    # be polite only when we actually hit the network
    time.sleep(polite_delay_s)

    return FetchResult(
        url=url,
        params=params,
        status_code=resp.status_code,
        retrieved_utc=retrieved,
        content_path=body_path,
        meta_path=meta_path,
        from_cache=False,
    )


if __name__ == "__main__":
    # Demo: cache a tiny GET to example.com
    demo_dir = Path(".") / "_tmp_cache_demo"
    r = fetch_cached(url="https://example.com", params={}, cache_dir=demo_dir, force=True)
    print("Fetched:", r.status_code, "bytes:", r.content_path.stat().st_size, "cache:", r.from_cache)
