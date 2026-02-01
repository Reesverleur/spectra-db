from __future__ import annotations

import json
from pathlib import Path

from spectra_db.util.paths import RepoPaths
from tools.scrapers.nist_webbook import normalize_cache


def _read_ndjson(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            out.append(json.loads(line))
    return out


def test_normalize_cache_dir_ingests_once_and_is_idempotent(monkeypatch, tmp_path: Path) -> None:
    # Fake repo layout
    repo_root = tmp_path / "repo"
    repo_root.mkdir(parents=True, exist_ok=True)
    (repo_root / "pyproject.toml").write_text("[tool.ruff]\n", encoding="utf-8")

    paths = RepoPaths(repo_root=repo_root)

    cache_dir = paths.raw_dir / "nist_webbook" / "cbook"
    cache_dir.mkdir(parents=True, exist_ok=True)

    paths.normalized_molecular_dir.mkdir(parents=True, exist_ok=True)
    paths.normalized_dir.mkdir(parents=True, exist_ok=True)

    # Make get_paths() in normalize_cache point at our temp repo
    monkeypatch.setattr(normalize_cache, "get_paths", lambda: paths)

    # Also ensure the underlying normalizer uses the same get_paths() (it imports it from its module)
    from tools.scrapers.nist_webbook import normalize_diatomic_constants as norm

    monkeypatch.setattr(norm, "get_paths", lambda: paths)

    # Minimal valid diatomic constants HTML
    html = """\
    <html><body>
      <table>
        <caption>Diatomic constants for 12C16O</caption>
        <tr><th>State</th><th>Te</th><th>we</th><th>wexe</th><th>weye</th><th>Be</th><th>ae</th><th>ge</th><th>De</th><th>be</th><th>re</th><th>Trans</th><th>nu00</th></tr>
        <tr>
          <td>X1Sigma+</td>
          <td>0<sup>1</sup></td>
          <td>2169.813<sup>7</sup></td>
          <td>13.288</td>
          <td></td>
          <td>1.93128</td>
          <td>0.01750</td>
          <td></td>
          <td>6.12e-6</td>
          <td></td>
          <td>1.128323</td>
          <td></td>
          <td>2143.271<a href="#ref-2">2</a></td>
        </tr>
      </table>
      <h2 id="Refs">References</h2>
      <ol><li id="ref-2">Some citation here.</li></ol>
    </body></html>
    """

    # Two cache entries: both Mask=1000, different IDs, same HTML is fine for test
    def write_cache(key: str, webbook_id: str) -> None:
        (cache_dir / f"{key}.body").write_text(html, encoding="utf-8")
        meta = {
            "content_sha256": "dummy",
            "content_type": "text/html",
            "params": {"ID": webbook_id, "Mask": "1000"},
            "retrieved_utc": "2026-01-31T00:00:00Z",
            "status_code": 200,
            "url": "https://webbook.nist.gov/cgi/cbook.cgi",
        }
        (cache_dir / f"{key}.meta.json").write_text(json.dumps(meta), encoding="utf-8")

    write_cache("aaa111", "C630080")
    write_cache("bbb222", "C1333740")

    # Run 1: should process both
    rr1 = normalize_cache.run(cache_dir=cache_dir)
    assert rr1.ok is True
    assert rr1.eligible == 2
    assert rr1.processed == 2

    # Ensure atomic normalized stayed untouched
    assert not (paths.normalized_dir / "parameters.ndjson").exists()

    # Ensure molecular NDJSON written
    params_path = paths.normalized_molecular_dir / "parameters.ndjson"
    assert params_path.exists()
    params = _read_ndjson(params_path)
    assert any(p["name"] == "we" for p in params)

    # Ensure ingest log created with both cache keys
    log_path = paths.normalized_molecular_dir / "webbook_ingested.ndjson"
    log = _read_ndjson(log_path)
    keys = {r["cache_key"] for r in log}
    assert keys == {"aaa111", "bbb222"}

    # Run 2: should skip both as already ingested
    rr2 = normalize_cache.run(cache_dir=cache_dir)
    assert rr2.ok is True
    assert rr2.processed == 0
    assert rr2.skipped_already_ingested == 2
