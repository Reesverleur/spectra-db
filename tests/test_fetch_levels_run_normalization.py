# tests/test_fetch_levels_run_normalization.py
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pytest

import tools.scrapers.nist_asd.fetch_levels as fetch_levels
from spectra_db.util.paths import RepoPaths


@dataclass
class _FakeFetchResult:
    status_code: int
    content_path: Path


def _read_ndjson(path: Path) -> list[dict]:
    assert path.exists(), f"Missing NDJSON: {path}"
    out: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        out.append(json.loads(line))
    return out


def test_fetch_levels_run_forward_fill_multiref_extras_and_dedupe(monkeypatch, tmp_path: Path) -> None:
    # Make a fake "repo root" so scraper writes into tmp_path, not the real repo data/.
    repo_root = tmp_path / "repo_root"
    paths = RepoPaths(repo_root=repo_root)

    # Patch get_paths() used inside fetch_levels module
    monkeypatch.setattr(fetch_levels, "get_paths", lambda: paths)

    # Build a fake cached HTML response file
    html = """
    <html><body>

      <!-- popded anchors (code -> URL mapping) -->
      <a class="bib" href="javascript:void(0)"
         onclick="popded('https://physics.nist.gov/cgi-bin/ASBib1/get_ASBib_ref.cgi?db=el&amp;db_id=1234&amp;type=E');return false">
         L1234a
      </a>
      <a class="bib" href="javascript:void(0)"
         onclick="popded('https://physics.nist.gov/cgi-bin/ASBib1/get_ASBib_ref.cgi?db=el&amp;db_id=5678&amp;type=E');return false">
         L5678b
      </a>

      <table>
        <tr>
          <th>Configuration</th>
          <th>Term</th>
          <th>J</th>
          <th>Level (cm-1)</th>
          <th>Unc. (cm-1)</th>
          <th>Ref.</th>
          <th>Landé g-factor</th>
          <th>Leading Percentages</th>
          <th>Foo</th>
        </tr>
        <tr>
          <td>2p</td>
          <td>2P°</td>
          <td>1/2</td>
          <td>82 258.9191133</td>
          <td>0.0001</td>
          <td>L1234a, L5678b</td>
          <td>1.002</td>
          <td>95% 2p</td>
          <td>bar</td>
        </tr>
        <!-- continuation row: Configuration/Term blank but must forward-fill -->
        <tr>
          <td></td>
          <td></td>
          <td>3/2</td>
          <td>82 259.2850014</td>
          <td>0.0001</td>
          <td>L1234a, L5678b</td>
          <td>1.003</td>
          <td>94% 2p</td>
          <td>baz</td>
        </tr>
      </table>
    </body></html>
    """.strip()

    raw_dir = paths.raw_dir / "nist_asd" / "levels"
    raw_dir.mkdir(parents=True, exist_ok=True)
    body_path = raw_dir / "fake.body"
    body_path.write_text(html, encoding="utf-8")

    def _fake_fetch_cached(*, url, params, cache_dir, force, **kwargs):
        # Return our deterministic fake body
        return _FakeFetchResult(status_code=200, content_path=body_path)

    monkeypatch.setattr(fetch_levels, "fetch_cached", _fake_fetch_cached)

    # ---- Run 1 ----
    res1 = fetch_levels.run(spectrum="Fe I", units="cm-1", force=False)
    assert res1.ok is True
    assert res1.written == 2

    states_path = paths.normalized_dir / "states.ndjson"
    refs_path = paths.normalized_dir / "refs.ndjson"

    states = _read_ndjson(states_path)
    refs = _read_ndjson(refs_path)

    assert len(states) == 2

    # Forward-fill must have happened
    assert states[0]["configuration"] == "2p"
    assert states[0]["term"] == "2P°"
    assert states[1]["configuration"] == "2p"
    assert states[1]["term"] == "2P°"

    # Thousand separators fixed in numeric energy_value
    assert states[0]["energy_value"] == pytest.approx(82258.9191133)
    assert states[1]["energy_value"] == pytest.approx(82259.2850014)

    # Multi-ref: primary ref_id is a KEY (E:<code>), full list preserved in extra_json
    assert states[0]["ref_id"] == "E:L1234a"
    ex0 = json.loads(states[0]["extra_json"])
    assert ex0["ref_codes"] == ["L1234a", "L5678b"]
    assert ex0["ref_keys"] == ["E:L1234a", "E:L5678b"]
    assert ex0["ref_urls"][0].startswith("https://physics.nist.gov/cgi-bin/ASBib1/get_ASBib_ref.cgi?")
    assert ex0["Foo"] == "bar"

    # Refs NDJSON contains one record per ref key
    ref_ids = {r["ref_id"] for r in refs}
    assert "E:L1234a" in ref_ids
    assert "E:L5678b" in ref_ids

    # ---- Run 2 (same input) must be deduped by stable IDs ----
    res2 = fetch_levels.run(spectrum="Fe I", units="cm-1", force=False)
    assert res2.ok is True
    assert res2.written == 0
    states2 = _read_ndjson(states_path)
    assert len(states2) == 2
