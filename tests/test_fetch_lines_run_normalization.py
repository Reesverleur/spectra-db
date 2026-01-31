# tests/test_fetch_lines_run_normalization.py
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from textwrap import dedent

import pytest

import tools.scrapers.nist_asd.fetch_lines as fetch_lines
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


def _fit_cell(value: str, width: int) -> str:
    """
    IMPORTANT: header strings must not overflow into neighboring columns, because
    parse_lines_response() slices header lines by fixed pipe positions.
    """
    s = str(value)
    if len(s) > width:
        s = s[:width]
    return s.ljust(width)


def _make_fixed_width_row(cells: list[str], widths: list[int]) -> str:
    parts = [_fit_cell(c, w) for c, w in zip(cells, widths, strict=True)]
    return " | ".join(parts) + " |"


def test_fetch_lines_run_packed_energies_term_spaces_url_reconstruction_and_dedupe(monkeypatch, tmp_path: Path) -> None:
    repo_root = tmp_path / "repo_root"
    paths = RepoPaths(repo_root=repo_root)
    monkeypatch.setattr(fetch_lines, "get_paths", lambda: paths)

    widths = [18, 8, 18, 8, 18, 22, 12, 10, 6, 12, 10, 6, 6, 10, 10, 12]

    # Keep headers SHORT so they fit within widths (no overflow -> no mislabeling)
    header_detail = _make_fixed_width_row(
        [
            "Obs Wavelength",  # contains "wavelength"
            "Unc",  # contains "unc"
            "Ritz Wavelength",  # contains "ritz" + "wavelength"
            "Unc",
            "Wavenumber",  # contains "wavenumber"
            "Ei Ek",  # contains Ei and Ek
            "Lower Conf",  # contains "conf"
            "Lower Term",
            "Lower J",
            "Upper Conf",  # contains "conf"
            "Upper Term",
            "Upper J",
            "Type",
            "TP Ref",
            "Line Ref",
            "ExtraCol",
        ],
        widths,
    )

    header_2 = _make_fixed_width_row([""] * len(widths), widths)

    data = _make_fixed_width_row(
        [
            "656.2800",
            "0.001",
            "656.2799",
            "0.002",
            "15233.0",
            "1872.5998 - 112 994.097",
            "2p",
            "2P° odd",
            "1/2",
            "3d",
            "2D even",
            "3/2",
            "E1",
            "T7771",
            "L8672c99",
            "hello",
        ],
        widths,
    )

    sep = "-" * len(header_detail)

    html = dedent(
        f"""\
        <html><body><pre>
        {sep}
        {header_detail}
        {header_2}
        {sep}
        {data}
        {sep}
        </pre></body></html>
        """
    ).encode("utf-8")

    raw_dir = paths.raw_dir / "nist_asd" / "lines"
    raw_dir.mkdir(parents=True, exist_ok=True)
    body_path = raw_dir / "fake.body"
    body_path.write_bytes(html)

    def _fake_fetch_cached(*, url, params, cache_dir, force, **kwargs):
        return _FakeFetchResult(status_code=200, content_path=body_path)

    monkeypatch.setattr(fetch_lines, "fetch_cached", _fake_fetch_cached)

    res1 = fetch_lines.run(
        spectrum="Fe I",
        min_wav=650.0,
        max_wav=660.0,
        unit="nm",
        wavelength_type="vacuum",
        force=False,
    )
    assert res1.ok is True
    assert res1.written == 1

    trans_path = paths.normalized_dir / "transitions.ndjson"
    refs_path = paths.normalized_dir / "refs.ndjson"

    trans = _read_ndjson(trans_path)
    refs = _read_ndjson(refs_path)

    assert len(trans) == 1
    t0 = trans[0]

    # Line ref preferred for transition.ref_id
    assert t0["ref_id"] == "L:L8672c99"

    payload = json.loads(t0["intensity_json"])
    assert payload["Ei_cm-1"] == pytest.approx(1872.5998)
    assert payload["Ek_cm-1"] == pytest.approx(112994.097)

    assert payload["lower"]["term"] == "2P° odd"
    assert payload["upper"]["term"] == "2D even"

    # URL reconstruction fallback (we provided no popded anchors)
    line_urls = payload.get("line_ref_urls") or []
    tp_urls = payload.get("tp_ref_urls") or []
    assert len(line_urls) == 1
    assert len(tp_urls) == 1
    assert "db_id=8672" in line_urls[0]
    assert "comment_code=c99" in line_urls[0]
    assert "type=L" in line_urls[0]

    assert "db_id=7771" in tp_urls[0]
    assert "type=T" in tp_urls[0]

    ex = json.loads(t0["extra_json"])
    assert ex["Transition Wavenumber"] == "15233.0"
    assert ex["ExtraCol"] == "hello"

    ref_ids = {r["ref_id"] for r in refs}
    assert "T:T7771" in ref_ids
    assert "L:L8672c99" in ref_ids

    # Run 2 should dedupe
    res2 = fetch_lines.run(
        spectrum="Fe I",
        min_wav=650.0,
        max_wav=660.0,
        unit="nm",
        wavelength_type="vacuum",
        force=False,
    )
    assert res2.ok is True
    assert res2.written == 0
    assert len(_read_ndjson(trans_path)) == 1
