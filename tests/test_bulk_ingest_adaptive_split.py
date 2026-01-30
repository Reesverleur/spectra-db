import json
from dataclasses import dataclass
from pathlib import Path

import tools.scrapers.nist_asd.bulk_ingest as bi


@dataclass
class FakeRes:
    ok: bool
    written: int
    status_code: int
    message: str
    raw_path: str


def _write_fake_response(tmp: Path, key: str, nrows: int, page_size: int) -> str:
    """Write a fake .body/.meta.json that parse_lines_response can read."""
    body = tmp / f"{key}.body"
    meta = tmp / f"{key}.meta.json"

    # Build a <pre> table with nrows data lines
    header = (
        "------------------------------------------------------------------------------------------------\n"
        "| Observed Wavelength Vac (nm) | Unc. (nm) | Ritz Wavelength (nm) |\n"
        "------------------------------------------------------------------------------------------------\n"
    )
    rows = []
    for i in range(nrows):
        rows.append(f"| {500 + i}.0 | 0.1 | {500 + i}.0 |\n")
    footer = "------------------------------------------------------------------------------------------------\n"
    body.write_bytes(("<pre>\n" + header + "".join(rows) + footer + "</pre>\n").encode("utf-8"))

    meta.write_text(
        json.dumps({"params": {"page_size": str(page_size)}}, indent=2),
        encoding="utf-8",
    )
    return str(body)


def test_adaptive_split_triggers(monkeypatch, tmp_path: Path) -> None:
    # Monkeypatch run_lines to always return a response with parsed_rows == page_size, implying truncation
    # For the first call on a bin, it returns "full", forcing split until min_bin.
    calls = {"n": 0}

    def fake_run_lines(*, spectrum, min_wav, max_wav, unit, wavelength_type, force):
        calls["n"] += 1
        key = f"{min_wav:.3f}_{max_wav:.3f}_{calls['n']}"
        raw = _write_fake_response(tmp_path, key, nrows=2, page_size=2)  # full
        return FakeRes(True, written=2, status_code=200, message="OK", raw_path=raw)

    monkeypatch.setattr(bi, "run_lines", fake_run_lines)

    cfg = bi.BulkConfig(
        mode="lines",
        units_levels="cm-1",
        line_unit="nm",
        wavelength_type="vacuum",
        wav_min=0.0,
        wav_max=2.0,
        initial_bin=2.0,  # one bin initially
        min_bin=0.5,  # will split until width <= 0.5
        polite_sleep_s=0.0,
        max_retries=0,
        backoff_base_s=0.0,
        force=False,
        resume=False,
        max_splits=100,
    )

    ckpt = tmp_path / "ckpt.jsonl"
    done_bins = set()

    ok, written, msg = bi.ingest_lines_adaptive("H I", cfg, ckpt, done_bins)
    assert ok is True
    # should have made multiple calls due to splits
    assert calls["n"] > 1
    assert ckpt.exists()
