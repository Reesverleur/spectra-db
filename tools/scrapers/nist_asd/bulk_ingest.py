from __future__ import annotations

import argparse
import json
import math
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tools.scrapers.nist_asd.fetch_levels import run as run_levels
from tools.scrapers.nist_asd.fetch_lines import run as run_lines
from tools.scrapers.nist_asd.parse_lines import parse_lines_response


@dataclass(frozen=True)
class BulkConfig:
    mode: str  # levels|lines|both

    # levels
    units_levels: str  # cm-1|eV

    # lines
    line_unit: str  # nm|Angstrom|um
    wavelength_type: str  # vacuum|vac+air
    wav_min: float
    wav_max: float
    initial_bin: float
    min_bin: float  # smallest allowed bin width before we stop splitting

    # networking/resume
    polite_sleep_s: float
    max_retries: int
    backoff_base_s: float
    force: bool
    resume: bool

    # safety
    max_splits: int  # maximum number of split operations per spectrum


def _progress(i: int, n: int) -> str:
    pct = 100.0 * i / max(n, 1)
    return f"[{i}/{n} {pct:5.1f}%]"


def _should_retry(status_code: int | None) -> bool:
    return status_code in {429, 502, 503, 504}


def _sleep_backoff(attempt: int, base: float) -> None:
    t = min(base * (2**attempt), 60.0)
    time.sleep(t)


def _load_lines_checkpoint(path: Path) -> set[tuple[str, float, float, str, str]]:
    """Load completed (spectrum, lo, hi, unit, wavelength_type) bins."""
    done: set[tuple[str, float, float, str, str]] = set()
    if not path.exists():
        return done
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if obj.get("kind") != "lines" or obj.get("ok") is not True:
                    continue
                spec = str(obj["spectrum"])
                lo = float(obj["lo"])
                hi = float(obj["hi"])
                unit = str(obj["unit"])
                wtype = str(obj["wavelength_type"])
                done.add((spec, lo, hi, unit, wtype))
            except Exception:
                continue
    return done


def _load_levels_checkpoint(path: Path) -> set[str]:
    """Load completed spectra for levels."""
    done: set[str] = set()
    if not path.exists():
        return done
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if obj.get("kind") != "levels" or obj.get("ok") is not True:
                    continue
                done.add(str(obj["spectrum"]))
            except Exception:
                continue
    return done


def _append_checkpoint(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def _derive_meta_path(raw_path: str | None) -> Path | None:
    if not raw_path:
        return None
    p = Path(raw_path)
    if p.suffix == ".body":
        return p.with_suffix(".meta.json")
    return None


def _read_page_size_from_meta(meta_path: Path | None) -> int | None:
    if not meta_path or not meta_path.exists():
        return None
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        params = meta.get("params", {})
        ps = params.get("page_size")
        if ps is None:
            return None
        return int(str(ps))
    except Exception:
        return None


def _count_lines_rows(raw_path: str | None) -> int | None:
    """Parse the saved raw response and return number of line rows (best-effort)."""
    if not raw_path:
        return None
    p = Path(raw_path)
    if not p.exists():
        return None
    try:
        df = parse_lines_response(p.read_bytes())
        return int(df.shape[0])
    except Exception:
        return None


def _make_bins(wmin: float, wmax: float, width: float) -> list[tuple[float, float]]:
    n = int(math.ceil((wmax - wmin) / width))
    out: list[tuple[float, float]] = []
    for i in range(n):
        lo = wmin + i * width
        hi = min(wmax, lo + width)
        out.append((lo, hi))
    return out


def ingest_levels(spec: str, cfg: BulkConfig, ckpt: Path) -> tuple[bool, int, str]:
    """Fetch levels for a single spectrum with retry/backoff and checkpoint logging."""
    for attempt in range(cfg.max_retries + 1):
        res = run_levels(spectrum=spec, units=cfg.units_levels, force=cfg.force)
        if res.ok:
            _append_checkpoint(
                ckpt,
                {
                    "kind": "levels",
                    "ok": True,
                    "spectrum": spec,
                    "written": res.written,
                    "status": res.status_code,
                    "message": res.message,
                    "raw_path": res.raw_path,
                    "ts_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                },
            )
            return True, res.written, "OK"

        # failure
        _append_checkpoint(
            ckpt,
            {
                "kind": "levels",
                "ok": False,
                "spectrum": spec,
                "written": 0,
                "status": res.status_code,
                "message": res.message,
                "raw_path": res.raw_path,
                "ts_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            },
        )

        if _should_retry(res.status_code) and attempt < cfg.max_retries:
            print(f"  levels RETRY {attempt + 1}/{cfg.max_retries}: {res.message}")
            _sleep_backoff(attempt, cfg.backoff_base_s)
            continue

        return False, 0, res.message

    return False, 0, "Levels retries exceeded"


def ingest_lines_adaptive(spec: str, cfg: BulkConfig, ckpt: Path, done_bins: set[tuple[str, float, float, str, str]]) -> tuple[bool, int, str]:
    """Fetch lines for one spectrum with adaptive bin splitting until bins are not truncated."""
    total_written = 0
    splits_used = 0

    queue = _make_bins(cfg.wav_min, cfg.wav_max, cfg.initial_bin)

    while queue:
        lo, hi = queue.pop(0)

        # normalize floats for checkpoint keys (avoid float noise)
        lo_k = float(f"{lo:.12g}")
        hi_k = float(f"{hi:.12g}")
        key = (spec, lo_k, hi_k, cfg.line_unit, cfg.wavelength_type)

        if cfg.resume and key in done_bins:
            # already completed successfully
            continue

        # Try the bin with retries
        bin_ok = False
        last_msg = ""
        last_status: int | None = None
        last_raw: str | None = None
        wrote_this = 0

        for attempt in range(cfg.max_retries + 1):
            res = run_lines(
                spectrum=spec,
                min_wav=lo,
                max_wav=hi,
                unit=cfg.line_unit,
                wavelength_type=cfg.wavelength_type,
                force=cfg.force,
            )
            last_msg = res.message
            last_status = res.status_code
            last_raw = res.raw_path
            wrote_this = res.written

            if res.ok:
                bin_ok = True
                break

            if _should_retry(res.status_code) and attempt < cfg.max_retries:
                print(f"    lines RETRY {attempt + 1}/{cfg.max_retries} bin [{lo:g},{hi:g}]: {res.message}")
                _sleep_backoff(attempt, cfg.backoff_base_s)
                continue

            break

        if not bin_ok:
            _append_checkpoint(
                ckpt,
                {
                    "kind": "lines",
                    "ok": False,
                    "spectrum": spec,
                    "lo": lo_k,
                    "hi": hi_k,
                    "unit": cfg.line_unit,
                    "wavelength_type": cfg.wavelength_type,
                    "written": 0,
                    "status": last_status,
                    "message": last_msg,
                    "raw_path": last_raw,
                    "ts_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                },
            )
            return (
                False,
                total_written,
                f"Lines failed in bin [{lo:g},{hi:g}] {cfg.line_unit}: {last_msg}",
            )

        # Determine if the response likely hit the page_size cap → split
        meta_path = _derive_meta_path(last_raw)
        page_size = _read_page_size_from_meta(meta_path) or 0
        parsed_rows = _count_lines_rows(last_raw) or 0

        total_written += wrote_this

        # Mark this bin as done (even if it was truncated; we’ll split to fill missing)
        _append_checkpoint(
            ckpt,
            {
                "kind": "lines",
                "ok": True,
                "spectrum": spec,
                "lo": lo_k,
                "hi": hi_k,
                "unit": cfg.line_unit,
                "wavelength_type": cfg.wavelength_type,
                "written": wrote_this,
                "parsed_rows": parsed_rows,
                "page_size": page_size,
                "status": last_status,
                "message": "OK",
                "raw_path": last_raw,
                "ts_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            },
        )
        done_bins.add(key)

        # Progress print
        width = hi - lo
        print(f"    lines [{lo:g},{hi:g}] {cfg.line_unit}  rows={parsed_rows} page_size={page_size}  +{wrote_this}")

        # Truncation heuristic: if parsed_rows >= page_size and page_size > 0, assume truncated.
        # (It’s not perfect, but it’s the best signal available without true pagination.)
        truncated = (page_size > 0) and (parsed_rows >= page_size)

        if truncated:
            if width <= cfg.min_bin:
                print(f"    WARNING: bin appears full but width<=min_bin ({cfg.min_bin}). Keeping as-is.")
            elif splits_used >= cfg.max_splits:
                print(f"    WARNING: max_splits reached ({cfg.max_splits}). Keeping as-is.")
            else:
                mid = (lo + hi) / 2.0
                splits_used += 1
                # Put smaller bins back on the queue
                queue.insert(0, (mid, hi))
                queue.insert(0, (lo, mid))
                print(f"    SPLIT: bin full → splitting into [{lo:g},{mid:g}] and [{mid:g},{hi:g}]")
                # Note: dedupe ensures repeated lines from overlapping bins are not duplicated.

        time.sleep(cfg.polite_sleep_s)

    return True, total_written, "OK"


def _load_spectra_list(path: Path) -> list[str]:
    return [ln.strip() for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]


def main() -> None:
    ap = argparse.ArgumentParser(description="Bulk ingest NIST ASD levels/lines with adaptive splitting + resume.")
    ap.add_argument("--mode", choices=["levels", "lines", "both"], default="levels")
    ap.add_argument(
        "--spectra-file",
        type=Path,
        required=True,
        help="Text file: one spectrum per line (e.g. 'Fe II').",
    )

    ap.add_argument("--units-levels", choices=["cm-1", "eV"], default="cm-1")

    ap.add_argument("--line-unit", choices=["nm", "Angstrom", "um"], default="nm")
    ap.add_argument("--wavelength-type", choices=["vacuum", "vac+air"], default="vacuum")

    ap.add_argument("--wav-min", type=float, default=0.0)
    ap.add_argument("--wav-max", type=float, default=200000.0)
    ap.add_argument(
        "--initial-bin",
        type=float,
        default=1000.0,
        help="Initial bin width (adaptive splitting will refine).",
    )
    ap.add_argument("--min-bin", type=float, default=0.5, help="Smallest bin width allowed during splitting.")

    ap.add_argument("--polite-sleep", type=float, default=0.2)
    ap.add_argument("--max-retries", type=int, default=3)
    ap.add_argument("--backoff-base", type=float, default=2.0)
    ap.add_argument("--force", action="store_true", help="Force refetch even if cached (not recommended).")
    ap.add_argument("--no-resume", action="store_true", help="Disable resume behavior.")
    ap.add_argument("--max-splits", type=int, default=2000, help="Max split operations per spectrum (safety).")

    ap.add_argument(
        "--checkpoint",
        type=Path,
        default=None,
        help="Checkpoint JSONL path (default: data/raw/nist_asd/bulk_checkpoint.jsonl)",
    )
    args = ap.parse_args()

    cfg = BulkConfig(
        mode=args.mode,
        units_levels=args.units_levels,
        line_unit=args.line_unit,
        wavelength_type=args.wavelength_type,
        wav_min=args.wav_min,
        wav_max=args.wav_max,
        initial_bin=args.initial_bin,
        min_bin=args.min_bin,
        polite_sleep_s=args.polite_sleep,
        max_retries=args.max_retries,
        backoff_base_s=args.backoff_base,
        force=args.force,
        resume=not args.no_resume,
        max_splits=args.max_splits,
    )

    spectra = _load_spectra_list(args.spectra_file)
    print(f"Spectra loaded: {len(spectra)} from {args.spectra_file}")

    ckpt = args.checkpoint or (Path("data") / "raw" / "nist_asd" / "bulk_checkpoint.jsonl")

    done_levels = _load_levels_checkpoint(ckpt) if cfg.resume else set()
    done_bins = _load_lines_checkpoint(ckpt) if cfg.resume else set()

    ok_specs = 0
    fail_specs = 0

    for i, spec in enumerate(spectra, start=1):
        print(f"\n{_progress(i, len(spectra))} {spec}")

        # Levels
        if cfg.mode in {"levels", "both"}:
            if cfg.resume and spec in done_levels:
                print("  levels: SKIP (already done)")
            else:
                ok, n, msg = ingest_levels(spec, cfg, ckpt)
                if ok:
                    done_levels.add(spec)
                    print(f"  levels: +{n} (OK)")
                else:
                    fail_specs += 1
                    print(f"  levels: ERROR: {msg}")
                    continue  # skip lines if levels failed

        # Lines
        if cfg.mode in {"lines", "both"}:
            ok, n, msg = ingest_lines_adaptive(spec, cfg, ckpt, done_bins)
            if ok:
                print(f"  lines: +{n} (OK)")
            else:
                fail_specs += 1
                print(f"  lines: ERROR: {msg}")
                continue

        ok_specs += 1

    print(f"\nDONE. Spectra OK: {ok_specs}, failed: {fail_specs}")
    print(f"Checkpoint: {ckpt} (resume={'on' if cfg.resume else 'off'})")


if __name__ == "__main__":
    main()
