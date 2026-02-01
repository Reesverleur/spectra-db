from __future__ import annotations

import argparse
import fnmatch
import json
from dataclasses import dataclass
from pathlib import Path

from spectra_db.scrapers.common.http import fetch_cached
from spectra_db.util.paths import get_paths

EXOMOL_API_BASE = "https://exomol.com/api"


@dataclass(frozen=True)
class RunResult:
    ok: bool
    written: int
    notes: str | None = None
    api_json_path: str | None = None
    manifest_txt_path: str | None = None


def fetch_api_json(*, molecule: str, force: bool, timeout_s: float = 60.0) -> Path:
    """
    Cache ExoMol API JSON for a molecule.

    We do NOT interpret the JSON here (structure can evolve). We simply cache it
    and write a readable copy alongside the cached .body/.meta.
    """
    paths = get_paths()
    cache_dir = paths.raw_dir / "exomol" / "api"
    res = fetch_cached(
        url=EXOMOL_API_BASE,
        params={"molecule": molecule},
        cache_dir=cache_dir,
        force=force,
        timeout_s=timeout_s,
    )

    # fetch_cached always returns content_path/meta_path; status_code is in the meta json
    content_path = res.content_path
    pretty_path = cache_dir / f"{molecule}.api.json"
    pretty_path.write_text(content_path.read_text(encoding="utf-8", errors="replace"), encoding="utf-8")
    return pretty_path


def fetch_manifest(*, manifest_url: str, out_dir: Path, force: bool, timeout_s: float = 60.0) -> Path:
    """
    Cache a manifest file (text) at a user-provided URL.
    """
    cache_dir = out_dir / "manifest"
    res = fetch_cached(
        url=manifest_url,
        params={},
        cache_dir=cache_dir,
        force=force,
        timeout_s=timeout_s,
    )
    txt_path = cache_dir / "manifest.txt"
    txt_path.write_text(res.content_path.read_text(encoding="utf-8", errors="replace"), encoding="utf-8")
    return txt_path


def parse_manifest_filenames(manifest_txt: str) -> list[str]:
    """
    ExoMol manifests list filenames (and often sizes / checksums). We keep parsing minimal:
    - ignore comments/blank lines
    - take the first token as the filename
    """
    out: list[str] = []
    for line in manifest_txt.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        parts = s.split()
        if parts:
            out.append(parts[0])
    return out


def download_files_from_manifest(
    *,
    base_url: str,
    manifest_path: Path,
    out_dir: Path,
    file_glob: str | None,
    max_files: int | None,
    force: bool,
    timeout_s: float = 120.0,
) -> int:
    """
    Download files referenced by manifest from base_url/<filename>, caching each via fetch_cached.
    """
    files_dir = out_dir / "files"
    files_dir.mkdir(parents=True, exist_ok=True)

    names = parse_manifest_filenames(manifest_path.read_text(encoding="utf-8", errors="replace"))
    if file_glob:
        names = [n for n in names if fnmatch.fnmatch(n, file_glob)]
    if max_files is not None:
        names = names[:max_files]

    written = 0
    for fname in names:
        url = f"{base_url.rstrip('/')}/{fname}"
        res = fetch_cached(
            url=url,
            params={},
            cache_dir=files_dir,
            force=force,
            timeout_s=timeout_s,
        )
        if res.status_code == 200:
            written += 1
    return written


def run_api_only(*, molecule: str, force: bool, timeout_s: float) -> RunResult:
    api_path = fetch_api_json(molecule=molecule, force=force, timeout_s=timeout_s)
    return RunResult(ok=True, written=1, notes="Cached ExoMol API JSON (no interpretation).", api_json_path=str(api_path))


def run_manifest_download(
    *,
    molecule: str,
    isotopologue_label: str,
    line_list_label: str,
    base_url: str,
    manifest_url: str,
    file_glob: str | None,
    max_files: int | None,
    force: bool,
    timeout_s: float,
) -> RunResult:
    """
    Deterministic download: YOU provide base_url + manifest_url (copied from cached API JSON).
    We do not guess the JSON structure.
    """
    paths = get_paths()
    out_dir = paths.raw_dir / "exomol" / "molecules" / molecule / isotopologue_label / line_list_label
    out_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = fetch_manifest(manifest_url=manifest_url, out_dir=out_dir, force=force, timeout_s=timeout_s)
    written = download_files_from_manifest(
        base_url=base_url,
        manifest_path=manifest_path,
        out_dir=out_dir,
        file_glob=file_glob,
        max_files=max_files,
        force=force,
        timeout_s=max(120.0, timeout_s),
    )

    summary = {
        "molecule": molecule,
        "isotopologue": isotopologue_label,
        "line_list": line_list_label,
        "base_url": base_url,
        "manifest_url": manifest_url,
        "filter_glob": file_glob,
        "max_files": max_files,
        "downloaded_count": written,
    }
    (out_dir / "download.summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    return RunResult(
        ok=True,
        written=written,
        notes="Downloaded ExoMol files listed by manifest (cache-first).",
        manifest_txt_path=str(manifest_path),
    )


def main() -> None:
    p = argparse.ArgumentParser(description="Cache ExoMol API JSON and download ExoMol files via manifest.")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_api = sub.add_parser("api", help="Fetch & cache ExoMol API JSON for a molecule (no interpretation).")
    p_api.add_argument("--molecule", default="CO")
    p_api.add_argument("--force", action="store_true")
    p_api.add_argument("--timeout-s", type=float, default=60.0)

    p_dl = sub.add_parser("download", help="Download files given base_url + manifest_url (copied from cached API JSON).")
    p_dl.add_argument("--molecule", default="CO")
    p_dl.add_argument("--isotopologue", required=True)
    p_dl.add_argument("--line-list", required=True)
    p_dl.add_argument("--base-url", required=True)
    p_dl.add_argument("--manifest-url", required=True)
    p_dl.add_argument("--file-glob", default=None)
    p_dl.add_argument("--max-files", type=int, default=None)
    p_dl.add_argument("--force", action="store_true")
    p_dl.add_argument("--timeout-s", type=float, default=60.0)

    args = p.parse_args()

    if args.cmd == "api":
        rr = run_api_only(molecule=args.molecule, force=args.force, timeout_s=args.timeout_s)
    else:
        rr = run_manifest_download(
            molecule=args.molecule,
            isotopologue_label=args.isotopologue,
            line_list_label=args.line_list,
            base_url=args.base_url,
            manifest_url=args.manifest_url,
            file_glob=args.file_glob,
            max_files=args.max_files,
            force=args.force,
            timeout_s=args.timeout_s,
        )

    print(json.dumps(rr.__dict__, indent=2))


if __name__ == "__main__":
    main()
