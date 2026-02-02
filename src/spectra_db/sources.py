from __future__ import annotations

import os
import shutil
import tempfile
from importlib import resources
from pathlib import Path

from spectra_db.util.paths import RepoPaths, get_paths, get_user_paths

SOURCES_PKG = "spectra_db_sources"

_EXPECTED_BY_PROFILE: dict[str, list[str]] = {
    "atomic": [
        "species.ndjson",
        "isotopologues.ndjson",
        "refs.ndjson",
        "states.ndjson",
        "transitions.ndjson",
        "parameters.ndjson",
    ],
    "molecular": [
        "species.ndjson",
        "isotopologues.ndjson",
        "refs.ndjson",
        "states.ndjson",
        "parameters.ndjson",
    ],
}


def _copy_file_atomic(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=dst.name + ".", suffix=".tmp", dir=str(dst.parent))
    try:
        tmp_path = Path(tmp_name)
        with src.open("rb") as fsrc, os.fdopen(fd, "wb") as fdst:
            shutil.copyfileobj(fsrc, fdst, length=16 * 1024 * 1024)
        tmp_path.replace(dst)
    except Exception:
        try:
            Path(tmp_name).unlink(missing_ok=True)
        except Exception:
            pass
        raise


def _resolve_install_paths(*, prefer_env: bool = True) -> RepoPaths:
    p = get_paths()
    if prefer_env and p.source == "env":
        return p
    return get_user_paths()


def _ndjson_dir(paths: RepoPaths, profile: str) -> Path:
    return paths.normalized_dir if profile == "atomic" else paths.normalized_molecular_dir


def sources_installed() -> bool:
    try:
        resources.files(SOURCES_PKG)
        return True
    except Exception:
        return False


def ensure_sources_available(*, profile: str, force: bool = False) -> Path | None:
    """
    If the sources wheel is installed, ensure NDJSON exists in the writable install location.
    Returns the NDJSON directory path if sources were installed/available, otherwise None.

    - force=False (default): copy only missing files (preserves user edits)
    - force=True: overwrite destination from packaged sources
    """
    if profile not in _EXPECTED_BY_PROFILE:
        raise ValueError(f"Unknown profile: {profile!r}")

    try:
        pkg_root = resources.files(SOURCES_PKG)
    except Exception:
        return None  # sources wheel not installed

    src_dir = pkg_root / "ndjson" / ("atomic" if profile == "atomic" else "molecular")
    if not src_dir.exists():
        return None

    paths = _resolve_install_paths(prefer_env=True)
    dst_dir = _ndjson_dir(paths, profile)
    dst_dir.mkdir(parents=True, exist_ok=True)

    for fname in _EXPECTED_BY_PROFILE[profile]:
        src = src_dir / fname
        if not src.exists():
            continue
        dst = dst_dir / fname
        if dst.exists() and not force:
            continue
        _copy_file_atomic(Path(str(src)), dst)

    return dst_dir


def ndjson_has_core_files(ndjson_dir: Path, profile: str) -> bool:
    # consider "present" if at least core identity files exist
    core = ["species.ndjson", "isotopologues.ndjson", "refs.ndjson"]
    for f in core:
        if not (ndjson_dir / f).exists():
            return False
    # plus one profile-specific table
    if profile == "atomic":
        return (ndjson_dir / "states.ndjson").exists() and (ndjson_dir / "transitions.ndjson").exists()
    return (ndjson_dir / "states.ndjson").exists() and (ndjson_dir / "parameters.ndjson").exists()
