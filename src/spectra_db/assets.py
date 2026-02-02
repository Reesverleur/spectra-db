from __future__ import annotations

import os
import shutil
import tempfile
from importlib import resources
from pathlib import Path

from spectra_db.util.paths import RepoPaths, get_paths, get_user_paths

ASSETS_PKG = "spectra_db_assets"

_DB_FILENAME_BY_PROFILE: dict[str, str] = {
    "atomic": "spectra.duckdb",
    "molecular": "spectra_molecular.duckdb",
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
    """
    Where to install DB assets:

    - If SPECTRA_DB_DATA_DIR is set and prefer_env=True -> install there (user explicitly requested).
    - Otherwise -> install into per-user data dir.
    """
    p = get_paths()
    if prefer_env and p.source == "env":
        return p
    return get_user_paths()


def ensure_profile_db_installed(*, profile: str, dest_db_path: Path) -> None:
    """
    Ensure the DuckDB file for `profile` exists at `dest_db_path`.
    Copies from the installed `spectra-db-assets` wheel if missing.
    """
    if profile not in _DB_FILENAME_BY_PROFILE:
        raise ValueError(f"Unknown profile: {profile!r}")

    if dest_db_path.exists():
        return

    filename = _DB_FILENAME_BY_PROFILE[profile]

    try:
        pkg_root = resources.files(ASSETS_PKG)
    except Exception as e:
        raise FileNotFoundError(f"Database not found at {dest_db_path} and assets package '{ASSETS_PKG}' is not installed.\nInstall the assets wheel (spectra-db-assets) and try again.") from e

    src = pkg_root / "db" / filename
    if not src.exists():
        raise FileNotFoundError(f"Assets package '{ASSETS_PKG}' is installed but missing db/{filename}.\nReinstall the correct assets wheel.")

    _copy_file_atomic(Path(str(src)), dest_db_path)


def ensure_db_available(*, profile: str) -> Path:
    """
    Ensure the DB for profile is present in the writable install location and return its path.

    This is what enables: install 3 wheels -> query anywhere, no paths.
    """
    paths = _resolve_install_paths(prefer_env=True)
    dest = paths.default_duckdb_path if profile == "atomic" else paths.default_molecular_duckdb_path
    ensure_profile_db_installed(profile=profile, dest_db_path=dest)
    return dest
