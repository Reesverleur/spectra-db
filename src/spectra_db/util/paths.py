from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

try:
    from platformdirs import user_data_dir
except Exception:  # pragma: no cover
    user_data_dir = None  # type: ignore[assignment]


@dataclass(frozen=True)
class RepoPaths:
    """
    Path policy for Spectra-DB.

    source meanings:
      - "repo": running from a repo checkout (pyproject.toml discovered upward from CWD)
      - "env":  user explicitly set SPECTRA_DB_DATA_DIR (data directory root)
      - "user": installed usage with no repo checkout; use per-user data directory
    """

    repo_root: Path
    data_root: Path | None = None
    source: str = "repo"  # "repo" | "env" | "user"

    @property
    def data_dir(self) -> Path:
        return self.data_root if self.data_root is not None else (self.repo_root / "data")

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def normalized_dir(self) -> Path:
        return self.data_dir / "normalized"

    @property
    def normalized_molecular_dir(self) -> Path:
        return self.data_dir / "normalized_molecular"

    @property
    def db_dir(self) -> Path:
        return self.data_dir / "db"

    @property
    def default_duckdb_path(self) -> Path:
        return self.db_dir / "spectra.duckdb"

    @property
    def default_molecular_duckdb_path(self) -> Path:
        return self.db_dir / "spectra_molecular.duckdb"


def get_repo_root() -> Path:
    here = Path.cwd().resolve()
    for p in [here, *here.parents]:
        if (p / "pyproject.toml").exists():
            return p
    return here


def _default_user_data_dir() -> Path:
    if user_data_dir is not None:
        return Path(user_data_dir(appname="spectra-db", appauthor=False)).resolve()
    return (Path.home() / ".local" / "share" / "spectra-db").resolve()


def get_user_paths() -> RepoPaths:
    """
    Always return per-user install paths, ignoring repo checkout detection.
    Used for installing assets/sources into a writable location.
    """
    data_root = _default_user_data_dir()
    return RepoPaths(repo_root=data_root, data_root=data_root, source="user")


def get_paths() -> RepoPaths:
    """
    Return the active path policy:

    1) If SPECTRA_DB_DATA_DIR is set: use that as the *data directory* root (source="env")
    2) Else if repo root discoverable: use <repo_root>/data (source="repo")
    3) Else: use per-user data dir (source="user")
    """
    env = os.environ.get("SPECTRA_DB_DATA_DIR")
    if env:
        data_root = Path(env).expanduser().resolve()
        return RepoPaths(repo_root=data_root, data_root=data_root, source="env")

    repo_root = get_repo_root()
    if (repo_root / "pyproject.toml").exists():
        return RepoPaths(repo_root=repo_root, data_root=None, source="repo")

    return get_user_paths()
