# src/spectra_db/util/paths.py
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

    Historically, the project assumed a "repo root" and stored all runtime assets under:
        <repo_root>/data/...

    For an *installed* package, there may be no repo root. In that case, the data directory is chosen by:
      1) SPECTRA_DB_DATA_DIR environment variable (points directly to the data directory)
      2) A platform-appropriate per-user data directory (via platformdirs)

    Backward compatibility:
      - If you pass only repo_root, data_dir defaults to <repo_root>/data (same as before).
      - Tests commonly construct RepoPaths(repo_root=tmp_repo_root); this still works.
    """

    repo_root: Path
    data_root: Path | None = None

    @property
    def data_dir(self) -> Path:
        # If data_root is set, treat it as the *data directory itself*.
        return self.data_root if self.data_root is not None else (self.repo_root / "data")

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def normalized_dir(self) -> Path:
        # Atomic normalized
        return self.data_dir / "normalized"

    @property
    def normalized_molecular_dir(self) -> Path:
        return self.data_dir / "normalized_molecular"

    @property
    def db_dir(self) -> Path:
        return self.data_dir / "db"

    @property
    def default_duckdb_path(self) -> Path:
        # Atomic DB
        return self.db_dir / "spectra.duckdb"

    @property
    def default_molecular_duckdb_path(self) -> Path:
        # Molecular DB
        return self.db_dir / "spectra_molecular.duckdb"


def get_repo_root() -> Path:
    """
    Find repo root by walking upward from current working directory until pyproject.toml is found.

    If not found, returns Path.cwd().
    """
    here = Path.cwd().resolve()
    for p in [here, *here.parents]:
        if (p / "pyproject.toml").exists():
            return p
    return here


def _default_user_data_dir() -> Path:
    """
    Choose a reasonable per-user data directory when running as an installed package.
    """
    if user_data_dir is not None:
        return Path(user_data_dir(appname="spectra-db", appauthor=False)).resolve()
    # Fallback without platformdirs
    return (Path.home() / ".local" / "share" / "spectra-db").resolve()


def get_paths() -> RepoPaths:
    """
    Return the active path policy.

    - If SPECTRA_DB_DATA_DIR is set, it is interpreted as the *data directory* (the directory that contains raw/, normalized/, db/, etc.)
    - Else, if a repo root is discoverable (pyproject.toml upward from cwd), use <repo_root>/data
    - Else, use a user data directory.
    """
    env = os.environ.get("SPECTRA_DB_DATA_DIR")
    if env:
        data_root = Path(env).expanduser().resolve()
        # repo_root is not meaningful in this mode; keep it as data_root for debugging.
        return RepoPaths(repo_root=data_root, data_root=data_root)

    repo_root = get_repo_root()
    if (repo_root / "pyproject.toml").exists():
        return RepoPaths(repo_root=repo_root)

    data_root = _default_user_data_dir()
    return RepoPaths(repo_root=data_root, data_root=data_root)
