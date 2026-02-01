# src/spectra_db/util/paths.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RepoPaths:
    repo_root: Path

    @property
    def data_dir(self) -> Path:
        return self.repo_root / "data"

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def normalized_dir(self) -> Path:
        # Atomic normalized
        return self.data_dir / "normalized"

    @property
    def normalized_molecular_dir(self) -> Path:
        # Molecular normalized (WebBook / ExoMol / etc.)
        return self.data_dir / "normalized_molecular"

    @property
    def db_dir(self) -> Path:
        # Atomic DB
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
    """
    here = Path.cwd().resolve()
    for p in [here, *here.parents]:
        if (p / "pyproject.toml").exists():
            return p
    # fallback: cwd
    return here


def get_paths() -> RepoPaths:
    return RepoPaths(repo_root=get_repo_root())
