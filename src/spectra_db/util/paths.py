from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RepoPaths:
    """Convenience paths relative to the repository root.

    This class assumes the repository layout described in README.md.
    """

    repo_root: Path

    @property
    def data_dir(self) -> Path:
        """Top-level data directory."""
        return self.repo_root / "data"

    @property
    def raw_dir(self) -> Path:
        """Raw snapshot storage directory."""
        return self.data_dir / "raw"

    @property
    def normalized_dir(self) -> Path:
        """Canonical editable dataset directory."""
        return self.data_dir / "normalized"

    @property
    def db_dir(self) -> Path:
        """Database artifact directory."""
        return self.data_dir / "db"

    @property
    def default_duckdb_path(self) -> Path:
        """Default DuckDB path."""
        return self.db_dir / "spectra.duckdb"


def find_repo_root(start: Path | None = None) -> Path:
    """Find the repository root by walking upward until pyproject.toml is found.

    Args:
        start: Starting path; defaults to this file's location.

    Returns:
        Path to repository root.

    Raises:
        FileNotFoundError: If pyproject.toml is not found.
    """
    here = (start or Path(__file__)).resolve()
    for p in [here, *here.parents]:
        if (p / "pyproject.toml").exists():
            return p
    raise FileNotFoundError("Could not find repo root (pyproject.toml not found).")


def get_paths() -> RepoPaths:
    """Get RepoPaths anchored at the repository root."""
    return RepoPaths(repo_root=find_repo_root())


if __name__ == "__main__":
    paths = get_paths()
    print("Repo root:", paths.repo_root)
    print("Normalized:", paths.normalized_dir)
    print("DB:", paths.default_duckdb_path)
