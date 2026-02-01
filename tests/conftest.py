from __future__ import annotations

import sys
from pathlib import Path


def pytest_sessionstart(session) -> None:
    """Ensure repo root and src/ are on sys.path so tests can import local modules."""
    repo_root = Path(__file__).resolve().parents[1]
    src_dir = repo_root / "src"

    # Prefer src/ (uninstalled package) over any globally installed spectra_db.
    for p in [src_dir, repo_root]:
        ps = str(p)
        if ps not in sys.path:
            sys.path.insert(0, ps)
