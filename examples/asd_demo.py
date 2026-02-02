"""
examples/asd_demo.py

Demonstrates the new convenience API for atomic data.

Run from repo root (venv active):

    python examples/asd_demo.py

This script demonstrates:
- atomic levels bundle (ground + N excited levels)
- atomic lines bundle (lines originating from low-lying levels)
"""

from __future__ import annotations

import json

from spectra_db import get_atomic_levels, get_atomic_lines


def main() -> None:
    # Example 1: atomic levels for H I (ground + 1 excited level)
    levels = get_atomic_levels("H I", n_excited=1)
    print("\n=== Atomic levels: H I (n_excited=1) ===")
    print(json.dumps(levels, indent=2, ensure_ascii=False))

    # Example 2: atomic lines for H I (use threshold from ground + 1 excited level)
    lines = get_atomic_lines("H I", n_excited=1, unit="nm", max_lines=200)
    print("\n=== Atomic lines: H I (n_excited=1, max_lines=200) ===")
    # Lines payloads can be large; print only a summary
    summary = {
        "profile": lines["profile"],
        "query": lines["query"],
        "species_id": lines["species_id"],
        "iso_id": lines["iso_id"],
        "n_excited": lines["n_excited"],
        "level_energy_threshold_cm-1": lines["level_energy_threshold_cm-1"],
        "n_lines_returned": len(lines["lines"]),
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
