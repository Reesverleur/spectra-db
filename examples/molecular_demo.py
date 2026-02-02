"""
examples/molecular_demo.py

Demonstrates the new convenience API for WebBook diatomic constants.

Run from repo root (venv active):

    python examples/molecular_demo.py

This script demonstrates:
- molecular diatomic constants (ground + N excited electronic states)
"""

from __future__ import annotations

import json

from spectra_db import get_diatomic_constants


def main() -> None:
    # Example 1: HF ground electronic state only (n_excited=0)
    hf = get_diatomic_constants("HF", n_excited=0, exact_first=True, include_citations=False)
    print("\n=== Diatomic constants: HF (ground state only) ===")
    print(json.dumps(hf, indent=2, ensure_ascii=False))

    # Example 2: CO ground + 2 excited electronic states (n_excited=2)
    co = get_diatomic_constants("CO", n_excited=2, exact_first=True, include_citations=False)
    print("\n=== Diatomic constants: CO (ground + 2 excited states) ===")
    # Print only a compact view
    compact = {
        "profile": co["profile"],
        "species_id": co["species_id"],
        "iso_id": co["iso_id"],
        "n_excited": co["n_excited"],
        "n_states": len(co["states"]),
        "state_labels": [s["state_label"] for s in co["states"]],
    }
    print(json.dumps(compact, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
