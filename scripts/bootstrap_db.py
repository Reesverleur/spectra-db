# scripts/bootstrap_db.py
from __future__ import annotations

import argparse
from pathlib import Path

from spectra_db.db.duckdb_store import DuckDBStore
from spectra_db.util.paths import get_paths


def main() -> None:
    ap = argparse.ArgumentParser(description="Bootstrap Spectra DB from normalized NDJSON.")
    ap.add_argument(
        "--profile",
        choices=["atomic", "molecular"],
        default="atomic",
        help="Which DB profile to bootstrap.",
    )
    ap.add_argument(
        "--normalized",
        type=Path,
        default=None,
        help="Override normalized directory. Defaults depend on profile.",
    )
    ap.add_argument(
        "--db-path",
        type=Path,
        default=None,
        help="Override output DuckDB path. Defaults depend on profile.",
    )
    ap.add_argument("--truncate-all", action="store_true", help="Delete existing rows before loading.")

    args = ap.parse_args()
    paths = get_paths()

    norm_dir = args.normalized
    if norm_dir is None:
        norm_dir = paths.normalized_dir if args.profile == "atomic" else paths.normalized_molecular_dir

    db_path = args.db_path
    if db_path is None:
        db_path = paths.default_duckdb_path if args.profile == "atomic" else paths.default_molecular_duckdb_path

    store = DuckDBStore(db_path=db_path)
    counts = store.bootstrap_from_normalized_dir(norm_dir, truncate_all=args.truncate_all, profile=args.profile)

    print(f"Bootstrapped profile={args.profile}")
    for k, v in counts.items():
        print(f"{k:26} {v:8}")


if __name__ == "__main__":
    main()
