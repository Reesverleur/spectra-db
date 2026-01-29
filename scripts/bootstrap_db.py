from __future__ import annotations

import argparse
import json
from pathlib import Path

from spectra_db.db.duckdb_store import DuckDBStore
from spectra_db.util.paths import get_paths


def main() -> None:
    """Build or refresh the local DuckDB database from canonical normalized datasets."""
    paths = get_paths()

    parser = argparse.ArgumentParser(description="Bootstrap DuckDB from data/normalized.")
    parser.add_argument(
        "--db",
        type=Path,
        default=paths.default_duckdb_path,
        help="Path to DuckDB file (default: data/db/spectra.duckdb).",
    )
    parser.add_argument(
        "--normalized",
        type=Path,
        default=paths.normalized_dir,
        help="Path to normalized dataset directory (default: data/normalized).",
    )
    parser.add_argument(
        "--truncate-all",
        action="store_true",
        help="If set, clears existing tables before inserting.",
    )
    args = parser.parse_args()

    store = DuckDBStore(args.db)
    store.init_schema()
    counts = store.bootstrap_from_normalized_dir(args.normalized, truncate_all=args.truncate_all)
    print(json.dumps({"db": str(args.db), "inserted": counts}, indent=2))


if __name__ == "__main__":
    main()
