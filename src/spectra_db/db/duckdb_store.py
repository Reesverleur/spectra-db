from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd

from spectra_db.util.paths import get_paths


@dataclass
class DuckDBStore:
    """DuckDB-backed store for spectra_db."""

    db_path: Path

    def connect(self) -> duckdb.DuckDBPyConnection:
        """Open a DuckDB connection."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(str(self.db_path))

    def init_schema(self) -> None:
        """Create tables if they do not exist, using schema.sql."""
        schema_path = Path(__file__).with_name("schema.sql")
        sql = schema_path.read_text(encoding="utf-8")
        with self.connect() as con:
            con.execute(sql)
            con.execute(
                "INSERT OR REPLACE INTO meta_info(key, value) VALUES (?, ?)",
                ("schema_version", "0"),
            )

    @staticmethod
    def _read_ndjson(path: Path) -> pd.DataFrame:
        """Read a NDJSON file (one JSON object per line) into a DataFrame."""
        if not path.exists():
            raise FileNotFoundError(str(path))
        if path.stat().st_size == 0:
            return pd.DataFrame()
        return pd.read_json(path, lines=True)

    def load_table_from_ndjson(self, table_name: str, ndjson_path: Path, *, truncate: bool = False) -> int:
        """Load rows from NDJSON into a DuckDB table, aligning columns.

        - Adds missing columns with None
        - Drops extra columns not in the table
        - Reorders columns to match the table
        """
        df = self._read_ndjson(ndjson_path)
        if df.empty:
            return 0

        with self.connect() as con:
            if truncate:
                con.execute(f"DELETE FROM {table_name}")

            info = con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            table_cols = [row[1] for row in info]

            for c in table_cols:
                if c not in df.columns:
                    df[c] = None

            df = df[table_cols]

            con.register("incoming_df", df)
            con.execute(f"INSERT INTO {table_name} SELECT * FROM incoming_df")

        return int(len(df))

    def bootstrap_from_normalized_dir(self, normalized_dir: Path, *, truncate_all: bool = False) -> dict[str, int]:
        """Load all canonical datasets from data/normalized into DuckDB."""
        mapping = {
            "refs": normalized_dir / "refs.ndjson",
            "species": normalized_dir / "species.ndjson",
            "isotopologues": normalized_dir / "isotopologues.ndjson",
            "states": normalized_dir / "states.ndjson",
            "transitions": normalized_dir / "transitions.ndjson",
            "spectroscopic_parameters": normalized_dir / "parameters.ndjson",
        }

        results: dict[str, int] = {}

        with self.connect() as con:
            if truncate_all:
                con.execute("DELETE FROM spectroscopic_parameters")
                con.execute("DELETE FROM transitions")
                con.execute("DELETE FROM states")
                con.execute("DELETE FROM isotopologues")
                con.execute("DELETE FROM species")
                con.execute("DELETE FROM refs")

        for table, path in mapping.items():
            if path.exists():
                results[table] = self.load_table_from_ndjson(table, path, truncate=False)
            else:
                results[table] = 0

        return results


if __name__ == "__main__":
    paths = get_paths()
    store = DuckDBStore(paths.default_duckdb_path)
    store.init_schema()
    counts = store.bootstrap_from_normalized_dir(paths.normalized_dir, truncate_all=False)
    print("Bootstrap counts:", json.dumps(counts, indent=2))
