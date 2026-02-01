# src/spectra_db/db/duckdb_store.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb


def _qident(name: str) -> str:
    """Quote an identifier for DuckDB SQL (table/column names)."""
    return '"' + name.replace('"', '""') + '"'


def _pragma_table_info_sql(table_name: str) -> str:
    """Build a PRAGMA table_info(...) statement for a table name."""
    safe = table_name.replace("'", "''")
    return f"PRAGMA table_info('{safe}')"


@dataclass
class DuckDBStore:
    db_path: Path

    def connect(self) -> duckdb.DuckDBPyConnection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(str(self.db_path))

    def _table_columns(self, con: duckdb.DuckDBPyConnection, table_name: str) -> list[str]:
        rows = con.execute(_pragma_table_info_sql(table_name)).fetchall()
        # (cid, name, type, notnull, dflt_value, pk)
        return [r[1] for r in rows]

    def _ensure_columns(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        columns: list[tuple[str, str]],
    ) -> None:
        """Best-effort schema migration: add missing columns (no drops / type changes)."""
        existing = set(self._table_columns(con, table_name))
        for col_name, col_type in columns:
            if col_name in existing:
                continue
            try:
                con.execute(f"ALTER TABLE {_qident(table_name)} ADD COLUMN {_qident(col_name)} {col_type}")
            except Exception:
                # DuckDB versions differ on IF NOT EXISTS support; keep best-effort.
                pass

    def init_schema(self, *, profile: str = "atomic") -> None:
        """Initialize database schema.

        profile:
          - "atomic": uses schema.sql (existing ASD schema)
          - "molecular": uses schema_molecular.sql (WebBook/ExoMol-friendly schema)
        """
        schema_dir = Path(__file__).resolve().parent

        if profile == "atomic":
            schema_path = schema_dir / "schema.sql"
        elif profile == "molecular":
            schema_path = schema_dir / "schema_molecular.sql"
        else:
            raise ValueError(f"Unknown profile: {profile}")

        sql = schema_path.read_text(encoding="utf-8")

        with self.connect() as con:
            con.execute(sql)

            # Minimal, backwards-compatible migrations for older dev DB files.
            if profile == "molecular":
                self._ensure_columns(
                    con,
                    "refs",
                    [
                        ("ref_type", "TEXT"),
                        ("citation", "TEXT"),
                        ("doi", "TEXT"),
                        ("url", "TEXT"),
                        ("notes", "TEXT"),
                    ],
                )

                # Ensure ref_type has a safe default even if the DB was created before we added it.
                try:
                    con.execute("ALTER TABLE refs ALTER COLUMN ref_type SET DEFAULT 'unknown'")
                except Exception:
                    pass
                try:
                    con.execute("UPDATE refs SET ref_type = 'unknown' WHERE ref_type IS NULL")
                except Exception:
                    pass

                self._ensure_columns(
                    con,
                    "states",
                    [
                        ("electronic_label", "TEXT"),
                        ("vibrational_json", "TEXT"),
                        ("rotational_json", "TEXT"),
                        ("parity", "TEXT"),
                        ("configuration", "TEXT"),
                        ("term", "TEXT"),
                        ("j_value", "DOUBLE"),
                        ("f_value", "DOUBLE"),
                        ("g_value", "DOUBLE"),
                        ("lande_g", "DOUBLE"),
                        ("leading_percentages", "TEXT"),
                        ("extra_json", "TEXT"),
                        ("energy_value", "DOUBLE"),
                        ("energy_unit", "TEXT"),
                        ("energy_uncertainty", "DOUBLE"),
                        ("ref_id", "TEXT"),
                        ("notes", "TEXT"),
                    ],
                )

            # record schema version marker per-profile (optional but helpful)
            # (meta_info exists in both schemas)
            key = f"schema_profile:{profile}"
            con.execute(
                "INSERT OR REPLACE INTO meta_info(key, value) VALUES (?, ?)",
                [key, "1"],
            )

    def load_table_from_ndjson(self, table_name: str, ndjson_path: Path, *, truncate: bool) -> int:
        """Load one NDJSON file into an existing table.

        Key behavior (critical for molecular evolution):
        - Insert aligns columns by NAME, not by position
        - NDJSON extra columns are ignored
        - Missing table columns are left NULL/DEFAULT

        This keeps atomic stable while allowing molecular schemas to diverge.
        """
        if not ndjson_path.exists():
            return 0

        with self.connect() as con:
            df = con.execute(
                "SELECT * FROM read_ndjson_auto(?)",
                [str(ndjson_path)],
            ).fetchdf()

            if len(df) == 0:
                return 0

            if truncate:
                con.execute(f"DELETE FROM {_qident(table_name)}")

            table_cols = self._table_columns(con, table_name)
            df_cols = list(df.columns)

            common = [c for c in df_cols if c in table_cols]
            if not common:
                raise ValueError(f"No matching columns between NDJSON {ndjson_path.name} ({df_cols}) and table {table_name} ({table_cols})")

            con.register("incoming_df", df)
            cols_sql = ", ".join(_qident(c) for c in common)
            con.execute(f"INSERT INTO {_qident(table_name)} ({cols_sql}) SELECT {cols_sql} FROM incoming_df")
            return len(df)

    def bootstrap_from_normalized_dir(
        self,
        normalized_dir: Path,
        *,
        truncate_all: bool = False,
        profile: str = "atomic",
    ) -> dict[str, int]:
        """Load NDJSON tables from a normalized directory into DuckDB."""
        self.init_schema(profile=profile)

        mapping: list[tuple[str, str]] = [
            ("species", "species.ndjson"),
            ("isotopologues", "isotopologues.ndjson"),
            ("refs", "refs.ndjson"),
            ("states", "states.ndjson"),
            ("transitions", "transitions.ndjson"),
            ("spectroscopic_parameters", "parameters.ndjson"),
        ]

        results: dict[str, int] = {}

        if truncate_all:
            # truncate in reverse dependency order
            with self.connect() as con:
                for t in ["spectroscopic_parameters", "transitions", "states", "refs", "isotopologues", "species"]:
                    con.execute(f"DELETE FROM {_qident(t)}")

        for table, fname in mapping:
            path = normalized_dir / fname
            results[table] = self.load_table_from_ndjson(table, path, truncate=False)

        return results
