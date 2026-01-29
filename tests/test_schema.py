from pathlib import Path

from spectra_db.db.duckdb_store import DuckDBStore


def test_schema_initializes(tmp_path: Path) -> None:
    db_path = tmp_path / "test.duckdb"
    store = DuckDBStore(db_path)
    store.init_schema()

    with store.connect() as con:
        tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    assert "species" in tables
    assert "spectroscopic_parameters" in tables
    assert "transitions" in tables
