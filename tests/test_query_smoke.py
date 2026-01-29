from pathlib import Path

from spectra_db.db.duckdb_store import DuckDBStore
from spectra_db.query.api import QueryAPI


def test_query_api_runs_on_empty_db(tmp_path: Path) -> None:
    db_path = tmp_path / "empty.duckdb"
    store = DuckDBStore(db_path)
    store.init_schema()

    con = store.connect()
    api = QueryAPI(con=con)

    assert api.find_species("CO") == []
