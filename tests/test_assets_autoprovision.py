from __future__ import annotations

from pathlib import Path

import pytest

from spectra_db.db.duckdb_store import DuckDBStore
from spectra_db.query import open_default_api


@pytest.mark.usefixtures("monkeypatch")
def test_open_default_api_autoprovisions_db(monkeypatch, tmp_path: Path) -> None:
    # Force installed/user-like mode via env override
    data_dir = tmp_path / "user_data"
    monkeypatch.setenv("SPECTRA_DB_DATA_DIR", str(data_dir))

    # Monkeypatch the asset copier to create a tiny DB at the expected location.
    # This avoids needing the real assets wheel during unit tests.
    def _fake_ensure_db_available(profile: str) -> Path:
        db_path = data_dir / "db" / ("spectra.duckdb" if profile == "atomic" else "spectra_molecular.duckdb")
        store = DuckDBStore(db_path)
        store.init_schema(profile=profile)
        return db_path

    import spectra_db.assets as assets

    monkeypatch.setattr(assets, "ensure_db_available", _fake_ensure_db_available)

    # Should succeed and return a QueryAPI using the newly created DB
    api = open_default_api(profile="atomic", read_only=True, ensure_schema=False)
    rows = api.find_species("H", limit=5)  # empty DB is fine; just ensures connection works
    assert isinstance(rows, list)
