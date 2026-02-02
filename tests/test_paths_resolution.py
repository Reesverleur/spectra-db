from __future__ import annotations

from pathlib import Path

from spectra_db.util.paths import get_paths


def test_paths_env_override(monkeypatch, tmp_path: Path) -> None:
    d = tmp_path / "my_data"
    monkeypatch.setenv("SPECTRA_DB_DATA_DIR", str(d))
    p = get_paths()
    assert p.source == "env"
    assert p.data_dir == d
    assert p.db_dir == d / "db"
