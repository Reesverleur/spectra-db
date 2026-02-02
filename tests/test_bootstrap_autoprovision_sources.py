from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

import spectra_db.cli as cli


def _write_atomic_minimal_ndjson(norm_dir: Path) -> None:
    norm_dir.mkdir(parents=True, exist_ok=True)

    # Minimal rows required for bootstrap to load something meaningful.
    (norm_dir / "species.ndjson").write_text(
        json.dumps({"species_id": "ASD:H:+0", "formula": "H", "name": "H I", "tags": "atomic"}) + "\n",
        encoding="utf-8",
    )
    (norm_dir / "isotopologues.ndjson").write_text(
        json.dumps({"iso_id": "ASD:H:+0/main", "species_id": "ASD:H:+0"}) + "\n",
        encoding="utf-8",
    )
    (norm_dir / "refs.ndjson").write_text(
        json.dumps({"ref_id": "L1", "url": "https://example.com"}) + "\n",
        encoding="utf-8",
    )
    (norm_dir / "states.ndjson").write_text(
        json.dumps(
            {
                "state_id": "S1",
                "iso_id": "ASD:H:+0/main",
                "state_type": "atomic",
                "configuration": "2p",
                "term": "2P°",
                "j_value": 0.5,
                "energy_value": 1.0,
                "energy_unit": "cm-1",
                "ref_id": "L1",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (norm_dir / "transitions.ndjson").write_text(
        json.dumps(
            {
                "transition_id": "T1",
                "iso_id": "ASD:H:+0/main",
                "quantity_value": 500.0,
                "quantity_unit": "nm",
                "selection_rules": "E1",
                "ref_id": "L1",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    # parameters.ndjson may or may not be used in atomic; include empty file for safety
    (norm_dir / "parameters.ndjson").write_text("", encoding="utf-8")


@pytest.mark.usefixtures("monkeypatch")
def test_cli_bootstrap_autoprovisions_sources(monkeypatch, tmp_path: Path) -> None:
    data_dir = tmp_path / "user_data"
    monkeypatch.setenv("SPECTRA_DB_DATA_DIR", str(data_dir))

    # Monkeypatch sources provisioning to drop minimal NDJSON into the expected location.
    import spectra_db.sources as sources

    def _fake_ensure_sources_available(profile: str, force: bool = False):
        norm_dir = (data_dir / "normalized") if profile == "atomic" else (data_dir / "normalized_molecular")
        _write_atomic_minimal_ndjson(norm_dir) if profile == "atomic" else norm_dir.mkdir(parents=True, exist_ok=True)
        return norm_dir

    monkeypatch.setattr(sources, "ensure_sources_available", _fake_ensure_sources_available)

    # Run bootstrap through CLI
    monkeypatch.setattr(sys, "argv", ["spectra-db", "--profile", "atomic", "bootstrap", "--truncate-all"])
    cli.main()

    db_path = data_dir / "db" / "spectra.duckdb"
    assert db_path.exists()
