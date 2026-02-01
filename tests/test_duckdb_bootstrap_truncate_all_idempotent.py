# tests/test_duckdb_bootstrap_truncate_all_idempotent.py
from __future__ import annotations

import json
from pathlib import Path

from spectra_db.db.duckdb_store import DuckDBStore


def _write_ndjson(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n", encoding="utf-8")


def test_bootstrap_truncate_all_is_idempotent_and_aligns_columns(tmp_path: Path) -> None:
    normalized = tmp_path / "normalized"

    # Include extra columns in NDJSON to ensure loader drops unknown fields (alignment behavior).
    _write_ndjson(
        normalized / "refs.ndjson",
        [{"ref_id": "L1", "citation": "c", "doi": None, "url": "https://example.com", "notes": None, "extra_field": "ignored"}],
    )
    _write_ndjson(
        normalized / "species.ndjson",
        [{"species_id": "ASD:H:+0", "formula": "H", "name": "H I", "charge": 0, "multiplicity": None, "inchi_key": None, "tags": "atomic", "notes": None}],
    )
    _write_ndjson(
        normalized / "isotopologues.ndjson",
        [{"iso_id": "ASD:H:+0/main", "species_id": "ASD:H:+0", "label": None, "composition_json": None, "nuclear_spins_json": None, "mass_amu": None, "abundance": None, "notes": None}],
    )
    _write_ndjson(
        normalized / "states.ndjson",
        [
            {
                "state_id": "S1",
                "iso_id": "ASD:H:+0/main",
                "state_type": "atomic",
                "electronic_label": "2p 2P° J=1/2",
                "vibrational_json": None,
                "rotational_json": None,
                "parity": None,
                "configuration": "2p",
                "term": "2P°",
                "j_value": 0.5,
                "f_value": None,
                "g_value": 2.0,
                "lande_g": 1.002,
                "leading_percentages": None,
                "extra_json": None,
                "energy_value": 82258.919,
                "energy_unit": "cm-1",
                "energy_uncertainty": 0.001,
                "ref_id": "L1",
                "notes": None,
            }
        ],
    )
    _write_ndjson(
        normalized / "transitions.ndjson",
        [
            {
                "transition_id": "T1",
                "iso_id": "ASD:H:+0/main",
                "upper_state_id": None,
                "lower_state_id": None,
                "quantity_value": 656.28,
                "quantity_unit": "nm",
                "quantity_uncertainty": 0.001,
                "intensity_json": json.dumps({"wavenumber_cm-1": 15233.0}, ensure_ascii=False),
                "extra_json": json.dumps({"X": "y"}, ensure_ascii=False),
                "selection_rules": "E1",
                "ref_id": "L1",
                "source": "NIST_ASD_LINES",
                "notes": None,
            }
        ],
    )
    _write_ndjson(
        normalized / "parameters.ndjson",
        [
            {
                "param_id": "P1",
                "iso_id": "ASD:H:+0/main",
                "model": "demo",
                "name": "B",
                "value": 1.0,
                "unit": "cm-1",
                "uncertainty": None,
                "context_json": None,
                "convention": None,
                "ref_id": "L1",
                "source": "demo",
                "notes": None,
            }
        ],
    )

    db_path = tmp_path / "spectra.duckdb"
    store = DuckDBStore(db_path)
    store.init_schema()

    # First bootstrap
    counts1 = store.bootstrap_from_normalized_dir(normalized, truncate_all=True)
    assert counts1["refs"] == 1
    assert counts1["species"] == 1
    assert counts1["isotopologues"] == 1
    assert counts1["states"] == 1
    assert counts1["transitions"] == 1
    assert counts1["spectroscopic_parameters"] == 1

    # Second bootstrap should still succeed and remain 1 row each due to truncation.
    counts2 = store.bootstrap_from_normalized_dir(normalized, truncate_all=True)
    assert counts2 == counts1

    with store.connect() as con:
        assert con.execute("SELECT COUNT(*) FROM refs").fetchone()[0] == 1
        assert con.execute("SELECT COUNT(*) FROM species").fetchone()[0] == 1
        assert con.execute("SELECT COUNT(*) FROM isotopologues").fetchone()[0] == 1
        assert con.execute("SELECT COUNT(*) FROM states").fetchone()[0] == 1
        assert con.execute("SELECT COUNT(*) FROM transitions").fetchone()[0] == 1
        assert con.execute("SELECT COUNT(*) FROM spectroscopic_parameters").fetchone()[0] == 1


def test_bootstrap_molecular_refs_allows_missing_ref_type_via_default(tmp_path: Path) -> None:
    normalized = tmp_path / "normalized_molecular"

    # Intentionally omit ref_type to simulate older molecular refs.ndjson.
    _write_ndjson(
        normalized / "refs.ndjson",
        [{"ref_id": "WB:C630080:Dia53", "citation": "Ref 53 citation.", "url": "https://example.com/ref53"}],
    )
    _write_ndjson(
        normalized / "species.ndjson",
        [{"species_id": "MOL:CO:+0", "formula": "CO", "name": "Carbon monoxide", "charge": 0, "multiplicity": None, "inchi_key": None, "tags": "webbook", "notes": None}],
    )
    _write_ndjson(
        normalized / "isotopologues.ndjson",
        [{"iso_id": "ISO:CO:12C16O", "species_id": "MOL:CO:+0", "label": "12C16O", "composition_json": None, "nuclear_spins_json": None, "mass_amu": None, "abundance": None, "notes": None}],
    )
    _write_ndjson(
        normalized / "states.ndjson",
        [
            {
                "state_id": "ST:CO:X",
                "iso_id": "ISO:CO:12C16O",
                "state_type": "molecular",
                "electronic_label": "X1Sigma+",
                "extra_json": json.dumps({"Trans_clean": "A ↔ X R"}, ensure_ascii=False),
                "energy_value": 0.0,
                "energy_unit": "cm-1",
                "energy_uncertainty": None,
                "ref_id": "WB:C630080:Dia53",
                "notes": None,
            }
        ],
    )
    _write_ndjson(
        normalized / "parameters.ndjson",
        [
            {
                "param_id": "P:CO:we",
                "iso_id": "ISO:CO:12C16O",
                "model": "webbook_diatomic_constants",
                "name": "we",
                "value": 2169.813,
                "unit": "cm-1",
                "uncertainty": None,
                "context_json": None,
                "convention": None,
                "ref_id": "WB:C630080:Dia53",
                "source": "webbook:C630080",
                "notes": None,
            }
        ],
    )

    db_path = tmp_path / "spectra_molecular.duckdb"
    store = DuckDBStore(db_path)

    counts = store.bootstrap_from_normalized_dir(normalized, truncate_all=True, profile="molecular")
    assert counts["refs"] == 1
    assert counts["species"] == 1
    assert counts["isotopologues"] == 1
    assert counts["states"] == 1
    assert counts["spectroscopic_parameters"] == 1

    with store.connect() as con:
        # The default should have filled ref_type
        ref_type = con.execute("SELECT ref_type FROM refs WHERE ref_id = ?", ["WB:C630080:Dia53"]).fetchone()[0]
        assert ref_type == "unknown"
