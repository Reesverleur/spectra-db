# tests/test_cli_query_flags.py
from __future__ import annotations

import json
import sys
from pathlib import Path

import scripts.query as cli
from spectra_db.db.duckdb_store import DuckDBStore
from spectra_db.query.api import QueryAPI


def _install_minimal_fixture_db(tmp_path: Path) -> QueryAPI:
    db_path = tmp_path / "t.duckdb"
    store = DuckDBStore(db_path)
    store.init_schema()

    con = store.connect()
    con.execute("INSERT INTO refs(ref_id, citation, doi, url, notes) VALUES ('L1','c',NULL,'https://example.com/ref','n')")
    con.execute("INSERT INTO species(species_id, formula, name, charge, multiplicity, inchi_key, tags, notes) VALUES ('ASD:H:+0','H','H I',0,NULL,NULL,'atomic',NULL)")
    con.execute(
        "INSERT INTO isotopologues(iso_id, species_id, label, composition_json, nuclear_spins_json, mass_amu, abundance, notes) VALUES ('ASD:H:+0/main','ASD:H:+0',NULL,NULL,NULL,NULL,NULL,NULL)"
    )

    con.execute(
        "INSERT INTO states(state_id, iso_id, state_type, electronic_label, vibrational_json, rotational_json, parity,"
        "configuration, term, j_value, f_value, g_value, lande_g, leading_percentages, extra_json,"
        "energy_value, energy_unit, energy_uncertainty, ref_id, notes) VALUES "
        "('S1','ASD:H:+0/main','atomic','2p 2P° J=1/2',NULL,NULL,NULL,"
        "'2p','2P°',0.5,NULL,2.0,1.002,NULL,?,"
        "82258.919,'cm-1',0.001,'L1',NULL)",
        [json.dumps({"ref_urls": ["https://example.com/ref", "https://example.com/ref2"]})],
    )

    payload = {
        "observed_wavelength": 656.28,
        "observed_wavelength_unc": 0.001,
        "ritz_wavelength": 656.2799,
        "ritz_wavelength_unc": 0.002,
        "Ei_cm-1": 0.0,
        "Ek_cm-1": 15233.0,
        "lower": {"configuration": "2p", "term": "2P° odd", "J": "1/2"},
        "upper": {"configuration": "3d", "term": "2D even", "J": "3/2"},
        "type": "E1",
        "tp_ref_urls": ["https://example.com/tp"],
        "line_ref_urls": ["https://example.com/line"],
    }
    con.execute(
        "INSERT INTO transitions(transition_id, iso_id, upper_state_id, lower_state_id, quantity_value, quantity_unit,"
        "quantity_uncertainty, intensity_json, extra_json, selection_rules, ref_id, source, notes) VALUES "
        "(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            "T1",
            "ASD:H:+0/main",
            None,
            None,
            656.28,
            "nm",
            0.001,
            json.dumps(payload),
            None,
            "E1",
            "L1",
            "NIST_ASD_LINES",
            None,
        ],
    )

    return QueryAPI(con=con)


def _parse_header_cols(line: str) -> list[str]:
    # robust against substring collisions ("g" in "Energy")
    parts = [p.strip() for p in line.split("|")]
    return [p for p in parts if p]


def test_cli_levels_flags(monkeypatch, tmp_path: Path, capsys) -> None:
    api = _install_minimal_fixture_db(tmp_path)
    monkeypatch.setattr(cli, "open_default_api", lambda *args, **kwargs: api)

    monkeypatch.setattr(sys, "argv", ["query.py", "levels", "H I", "--limit", "5", "--no-refs"])
    cli.main()
    out = capsys.readouterr().out
    assert "Ref URL" not in out
    assert "Energy" in out
    assert "J" in out
    assert "g" in out

    monkeypatch.setattr(sys, "argv", ["query.py", "levels", "H I", "--columns", "Energy,J,g,RefURL"])
    cli.main()
    out2 = capsys.readouterr().out

    header_line = next(line for line in out2.splitlines() if "Energy" in line and "Ref URL" in line)
    cols = _parse_header_cols(header_line)
    assert cols[:4] == ["Energy", "J", "g", "Ref URL"]


def test_cli_lines_flags(monkeypatch, tmp_path: Path, capsys) -> None:
    api = _install_minimal_fixture_db(tmp_path)
    monkeypatch.setattr(cli, "open_default_api", lambda *args, **kwargs: api)

    monkeypatch.setattr(sys, "argv", ["query.py", "lines", "H I", "--limit", "5", "--no-refs"])
    cli.main()
    out = capsys.readouterr().out
    assert "TP Ref URL" not in out
    assert "Line Ref URL" not in out
    assert "Observed λ" in out

    monkeypatch.setattr(sys, "argv", ["query.py", "lines", "H I", "--columns", "Obs,Lower,Upper,Type,LineRefURL"])
    cli.main()
    out2 = capsys.readouterr().out
    header_line = next(line for line in out2.splitlines() if "Observed" in line and "Line Ref URL" in line)
    assert header_line.index("Observed") < header_line.index("Lower") < header_line.index("Upper") < header_line.index("Type") < header_line.index("Line Ref URL")
