import json
from pathlib import Path

from spectra_db.db.duckdb_store import DuckDBStore
from spectra_db.query.api import QueryAPI


def test_query_api_levels_and_lines(tmp_path: Path) -> None:
    db_path = tmp_path / "t.duckdb"
    store = DuckDBStore(db_path)
    store.init_schema()

    with store.connect() as con:
        # Minimal refs/species/iso
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
            "'2p','2P°',0.5,NULL,2.0,1.002,NULL,NULL,"
            "82258.919,'cm-1',0.001,'L1',NULL)"
        )

        payload = {"wavenumber_cm-1": 15233.0, "Ei_cm-1": 0.0, "Ek_cm-1": 15233.0}
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
                json.dumps({"SomeExtraCol": "abc"}),
                "E1",
                "L1",
                "NIST_ASD_LINES",
                None,
            ],
        )

        api = QueryAPI(con=con)

        levels = api.atomic_levels("ASD:H:+0/main", limit=10, max_energy=None)
        assert len(levels) == 1
        assert levels[0]["lande_g"] == 1.002
        assert levels[0]["ref_url"] == "https://example.com/ref"

        lines = api.lines("ASD:H:+0/main", unit="nm", min_wav=650, max_wav=660, limit=10, parse_payload=True)
        assert len(lines) == 1
        assert lines[0]["ref_url"] == "https://example.com/ref"

        payload_out = lines[0]["payload"]
        assert isinstance(payload_out, dict)
        assert "wavenumber_cm-1" in payload_out, f"payload keys: {sorted(payload_out.keys())}"
        assert payload_out["wavenumber_cm-1"] == 15233.0

        assert lines[0]["extra_json"] is not None
