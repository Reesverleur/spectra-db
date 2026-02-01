from __future__ import annotations

from pathlib import Path

from spectra_db.db.duckdb_store import DuckDBStore
from spectra_db.query.api import QueryAPI


def _init_molecular_db(tmp_path: Path) -> QueryAPI:
    db_path = tmp_path / "molecular.duckdb"
    store = DuckDBStore(db_path)
    # molecular profile provides species.extra_json, but this test doesn’t depend on it;
    # using molecular keeps us consistent with your use-case.
    store.init_schema(profile="molecular")

    con = store.connect()
    return QueryAPI(con=con)


def test_reverse_formula_tokens_basic_and_charge(tmp_path: Path) -> None:
    api = _init_molecular_db(tmp_path)

    # Classmethod on the API class
    rev = api._reverse_formula_tokens("HF")
    assert rev == "FH"

    rev = api._reverse_formula_tokens("DH+")
    assert rev == "HD+"

    rev = api._reverse_formula_tokens("HfO")
    assert rev == "OHf"

    # Non-formula inputs should return None (no reversal)
    assert api._reverse_formula_tokens("Hydrogen fluoride") is None


def test_find_species_exact_matches_reversed_formula(tmp_path: Path) -> None:
    api = _init_molecular_db(tmp_path)

    # Insert HF stored as FH
    api.con.execute(
        "INSERT INTO species(species_id, formula, name, charge, multiplicity, inchi_key, tags, notes, extra_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ["MOL:HF:+0", "FH", "Hydrogen fluoride", 0, None, None, "webbook", None, None],
    )

    # Exact formula query should match even though stored formula is reversed
    rows = api.find_species_exact("HF", by=("formula",), limit=10, include_formula_reversal=True)
    assert rows, "Expected HF to match FH via formula reversal"
    assert rows[0]["species_id"] == "MOL:HF:+0"

    # If reversal is disabled, it should NOT match
    rows2 = api.find_species_exact("HF", by=("formula",), limit=10, include_formula_reversal=False)
    assert rows2 == []


def test_resolve_species_id_prefers_exact_over_fuzzy(tmp_path: Path) -> None:
    api = _init_molecular_db(tmp_path)

    # Two overlapping possibilities: HF (stored as FH) and HfO
    api.con.execute(
        "INSERT INTO species(species_id, formula, name, charge, multiplicity, inchi_key, tags, notes, extra_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ["MOL:HF:+0", "FH", "Hydrogen fluoride", 0, None, None, "webbook", None, None],
    )
    api.con.execute(
        "INSERT INTO species(species_id, formula, name, charge, multiplicity, inchi_key, tags, notes, extra_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ["MOL:HFO:+0", "HfO", "Hafnium oxide", 0, None, None, "webbook", None, None],
    )

    # Monkeypatch fuzzy to be "wrong" on purpose: it returns HfO first
    def fake_find_species(q: str, limit: int = 25):
        return [{"species_id": "MOL:HFO:+0", "formula": "HfO", "name": "Hafnium oxide"}]

    api.find_species = fake_find_species  # type: ignore[assignment]

    # Exact-first should still resolve HF correctly (via reversal)
    sid = api.resolve_species_id("HF", exact_first=True, fuzzy_fallback=True, fuzzy_limit=10, include_formula_reversal=True)
    assert sid == "MOL:HF:+0"
