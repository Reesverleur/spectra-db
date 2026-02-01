# src/spectra_db/query/api.py
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from spectra_db.db.duckdb_store import DuckDBStore
from spectra_db.util.paths import get_paths


@dataclass
class QueryAPI:
    """High-level query helpers for the local spectroscopic database."""

    con: duckdb.DuckDBPyConnection
    profile: str = "atomic"

    def find_species(self, text: str, limit: int = 20) -> list[dict[str, Any]]:
        q = """
        SELECT species_id, formula, name, charge, multiplicity, tags
        FROM species
        WHERE lower(formula) LIKE lower(?) OR lower(name) LIKE lower(?)
        ORDER BY formula
        LIMIT ?
        """
        like = f"%{text}%"
        rows = self.con.execute(q, [like, like, limit]).fetchall()
        cols = ["species_id", "formula", "name", "charge", "multiplicity", "tags"]
        return [dict(zip(cols, r, strict=True)) for r in rows]

    def isotopologues_for_species(self, species_id: str) -> list[dict[str, Any]]:
        q = """
        SELECT iso_id, label, mass_amu, abundance, notes
        FROM isotopologues
        WHERE species_id = ?
        ORDER BY label
        """
        rows = self.con.execute(q, [species_id]).fetchall()
        cols = ["iso_id", "label", "mass_amu", "abundance", "notes"]
        return [dict(zip(cols, r, strict=True)) for r in rows]

    def parameters(
        self,
        iso_id: str,
        *,
        name_like: str | None = None,
        model: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        """
        Fetch spectroscopic parameters for an isotopologue.

        Atomic profile returns numeric-focused columns.
        Molecular profile includes optional text/suffix/refs columns (if present in schema_molecular).
        """
        clauses = ["iso_id = ?"]
        args: list[Any] = [iso_id]

        if name_like:
            clauses.append("lower(name) LIKE lower(?)")
            args.append(f"%{name_like}%")
        if model:
            clauses.append("lower(model) = lower(?)")
            args.append(model)

        where = " AND ".join(clauses)

        if self.profile == "molecular":
            q = f"""
            SELECT param_id, model, name,
                   value, unit, uncertainty,
                   text_value, value_suffix, markers_json, ref_ids_json, context_json, raw_text,
                   convention, ref_id, source
            FROM spectroscopic_parameters
            WHERE {where}
            ORDER BY model, name
            LIMIT ?
            """
            cols = [
                "param_id",
                "model",
                "name",
                "value",
                "unit",
                "uncertainty",
                "text_value",
                "value_suffix",
                "markers_json",
                "ref_ids_json",
                "context_json",
                "raw_text",
                "convention",
                "ref_id",
                "source",
            ]
        else:
            q = f"""
            SELECT param_id, model, name, value, unit, uncertainty, convention, ref_id, source
            FROM spectroscopic_parameters
            WHERE {where}
            ORDER BY model, name
            LIMIT ?
            """
            cols = [
                "param_id",
                "model",
                "name",
                "value",
                "unit",
                "uncertainty",
                "convention",
                "ref_id",
                "source",
            ]

        args.append(limit)
        rows = self.con.execute(q, args).fetchall()
        return [dict(zip(cols, r, strict=True)) for r in rows]

    def lines(
        self,
        iso_id: str,
        *,
        unit: str = "nm",
        min_wav: float | None = None,
        max_wav: float | None = None,
        limit: int = 100,
        parse_payload: bool = True,
    ) -> list[dict[str, Any]]:
        if self.profile != "atomic":
            raise ValueError("lines() is only available on the atomic profile for now.")

        clauses = ["t.iso_id = ?", "t.quantity_unit = ?"]
        args: list[Any] = [iso_id, unit]

        if min_wav is not None:
            clauses.append("t.quantity_value >= ?")
            args.append(min_wav)
        if max_wav is not None:
            clauses.append("t.quantity_value <= ?")
            args.append(max_wav)

        where = " AND ".join(clauses)
        q = f"""
        SELECT t.quantity_value, t.quantity_unit, t.quantity_uncertainty,
               t.intensity_json, t.extra_json, t.selection_rules,
               r.url AS ref_url
        FROM transitions t
        LEFT JOIN refs r ON t.ref_id = r.ref_id
        WHERE {where}
        ORDER BY t.quantity_value
        LIMIT ?
        """
        args.append(limit)
        rows = self.con.execute(q, args).fetchall()

        out: list[dict[str, Any]] = []
        for wav, u, unc, intensity_json, extra_json, sel, ref_url in rows:
            rec: dict[str, Any] = {
                "wavelength": wav,
                "unit": u,
                "unc": unc,
                "selection_rules": sel,
                "ref_url": ref_url,
                "extra_json": extra_json,
            }
            if parse_payload and intensity_json:
                try:
                    rec["payload"] = json.loads(intensity_json)
                except Exception:
                    rec["payload"] = {}
            else:
                rec["payload"] = None
            out.append(rec)

        return out

    def atomic_levels(
        self,
        iso_id: str,
        limit: int = 50,
        max_energy: float | None = None,
    ) -> list[dict[str, Any]]:
        if self.profile != "atomic":
            raise ValueError("atomic_levels() is only available on the atomic profile.")

        clauses = ["s.iso_id = ?", "s.state_type = 'atomic'"]
        args: list[Any] = [iso_id]

        if max_energy is not None:
            clauses.append("s.energy_value <= ?")
            args.append(max_energy)

        where = " AND ".join(clauses)
        q = f"""
        SELECT s.state_id, s.configuration, s.term, s.j_value, s.f_value, s.g_value,
               s.lande_g, s.leading_percentages, s.extra_json,
               s.energy_value, s.energy_unit, s.energy_uncertainty,
               r.url AS ref_url
        FROM states s
        LEFT JOIN refs r ON s.ref_id = r.ref_id
        WHERE {where}
        ORDER BY s.energy_value, s.j_value
        LIMIT ?
        """
        args.append(limit)
        rows = self.con.execute(q, args).fetchall()
        cols = [
            "state_id",
            "configuration",
            "term",
            "j_value",
            "f_value",
            "g_value",
            "lande_g",
            "leading_percentages",
            "extra_json",
            "energy_value",
            "energy_unit",
            "energy_uncertainty",
            "ref_url",
        ]
        return [dict(zip(cols, r, strict=True)) for r in rows]


def open_default_api(*, profile: str = "atomic", db_path: Path | None = None) -> QueryAPI:
    """
    Open the default local DB for a profile.

    - atomic: data/spectra.duckdb
    - molecular: data/spectra_molecular.duckdb
    """
    paths = get_paths()
    if db_path is None:
        db_path = paths.default_duckdb_path if profile == "atomic" else paths.default_molecular_duckdb_path

    store = DuckDBStore(db_path=db_path)
    store.init_schema(profile=profile)

    con = store.connect()
    return QueryAPI(con=con, profile=profile)
