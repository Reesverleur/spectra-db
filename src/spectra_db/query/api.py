# src/spectra_db/query/api.py
from __future__ import annotations

import json
import re
from collections.abc import Iterable, Sequence
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

    _FORMULA_TOKEN_RE = re.compile(r"([A-Z][a-z]?)(\d*)")
    _CHARGE_RE = re.compile(r"([+-]\d*)$")

    @classmethod
    def _reverse_formula_tokens(cls, s: str) -> str | None:
        """
        If s looks like a plain chemical formula (e.g. HF, H2O, HfO, DH+, CO-),
        return a reversed-token version (e.g. FH, OH2, OHf, HD+, OC-).
        Otherwise return None.

        Notes:
        - Tokenization is by element symbol [A-Z][a-z]? with optional integer count.
        - Trailing charge like +, -, +2, -1 is preserved at the end.
        - If the string contains unsupported characters (spaces, commas, etc.), returns None.
        """
        q = (s or "").strip()
        if not q:
            return None

        # Peel off a trailing charge suffix if present
        charge = ""
        m_charge = cls._CHARGE_RE.search(q)
        if m_charge:
            charge = m_charge.group(1)
            q_core = q[: -len(charge)]
        else:
            q_core = q

        # Tokenize the core
        tokens: list[tuple[str, str]] = []
        pos = 0
        for m in cls._FORMULA_TOKEN_RE.finditer(q_core):
            if m.start() != pos:
                # unsupported characters (e.g., parentheses, dots, spaces)
                return None
            el = m.group(1)
            cnt = m.group(2) or ""
            tokens.append((el, cnt))
            pos = m.end()

        if pos != len(q_core):
            return None
        if len(tokens) < 2:
            # No point reversing a single token formula
            return None

        rev = "".join(f"{el}{cnt}" for (el, cnt) in reversed(tokens)) + charge
        if rev.lower() == s.strip().lower():
            return None
        return rev

    def find_species_smart(self, query: str, *, limit: int = 50, include_formula_reversal: bool = True) -> list[dict[str, Any]]:
        """
        Fuzzy species search, but if the query looks like a formula, also search the
        reversed-token formula and merge results.

        This improves cases like:
          HF (user) vs FH (stored)
          HfO vs OHf (stored)
        """
        q = (query or "").strip()
        if not q:
            return []

        primary = self.find_species(q, limit=limit)
        if not include_formula_reversal:
            return primary

        rev = self._reverse_formula_tokens(q)
        if not rev:
            return primary

        secondary = self.find_species(rev, limit=limit)

        # Merge and dedupe by species_id, preserving order (primary first)
        out: list[dict[str, Any]] = []
        seen: set[str] = set()

        for r in primary + secondary:
            sid = r.get("species_id")
            if isinstance(sid, str) and sid:
                if sid in seen:
                    continue
                seen.add(sid)
            out.append(r)

        return out[:limit]

    def find_species_exact(
        self,
        query: str,
        *,
        by: Iterable[str] = ("species_id", "formula", "name"),
        limit: int = 25,
        include_formula_reversal: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Exact-match species search against the `species` table.

        - species_id: exact match
        - formula: case-insensitive exact match (+ optional reversed-token formula)
        - name: case-insensitive exact match

        include_formula_reversal:
            If True, and query looks like a formula, also match reversed token order.
            Example: "HF" matches stored "FH".
        """
        q = (query or "").strip()
        if not q:
            return []

        clauses: list[str] = []
        params: list[Any] = []

        for field in by:
            f = field.lower().strip()

            if f == "species_id":
                clauses.append("SELECT * FROM species WHERE species_id = ?")
                params.append(q)

            elif f == "formula":
                rev = self._reverse_formula_tokens(q) if include_formula_reversal else None
                if rev:
                    clauses.append("SELECT * FROM species WHERE lower(formula) = lower(?) OR lower(formula) = lower(?)")
                    params.extend([q, rev])
                else:
                    clauses.append("SELECT * FROM species WHERE lower(formula) = lower(?)")
                    params.append(q)

            elif f == "name":
                clauses.append("SELECT * FROM species WHERE name IS NOT NULL AND lower(name) = lower(?)")
                params.append(q)

            else:
                raise ValueError(f"Unsupported exact-match field: {field!r}")

        if not clauses:
            return []

        sql = " UNION ".join(clauses) + " LIMIT ?"
        params.append(int(limit))

        return self._fetch_dicts(sql, params)

    def resolve_species_id(
        self,
        query: str,
        *,
        exact_first: bool = True,
        fuzzy_fallback: bool = True,
        fuzzy_limit: int = 25,
        include_formula_reversal: bool = True,
    ) -> str | None:
        """
        Resolve a query to a single best species_id.

        exact_first=True:
          tries exact in priority order: species_id -> formula -> name
          formula matching also considers reversed-token formula if enabled.

        fuzzy_fallback=True:
          if exact match fails, uses fuzzy search.
          If query looks formula-like, fuzzy search also tries reversed-token query via find_species_smart.
        """
        q = (query or "").strip()
        if not q:
            return None

        if exact_first:
            # exact species_id
            rows = self.find_species_exact(q, by=("species_id",), limit=1, include_formula_reversal=include_formula_reversal)
            if rows:
                sid = rows[0].get("species_id")
                if isinstance(sid, str) and sid:
                    return sid

            # exact formula (with reversal)
            rows = self.find_species_exact(q, by=("formula",), limit=1, include_formula_reversal=include_formula_reversal)
            if rows:
                sid = rows[0].get("species_id")
                if isinstance(sid, str) and sid:
                    return sid

            # exact name
            rows = self.find_species_exact(q, by=("name",), limit=1, include_formula_reversal=include_formula_reversal)
            if rows:
                sid = rows[0].get("species_id")
                if isinstance(sid, str) and sid:
                    return sid

        if fuzzy_fallback:
            rows = self.find_species_smart(q, limit=int(fuzzy_limit), include_formula_reversal=include_formula_reversal)
            if rows:
                sid = rows[0].get("species_id")
                if isinstance(sid, str) and sid:
                    return sid

        return None

    def _fetch_dicts(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        """
        Execute a query and return rows as dictionaries using cursor description.
        Works across atomic/molecular profiles even if column order differs.
        """
        cur = self.con.execute(sql, params or [])
        cols = [d[0] for d in cur.description]  # type: ignore[attr-defined]
        rows = cur.fetchall()
        return [dict(zip(cols, r, strict=True)) for r in rows]

    def resolve_species_ids_exact(
        self,
        query: str,
        *,
        by: Iterable[str] = ("species_id", "formula", "name"),
        limit: int = 25,
    ) -> list[str]:
        """
        Return only species_id values for exact matches.
        """
        rows = self.find_species_exact(query, by=by, limit=limit)
        out: list[str] = []
        for r in rows:
            sid = r.get("species_id")
            if isinstance(sid, str) and sid:
                out.append(sid)
        # stable + unique
        seen = set()
        uniq = []
        for sid in out:
            if sid not in seen:
                uniq.append(sid)
                seen.add(sid)
        return uniq

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

    - atomic: data/db/spectra.duckdb
    - molecular: data/db/spectra_molecular.duckdb
    """
    paths = get_paths()
    if db_path is None:
        db_path = paths.default_duckdb_path if profile == "atomic" else paths.default_molecular_duckdb_path

    store = DuckDBStore(db_path=db_path)
    store.init_schema(profile=profile)

    con = store.connect()
    return QueryAPI(con=con, profile=profile)
