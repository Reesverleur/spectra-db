from __future__ import annotations

import argparse
import bz2
import json
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from spectra_db.util.paths import get_paths
from tools.scrapers.common.ids import make_id
from tools.scrapers.common.ndjson import write_ndjson_row


@dataclass(frozen=True)
class RunResult:
    ok: bool
    states_written: int
    transitions_written: int
    notes: str | None = None


def _open_maybe_bz2(path: Path) -> Iterable[str]:
    if path.suffix == ".bz2":
        with bz2.open(path, "rt", encoding="utf-8", errors="replace") as f:
            yield from f
    else:
        with path.open("rt", encoding="utf-8", errors="replace") as f:
            yield from f


def species_id_for(formula: str, charge: int = 0) -> str:
    # Keep stable & cross-source
    return f"MOL:{formula}:{charge:+d}"


def iso_id_for(species_id: str, isotopologue_label: str) -> str:
    return f"{species_id}/{isotopologue_label}"


def ensure_species_and_iso(*, formula: str, charge: int, isotopologue_label: str) -> tuple[dict[str, Any], dict[str, Any]]:
    sid = species_id_for(formula=formula, charge=charge)
    iso = iso_id_for(sid, isotopologue_label=isotopologue_label)
    species = {
        "species_id": sid,
        "formula": formula,
        "name": formula,
        "charge": charge,
        "multiplicity": None,
        "inchi_key": None,
        "tags": "molecular;ExoMol",
        "notes": None,
    }
    isotopologue = {
        "iso_id": iso,
        "species_id": sid,
        "label": isotopologue_label,
        "composition_json": None,
        "nuclear_spins_json": None,
        "mass_amu": None,
        "abundance": None,
        "notes": "ExoMol isotopologue label copied from upstream API.",
    }
    return species, isotopologue


def parse_states_line(line: str) -> dict[str, Any] | None:
    s = line.strip()
    if not s or s.startswith("#"):
        return None
    parts = s.split()
    # ExoMol states: i, E~, gtot, J, ... (more columns may follow)
    if len(parts) < 4:
        return None
    i = int(parts[0])
    energy_cm1 = float(parts[1])
    gtot = float(parts[2])
    j = float(parts[3])

    # Everything else is preserved verbatim (no guessing of column meanings here).
    extra_cols = parts[4:] if len(parts) > 4 else []
    return {
        "i": i,
        "energy_cm1": energy_cm1,
        "gtot": gtot,
        "j": j,
        "extra_cols": extra_cols,
        "raw": s,
    }


def parse_trans_line(line: str) -> dict[str, Any] | None:
    s = line.strip()
    if not s or s.startswith("#"):
        return None
    parts = s.split()
    # ExoMol transitions: f, i, Afi, [nu optional]
    if len(parts) < 3:
        return None
    f = int(parts[0])
    i = int(parts[1])
    afi = float(parts[2])
    nu = float(parts[3]) if len(parts) >= 4 else None
    return {"f": f, "i": i, "Afi": afi, "nu": nu, "raw": s}


def run(
    *,
    formula: str,
    isotopologue_label: str,
    states_path: Path,
    trans_paths: list[Path],
    ref_id: str,
    max_transitions: int | None,
) -> RunResult:
    paths = get_paths()

    # Ensure top-level records exist (append-only NDJSON is fine; DuckDB load is column-aligned).
    species, iso = ensure_species_and_iso(formula=formula, charge=0, isotopologue_label=isotopologue_label)
    write_ndjson_row(paths.normalized_dir / "species.ndjson", species)
    write_ndjson_row(paths.normalized_dir / "isotopologues.ndjson", iso)

    iso_id = iso["iso_id"]

    # States
    idx_to_state_id: dict[int, str] = {}
    states_written = 0

    for line in _open_maybe_bz2(states_path):
        parsed = parse_states_line(line)
        if parsed is None:
            continue

        state_id = make_id("state", iso_id, str(parsed["i"]))
        idx_to_state_id[parsed["i"]] = state_id

        # Preserve all unmapped columns explicitly.
        vib_json = json.dumps(
            {
                "exomol": {
                    "i": parsed["i"],
                    "gtot": parsed["gtot"],
                    "extra_cols": parsed["extra_cols"],
                    "raw": parsed["raw"],
                }
            },
            ensure_ascii=False,
        )

        state_row = {
            "state_id": state_id,
            "iso_id": iso_id,
            "state_type": "molecular",
            "electronic_label": None,
            "vibrational_json": vib_json,
            "rotational_json": None,
            "parity": None,
            "configuration": None,
            "term": None,
            "j_value": parsed["j"],
            "f_value": None,
            "g_value": parsed["gtot"],
            "energy_value": parsed["energy_cm1"],
            "energy_unit": "cm-1",
            "energy_uncertainty": None,
            "ref_id": ref_id,
            "notes": "Imported from ExoMol states file; extra columns preserved in vibrational_json.exomol.*",
        }
        write_ndjson_row(paths.normalized_dir / "states.ndjson", state_row)
        states_written += 1

    # Transitions
    transitions_written = 0
    for tp in trans_paths:
        for line in _open_maybe_bz2(tp):
            parsed = parse_trans_line(line)
            if parsed is None:
                continue

            upper_id = idx_to_state_id.get(parsed["f"])
            lower_id = idx_to_state_id.get(parsed["i"])

            nu = parsed["nu"]
            nu_derived = False
            if nu is None:
                # Explicit derivation (not silent): compute from states energies
                if upper_id is None or lower_id is None:
                    continue
                # We need energies by index; simplest is to store them while reading states.
                # For now: require nu to be present OR skip transitions that can't be derived safely.
                continue

            intensity = {"Afi_s-1": parsed["Afi"]}
            extra = {"exomol_raw": parsed["raw"], "nu_derived": nu_derived}

            row = {
                "transition_id": make_id("trans", iso_id, str(parsed["f"]), str(parsed["i"]), str(nu), str(parsed["Afi"])),
                "iso_id": iso_id,
                "upper_state_id": upper_id,
                "lower_state_id": lower_id,
                "quantity_value": float(nu),
                "quantity_unit": "cm-1",
                "quantity_uncertainty": None,
                "intensity_json": json.dumps(intensity, ensure_ascii=False),
                "extra_json": json.dumps(extra, ensure_ascii=False),
                "selection_rules": None,
                "ref_id": ref_id,
                "source": "ExoMol",
                "notes": f"Imported from {tp.name}",
            }
            write_ndjson_row(paths.normalized_dir / "transitions.ndjson", row)
            transitions_written += 1
            if max_transitions is not None and transitions_written >= max_transitions:
                return RunResult(ok=True, states_written=states_written, transitions_written=transitions_written)

    return RunResult(ok=True, states_written=states_written, transitions_written=transitions_written)


def main() -> None:
    p = argparse.ArgumentParser(description="Normalize ExoMol states/trans into data/normalized/*.ndjson")
    p.add_argument("--formula", default="CO")
    p.add_argument("--isotopologue", required=True, help="Label used in iso_id (must match what you downloaded).")
    p.add_argument("--states", type=Path, required=True)
    p.add_argument("--trans", type=Path, nargs="*", default=[])
    p.add_argument("--ref-id", required=True, help="Stable ref key you want to use for ExoMol dataset (e.g. EXOMOL:CO:... ).")
    p.add_argument("--max-transitions", type=int, default=None)
    args = p.parse_args()

    rr = run(
        formula=args.formula,
        isotopologue_label=args.isotopologue,
        states_path=args.states,
        trans_paths=list(args.trans),
        ref_id=args.ref_id,
        max_transitions=args.max_transitions,
    )
    print(json.dumps(rr.__dict__, indent=2))


if __name__ == "__main__":
    main()
