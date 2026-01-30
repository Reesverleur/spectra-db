from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from tools.scrapers.common.ids import make_id

# Roman numeral parser supporting large values (e.g. LXVII, XXXVII, etc.)
_ROMAN_VALUES = {"I": 1, "V": 5, "X": 10, "L": 50, "C": 100, "D": 500, "M": 1000}


@dataclass(frozen=True)
class ParsedSpectrum:
    """Parsed atomic spectrum designation.

    Example:
        label "Fe II" -> element "Fe", charge 1, asd_label "Fe II"
        label "Po LXVII" -> element "Po", charge 66, asd_label "Po LXVII"
        label "Ar 15+" -> element "Ar", charge 15, asd_label "Ar 15+"
    """

    element: str
    charge: int
    asd_label: str


def roman_to_int(s: str) -> int:
    """Convert a Roman numeral to an integer (supports ASD ranges beyond XX).

    Args:
        s: Roman numeral string (e.g. "XXI", "LXVII").

    Returns:
        Integer value.

    Raises:
        ValueError: If the string contains invalid Roman characters.
    """
    s = s.strip().upper()
    if not s or any(ch not in _ROMAN_VALUES for ch in s):
        raise ValueError(f"Unsupported roman numeral: {s!r}")

    total = 0
    prev = 0
    for ch in reversed(s):
        val = _ROMAN_VALUES[ch]
        if val < prev:
            total -= val
        else:
            total += val
            prev = val
    return total


def parse_spectrum_label(label: str) -> ParsedSpectrum:
    """Parse an ASD spectrum label into element + charge.

    Supports:
      - "Fe I", "Fe II", "Po LXVII"
      - "Ar 15+"

    Returns:
        ParsedSpectrum.

    Raises:
        ValueError: If format is not recognized.
    """
    s = " ".join(label.strip().split()).replace("\u00a0", " ")

    # Ar 15+
    m = re.match(r"^([A-Za-z]{1,2})\s+(\d+)\+$", s)
    if m:
        el = m.group(1).capitalize()
        ch = int(m.group(2))
        return ParsedSpectrum(element=el, charge=ch, asd_label=f"{el} {ch}+")

    # Fe II / Po LXVII
    m = re.match(r"^([A-Za-z]{1,2})\s+([IVXLCDMivxlcdm]+)$", s)
    if m:
        el = m.group(1).capitalize()
        stage = roman_to_int(m.group(2))  # I -> 1, II -> 2, ...
        ch = stage - 1
        return ParsedSpectrum(element=el, charge=ch, asd_label=f"{el} {m.group(2).upper()}")

    raise ValueError(f"Unrecognized spectrum label format: {label!r}")


def species_id_for(ps: ParsedSpectrum) -> str:
    """Create a stable internal species_id for an atomic ion stage."""
    return f"ASD:{ps.element}:{ps.charge:+d}"


def iso_id_for(species_id: str) -> str:
    """Create a default isotopologue ID for an atomic ion stage."""
    return f"{species_id}/main"


def make_species_record(ps: ParsedSpectrum) -> dict[str, Any]:
    """Create a canonical 'species' record for this atomic spectrum."""
    sid = species_id_for(ps)
    return {
        "species_id": sid,
        "formula": ps.element,
        "name": ps.asd_label,
        "charge": ps.charge,
        "multiplicity": None,
        "inchi_key": None,
        "tags": "atomic;NIST_ASD",
        "notes": None,
    }


def make_isotopologue_record(species_id: str) -> dict[str, Any]:
    """Create a placeholder isotopologue record for an atomic ion stage."""
    return {
        "iso_id": iso_id_for(species_id),
        "species_id": species_id,
        "label": None,
        "composition_json": None,
        "nuclear_spins_json": None,
        "mass_amu": None,
        "abundance": None,
        "notes": "Default placeholder isotopologue for atomic ion (no isotope specificity yet).",
    }


def parse_quantum_number(x: str | None) -> float | None:
    """Parse quantum numbers like '3/2' or '2' into float."""
    if x is None:
        return None
    s = str(x).strip()
    if not s or s.lower() == "nan":
        return None
    if "/" in s:
        a, b = s.split("/", 1)
        try:
            return float(a) / float(b)
        except Exception:
            return None
    try:
        return float(s)
    except Exception:
        return None


def make_state_record(
    *,
    iso_id: str,
    config: str,
    term: str,
    j_raw: str,
    energy_value: float | None,
    energy_unit: str,
    uncertainty: float | None,
    ref_id: str | None,
    notes: str | None,
) -> dict[str, Any]:
    """Create a canonical 'states' record for an atomic energy level.

    Uses stable IDs so dedupe works across runs.
    """
    jv = parse_quantum_number(j_raw)
    g_value = (2.0 * jv + 1.0) if jv is not None else None

    state_id = make_id(
        "state",
        iso_id,
        config or "",
        term or "",
        j_raw or "",
        str(energy_value),
        ref_id or "",
    )

    label = " ".join([p for p in [config, term, (f"J={j_raw}" if j_raw else "")] if p]).strip()

    return {
        "state_id": state_id,
        "iso_id": iso_id,
        "state_type": "atomic",
        "electronic_label": label,
        "vibrational_json": None,
        "rotational_json": None,
        "parity": None,
        "configuration": config or None,
        "term": term or None,
        "j_value": jv,
        "f_value": None,  # ASD typically does not provide hyperfine F
        "g_value": g_value,
        "energy_value": energy_value,
        "energy_unit": energy_unit,
        "energy_uncertainty": uncertainty,
        "ref_id": ref_id,
        "notes": notes,
    }


def make_transition_record(
    *,
    iso_id: str,
    quantity_value: float,
    quantity_unit: str,
    quantity_uncertainty: float | None,
    intensity_json: str | None,
    extra_json: str | None,
    selection_rules: str | None,
    ref_id: str | None,
    source: str,
    notes: str | None,
    upper_state_id: str | None = None,
    lower_state_id: str | None = None,
    dedupe_key: str | None = None,
) -> dict[str, Any]:
    """Create a canonical 'transitions' record for an atomic spectral line."""
    transition_id = make_id(
        "trans",
        iso_id,
        str(quantity_value),
        quantity_unit,
        str(quantity_uncertainty),
        selection_rules or "",
        ref_id or "",
        dedupe_key or "",
    )

    return {
        "transition_id": transition_id,
        "iso_id": iso_id,
        "upper_state_id": upper_state_id,
        "lower_state_id": lower_state_id,
        "quantity_value": float(quantity_value),
        "quantity_unit": quantity_unit,
        "quantity_uncertainty": quantity_uncertainty,
        "intensity_json": intensity_json,
        "extra_json": extra_json,
        "selection_rules": selection_rules,
        "ref_id": ref_id,
        "source": source,
        "notes": notes,
    }


if __name__ == "__main__":
    ps = parse_spectrum_label("Po LXVII")
    sid = species_id_for(ps)
    iso = iso_id_for(sid)
    print("Parsed:", ps)
    print("Species record:", make_species_record(ps))
    print("Isotopologue record:", make_isotopologue_record(sid))
    print(
        "Example transition record:",
        make_transition_record(
            iso_id=iso,
            quantity_value=656.28,
            quantity_unit="nm",
            quantity_uncertainty=0.01,
            intensity_json='{"Aki_s-1": 1.0e8}',
            selection_rules="E1",
            ref_id="L1234",
            source="NIST_ASD_LINES",
            notes="demo",
        ),
    )
