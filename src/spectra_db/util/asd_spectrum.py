from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class ParsedSpectrum:
    element: str
    charge: int
    asd_label: str


_ROMAN_VALUES = {
    "I": 1,
    "V": 5,
    "X": 10,
    "L": 50,
    "C": 100,
    "D": 500,
    "M": 1000,
}


def roman_to_int(s: str) -> int:
    """Convert a Roman numeral (up to reasonably large values) to an integer.

    Supports forms used by NIST ASD (e.g., XXI, XXXVII, LXVII, etc.).
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
    """Parse labels like 'Fe I', 'Fe II', 'Po LXVII', 'Ar 15+' into element + charge."""
    s = " ".join(label.strip().split()).replace("\u00a0", " ")

    # Ar 15+
    m = re.match(r"^([A-Za-z]{1,2})\s+(\d+)\+$", s)
    if m:
        el = m.group(1).capitalize()
        ch = int(m.group(2))
        return ParsedSpectrum(element=el, charge=ch, asd_label=f"{el} {ch}+")

    # Fe II / Po LXVII
    m = re.match(r"^([A-Za-z]{1,2})\s+([IVXLCDM]+)$", s)
    if m:
        el = m.group(1).capitalize()
        stage = roman_to_int(m.group(2))
        charge = stage - 1
        return ParsedSpectrum(element=el, charge=charge, asd_label=f"{el} {m.group(2).upper()}")

    raise ValueError(f"Unrecognized spectrum label format: {label!r}")
