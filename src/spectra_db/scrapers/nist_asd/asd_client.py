from __future__ import annotations

from dataclasses import dataclass
from typing import Any

LINES_URL = "https://physics.nist.gov/cgi-bin/ASD/lines1.pl"
LEVELS_URL = "https://physics.nist.gov/cgi-bin/ASD/energy1.pl"

# From astroquery payloads (stable, widely used)
# 'unit' codes: Angstrom=0, nm=1, um=2
UNIT_CODE = {"Angstrom": 0, "nm": 1, "um": 2}

# Wavelength type codes used by ASD lines:
# vacuum=3, vac+air=4
WAVELENGTH_TYPE_CODE = {"vacuum": 3, "vac+air": 4}

# Energy level unit codes for lines endpoint (en_unit)
ENERGY_LEVEL_CODE = {"cm-1": 0, "eV": 1, "Rydberg": 2}


@dataclass(frozen=True)
class LinesQuery:
    """Parameters for a NIST ASD lines query."""

    spectra: str  # e.g. "H I", "Fe I", "Na;Mg"
    low_w: float
    upp_w: float
    unit: str = "nm"  # "nm" | "Angstrom" | "um"
    wavelength_type: str = "vacuum"  # "vacuum" | "vac+air"
    energy_level_unit: str = "cm-1"  # "cm-1" | "eV" | "Rydberg"
    output_order: str = "wavelength"  # "wavelength" | "multiplet"
    format_code: int = 1  # try CSV first; fall back in parser


@dataclass(frozen=True)
class LevelsQuery:
    """Parameters for a NIST ASD energy levels query."""

    spectrum: str  # e.g. "He I"
    units: int = 0  # 0=cm-1, 1=eV (as seen in example outputs)
    format_code: int = 0  # try tab-delimited firstaw output first


def build_lines_params(q: LinesQuery) -> dict[str, Any]:
    """Build query params for ASD lines1.pl endpoint."""
    return {
        "spectra": q.spectra,
        "low_w": q.low_w,
        "upp_w": q.upp_w,
        "unit": UNIT_CODE[q.unit],
        "submit": "Retrieve Data",
        "format": q.format_code,
        "line_out": 0,
        "en_unit": ENERGY_LEVEL_CODE[q.energy_level_unit],
        "output_type": 0,
        "bibrefs": 1,
        "show_obs_wl": 1,
        "show_calc_wl": 1,
        "order_out": 0 if q.output_order == "wavelength" else 1,
        "max_low_enrg": "",
        "show_av": WAVELENGTH_TYPE_CODE[q.wavelength_type],
        "max_upp_enrg": "",
        "tsb_value": 0,
        "min_str": "",
        "A_out": 0,
        "f_out": "on",
        "intens_out": "on",
        "max_str": "",
        "allowed_out": 1,
        "forbid_out": 1,
        "min_accur": "",
        "min_intens": "",
        "conf_out": "on",
        "term_out": "on",
        "enrg_out": "on",
        "J_out": "on",
        "g_out": "on",
        "page_size": 2000,
        "remove_js": "on",
        "show_wn": 1,
        "unc_out": 1,
        "de": 0,
        "output": 0,
    }


def build_levels_params(q: LevelsQuery) -> dict[str, Any]:
    """Build query params for ASD energy1.pl endpoint."""
    return {
        "spectrum": q.spectrum,
        "units": q.units,  # 0=cm-1, 1=eV
        "format": q.format_code,  # 0=HTML formatted (works well); 1=ASCII; etc.
        "output": 0,
        "page_size": 2000,
        "multiplet_ordered": 0,
        # Output toggles (these are valid on energy1.pl)
        "conf_out": "on",
        "term_out": "on",
        "level_out": "on",
        "unc_out": 1,
        "j_out": "on",
        "lande_out": "on",
        "perc_out": "on",
        "biblio": "on",
        "de": 0,
        "temp": "",
        "submit": "Retrieve Data",
    }


if __name__ == "__main__":
    # Demo: show params for a lines query
    q = LinesQuery(spectra="H I", low_w=400, upp_w=700, unit="nm")
    print(build_lines_params(q))
