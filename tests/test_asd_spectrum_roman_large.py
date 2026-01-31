# tests/test_asd_spectrum_roman_large.py
import pytest

from spectra_db.util.asd_spectrum import parse_spectrum_label


@pytest.mark.parametrize(
    "label, element, charge",
    [
        ("Po LXVII", "Po", 66),
        ("Fe XXI", "Fe", 20),
    ],
)
def test_parse_spectrum_label_large_roman(label: str, element: str, charge: int) -> None:
    ps = parse_spectrum_label(label)
    assert ps.element == element
    assert ps.charge == charge
