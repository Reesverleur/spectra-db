import pytest

from spectra_db.scrapers.nist_asd.normalize_atomic import parse_spectrum_label


@pytest.mark.parametrize(
    "label, element, charge",
    [
        ("Fe I", "Fe", 0),
        ("Fe II", "Fe", 1),
        ("v ii", "V", 1),
        ("Ar 15+", "Ar", 15),
    ],
)
def test_parse_spectrum_label(label: str, element: str, charge: int) -> None:
    ps = parse_spectrum_label(label)
    assert ps.element == element
    assert ps.charge == charge
