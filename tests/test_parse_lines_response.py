from tools.scrapers.nist_asd.parse_lines import parse_lines_response


def test_parse_lines_multiline_header_pre() -> None:
    html = b"""
    <html><body><pre>
    ------------------------------------------------------------------------------------------------
    | Observed  | Unc. | Ritz     | Wavenumber | Ei      | Ek      | Type | Ref |
    | Wavelength Vac (nm) | (nm)  | Wavelength (nm) | (cm-1)    | (cm-1) | (cm-1) |      |     |
    ------------------------------------------------------------------------------------------------
    | 656.2800 | 0.001 | 656.2799 | 15233.0    | 0.0     | 15233.0 | E1   | L9999 |
    | 486.1330 | 0.001 | 486.1329 | 20564.0    | 0.0     | 20564.0 | E1   | L9999 |
    ------------------------------------------------------------------------------------------------
    </pre></body></html>
    """
    df = parse_lines_response(html)
    assert df.shape[0] == 2
    # Column names should be combined
    assert any("Observed" in c and "Wavelength" in c for c in df.columns)
    assert any("Wavenumber" in c for c in df.columns)
