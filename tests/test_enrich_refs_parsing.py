from spectra_db.scrapers.nist_asd.enrich_refs import _extract_citation_and_doi  # type: ignore


def test_extract_citation_and_doi() -> None:
    html = """
    <html><body>
    <h1>ASBib Reference</h1>
    <p>Some Author; Another Author (1999). Journal of Testing 12, 34-56. DOI: 10.1234/ABC.DEF.5678</p>
    </body></html>
    """
    citation, doi = _extract_citation_and_doi(html)
    assert citation is not None and "Journal of Testing" in citation
    assert doi == "10.1234/ABC.DEF.5678"
