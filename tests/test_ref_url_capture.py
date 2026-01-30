from tools.scrapers.nist_asd.fetch_levels import extract_ref_urls_from_html as extract_levels
from tools.scrapers.nist_asd.fetch_lines import extract_ref_urls_from_html as extract_lines


def test_extract_ref_urls_from_html_popded() -> None:
    html = """
    <html><body>
      <a class="bib" href="javascript:void(0)"
         onclick="popded('https://physics.nist.gov/cgi-bin/ASBib1/get_ASBib_ref.cgi?db=el&amp;db_id=8672&amp;
         comment_code=c99&amp;element=Ac&amp;spectr_charge=21&amp;ref=8672&amp;type=E');return false">
         L8672c99
      </a>
    </body></html>
    """
    m1 = extract_levels(html)
    m2 = extract_lines(html)
    assert m1["L8672c99"].startswith("https://physics.nist.gov/cgi-bin/ASBib1/get_ASBib_ref.cgi?")
    assert "comment_code=c99" in m1["L8672c99"]
    assert m1 == m2
