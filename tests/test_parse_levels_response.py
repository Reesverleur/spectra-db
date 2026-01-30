from tools.scrapers.nist_asd.fetch_levels import _safe_float  # type: ignore
from tools.scrapers.nist_asd.parse_levels import parse_levels_response


def test_parse_levels_selects_levels_table() -> None:
    # Two tables: first irrelevant, second is levels-like
    html = """
    <html><body>
      <table>
        <tr><th>Not</th><th>Levels</th></tr>
        <tr><td>a</td><td>b</td></tr>
      </table>

      <table>
        <tr>
          <th>Configuration</th><th>Term</th><th>J</th><th>Level (cm-1)</th><th>Unc. (cm-1)</th><th>Ref.</th>
        </tr>
        <tr><td>2p</td><td>2P°</td><td>1/2</td><td>82 258.9191133</td><td>0.0001</td><td>L1234a</td></tr>
        <tr><td></td><td></td><td>3/2</td><td>82 259.2850014</td><td>0.0001</td><td>L1234a</td></tr>
      </table>
    </body></html>
    """
    df = parse_levels_response(html.encode("utf-8"))
    assert not df.empty
    assert "Configuration" in df.columns
    assert "Term" in df.columns
    assert "J" in df.columns
    level_col = [c for c in df.columns if "Level" in str(c)][0]
    assert _safe_float(df.iloc[0][level_col]) == 82258.9191133
    assert _safe_float(df.iloc[1][level_col]) == 82259.2850014
