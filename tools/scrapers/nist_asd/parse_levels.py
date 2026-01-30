from __future__ import annotations

import io
import re

import pandas as pd


def parse_levels_response(content: bytes) -> pd.DataFrame:
    """Parse NIST ASD energy1.pl output into a DataFrame.

    For format=0, NIST returns an HTML page containing an HTML table of levels.
    The header text is often split across multiple lines in text extraction, so
    we prioritize parsing HTML tables directly via pandas.read_html().

    Returns:
        DataFrame of energy levels. Expected columns typically include:
        - Configuration
        - Term
        - J
        - Level (cm-1) or Level (eV)
        - Uncertainty (...)
        - Reference
    """
    html = content.decode("utf-8", errors="replace")

    # 1) Primary: parse HTML tables and pick the most "level-like" one.
    try:
        tables = pd.read_html(io.StringIO(html))
    except Exception:
        tables = []

    if tables:
        # Prefer tables that contain these columns.
        def score(df: pd.DataFrame) -> int:
            cols = {str(c).strip().lower() for c in df.columns}
            want = {"configuration", "term", "j"}
            return int(len(want.intersection(cols)) * 100 + df.shape[0])

        tables.sort(key=score, reverse=True)
        best = tables[0]
        # Basic sanity: should have at least a few rows and at least one of the main columns
        if best.shape[0] > 0:
            return best

    # 2) Fallback: try fixed-width parsing from <pre> if present
    pre = re.search(r"<pre>(.*)</pre>", html, flags=re.DOTALL | re.IGNORECASE)
    if pre:
        pre_text = pre.group(1)
        try:
            return pd.read_fwf(io.StringIO(pre_text))
        except Exception:
            pass

    return pd.DataFrame()


if __name__ == "__main__":
    # Demo: read a local file if you want to test quickly
    print("parse_levels_response: module loaded (run fetch_levels to test).")
