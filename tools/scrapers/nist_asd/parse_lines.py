from __future__ import annotations

import html as _html
import re

import pandas as pd

_PRE_RE = re.compile(r"<pre>(.*)</pre>", flags=re.DOTALL | re.IGNORECASE)


def _extract_pre(text: str) -> str:
    m = _PRE_RE.search(text)
    return m.group(1) if m else text


def _is_separator(line: str) -> bool:
    """True for lines that are mostly separators in the ASD <pre> output."""
    s = line.strip()
    if not s:
        return False
    # Typical separator lines are many '-' and only contain -|+ and spaces
    return s.count("-") > 10 and set(s) <= set("-|+ ")


def _split_cells(line: str, ncols: int) -> list[str]:
    parts = [p.strip() for p in line.split("|")]
    if len(parts) < ncols:
        parts += [""] * (ncols - len(parts))
    return parts[:ncols]


def parse_lines_response(content: bytes) -> pd.DataFrame:
    """Parse NIST ASD lines1.pl response into a DataFrame.

    ASD format=1 typically returns a <pre> block with:
      separator
      multi-line header
      separator
      data rows
      separator

    We:
      1) locate separator lines
      2) combine header rows column-wise
      3) parse data rows until the next separator
    """
    text = content.decode("utf-8", errors="replace")
    pre = _html.unescape(_extract_pre(text))
    lines = pre.splitlines()

    sep_idxs = [i for i, ln in enumerate(lines) if _is_separator(ln)]
    if len(sep_idxs) < 2:
        return pd.DataFrame()

    s0, s1 = sep_idxs[0], sep_idxs[1]
    header_lines = [ln for ln in lines[s0 + 1 : s1] if ln.strip()]
    if not header_lines:
        return pd.DataFrame()

    ncols = max(len(ln.split("|")) for ln in header_lines)

    header_parts = [_split_cells(ln, ncols) for ln in header_lines]
    headers: list[str] = []
    for c in range(ncols):
        pieces = [hp[c] for hp in header_parts if hp[c]]
        name = re.sub(r"\s+", " ", " ".join(pieces)).strip()
        headers.append(name)

    # Deduplicate header names
    seen: dict[str, int] = {}
    final_headers: list[str] = []
    for i, h in enumerate(headers):
        base = h if h else f"col_{i}"
        if base in seen:
            seen[base] += 1
            final_headers.append(f"{base}__{seen[base]}")
        else:
            seen[base] = 1
            final_headers.append(base)

    s2 = sep_idxs[2] if len(sep_idxs) > 2 else len(lines)
    data_lines = lines[s1 + 1 : s2]

    records: list[list[str]] = []
    for ln in data_lines:
        if "|" not in ln:
            continue
        if _is_separator(ln):
            continue
        row = _split_cells(ln, ncols)
        if not any(row):
            continue
        records.append(row)

    return pd.DataFrame(records, columns=final_headers)


if __name__ == "__main__":
    demo = b"""
    <pre>
    ------------------------------------------------------------
    | Observed | Unc. | Ritz |
    | Wavelength Vac (nm) | (nm) | Wavelength (nm) |
    ------------------------------------------------------------
    | 656.28 | 0.01 | 656.28 |
    ------------------------------------------------------------
    </pre>
    """
    df = parse_lines_response(demo)
    print(df.columns.tolist())
    print(df)
