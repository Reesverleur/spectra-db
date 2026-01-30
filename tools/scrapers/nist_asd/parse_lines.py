from __future__ import annotations

import html as _html
import re

import pandas as pd

_PRE_RE = re.compile(r"<pre>(.*)</pre>", flags=re.DOTALL | re.IGNORECASE)


def _extract_pre(text: str) -> str:
    m = _PRE_RE.search(text)
    return m.group(1) if m else text


def _is_separator(line: str) -> bool:
    """
    NIST ASD ASCII table separators are long runs of '-' (sometimes with + or |).
    """
    s = line.rstrip("\n")
    t = s.strip()
    if not t:
        return False
    # Allow '-', '|', '+', and spaces; require lots of '-' to avoid false positives.
    allowed = set("-|+ ")
    return (t.count("-") > 10) and (set(t) <= allowed)


def _pipe_positions(template_line: str) -> list[int]:
    return [i for i, ch in enumerate(template_line) if ch == "|"]


def _split_fixed_width(line: str, pipe_pos: list[int]) -> list[str]:
    """
    Split a fixed-width ASCII row by slicing between pipe positions.
    This remains aligned even when some header rows omit internal pipes
    (spanning group headers).
    """
    if not pipe_pos:
        return [line.strip()]

    # Ensure the line is long enough for slicing.
    need_len = pipe_pos[-1] + 1
    if len(line) < need_len:
        line = line + (" " * (need_len - len(line)))

    starts = [0] + [p + 1 for p in pipe_pos]
    ends = pipe_pos + [len(line)]
    return [line[s:e].strip() for s, e in zip(starts, ends, strict=False)]


def _normalize_header_cell(s: str) -> str:
    s2 = re.sub(r"\s+", " ", (s or "").strip())
    return s2


def _dedupe_headers(headers: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    out: list[str] = []
    for i, h in enumerate(headers):
        base = h if h else f"col_{i}"
        if base in seen:
            seen[base] += 1
            out.append(f"{base}__{seen[base]}")
        else:
            seen[base] = 1
            out.append(base)
    return out


def _find_best_template_line(header_lines: list[str], data_lines: list[str]) -> str | None:
    """
    Prefer a data row (it almost always has the full set of '|' positions).
    Fall back to the most pipe-rich header row.
    """
    data_with_pipes = [ln for ln in data_lines if "|" in ln]
    if data_with_pipes:
        return max(data_with_pipes, key=lambda s: s.count("|"))

    hdr_with_pipes = [ln for ln in header_lines if "|" in ln]
    if hdr_with_pipes:
        return max(hdr_with_pipes, key=lambda s: s.count("|"))

    return None


def _build_headers(header_lines: list[str], pipe_pos: list[int]) -> list[str]:
    """
    Build clean, stable column names.
    We do two things:
      1) Merge multi-line headers column-wise.
      2) Override known structural groups (Lower/Upper Conf/Term/J, TP Ref, Line Ref, etc.)
         so they don’t inherit garbled spanning-header fragments.
    """
    if not header_lines:
        return []

    hdr_rows = [_split_fixed_width(ln, pipe_pos) for ln in header_lines]
    ncols = max(len(r) for r in hdr_rows)
    hdr_rows = [r + [""] * (ncols - len(r)) for r in hdr_rows]

    merged: list[str] = []
    for c in range(ncols):
        pieces = [_normalize_header_cell(row[c]) for row in hdr_rows if _normalize_header_cell(row[c])]
        merged.append(_normalize_header_cell(" ".join(pieces)))

    # Use the most pipe-rich header row as the “detail row” for structure detection
    detail_row_line = max(header_lines, key=lambda s: s.count("|")) if header_lines else ""
    detail_cells = _split_fixed_width(detail_row_line, pipe_pos)
    detail_cells = detail_cells + [""] * (ncols - len(detail_cells))

    names = [m if m else f"col_{i}" for i, m in enumerate(merged)]

    # Detect Lower/Upper Conf/Term/J triplets by locating "Conf." in the detailed header row
    conf_idx = [i for i, cell in enumerate(detail_cells) if re.search(r"\bconf\b", cell, flags=re.I)]
    if len(conf_idx) >= 2:
        low, up = conf_idx[0], conf_idx[1]
        for prefix, base in [("Lower", low), ("Upper", up)]:
            if base + 0 < ncols:
                names[base + 0] = f"{prefix} Conf."
            if base + 1 < ncols:
                names[base + 1] = f"{prefix} Term"
            if base + 2 < ncols:
                names[base + 2] = f"{prefix} J"

    # Keyword-based cleanup for the rest (keeps your downstream _find_col() happy)
    def has(i: int, *words: str) -> bool:
        s = (merged[i] or "").lower()
        return all(w.lower() in s for w in words)

    for i in range(ncols):
        if has(i, "observed", "wavelength"):
            names[i] = "Observed Wavelength"
        elif has(i, "ritz", "wavelength"):
            names[i] = "Ritz Wavelength"
        elif has(i, "wavenumber"):
            names[i] = "Transition Wavenumber"
        elif has(i, "rel", "int"):
            names[i] = "Rel Int"
        elif has(i, "aki"):
            names[i] = "Aki"
        elif has(i, "acc"):
            names[i] = "Acc"
        elif has(i, "type"):
            names[i] = "Type"
        elif has(i, "tp", "ref"):
            names[i] = "TP Ref"
        elif has(i, "line", "ref"):
            names[i] = "Line Ref"
        elif re.search(r"\bei\b", merged[i] or "", flags=re.I) and re.search(r"\bek\b", merged[i] or "", flags=re.I):
            names[i] = "Ei - Ek (cm-1)"
        elif re.search(r"\bgi\b", merged[i] or "", flags=re.I) and re.search(r"\bgk\b", merged[i] or "", flags=re.I):
            names[i] = "gi gk"

    # Uncertainties: assign by adjacency if present
    try:
        obs_idx = names.index("Observed Wavelength")
        if obs_idx + 1 < ncols and "unc" in (merged[obs_idx + 1] or "").lower():
            names[obs_idx + 1] = "Observed Unc."
    except ValueError:
        pass

    try:
        ritz_idx = names.index("Ritz Wavelength")
        if ritz_idx + 1 < ncols and "unc" in (merged[ritz_idx + 1] or "").lower():
            names[ritz_idx + 1] = "Ritz Unc."
    except ValueError:
        pass

    return _dedupe_headers(names)


def parse_lines_response(content: bytes) -> pd.DataFrame:
    """
    Parse NIST ASD lines1.pl response (<pre> ASCII table) into a DataFrame.

    Key property: uses FIXED-WIDTH slicing by '|' positions from a pipe-rich template line,
    so spanning headers cannot misalign columns (the Fe II failure mode).
    """
    text = content.decode("utf-8", errors="replace")
    pre = _html.unescape(_extract_pre(text))
    lines = pre.splitlines()

    sep_idxs = [i for i, ln in enumerate(lines) if _is_separator(ln)]
    if len(sep_idxs) < 2:
        return pd.DataFrame()

    # First separator starts header, second ends header.
    s0, s1 = sep_idxs[0], sep_idxs[1]
    header_lines = [ln for ln in lines[s0 + 1 : s1] if ln.strip()]

    # Data runs until the last separator after s1 (usually the final border).
    s_last = sep_idxs[-1] if len(sep_idxs) >= 3 else len(lines)
    data_lines = lines[s1 + 1 : s_last]

    template = _find_best_template_line(header_lines, data_lines)
    if not template:
        return pd.DataFrame()

    pipe_pos = _pipe_positions(template)
    if not pipe_pos:
        return pd.DataFrame()

    headers = _build_headers(header_lines, pipe_pos)
    ncols = len(headers)

    rows: list[list[str]] = []
    for ln in data_lines:
        if _is_separator(ln):
            continue
        if "|" not in ln:
            continue
        cells = _split_fixed_width(ln, pipe_pos)
        if len(cells) < ncols:
            cells = cells + [""] * (ncols - len(cells))
        else:
            cells = cells[:ncols]
        if not any(c.strip() for c in cells):
            continue
        rows.append(cells)

    df = pd.DataFrame(rows, columns=headers)

    # Drop fully-empty trailing “col_*” columns if they exist
    drop_cols = []
    for c in df.columns:
        if str(c).startswith("col_"):
            if df[c].astype(str).str.strip().eq("").all():
                drop_cols.append(c)
    if drop_cols:
        df = df.drop(columns=drop_cols)

    return df


if __name__ == "__main__":
    # Minimal deterministic smoke test against a cached .body file (adjust path as needed).
    import pathlib

    p = pathlib.Path("data/raw/nist_asd/lines/example.body")
    if p.exists():
        b = p.read_bytes()
        df = parse_lines_response(b)
        print(df.columns.tolist())
        print(df.head(5))
    else:
        # Tiny synthetic demo
        demo = b"""
        <pre>
        ------------------------------------------------------------
            Observed   | Unc. |   Ritz    |      Ei           Ek     |   Type |  TP   | Line  |
           Wavelength  | (nm) | Wavelength|     (cm-1)               |        | Ref.  | Ref.  |
            Vac (nm)   |      |  Vac (nm) |                           |        |       |       |
        ------------------------------------------------------------
              15.260   |      |  15.2600  | 21393.0  -   676700      |        |       |  L78  |
        ------------------------------------------------------------
        </pre>
        """
        df = parse_lines_response(demo)
        print(df.columns.tolist())
        print(df)
