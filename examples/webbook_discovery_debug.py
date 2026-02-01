from __future__ import annotations

import re
import sys
from pathlib import Path
from urllib.request import Request, urlopen

DEFAULT_URL = "https://webbook.nist.gov/cgi/cbook.cgi?Formula=H&AllowOther=on&AllowExtra=on&Units=SI&cDI=on"

ID_PARAM_RE = re.compile(r"[?&]ID=([^&#]+)")
RAW_ID_RE = re.compile(r"\bID=([A-Za-z0-9]+)\b")


def fetch(url: str) -> tuple[str, str]:
    req = Request(
        url,
        headers={
            "User-Agent": "spectra-db-debug/1.0",
            "Accept": "text/html,application/xhtml+xml",
        },
        method="GET",
    )
    with urlopen(req, timeout=30) as resp:
        final_url = resp.geturl()
        body = resp.read().decode("utf-8", errors="replace")
    return final_url, body


def main() -> None:
    url = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_URL
    final_url, html = fetch(url)

    out_path = Path("examples/webbook_debug.html")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    print("Final URL:", final_url)
    print("Saved HTML:", out_path.resolve())
    print("HTML length:", len(html))

    # Basic title sniff
    m = re.search(r"<title>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
    print("Title:", (m.group(1).strip() if m else "(no <title>)"))

    # Find hrefs
    hrefs = re.findall(r'href="([^"]+)"', html, flags=re.IGNORECASE)
    print("\n# hrefs found:", len(hrefs))

    hrefs_with_id = [h for h in hrefs if "ID=" in h]
    print("# hrefs containing 'ID=':", len(hrefs_with_id))
    print("Sample hrefs with ID (up to 10):")
    for h in hrefs_with_id[:10]:
        print("  ", h)

    # Find option values
    option_vals = re.findall(r'<option[^>]*value="([^"]+)"', html, flags=re.IGNORECASE)
    print("\n# <option value> found:", len(option_vals))
    opt_with_id = [v for v in option_vals if "ID=" in v or ID_PARAM_RE.search(v)]
    print("# option values containing ID:", len(opt_with_id))
    print("Sample option values with ID (up to 10):")
    for v in opt_with_id[:10]:
        print("  ", v)

    # Raw occurrences of ID=....
    raw_ids = RAW_ID_RE.findall(html)
    print("\n# raw 'ID=XXXX' occurrences anywhere:", len(raw_ids))
    if raw_ids:
        print("Sample raw IDs (up to 20):", raw_ids[:20])

    # Try to extract IDs directly from any link-like text
    extracted = set()
    for h in hrefs:
        m2 = ID_PARAM_RE.search(h)
        if m2:
            extracted.add(m2.group(1))
    for v in option_vals:
        m3 = ID_PARAM_RE.search(v)
        if m3:
            extracted.add(m3.group(1))
    print("\n# extracted candidate IDs via ID= parsing:", len(extracted))
    if extracted:
        print("Sample extracted IDs (up to 30):", list(sorted(extracted))[:30])


if __name__ == "__main__":
    main()
