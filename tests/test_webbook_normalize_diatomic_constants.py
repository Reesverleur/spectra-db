from __future__ import annotations

import json
from pathlib import Path

import pytest

from spectra_db.scrapers.nist_webbook import normalize_diatomic_constants as norm
from spectra_db.util.paths import RepoPaths


def _read_ndjson(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            out.append(json.loads(line))
    return out


def test_webbook_normalizer_scrapes_notes_references_and_footnotes_and_keeps_cell_targets(monkeypatch, tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    (repo_root / "data" / "normalized_molecular").mkdir(parents=True, exist_ok=True)

    paths = RepoPaths(repo_root=repo_root)
    monkeypatch.setattr(norm, "get_paths", lambda: paths)

    html = """\
    <html><body>
      <h2>Notes</h2>
      <p>This is the upper notes section we want to keep.</p>

      <table>
        <caption>Diatomic constants for 12C16O</caption>
        <tr>
          <th>State</th><th>Te</th><th>we</th><th>wexe</th><th>weye</th><th>Be</th><th>ae</th><th>ge</th>
          <th>De</th><th>be</th><th>re</th><th>Trans</th><th>nu00</th>
        </tr>
        <tr>
          <td class="nowrap"> A <sup>1</sup>&#928;</td>
          <td> 65075.7<sub>7</sub></td>
          <td> 1518.2<sub>4</sub></td>
          <td> 19.4 <a href="#Dia46">46</a></td>
          <td>&nbsp;</td>
          <td> 1.6115 <a href="#Dia47">47</a></td>
          <td> 0.02325 <a href="#Dia48">48</a></td>
          <td>&nbsp;</td>
          <td> 0.00000733 <a href="#Dia49">49</a></td>
          <td>&nbsp;</td>
          <td> 1.2353</td>
          <td class="nowrap"> A &#8596; X <a href="#Dia50">50</a> R</td>
          <td> 64748.48 <a href="#Dia53">53</a> Z</td>
        </tr>
      </table>

      <!-- Footnotes (CO-style): List of Notes table where left td is DiaNN anchor and right td is the text -->
      <table role="presentation" aria-label="List of Notes">
        <tr>
          <td style="vertical-align: top;"><a id="Dia50">50</a></td>
          <td>Footnote 50 text referencing <a href="#ref-1">Some Ref</a>.</td>
        </tr>
        <tr>
          <td style="vertical-align: top;"><a id="Dia53">53</a></td>
          <td>Footnote 53 text. See also <a href="#Dia46">46</a> and cite <a href="#ref-1">Ref 1</a>.</td>
        </tr>
      </table>

      <h2 id="Refs">References</h2>
      <p class="section-head"><strong>Go To:</strong> <a href="#Top">Top</a>, <a href="#Diatomic">Constants</a>, <a href="#Notes">Notes</a></p>
      <p class="section-head"><strong>Data compilation copyright by NIST.</strong></p>

      <p>
        <span id="ref-1"><strong>Author, Title, Journal (2020)</strong></span><br />
        <span class="Z3988" title="rft_id=info:doi/10.1234/abcd.2020.01"></span>
        Author; <strong>Title</strong>, Journal, 2020. <a href="https://doi.org/10.1234/abcd.2020.01">doi</a>
      </p>

      <!-- Footer-ish noise notes we do NOT want -->
      <div id="footer">
        <h2>Notes</h2>
        <p>Random NIST footer content.</p>
      </div>
    </body></html>
    """

    body_path = repo_root / "fake.body"
    body_path.write_text(html, encoding="utf-8")

    rr = norm.run(webbook_id="C630080", body_path=body_path)
    assert rr.ok is True

    outdir = getattr(paths, "normalized_molecular_dir", paths.normalized_dir)

    species = _read_ndjson(outdir / "species.ndjson")
    params = _read_ndjson(outdir / "parameters.ndjson")
    refs = _read_ndjson(outdir / "refs.ndjson")
    states = _read_ndjson(outdir / "states.ndjson")

    assert species, "species.ndjson should not be empty"
    sx = json.loads(species[0].get("extra_json") or "{}")

    # Upper notes kept
    assert "upper notes section" in (sx.get("webbook_notes_text") or "").lower()

    # Footnotes stored as structured objects keyed by DiaNN
    foot = sx.get("webbook_footnotes_by_id") or {}
    assert "Dia53" in foot
    assert isinstance(foot["Dia53"], dict)
    assert "Footnote 53 text" in (foot["Dia53"].get("text") or "")
    assert foot["Dia53"].get("ref_targets") == ["ref-1"]
    assert foot["Dia53"].get("dia_targets") == ["Dia46"]

    # Embedded webbook_references should include ref-1
    embedded_refs = sx.get("webbook_references") or []
    assert any(r.get("ref_id") == "WB:C630080:ref-1" for r in embedded_refs)

    # True bibliographic refs should be present in refs.ndjson
    assert refs, "refs.ndjson should not be empty"
    r0 = [r for r in refs if r.get("ref_id") == "WB:C630080:ref-1"][0]
    assert r0["ref_type"] == "webbook_reference"
    assert r0["doi"] == "10.1234/abcd.2020.01"

    # nu00: numeric value + suffix preserved via context_json/value_suffix; note targets recorded
    nu00 = [p for p in params if p["name"] == "nu00"][0]
    assert nu00["value"] == pytest.approx(64748.48)
    ctx = json.loads(nu00["context_json"])
    assert ctx["cell_note_targets"] == ["Dia53"]
    assert ctx["value_suffix"] == "Z"

    # Trans is stored on state extra_json and keeps R suffix; note targets recorded
    st = states[0]
    extra = json.loads(st["extra_json"])
    assert (extra.get("Trans_clean") or "").endswith("R")
    assert extra["Trans_note_targets"] == ["Dia50"]
