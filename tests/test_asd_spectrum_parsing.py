import json
from typing import Any


def lines(
    self,
    iso_id: str,
    *,
    unit: str = "nm",
    min_wav: float | None = None,
    max_wav: float | None = None,
    limit: int = 100,
    parse_payload: bool = True,
) -> list[dict[str, Any]]:
    """Fetch spectral lines for an isotopologue within an optional wavelength range.

    Returns a list of dicts. If parse_payload=True, `payload` is a dict parsed from
    transitions.intensity_json (best-effort).
    """
    clauses = ["t.iso_id = ?", "t.quantity_unit = ?"]
    args: list[Any] = [iso_id, unit]

    if min_wav is not None:
        clauses.append("t.quantity_value >= ?")
        args.append(min_wav)
    if max_wav is not None:
        clauses.append("t.quantity_value <= ?")
        args.append(max_wav)

    where = " AND ".join(clauses)
    q = f"""
    SELECT t.quantity_value, t.quantity_unit, t.quantity_uncertainty,
           t.intensity_json, t.extra_json, t.selection_rules,
           r.url AS ref_url
    FROM transitions t
    LEFT JOIN refs r ON t.ref_id = r.ref_id
    WHERE {where}
    ORDER BY t.quantity_value
    LIMIT ?
    """
    args.append(limit)
    rows = self.con.execute(q, args).fetchall()

    out: list[dict[str, Any]] = []
    for wav, u, unc, intensity_json, extra_json, sel, ref_url in rows:
        rec: dict[str, Any] = {
            "wavelength": wav,
            "unit": u,
            "unc": unc,
            "selection_rules": sel,
            "ref_url": ref_url,
            "extra_json": extra_json,
        }
        if parse_payload and intensity_json:
            try:
                rec["payload"] = json.loads(intensity_json)
            except Exception:
                rec["payload"] = {}
        else:
            rec["payload"] = None
        out.append(rec)

    return out
