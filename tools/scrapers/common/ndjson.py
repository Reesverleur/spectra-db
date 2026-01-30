from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any


def append_ndjson_dedupe(path: Path, records: Iterable[dict[str, Any]], id_field: str) -> int:
    """Append records to NDJSON, skipping duplicates by id_field.

    Scans existing file once to build a set of seen IDs.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    seen: set[str] = set()
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    rid = obj.get(id_field)
                    if rid is not None:
                        seen.add(str(rid))
                except Exception:
                    continue

    n = 0
    with path.open("a", encoding="utf-8") as f:
        for rec in records:
            rid = rec.get(id_field)
            if rid is None:
                continue
            rid_s = str(rid)
            if rid_s in seen:
                continue
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            seen.add(rid_s)
            n += 1
    return n
