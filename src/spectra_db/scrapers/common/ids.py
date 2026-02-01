from __future__ import annotations

import hashlib


def short_hash(text: str, n: int = 16) -> str:
    """Return a short stable hex hash of text."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:n]


def make_id(prefix: str, *parts: str) -> str:
    """Make a stable ID from parts."""
    blob = prefix + "|" + "|".join(parts)
    return f"{prefix}_{short_hash(blob)}"


if __name__ == "__main__":
    print(make_id("state", "ASD:He:0/main", "1s2", "1S", "0", "0.0"))
