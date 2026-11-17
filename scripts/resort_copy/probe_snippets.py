"""One search each for the regression resorts. Counts snippets the entity filter drops.

This does not call Ollama and does not write a page.
"""
from __future__ import annotations

import sys
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parent.parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from resort_copy import bedrock  # noqa: E402
from resort_copy.snippets import _drop_pages_about_other_places  # noqa: E402

RESORTS = [
    ("Mount Buller", "Australia"),
    ("Cerro Catedral", "Argentina"),
    ("Malbun", "Liechtenstein"),
    ("Ordino-Arcalis", "Andorra"),
    ("Caviahue", "Argentina"),
    ("Ben Eoin", "Canada"),
    ("Tahko", "Finland"),
    ("Gulmarg", "India"),
    ("Appi Kogen", "Japan"),
]


def main() -> int:
    for name, country in RESORTS:
        query = f'"{name}" ski resort {country}'
        try:
            rows = bedrock.fetch_duckduckgo_snippets([query], max_per_query=4, max_total=8)
        except Exception as exc:
            print(f"{name}\tERROR\t{exc}", flush=True)
            continue
        snippets = []
        for index, row in enumerate(rows):
            snippets.append(
                {
                    "id": str(index),
                    "title": row.get("title") or "",
                    "url": row.get("url") or "",
                    "text": row.get("text") or row.get("body") or row.get("snippet") or "",
                }
            )
        kept, dropped = _drop_pages_about_other_places(snippets, name, country=country)
        print(f"{name}\treturned={len(rows)}\tkept={len(kept)}\tdropped={dropped}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
