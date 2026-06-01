#!/usr/bin/env python3
"""CLI: manifest.json → layout_plan.json"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from atlas.book_gen.pack_pages import pack_manifest_entries, write_layout_plan


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", type=Path)
    parser.add_argument("-o", "--output", type=Path, default=None)
    args = parser.parse_args()
    payload = json.loads(args.manifest.read_text(encoding="utf-8"))
    manifest = payload.get("entries") or payload
    plan = pack_manifest_entries(manifest)
    out = args.output or args.manifest.parent / "layout_plan.json"
    write_layout_plan(out, plan)
    print(f"Wrote {plan['physical_page_count']} pages → {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
