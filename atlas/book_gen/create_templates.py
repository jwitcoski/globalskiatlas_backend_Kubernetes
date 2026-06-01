#!/usr/bin/env python3
"""
Generate Scribus SLA entry templates with %VAR% placeholders (ScribusGenerator-compatible).

The main pipeline uses sla_compose.py for filled chapters; these templates are for
manual tweaks or ScribusGenerator mail-merge experiments.
"""

from __future__ import annotations

from pathlib import Path

from atlas.book_gen.sla_compose import compose_chapter_sla, write_sla

PLACEHOLDER_MANIFEST = {
    "demo": {
        "pageId": "demo-resort",
        "mapPath": None,
        "scribusFields": {
            "location": "STATE, COUNTRY",
            "title": "%title%",
            "subtitle": "%subtitle%",
            "stats_block": "%stats_block%",
            "trail_breakdown": "%trail_breakdown%",
            "body": "%body%",
            "drop_cap": "%drop_cap%",
            "body_after_cap": "%body_after_cap%",
            "footer_line": "%footer_line%",
        },
    }
}


def _single_page_plan(slot: str) -> dict:
    return {
        "pages": [
            {
                "page_index": 0,
                "page_type": slot,
                "placements": [
                    {
                        "pageId": "demo",
                        "slot": slot,
                        "x": 0.0,
                        "y": 0.0,
                        "w": 1.0,
                        "h": 1.0,
                        "page_index": 0,
                    }
                ],
            }
        ],
        "physical_page_count": 1,
    }


def main() -> int:
    repo = Path(__file__).resolve().parents[2]
    templates = repo / "atlas" / "book_gen" / "templates"
    templates.mkdir(parents=True, exist_ok=True)

    for slot in ("quarter", "half", "full"):
        plan = _single_page_plan(slot)
        tree = compose_chapter_sla(plan, PLACEHOLDER_MANIFEST)
        write_sla(tree, templates / f"entry_{slot}.sla")
        print(f"  {templates / f'entry_{slot}.sla'}")

    # Chapter shell (title only)
    shell_plan = {"pages": [], "physical_page_count": 0}
    tree = compose_chapter_sla(
        shell_plan, {}, chapter_title="Chapter Title — Ski Atlas"
    )
    write_sla(tree, templates / "chapter_shell.sla")
    print(f"  {templates / 'chapter_shell.sla'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
