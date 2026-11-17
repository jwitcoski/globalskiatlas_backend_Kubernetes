"""Compare writer models and the section / length-loop ablations on one note set.

The notes are a fixture. They are not a published resort page.

  python scripts/resort_copy/bench_writer.py --models llama3.1:8b qwen2.5:14b
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parent.parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from resort_copy.ollama_client import ollama_base_url  # noqa: E402
from resort_copy.validators import deterministic_critique, length_report, unused_notes  # noqa: E402
from resort_copy.writing import length_control_loop, produce_draft  # noqa: E402

NOTES = [
    {"topic": "year_started", "text": "Fixture Hill opened in 1968."},
    {"topic": "public_or_private", "text": "Fixture Hill is a private resort."},
    {"topic": "access", "text": "Fixture Hill is 248 km from Melbourne."},
    {"topic": "access", "text": "The nearest airport is Melbourne Airport, about a three hour drive."},
    {"topic": "price", "text": "An adult day ticket is $149 AUD."},
    {"topic": "pass", "text": "Fixture Hill is not on the Epic Pass."},
    {"topic": "lodging", "text": "The Horse Hill lodge has 42 rooms on the hill."},
    {"topic": "terrain", "text": "The area has 22 lifts and night skiing on the village runs."},
    {"topic": "school", "text": "The ski school runs group lessons and a kids club."},
    {"topic": "season", "text": "The season usually opens in June and closes in October, with snowmaking on the lower runs."},
    {"topic": "scene", "text": "The village has two mountain restaurants and a grocery."},
    {"topic": "who", "text": "Most winter visitors are families on a weekend trip."},
    {"topic": "history", "text": "A local club built the first rope tow in 1968."},
    {"topic": "food", "text": "The lodge restaurant serves dinner on Friday and Saturday."},
]


def _row(model: str, mode: str, text: str, tier: str) -> dict:
    report = length_report(text, tier)
    critique = deterministic_critique(text, NOTES, name="Fixture Hill", country="Australia", tier=tier)
    leftover = unused_notes(text, NOTES)
    return {
        "model": model,
        "mode": mode,
        "tier": tier,
        "characters": report["characters"],
        "paragraphs": report["paragraphs"],
        "lines": report["lines"],
        "character_pct": report["character_pct"],
        "notes": len(NOTES),
        "notes_unused": len(leftover),
        "unsupported": len(critique["unsupported"]),
        "contradictions": len(critique["contradictions"]),
        "short": report["short"],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--models", nargs="+", default=["llama3.1:8b"])
    parser.add_argument("--host", default=None)
    parser.add_argument("--timeout", type=float, default=240)
    parser.add_argument("--skip-ablation", action="store_true")
    args = parser.parse_args()
    base = ollama_base_url(args.host)
    rows: list[dict] = []
    for model in args.models:
        print(f"single-pass small {model}", file=sys.stderr, flush=True)
        text = produce_draft(
            NOTES,
            [note["text"] for note in NOTES],
            resort_label="Fixture Hill, Australia",
            tier="small",
            base_url=base,
            model=model,
            num_ctx=4096,
            timeout=args.timeout,
            name="Fixture Hill",
            section_writing=False,
        )
        rows.append(_row(model, "single", text, "small"))
        print(json.dumps(rows[-1]), flush=True)
        if args.skip_ablation or model != args.models[0]:
            continue
        print(f"medium single {model}", file=sys.stderr, flush=True)
        single = produce_draft(
            NOTES,
            [note["text"] for note in NOTES],
            resort_label="Fixture Hill, Australia",
            tier="medium",
            base_url=base,
            model=model,
            num_ctx=4096,
            timeout=args.timeout,
            name="Fixture Hill",
            section_writing=False,
        )
        rows.append(_row(model, "single", single, "medium"))
        print(json.dumps(rows[-1]), flush=True)
        print(f"medium sections {model}", file=sys.stderr, flush=True)
        sections = produce_draft(
            NOTES,
            [],
            resort_label="Fixture Hill, Australia",
            tier="medium",
            base_url=base,
            model=model,
            num_ctx=4096,
            timeout=args.timeout,
            name="Fixture Hill",
            section_writing=True,
        )
        rows.append(_row(model, "sections", sections, "medium"))
        print(json.dumps(rows[-1]), flush=True)
        print(f"medium sections plus length loop {model}", file=sys.stderr, flush=True)
        longer, reason, _left = length_control_loop(
            sections,
            NOTES,
            resort_label="Fixture Hill, Australia",
            tier="medium",
            base_url=base,
            model=model,
            num_ctx=4096,
            timeout=args.timeout,
            passes=1,
        )
        item = _row(model, "sections+length", longer, "medium")
        item["short_reason"] = reason
        rows.append(item)
        print(json.dumps(rows[-1]), flush=True)
    print(json.dumps(rows, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
