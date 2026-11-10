# resort_copy

Local Ollama pipeline that writes the atlas resort blurb (small, medium, large plates).
Run it through the unchanged entry point:

```
python scripts/generate_resort_copy_ollama.py --demo-query "Massanutten" --demo-state Virginia --tier small
python scripts/generate_resort_copy_ollama.py -i output/combined/ski_areas_analyzed.parquet --batch 50
```

Single-resort runs save an atomic JSON checkpoint after research, each completed
page-fact extraction, each search subpass (anchors, key points, bullets), and
each writing stage. The default is
`output/ollama_resort_copy/<resort>.json`; use `--out-json` to choose another
path, including a writable network share. If a later Ollama call fails, the JSON
keeps the last completed stage and includes `status: "failed"` plus the error.
The script still needs network access to the configured Ollama host while each
model call is running.

## Pass order

1. **Search**: build queries, fetch pages, pick snippets, extract cited NOTES.
2. **Outline**: order the notes into beats.
3. **Write**: paragraphs from outline + notes.
4. **Critique**: unsupported claims and clichés (no new facts).
5. **Edit / extend**: apply fixes, fit the print layout, add notes-only material if short.

Medium and large drafts also go through `grounding.py` after each writing pass.

## Modules

Imports only point down this list, so there are no cycles.

| Module | Role |
|---|---|
| `config.py` | Tier tables (facts, snippets, pages, beats, context), topic lists, default host/model, `_tier_key`, `_cell`, `fact_min_for` |
| `ollama_client.py` | `ollama_chat`, `_chat_json`, model list/unload, JSON parsing |
| `queries.py` | Search query builders (dossier, reader, record, mega, depth, follow-up) |
| `snippets.py` | Snippet scoring, trimming, selection; drops pages about other resorts |
| `web_fetch.py` | Live and archived page fetch, block extraction, excerpts |
| `notes.py` | Search pass: snippets and pages to NOTES, topic tagging, de-duplication, page facts |
| `grounding.py` | Anti-hallucination rule and `_strip_ungrounded` (mood words, unbacked numbers, low note overlap) |
| `writing.py` | Outline, writer, critic, editor, extend, layout fitting |
| `key_points.py` | Structured key points (prices, passes, audience, links) and `key_points.jsonl` upsert |
| `pipeline.py` | `run_passes` for one resort, `key_point_record` |
| `cli.py` | Arguments, test-row picking, batch loop, `main` |

`__init__.py` puts `scripts/` on `sys.path` and exposes `bedrock`
(`generate_resort_copy_bedrock`), which owns the print layout goals.

## Where to change things

- Too much mood or filler on medium/large: `grounding.py` (`_ATMOSPHERE`, `_GROUNDING_RULE`).
- Prompt wording for a pass: `writing.py` (or `notes.py` for the search pass).
- How many facts, pages, or beats per tier: `config.py`.
- Which sites or words count as a resort source: `snippets.py`, `web_fetch.py`.
