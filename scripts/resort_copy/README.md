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

1. **Search**: build queries, fetch pages, answer the sticky questions, then top up: dedupe, mine the fetched pages in fact windows, search gap topics (`topup.py`).
2. **Outline**: order the notes into beats.
3. **Write**: paragraphs from outline + notes.
4. **Critique**: unsupported claims and clichés (no new facts).
5. **Edit / extend**: apply fixes, fit the print layout, add notes-only material if short.

Medium and large drafts also go through `grounding.py` after each writing pass.

## Modules

Imports only point down this list, so there are no cycles.

| Module | Role |
|---|---|
| `config.py` | Tier tables (facts, snippets, pages, beats, context), topic lists, default host/model, `_tier_key`, `_cell`, `fact_min_for`, `write_floor_for` (note count), `write_char_floor_for` (plate min characters), `search_ladder_for` (small→medium, medium→large) |
| `ollama_client.py` | `ollama_chat`, `_chat_json`, model list/unload, JSON parsing |
| `queries.py` | Search query builders (dossier, reader, record, large-area, topic, depth, follow-up) |
| `snippets.py` | Snippet scoring, trimming, selection; drops pages about other resorts |
| `web_fetch.py` | Live and archived page fetch, block extraction, excerpts |
| `notes.py` | Search pass: snippets and pages to NOTES, topic tagging, de-duplication, page facts |
| `stickies.py` | Sticky-question pass: one grounded note per reader question against a growing page pile |
| `topup.py` | After stickies: search pass/price first, mine fetched pages (pass sites before Wikipedia; skip Stay22/booking). Write needs **both** the note-count floor and the plate character minimum. Search keeps going until pantry chars hit 2× the plate or 24 windows. Empty searches stop. |
| `grounding.py` | Anti-hallucination rule and `_strip_ungrounded` (mood words, unbacked numbers, low note overlap) |
| `writing.py` | Outline, writer, critic, editor, extend, layout fitting |
| `key_points.py` | Structured key points (prices, passes, audience, links) and `key_points.jsonl` upsert |
| `facts.py` / `fact_store.py` | Typed facts table from `filters.jsonl` + `key_points.jsonl` (no LLM, no invented cells) |
| `facts_api.py` | `GET /resorts/facts` and `/resorts/facts/stats` |
| `pipeline.py` | `run_passes` for one resort, `key_point_record` |
| `cli.py` | Arguments, test-row picking, batch loop, `main` |

`__init__.py` puts `scripts/` on `sys.path` and exposes `bedrock`
(`generate_resort_copy_bedrock`), which owns the print layout goals.

## Where to change things

- Too much mood or filler on medium/large: `grounding.py` (`_ATMOSPHERE`, `_GROUNDING_RULE`).
- Prompt wording for a pass: `writing.py` (or `notes.py` for the search pass).
- How many facts, pages, or beats per tier: `config.py`.
- Which sites or words count as a resort source: `snippets.py`, `web_fetch.py`.

## Running it on the AI boxes (3070 and homeserver)

Nothing here requires the laptop. Two boxes carry the same stack and run
**independently**; neither talks to the other's Ollama or SearXNG.

| Box | Address | Hostname |
| --- | --- | --- |
| OptiPlex 3070 | `192.168.68.59` | `3070` |
| homeserver | `192.168.68.58` | `homeserver` |

Run the process *on* a box so checkpoints stay on that machine when the PC
sleeps. Identical layout on both:

| Path | Role |
| --- | --- |
| `~/ski/repo/` | Scripts + parquet input |
| `~/ski/venv/` | Python deps (`pandas`, `pyarrow`, `ddgs`) |
| `~/ski/output/` | Checkpoints and `key_points.jsonl` (auto when hostname is `3070` or `homeserver`) |
| `~/ski/cache/` | Fetched-page and registry cache (auto on those hosts) |
| `~/ski/searxng/` | Local SearXNG on `127.0.0.1:8888` |
| `~/ollama/` | Ollama binary, models, `start.sh` (cron `@reboot`) |
| `~/ski/run_state.sh` | Runner: `run_state.sh "<State>"` writes `output/ollama_resort_copy/<state>.jsonl` |

| Setting | Default | Purpose |
| --- | --- | --- |
| `OLLAMA_HOST` | `http://192.168.68.59:11434` | Where the model runs. On a box itself use `http://127.0.0.1:11434`. |
| `AI_BOX` | unset | `1` forces AI-box defaults on any host, `0` forces them off. |
| `GLOBALS_SKI_OUTPUT` | `~/ski/output` on an AI box, else `output` | Override checkpoint root. |
| `GLOBALS_SKI_CACHE` | `~/ski/cache` on an AI box, else `cache` | Override fetch cache root. |
| `BRAVE_API_KEY` | unset | Optional Brave Search API (still useful alongside SearXNG). |
| `SEARXNG_URL` | unset | Local SearXNG base URL (on a box: `http://127.0.0.1:8888`). Prefer this over DuckDuckGo. |

### Keeping the two boxes apart

`cli._finished_ids` reads one JSONL back to skip done resorts. Two processes
appending the same file will double-work and interleave lines, so give each box
its own slice and its own output file:

```bash
# on 3070
~/ski/run_state.sh Vermont
# on homeserver
~/ski/run_state.sh "New Hampshire"
```

Both boxes leave the house on one WAN address, so two SearXNG instances share
upstream rate limits. If engines start returning 429s, raise `--delay-seconds`.

On the box:

```bash
cd ~/ski/repo
source ~/ski/venv/bin/activate
export OLLAMA_HOST=http://127.0.0.1:11434
python scripts/generate_resort_copy_ollama.py \
    -i output/combined/ski_areas_analyzed.parquet --batch 50
```

Then close the laptop. The batch writes per-resort checkpoints as it goes, so an
interrupted box resumes with the same command: finished ids are skipped by reading
the JSONL output back (`cli._finished_ids`).

Batch output is append-only and safe to leave running overnight. To keep it
running after you disconnect, start it under `nohup`, `tmux`, or systemd:

```bash
tmux new -s copy
# ... run the command above ...
# Ctrl-b d to detach; tmux attach -t copy to come back
```

### Note on search

Prefer the box's own SearXNG (`SEARXNG_URL=http://127.0.0.1:8888`). DuckDuckGo
rate-limits aggressively and often returns empty under batch load. Brave remains
optional via `BRAVE_API_KEY`. Follow-up / sticky queries use SearXNG when the
env var is set.

### Facts table (filter chips / price strip)

Harvested comparable facts, not the article body:

```bash
python scripts/resort_copy/facts_table.py --ingest --copy output/ollama_resort_copy/vermont.jsonl
python scripts/resort_copy/facts_table.py --query --state Vermont --pass Ikon
python scripts/resort_copy/facts_table.py --stats --state Vermont
python scripts/resort_copy/facts_table.py --serve
# GET http://127.0.0.1:8091/resorts/facts?state=Vermont&amenity=night_skiing&price_band=$$
```

On an AI box the JSONL sources are `~/ski/output/resort_filters/filters.jsonl` and
`~/ski/output/resort_key_points/key_points.jsonl`. The OSM trail/lift table stays separate.

Upload the same rows to DynamoDB `atlas-ResortFacts` (same key as wiki/amenities: `winterSportsId`):

```bash
python scripts/resort_copy/facts_table.py --create-table
python scripts/resort_copy/facts_table.py --ingest --push --copy output/ollama_resort_copy/vermont.jsonl
python scripts/resort_copy/facts_table.py --set 1094522661 night_skiing true
# or set RESORT_FACTS_PUSH=1 on the copy pipeline (harvest skips locked wiki cells)
```
