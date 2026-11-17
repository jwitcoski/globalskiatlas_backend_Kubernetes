# Test results (local laptop, 17 Nov 2026)

The OptiPlex at 192.168.68.59 was offline (port 11434 closed). Everything below ran on this laptop. Model sizes are what matter for the 24 GB, no-GPU OptiPlex. Timings are from the laptop's GTX 1660 Ti and are not the OptiPlex's CPU times.

## A. Environment

| Machine | RAM | GPU | Ollama |
|---|---|---|---|
| This laptop (measured) | 16 GB | GTX 1660 Ti, 6 GB | `http://127.0.0.1:11434` |
| OptiPlex 3070 (not measured; your spec) | 24 GB | none | `192.168.68.59:11434` unreachable |

Models already installed: `llama3.1:8b` (Q4_K_M, 4.9 GB), `qwen2.5:14b` (Q4_K_M, 9.0 GB). Pulled this session: `qwen2.5:7b` (4.7 GB). A 14B Q4 weight file fits in 24 GB with an 8k context. A 32B weight file does not.

## B. Unit tests

`python scripts/resort_copy/test_quality.py` — 19 tests, all passed.

Covered: ISO timestamp and post-office year rejected; 1924 kept; San Telmo ownership rejected; price without currency rejected; distance without a unit rejected; Liftopia Epic Pass rejected; marketing line rejected; AfriSki/Lesotho snippet dropped; foreign sentence removed; 208 km rewritten to the note's 248 km; placeholder "A venir" skipped by `run_passes`; fenced JSON parsed.

## C. Search snippets (live DuckDuckGo, one quoted query, no model)

Fact extraction was not run, so this table is snippets in and snippets rejected, not facts per topic.

| Resort | Returned | Kept | Dropped |
|---|---:|---:|---:|
| Mount Buller | 4 | 4 | 0 |
| Cerro Catedral | 4 | 4 | 0 |
| Malbun | 4 | 4 | 0 |
| Ordino-Arcalis | 4 | 1 | 3 |
| Caviahue | 4 | 4 | 0 |
| Ben Eoin | 4 | 4 | 0 |
| Tahko | 4 | 4 | 0 |
| Gulmarg | 4 | 4 | 0 |
| Appi Kogen | 4 | 4 | 0 |

A quoted query is already on the resort name, so most hits survive. Ordino-Arcalis dropped 3 of 4. The AfriSki case is the unit test: a fetched Lesotho snippet is no longer kept just because the page was fetched.

## D. Writer comparison

Same 14 fixture notes. Small target is 1450–1650 characters. `character_pct` is characters divided by 1450.

| Model | Mode | Chars | Paragraphs | Lines | % of small minimum | Notes unused | Unsupported |
|---|---|---:|---:|---:|---:|---:|---:|
| llama3.1:8b | single | 617 | 3 | 12 | 42.6 | 1 | 0 |
| qwen2.5:7b | single | 872 | 1 | 11 | 60.1 | 1 | 1 |
| qwen2.5:14b | single | 1229 | 2 | 18 | 84.8 | 1 | 0 |

## E. Ablation (llama3.1:8b, medium target 3700 characters)

The fixture has 14 notes. That is below the medium minimum of 22, so a real run would skip writing. The ablation forces the writer anyway, to see what it does with a thin note set.

| Mode | Chars | % of 3700 | Paragraphs | Unsupported | short_reason |
|---|---:|---:|---:|---:|---|
| single pass | 1261 | 34.1 | 6 | 3 | would be set |
| section by section | 999 | 27.0 | 6 | 0 | |
| sections + length loop | 999 | 27.0 | 6 | 0 | writer_underused_notes |

Section writing was shorter and did not invent. The single pass was longer and added 3 unsupported claims. The length loop did not grow the page once the model stopped using new notes. It did not pad.

Critic, on the draft "Mount Buller opened in 1968. The drive is 208 km from Melbourne. The fireplaces make the lodge cozy."

| Checker | What it did |
|---|---|
| Deterministic | Flagged 208 (notes say 248) and the opening that repeats the title. Did not call the true 1968 sentence unsupported. |
| llama3.1:8b | Called the true sentence "Mount Buller opened in 1968" unsupported, and told the editor to add fireplaces. |

## F. Regression

| Case | Result |
|---|---|
| "A venir" | `run_passes` returns `status: skipped`, `error: placeholder_name`, no model call |
| Duplicate names | `cli._dedupe_rows` keeps one row per name + country |
| JSON fence | `_parse_json_loose` accepts a fenced object. A bad reply is logged and repaired once in `_chat_json` |
| Germundo, Ranch Ski Center, Klinovec, Abries | Not re-run live. Thin or bad notes now stop at `insufficient_data` or `failed` instead of inventing a page |

## G. Full 15-resort run

Not run. One medium resort now searches until it has 22 valid notes or gives up, then may write several section calls. Fifteen resorts on this laptop would be hours, and the OptiPlex was offline. The gate is the point: a resort under the fact minimum is skipped instead of filled in.

## Recommended models

| Stage | Model | Why |
|---|---|---|
| Search, outline | `llama3.1:8b` | Many JSON calls. The 8B Q4 file is about 5 GB, so it fits the 24 GB box with room for the system. |
| Writer, editor | `qwen2.5:14b` | Closest to the small target (84.8%) with no unsupported numbers or names. The 9 GB Q4 file fits in 24 GB. |
| Critic | deterministic (no model) | The 8B critic contradicted a supported sentence and proposed a fireplace. |

Pass `--writer-model qwen2.5:14b` on the command line. The CLI default remains `llama3.1:8b` so a machine that has not pulled the 14B still runs. `tier_config.json` records the recommendation.
