# Resort copy pipeline

Maps each requested fix to the code that now does it.

1. **Quality gate.** `config.py` and `tier_config.json` set search fact minima to 6 / 22 / 45. `pipeline.run_passes` runs topic searches until the minimum is met or the retry count is used up, then returns `status: insufficient_data` and does not write. Placeholder names return `status: skipped`.
2. **Field checks.** `validators.py` (`opening_year`, `ownership_kind`, `price_has_currency`, `distance_has_unit`) and `sanitize_notes`. Failed notes are listed in `validation_errors` and the missing topic is searched again.
3. **Per-topic search.** `queries.topic_queries` builds one query per thin topic, including a Wikipedia query for medium and large. `web_fetch.py` reads Wikipedia and the resort's own host first.
4. **Entity filter.** `snippets._drop_pages_about_other_places` requires the resort name and rejects another country. Fetched pages are no longer exempt. `drop_foreign_sentences` removes a sentence that names a place absent from the notes.
5. **Source quality.** Marketing lines and ticket-reseller pass claims are dropped or down-ranked in `validators.sanitize_notes` and `snippets._snippet_score`.
6. **Section writing.** `writing.write_by_section` writes one paragraph per topic for medium and large. Budgets come from `validators.paragraph_budgets`. Small stays one pass.
7. **Length loop.** `writing.length_control_loop` sends unused notes back to the editor. If the page is still short, `short_reason` is `facts_exhausted` or `writer_underused_notes`. The loop does not add filler.
8. **Deterministic critic.** `validators.deterministic_critique` checks numbers, names, clichés, and kilometre contradictions. It is the default. `--llm-critic` adds the model critic, and its items must be phrases that appear in the draft.
9. **JSON failures.** `ollama_client._chat_json` uses Ollama JSON mode, strips fences, retries once with the broken reply, and logs the raw text. A bad row is written with `status: failed` instead of stopping the batch.
10. **Batch robustness.** `cli._dedupe_rows` collapses duplicate names. Each resort is tried three times with backoff. `_finished_ids` skips `complete` and `skipped` rows. `stage_timings` is on each result. Targets and the recommended models live in `tier_config.json`.
11. **Style.** Writer prompts in `writing.py` forbid opening on the resort name, the clichés nestled / hidden gem / world-class, and any fact that is not in the notes.

The mega plate is not a tier. Old `mega_resort` size labels still map to the large plate in the shared layout table.

Amenity filters live in `filters.py` and, when `RESORT_AMENITY_PUSH=1`, in the DynamoDB table `atlas-ResortAmenities` keyed by `winterSportsId`. An AI write skips a cell a user has locked. Trail count, vertical drop, and lift count stay on the OSM and DEM table.
