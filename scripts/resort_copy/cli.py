"""Command-line entry point, batch loop, and test-row picking."""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from . import bedrock

from .config import (
    DEFAULT_HOST,
    DEFAULT_MODEL,
    DEFAULT_NUM_CTX,
    TIER_CHOICES,
    _cell,
    output_path,
)
from .amenity_store import (
    TABLE_NAME,
    push_rows,
)
from .filters import (
    filter_row_from_result,
    upsert_filter_row,
)
from .facts import persist_result
from .key_points import (
    upsert_key_points,
)
from .ollama_client import (
    list_models,
    ollama_base_url,
    ollama_chat,
    unload_model,
)
from .pipeline import (
    key_point_record,
    run_passes,
)
from .queries import (
    _known_state_country,
    add_web_snippets,
    dossier_queries,
)
from .stay22 import (
    add_stay22_lodging_snippet,
)


def _resort_name(row: dict[str, Any]) -> str | None:
    """Skip bare OpenStreetMap ids that were stored in the name column."""
    for key in ("english_name", "name"):
        text = _cell(row, key)
        if text and not text.isdigit():
            return text
    return None


def _round_robin(rows: list[dict[str, Any]], n: int) -> list[dict[str, Any]]:
    """Spread a sample across countries instead of taking the first rows of one nation."""
    if n <= 0 or not rows:
        return []
    by_country: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_country.setdefault(_cell(row, "country") or "?", []).append(row)
    for group in by_country.values():
        group.sort(key=lambda row: (_resort_name(row) or "").lower())
    countries = sorted(by_country)
    picked: list[dict[str, Any]] = []
    index = 0
    while len(picked) < n and any(by_country.values()):
        group = by_country[countries[index % len(countries)]]
        index += 1
        if group:
            picked.append(group.pop(0))
        if index > n * len(countries) + 5:
            break
    return picked


# 50-resort mix across the three plates. There is no mega plate.
_TEST_QUOTA: dict[str, int] = {"small": 18, "medium": 16, "large": 16}


def pick_test_rows(rows: list[dict[str, Any]], n: int = 50) -> list[dict[str, Any]]:
    usable = [
        row
        for row in rows
        if not bedrock._is_not_downhill_row(row) and _resort_name(row)
    ]
    buckets: dict[str, list[dict[str, Any]]] = {tier: [] for tier in _TEST_QUOTA}
    for row in usable:
        tier = bedrock.layout_tier_for_parquet_row(row)
        buckets.setdefault(tier, []).append(row)
    picked: list[dict[str, Any]] = []
    seen: set[str] = set()

    def row_id(row: dict[str, Any]) -> str:
        return _cell(row, "winter_sports_id") or _cell(row, "name") or ""

    scale = n / 50 if n != 50 else 1
    for tier, quota in _TEST_QUOTA.items():
        take = max(1, round(quota * scale)) if n >= 4 else n
        for row in _round_robin(buckets.get(tier, []), take):
            key = row_id(row)
            if not key or key in seen:
                continue
            seen.add(key)
            picked.append(row)
            if len(picked) >= n:
                return picked
    if len(picked) < n:
        rest = [row for row in usable if row_id(row) not in seen]
        picked.extend(_round_robin(rest, n - len(picked)))
    return picked[:n]


def _in_state(row: dict[str, Any], wanted: str) -> bool:
    needle = wanted.strip().lower()
    state, _country = _known_state_country(row)
    if (state or "").lower() == needle:
        return True
    region = (_cell(row, "region") or "").lower().replace("_", "-")
    return needle.replace(" ", "-") in region


def _print_key_points(points: dict[str, Any]) -> None:
    for key, value in points.items():
        if not value:
            continue
        if isinstance(value, list):
            shown = ", ".join(
                str(item.get("value") or item.get("url") or item) for item in value if isinstance(item, dict)
            )
            if shown:
                print(f"    {key}: {shown}", file=sys.stderr)
        elif isinstance(value, dict) and value.get("value"):
            print(f"    {key}: {value['value']}", file=sys.stderr)


def _dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One row per resort name and country. Germundo-style duplicates are dropped."""
    seen: set[tuple[str, str]] = set()
    kept: list[dict[str, Any]] = []
    for row in rows:
        name = (_resort_name(row) or "").strip().lower()
        country = (_cell(row, "country") or "").strip().lower()
        if not name:
            continue
        key = (name, country)
        if key in seen:
            print(f"dedupe skip {name}", file=sys.stderr)
            continue
        seen.add(key)
        kept.append(row)
    return kept


def _finished_ids(path: Path) -> set[str]:
    """Skip resorts that already finished or were rejected as placeholders."""
    done: set[str] = set()
    if not path.is_file():
        return done
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        wid = item.get("winterSportsId")
        if not wid:
            continue
        if item.get("ok") or item.get("status") in {"complete", "skipped"}:
            done.add(str(wid))
    return done


def _write_json_checkpoint(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_path = Path(handle.name)
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
        temporary_path = None
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()


def _resolve_inputs(args: argparse.Namespace) -> tuple[str, str | None, str | None, dict[str, Any] | None, str]:
    row: dict[str, Any] | None = None
    if args.input and args.name:
        row = bedrock.load_resort_row(args.input, args.name)
        label = bedrock.row_to_label(row)
        name = str(row.get("english_name") or row.get("name") or args.name)
        state, country = _known_state_country(row)
        tier = bedrock.resolve_layout_tier(args.tier, row)
        return label, state, country, row, tier
    if args.demo_massanutten:
        return "Massanutten, Virginia, United States", "Virginia", "United States", None, args.tier
    if args.demo_query:
        parts = [args.demo_query]
        if args.demo_state:
            parts.append(args.demo_state)
        if args.demo_country:
            parts.append(args.demo_country)
        tier = "small" if args.tier == "auto" else args.tier
        return ", ".join(parts), args.demo_state, args.demo_country, None, tier
    raise SystemExit("Pass --demo-query, --demo-massanutten, or -i with --name.")


def main() -> int:
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
            sys.stderr.reconfigure(encoding="utf-8")
        except Exception:
            pass
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default=None, help=f"Ollama base URL (default {DEFAULT_HOST} or OLLAMA_HOST)")
    ap.add_argument("--model", default=DEFAULT_MODEL, help=f"Ollama model tag (default {DEFAULT_MODEL})")
    ap.add_argument("--search-model", default=None, help="Tag for search and outline. Defaults to --model.")
    ap.add_argument("--writer-model", default=None, help="Tag for the draft and the edit. Defaults to --model.")
    ap.add_argument(
        "--critic-model",
        default=None,
        help="Optional LLM fact checker. Omit this to use the deterministic checker.",
    )
    ap.add_argument(
        "--llm-critic",
        action="store_true",
        help="Run the model critic as well as the deterministic checker.",
    )
    ap.add_argument("--num-ctx", type=int, default=DEFAULT_NUM_CTX, help="Context length; 4096 fits an 8B model on 8 GB")
    ap.add_argument("--timeout", type=float, default=180.0, help="Seconds to wait for each Ollama call")
    ap.add_argument("--snippet-limit", type=int, default=8, help="How many research snippets to send the fact agent")
    ap.add_argument("--list-models", action="store_true")
    ap.add_argument("--smoke", action="store_true", help="One short completion, then exit")
    ap.add_argument(
        "--tier",
        default="auto",
        choices=list(TIER_CHOICES) + ["auto"],
        help="Word ceiling. auto follows the parquet size bucket. A demo with no row uses small.",
    )
    ap.add_argument("--batch", type=int, default=None, metavar="N", help="Run N resorts (50 for the overnight test)")
    ap.add_argument("--state", type=str, default=None, help="Only resorts in this state, in parquet order")
    ap.add_argument("--search-only", action="store_true", help="Search and key points only. Skip outline, draft, and edit.")
    ap.add_argument("--dry-batch", action="store_true", help="Print the batch list and exit. Does not call Ollama.")
    ap.add_argument(
        "--out-jsonl",
        type=Path,
        default=None,
        help="Batch output, one JSON object per line (default output/ollama_resort_copy/test50.jsonl)",
    )
    ap.add_argument("--delay-seconds", type=float, default=1.0, help="Pause between resorts in a batch")
    ap.add_argument("-i", "--input", type=Path, default=None)
    ap.add_argument("--name", type=str, default=None)
    ap.add_argument("--demo-massanutten", action="store_true")
    ap.add_argument("--demo-query", type=str, default=None)
    ap.add_argument("--demo-state", type=str, default=None)
    ap.add_argument("--demo-country", type=str, default=None)
    ap.add_argument("--research-only", action="store_true")
    ap.add_argument("--no-ddg", action="store_true")
    ap.add_argument("--brave-api-key", type=str, default=None)
    ap.add_argument(
        "--searxng-url",
        type=str,
        default=None,
        help="SearXNG base URL (default: env SEARXNG_URL, e.g. http://127.0.0.1:8888)",
    )
    ap.add_argument(
        "--out-json",
        type=Path,
        default=None,
        help="Incremental single-resort JSON checkpoint (default output/ollama_resort_copy/<resort>.json)",
    )
    args = ap.parse_args()

    if args.dry_batch or args.batch or args.state:
        if not args.input:
            raise SystemExit("--batch needs -i/--input parquet")
        count = args.batch if args.batch else 50
        rows = bedrock.load_parquet_resorts_for_batch(args.input, skip_not_downhill=True)
        if args.state:
            chosen = [row for row in rows if _resort_name(row) and _in_state(row, args.state)]
            if args.batch:
                chosen = chosen[: args.batch]
        else:
            chosen = pick_test_rows(rows, count)
        chosen = _dedupe_rows(chosen)
        critic_model = args.critic_model or (args.model if args.llm_critic else None)
        if args.dry_batch:
            for i, row in enumerate(chosen, start=1):
                tier = bedrock.layout_tier_for_parquet_row(row)
                name = _resort_name(row)
                place = ", ".join(p for p in (_cell(row, "state"), _cell(row, "country")) if p)
                print(f"{i:02d}\t{tier}\t{name}\t{place}")
            return 0
        out_path = args.out_jsonl or (
            output_path("ollama_resort_copy", "pennsylvania_search.jsonl")
            if args.search_only and args.state
            else output_path("ollama_resort_copy", "test50.jsonl")
        )
        out_path.parent.mkdir(parents=True, exist_ok=True)
        done = _finished_ids(out_path)
        base = ollama_base_url(args.host)
        brave_key = (args.brave_api_key or os.environ.get("BRAVE_API_KEY") or "").strip() or None
        searx_url = (args.searxng_url or os.environ.get("SEARXNG_URL") or "").strip() or None
        ok = 0
        failed = 0
        started = time.time()
        for index, row in enumerate(chosen, start=1):
            wid = _cell(row, "winter_sports_id") or ""
            if wid and wid in done:
                print(f"[{index}/{len(chosen)}] skip {wid}", file=sys.stderr)
                continue
            name = _resort_name(row) or "Unknown"
            state, country = _known_state_country(row)
            label = ", ".join(p for p in (name, state, country) if p)
            tier = bedrock.resolve_layout_tier(args.tier, row)
            t0 = time.time()
            print(f"[{index}/{len(chosen)}] {label} ({tier})", file=sys.stderr, flush=True)
            result: dict[str, Any] | None = None
            last_error: Exception | None = None
            for attempt in range(3):
                try:
                    snippets = bedrock.gather_research_snippets(
                        name=name,
                        state=state,
                        country=country,
                        brave_api_key=brave_key,
                        use_ddg=not args.no_ddg,
                        searxng_url=searx_url,
                        parquet_row=row,
                    )
                    add_web_snippets(
                        snippets,
                        dossier_queries(name, state, country, tier),
                        searxng_url=searx_url,
                        use_ddg=not args.no_ddg,
                    )
                    add_stay22_lodging_snippet(snippets, name=name, row=row)
                    result = run_passes(
                        resort_label=label,
                        snippets=snippets,
                        tier=tier,
                        base_url=base,
                        model=args.model,
                        num_ctx=args.num_ctx,
                        timeout=args.timeout,
                        snippet_limit=args.snippet_limit,
                        name=name,
                        state=state,
                        country=country,
                        use_ddg=not args.no_ddg,
                        search_model=args.search_model,
                        writer_model=args.writer_model,
                        critic_model=critic_model,
                        search_only=args.search_only,
                    )
                    break
                except Exception as exc:
                    last_error = exc
                    print(f"    attempt {attempt + 1} failed: {exc}", file=sys.stderr, flush=True)
                    if attempt < 2:
                        time.sleep(2 ** attempt)
            if result is None:
                result = {
                    "ok": False,
                    "status": "failed",
                    "winterSportsId": wid,
                    "resort_label": label,
                    "tier": tier,
                    "error": str(last_error),
                    "validation_errors": [],
                    "short_reason": None,
                    "unused_notes": [],
                }
                failed += 1
            else:
                result["winterSportsId"] = wid
                result["pageId"] = bedrock.wiki_page_id_from_row(row)
                if result.get("final"):
                    result["contentMarkdown"] = bedrock.wiki_content_markdown_from_row(row, result["final"])
                if result.get("status") == "complete":
                    result["ok"] = True
                    ok += 1
                else:
                    result["ok"] = False
                    failed += 1
                print(
                    f"    status={result.get('status')} facts={result.get('search_fact_count', 0)}"
                    + (
                        f", {result.get('final_char_count')} characters"
                        if result.get("final_char_count") is not None
                        else ""
                    )
                    + f" in {time.time() - t0:.0f}s",
                    file=sys.stderr,
                    flush=True,
                )
                if args.search_only:
                    _print_key_points(result.get("key_points") or {})
            with out_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(result, ensure_ascii=False) + "\n")
            if result.get("ok") and result.get("key_points"):
                upsert_key_points(
                    key_point_record(
                        result,
                        winter_sports_id=wid or None,
                        page_id=result.get("pageId"),
                    )
                )
            filter_record = None
            if result.get("filters"):
                filter_record = filter_row_from_result(
                    result,
                    winter_sports_id=wid or None,
                    page_id=result.get("pageId"),
                    country=country,
                    state=state,
                )
                upsert_filter_row(filter_record)
                if os.environ.get("RESORT_AMENITY_PUSH") == "1" and filter_record.get("winterSportsId"):
                    try:
                        push_rows([filter_record], table_name=TABLE_NAME)
                    except Exception as exc:
                        print(f"    amenity table: {exc}", file=sys.stderr, flush=True)
            try:
                persist_result(result, filters=filter_record)
            except Exception as exc:
                print(f"    facts table: {exc}", file=sys.stderr, flush=True)
            if args.delay_seconds > 0 and index < len(chosen):
                time.sleep(args.delay_seconds)
        print(
            f"done {ok} ok, {failed} failed, {time.time() - started:.0f}s -> {out_path}",
            file=sys.stderr,
        )
        loaded = [args.search_model or args.model, args.writer_model, args.critic_model]
        seen: set[str] = set()
        for tag in loaded:
            if tag and tag not in seen:
                seen.add(tag)
                unload_model(base, tag)
        return 0 if failed == 0 else 1

    base = ollama_base_url(args.host)
    if args.list_models:
        for name in list_models(base):
            print(name)
        return 0
    if args.smoke:
        text = ollama_chat(
            base_url=base,
            model=args.model,
            system_text="Reply with exactly three words.",
            user_text="Say hello.",
            temperature=0.0,
            num_predict=16,
            num_ctx=args.num_ctx,
            timeout=args.timeout,
        )
        print(text)
        return 0

    label, state, country, row, tier = _resolve_inputs(args)
    if tier == "auto":
        raise SystemExit("--tier auto needs -i and --name.")
    brave_key = (args.brave_api_key or os.environ.get("BRAVE_API_KEY") or "").strip() or None
    searx_url = (args.searxng_url or os.environ.get("SEARXNG_URL") or "").strip() or None
    name = label.split(",")[0].strip()
    critic_model = args.critic_model or (args.model if args.llm_critic else None)
    safe_name = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_") or "resort"
    out_json = args.out_json or output_path("ollama_resort_copy") / f"{safe_name}.json"
    checkpoint_data: dict[str, Any] = {
        "status": "in_progress",
        "stage": "starting_research",
        "resort_label": label,
        "tier": tier,
        "host": base,
        "models": {
            "search": args.search_model or args.model,
            "writer": args.writer_model or args.model,
            "critic": critic_model or "deterministic",
        },
        "progress": {},
    }
    _write_json_checkpoint(out_json, checkpoint_data)

    def save_checkpoint(stage: str, values: dict[str, Any]) -> None:
        progress = values.pop("progress", None)
        if isinstance(progress, dict):
            checkpoint_data["progress"].update(progress)
        checkpoint_data.update(values)
        checkpoint_data["stage"] = stage
        checkpoint_data["status"] = "in_progress"
        _write_json_checkpoint(out_json, checkpoint_data)
        print(f"checkpoint: {stage} -> {out_json}", file=sys.stderr, flush=True)

    try:
        snippets = bedrock.gather_research_snippets(
            name=name,
            state=state,
            country=country,
            brave_api_key=brave_key,
            use_ddg=not args.no_ddg,
            searxng_url=searx_url,
            parquet_row=row,
        )
        add_web_snippets(
            snippets,
            dossier_queries(name, state, country, tier),
            searxng_url=searx_url,
            use_ddg=not args.no_ddg,
        )
        add_stay22_lodging_snippet(snippets, name=name, row=row)
        print(f"research: {len(snippets)} snippets for {label}", file=sys.stderr)
        save_checkpoint(
            "research_complete",
            {
                "progress": {
                    "research_snippets": [
                        {
                            "id": s.get("id"),
                            "source": s.get("source"),
                            "title": s.get("title"),
                            "url": s.get("url"),
                            "text": s.get("text"),
                        }
                        for s in snippets
                    ]
                }
            },
        )
        if args.research_only:
            print(bedrock.format_snippets_for_prompt(snippets))
            checkpoint_data["status"] = "complete"
            checkpoint_data["stage"] = "research_complete"
            _write_json_checkpoint(out_json, checkpoint_data)
            return 0

        result = run_passes(
            resort_label=label,
            snippets=snippets,
            tier=tier,
            base_url=base,
            model=args.model,
            num_ctx=args.num_ctx,
            timeout=args.timeout,
            snippet_limit=args.snippet_limit,
            name=name,
            state=state,
            country=country,
            use_ddg=not args.no_ddg,
            search_model=args.search_model,
            writer_model=args.writer_model,
            critic_model=critic_model,
            checkpoint=save_checkpoint,
        )
    except Exception as exc:
        checkpoint_data["status"] = "failed"
        checkpoint_data["error"] = str(exc)
        try:
            _write_json_checkpoint(out_json, checkpoint_data)
        except OSError as write_error:
            raise RuntimeError(
                f"Generation failed: {exc}; checkpoint write also failed: {write_error}"
            ) from write_error
        raise
    if row is not None:
        result["winterSportsId"] = _cell(row, "winter_sports_id")
        result["pageId"] = bedrock.wiki_page_id_from_row(row)
        if result.get("final"):
            result["contentMarkdown"] = bedrock.wiki_content_markdown_from_row(row, result["final"])
    if result.get("key_points"):
        upsert_key_points(
            key_point_record(
                result,
                winter_sports_id=result.get("winterSportsId"),
                page_id=result.get("pageId"),
            )
        )
    filter_record = None
    if result.get("filters"):
        filter_record = filter_row_from_result(
            result,
            winter_sports_id=result.get("winterSportsId"),
            page_id=result.get("pageId"),
            country=country,
            state=state,
        )
        upsert_filter_row(filter_record)
        if os.environ.get("RESORT_AMENITY_PUSH") == "1" and filter_record.get("winterSportsId"):
            try:
                push_rows([filter_record], table_name=TABLE_NAME)
            except Exception as exc:
                print(f"    amenity table: {exc}", file=sys.stderr)
    try:
        persist_result(result, filters=filter_record if result.get("filters") else None)
    except Exception as exc:
        print(f"    facts table: {exc}", file=sys.stderr)
    result.setdefault("status", "complete")
    result["stage"] = result["status"]
    if "progress" in checkpoint_data:
        result["progress"] = checkpoint_data["progress"]
    _write_json_checkpoint(out_json, result)
    print(f"wrote {out_json} ({result['status']})", file=sys.stderr)
    if not result.get("final"):
        print(result.get("error") or result["status"], file=sys.stderr)
        return 0 if result["status"] == "skipped" else 1
    print(result["final"])
    goal_out = result["layout_goal"]
    print(
        f"\n[{result['final_char_count']} characters ({goal_out['characters'][0]}-{goal_out['characters'][1]}), "
        f"{result['final_line_count']} lines ({goal_out['lines'][0]}-{goal_out['lines'][1]}), "
        f"{result['final_paragraph_count']} paragraphs ({goal_out['paragraphs'][0]}-{goal_out['paragraphs'][1]}), "
        f"{args.model}]",
        file=sys.stderr,
    )
    return 0
