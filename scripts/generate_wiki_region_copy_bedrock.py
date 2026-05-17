#!/usr/bin/env python3
"""
Generate country / state-province wiki overview prose (not single-resort profiles).

Uses a dedicated Bedrock pipeline: regional research queries, snippet filtering to drop
dominant single-resort SEO hits, region-themed miners, and writer rules focused on
*why visit* the country or state for skiing — not Vail-style resort pages or repeated
lift-price disclaimer boilerplate.

Output JSON matches globalskiatlas.resort_wiki_content_v1 for wiki-bulk-update-resort-content.js.

Usage:
  python scripts/generate_wiki_region_copy_bedrock.py \\
    -i output/combined/ski_areas_analyzed.parquet \\
    --out-json output/wiki_regions_full.json

  # Smoke test
  python scripts/generate_wiki_region_copy_bedrock.py -i output/combined/ski_areas_analyzed.parquet \\
    --out-json output/wiki_regions_smoke.json --max-countries 2 --max-state-pairs 3
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Shared Bedrock + search helpers from resort script (do not use resort run_pipeline here).
from generate_resort_copy_bedrock import (
    DDGS,
    DEFAULT_CHEAP_MODEL,
    DEFAULT_WRITER_MODEL,
    REGION_WIKI_LAYOUT_TIER,
    TIER_WORD_BANDS,
    _BANNED_TRAVEL_CLICHES_RULE,
    _ATLAS_PROSE_LEADIN_RULE,
    _parse_json_object,
    _snippets_meaningful,
    _word_count,
    converse_with_backoff,
    fetch_brave_web_snippets,
    fetch_duckduckgo_snippets,
    fetch_wikipedia_snippets,
    strip_banned_travel_cliches,
    wiki_country_page_id,
    wiki_state_page_id,
)

REGION_TIER = REGION_WIKI_LAYOUT_TIER
REGION_WMIN, REGION_WMAX = TIER_WORD_BANDS[REGION_TIER]

_REGION_SCOPE_RULE = (
    "SCOPE: This is a country or state/province OVERVIEW for skiers planning a trip across "
    "many hills — not a page for one ski resort. Lead with why the region is worth visiting: "
    "mountain geography, snow character, cultural context, variety of areas, access hubs. "
    "Name at most TWO individual ski areas in the entire piece, only in passing as examples; "
    "never devote a paragraph to one resort (no resort history, pass roster, lodging, or "
    "lift operations for a single hill). "
    "Never write boilerplate such as 'lift, lodging, and pass prices change by season and "
    "must be verified on official sites' or similar price-disclaimer sentences."
)

_PRICE_DISCLAIMER_RE = re.compile(
    r"\b(?:current\s+)?(?:lift|lodging|pass).*?(?:verified|confirm(?:ed)?)\s+on\s+official\s+sites?\.?",
    re.IGNORECASE | re.DOTALL,
)


def strip_region_boilerplate(text: str) -> str:
    s = strip_banned_travel_cliches(text)
    s = _PRICE_DISCLAIMER_RE.sub("", s)
    s = re.sub(r"[ \t]{2,}", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def _wiki_slug(value: str) -> str:
    s = (value or "").strip().lower()
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"[^a-z0-9-]", "", s)
    return re.sub(r"-+", "-", s).strip("-") or "unknown"


def load_unique_countries_and_states(parquet_path: Path) -> tuple[list[str], list[tuple[str, str]]]:
    import pandas as pd

    df = pd.read_parquet(parquet_path)
    countries = sorted({str(x).strip() for x in df["country"].dropna() if str(x).strip()})
    pairs: list[tuple[str, str]] = []
    if "state" in df.columns:
        for r in df[["state", "country"]].dropna().to_dict("records"):
            s, c = str(r.get("state", "")).strip(), str(r.get("country", "")).strip()
            if s and c:
                pairs.append((s, c))
    return countries, sorted(set(pairs))


def downhill_counts(parquet_path: Path) -> tuple[dict[str, int], dict[tuple[str, str], int]]:
    import pandas as pd

    df = pd.read_parquet(parquet_path)
    if "resort_type" in df.columns:
        rt = df["resort_type"].fillna("").astype(str).str.lower().str.strip()
        df = df.loc[rt != "not a downhill ski resort"]
    by_c: dict[str, int] = {}
    if "country" in df.columns:
        by_c = {
            str(k).strip(): int(v)
            for k, v in df.groupby(df["country"].astype(str).str.strip()).size().items()
            if str(k).strip()
        }
    by_sc: dict[tuple[str, str], int] = {}
    if "state" in df.columns and "country" in df.columns:
        g = df.groupby(
            [df["state"].astype(str).str.strip(), df["country"].astype(str).str.strip()]
        ).size()
        for (s, c), v in g.items():
            if s and c:
                by_sc[(s, c)] = int(v)
    return by_c, by_sc


def resort_names_in_region(
    parquet_path: Path,
    *,
    country: str,
    state: str | None,
    limit: int = 120,
) -> list[str]:
    """Resort titles in dataset — used to filter single-resort snippets and ban centering copy on them."""
    import pandas as pd

    df = pd.read_parquet(parquet_path)
    if "resort_type" in df.columns:
        rt = df["resort_type"].fillna("").astype(str).str.lower().str.strip()
        df = df.loc[rt != "not a downhill ski resort"]
    df = df.loc[df["country"].astype(str).str.strip() == country.strip()]
    if state is not None:
        df = df.loc[df["state"].astype(str).str.strip() == state.strip()]
    names: list[str] = []
    for col in ("english_name", "name", "Name"):
        if col in df.columns:
            for v in df[col].dropna().astype(str):
                t = v.strip()
                if t and t not in names:
                    names.append(t)
    names.sort(key=len, reverse=True)
    return names[:limit]


def research_queries_country(country: str) -> list[str]:
    c = country.strip()
    return [
        f"{c} skiing travel guide",
        f"why ski in {c}",
        f"{c} mountains winter tourism",
        f"alpine skiing {c} regions",
        f"{c} ski season climate",
    ]


def research_queries_state(state: str, country: str) -> list[str]:
    s, co = state.strip(), country.strip()
    return [
        f"{s} {co} skiing overview",
        f"ski areas in {s} {co}",
        f"{s} Rocky Mountains winter" if "colorado" in s.lower() else f"{s} mountains ski",
        f"why ski {s} {co}",
        f"{s} {co} winter sports travel",
    ]


def wikipedia_query_country(country: str) -> str:
    return country.strip()


def wikipedia_query_state(state: str, country: str) -> str:
    co = country.strip()
    s = state.strip()
    if re.search(r"united states|usa|u\.s\.", co, re.I):
        return f"{s} (U.S. state)"
    return f"{s}, {co}"


def _region_dataset_stub(
    *, kind: str, country: str, state: str | None, resort_count: int
) -> dict[str, Any]:
    if kind == "country":
        title = f"{country} — atlas dataset"
        text = (
            f"The Global Ski Atlas dataset lists {resort_count} downhill ski areas in {country}. "
            "Use this count once in overview prose; do not invent a different number."
        )
    else:
        title = f"{state}, {country} — atlas dataset"
        text = (
            f"The dataset lists {resort_count} downhill ski areas in {state}, {country}. "
            "Overview scope only — not a single-resort article."
        )
    return {"id": "P1", "source": "dataset", "title": title, "url": "", "text": text}


def filter_snippets_for_region(
    snippets: list[dict[str, Any]],
    *,
    region_label: str,
    resort_names: list[str],
) -> list[dict[str, Any]]:
    """Drop search hits that are clearly one-resort pages dominating regional overview."""
    rl = region_label.lower()
    kept: list[dict[str, Any]] = []
    for s in snippets:
        if s.get("source") == "dataset":
            kept.append(s)
            continue
        title = (s.get("title") or "").strip()
        tl = title.lower()
        text_head = ((s.get("text") or "")[:400]).lower()
        if rl in tl or rl in text_head:
            kept.append(s)
            continue
        if s.get("source") == "wikipedia":
            # Keep state/country articles; drop if title is clearly a ski resort page.
            if any(
                x in tl
                for x in (" ski resort", " ski area", " mountain resort", " snow resort")
            ):
                if rl not in tl:
                    continue
            kept.append(s)
            continue
        dominated = False
        for name in resort_names:
            nl = name.lower()
            if len(nl) < 5:
                continue
            if tl == nl or tl.startswith(nl + " ") or tl.startswith(nl + "-"):
                dominated = True
                break
            if " ski resort" in tl and nl in tl and rl not in tl:
                dominated = True
                break
        if not dominated:
            kept.append(s)
    return kept if kept else snippets


def gather_snippets_country(
    country: str,
    *,
    resort_count: int,
    resort_names: list[str],
    brave_api_key: str | None,
    use_ddg: bool,
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    try:
        merged.extend(fetch_wikipedia_snippets(wikipedia_query_country(country)))
    except Exception:
        pass
    queries = research_queries_country(country)
    if brave_api_key:
        try:
            merged.extend(fetch_brave_web_snippets(queries[0], brave_api_key))
        except Exception:
            pass
    if use_ddg and DDGS is not None:
        try:
            merged.extend(
                fetch_duckduckgo_snippets(queries, max_per_query=4, max_total=18, id_start=1)
            )
        except Exception as e:
            print(f"ddg country: {e}", file=sys.stderr)
    merged.append(
        _region_dataset_stub(kind="country", country=country, state=None, resort_count=resort_count)
    )
    merged = filter_snippets_for_region(merged, region_label=country, resort_names=resort_names)
    for i, s in enumerate(merged, start=1):
        s["id"] = f"S{i}"
    return merged


def gather_snippets_state(
    state: str,
    country: str,
    *,
    resort_count: int,
    resort_names: list[str],
    brave_api_key: str | None,
    use_ddg: bool,
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    label = f"{state}, {country}"
    try:
        merged.extend(fetch_wikipedia_snippets(wikipedia_query_state(state, country)))
    except Exception:
        pass
    queries = research_queries_state(state, country)
    if brave_api_key:
        try:
            merged.extend(fetch_brave_web_snippets(queries[0], brave_api_key))
        except Exception:
            pass
    if use_ddg and DDGS is not None:
        try:
            merged.extend(
                fetch_duckduckgo_snippets(queries, max_per_query=4, max_total=18, id_start=1)
            )
        except Exception as e:
            print(f"ddg state: {e}", file=sys.stderr)
    if not _snippets_meaningful(merged, min_chars=140) and use_ddg and DDGS is not None:
        fb = [f"{state} {country} mountain skiing", f"{state} winter tourism"]
        try:
            merged.extend(
                fetch_duckduckgo_snippets(fb, max_per_query=3, max_total=10, id_start=len(merged) + 1)
            )
        except Exception:
            pass
    merged.append(
        _region_dataset_stub(
            kind="state", country=country, state=state, resort_count=resort_count
        )
    )
    merged = filter_snippets_for_region(merged, region_label=state, resort_names=resort_names)
    for i, s in enumerate(merged, start=1):
        s["id"] = f"S{i}"
    return merged


def format_snippets_block(snippets: list[dict[str, Any]]) -> str:
    lines = []
    for s in snippets:
        lines.append(
            f"[{s['id']}] source={s.get('source', '?')} | {s.get('title', '')}\n{s.get('text', '')}\n"
        )
    return "\n".join(lines).strip()


def _miner(
    snippets_block: str,
    *,
    system: str,
    region: str,
    model_id: str,
    max_tokens: int = 500,
) -> dict[str, Any]:
    raw, _ = converse_with_backoff(
        region=region,
        model_id=model_id,
        system_text=system,
        user_text=f"SNIPPETS:\n{snippets_block}\n\nReturn JSON only.",
        max_tokens=max_tokens,
        temperature=0.1,
    )
    return _parse_json_object(raw)


def miner_region_landscape(snippets_block: str, *, region: str, model_id: str) -> dict[str, Any]:
    return _miner(
        snippets_block,
        region=region,
        model_id=model_id,
        system=(
            "Extract REGIONAL geography and mountain setting from SNIPPETS only (country or state scope). "
            "Facts: ranges, elevation character, snow climate, scenery, borders, notable valleys — "
            "not one resort's vertical or lift count. "
            'Schema: {"facts":[{"fact":"<=32 words","snippet_ids":["S1"]}],"insufficient":false,"gap":null}. '
            "At most 4 facts. JSON only."
        ),
    )


def miner_region_ski_culture(snippets_block: str, *, region: str, model_id: str) -> dict[str, Any]:
    return _miner(
        snippets_block,
        region=region,
        model_id=model_id,
        system=(
            "Extract REGIONAL ski culture from SNIPPETS: typical season months, variety of hill sizes, "
            "Olympics or major events if stated for the region, skiing history at state/country level. "
            "Do not extract single-resort pass products or one resort's opening dates. "
            'Schema: {"facts":[{"fact":"<=32 words","snippet_ids":["S2"]}],"insufficient":false,"gap":null}. '
            "At most 4 facts. JSON only."
        ),
    )


def miner_region_visit_why(snippets_block: str, *, region: str, model_id: str) -> dict[str, Any]:
    return _miner(
        snippets_block,
        region=region,
        model_id=model_id,
        system=(
            "Extract why a skier should visit this REGION from SNIPPETS: trip character, towns/cities, "
            "culture, food, cross-border access, airports or highway corridors, visitor praise at regional level. "
            "Skip single-resort marketing pages. "
            'Schema: {"facts":[{"fact":"<=32 words","snippet_ids":["S3"]}],"insufficient":false,"gap":null}. '
            "At most 5 facts. JSON only."
        ),
    )


def curator_region(
    miners: dict[str, Any],
    *,
    region_title: str,
    region: str,
    model_id: str,
) -> dict[str, Any]:
    system = (
        "Merge miner JSON into curator bullets for a REGION wiki page (country or state overview). "
        "No new facts. Drop bullets that describe one named ski resort in detail. "
        "Prioritize: why visit, geography, ski culture, access — not lift ticket prices. "
        f"{_REGION_SCOPE_RULE} "
        f'Schema: {{"bullets":[{{"text":"string","snippet_ids":["S1"]}}],'
        f'"use_in_lead_index":0,"region_title":"{region_title}",'
        f'"word_min":{REGION_WMIN},"word_max":{REGION_WMAX}}}. '
        "At most 8 bullets; each a distinct theme."
    )
    raw, _ = converse_with_backoff(
        region=region,
        model_id=model_id,
        system_text=system,
        user_text=json.dumps(miners, ensure_ascii=False),
        max_tokens=900,
        temperature=0.2,
    )
    return _parse_json_object(raw)


def writer_region(
    curator: dict[str, Any],
    *,
    region_title: str,
    region: str,
    model_id: str,
) -> str:
    system = (
        "You write Global Ski Atlas REGION overview pages — why skiers visit this country or state, "
        "in a confident travel-guide voice (like a good Andorra country intro: place, mountains, variety, season). "
        f"{_REGION_SCOPE_RULE} "
        "Do not recap trail counts, vertical, or lift totals. "
        "Do not name Epic/Ikon/Indy unless a bullet explicitly states regional pass context. "
        f"Target {REGION_WMIN}-{REGION_WMAX} words; never exceed {REGION_WMAX}. "
        f"{_BANNED_TRAVEL_CLICHES_RULE} "
        f"{_ATLAS_PROSE_LEADIN_RULE} "
        "Plain paragraphs only; no markdown title (added separately)."
    )
    user = f"Region: {region_title}\n\nCURATOR_JSON:\n{json.dumps(curator, ensure_ascii=False)}"
    raw, _ = converse_with_backoff(
        region=region,
        model_id=model_id,
        system_text=system,
        user_text=user,
        max_tokens=1200,
        temperature=0.38,
    )
    return raw.strip()


def editor_region(
    draft: str,
    *,
    region_title: str,
    curator: dict[str, Any],
    region: str,
    model_id: str,
) -> str:
    system = (
        "Finishing editor for a REGION wiki page. "
        f"{_REGION_SCOPE_RULE} "
        "If any paragraph centers on one ski resort (e.g. only Vail, only Cortina), rewrite to statewide/countrywide scope. "
        "Delete price-verification boilerplate and resort pass roster paragraphs. "
        f"Word count {REGION_WMIN}-{REGION_WMAX}. "
        f"{_BANNED_TRAVEL_CLICHES_RULE} "
        "Plain paragraphs only."
    )
    user = (
        f"Region: {region_title}\n\nCURATOR_JSON:\n{json.dumps(curator, ensure_ascii=False)}\n\n"
        f"DRAFT:\n{draft}\n"
    )
    raw, _ = converse_with_backoff(
        region=region,
        model_id=model_id,
        system_text=system,
        user_text=user,
        max_tokens=1200,
        temperature=0.3,
    )
    return raw.strip()


def trim_region(text: str, *, region: str, model_id: str) -> str:
    wc = _word_count(text)
    if wc <= REGION_WMAX:
        return text
    raw, _ = converse_with_backoff(
        region=region,
        model_id=model_id,
        system_text=(
            f"Cut to at most {REGION_WMAX} words by deleting whole sentences. "
            "Keep regional 'why visit' content; drop single-resort paragraphs and price disclaimers. "
            "Do not add new sentences."
        ),
        user_text=text,
        max_tokens=1000,
        temperature=0.12,
    )
    return raw.strip()


def expand_region(
    text: str,
    *,
    curator: dict[str, Any],
    region_title: str,
    region: str,
    model_id: str,
) -> str:
    wc = _word_count(text)
    if wc >= REGION_WMIN:
        return text
    raw, _ = converse_with_backoff(
        region=region,
        model_id=model_id,
        system_text=(
            f"Expand to at least {REGION_WMIN} words, max {REGION_WMAX}, using unused CURATOR_JSON bullets. "
            f"{_REGION_SCOPE_RULE} "
            "Add regional visit angles only — no price disclaimer sentences."
        ),
        user_text=(
            f"Region: {region_title}\n\nCURATOR_JSON:\n{json.dumps(curator, ensure_ascii=False)}\n\n"
            f"DRAFT:\n{text}\n"
        ),
        max_tokens=1200,
        temperature=0.35,
    )
    out = raw.strip()
    if _word_count(out) > REGION_WMAX:
        out = trim_region(out, region=region, model_id=model_id)
    return out


def run_region_pipeline(
    *,
    region_title: str,
    snippets: list[dict[str, Any]],
    bedrock_region: str,
    cheap_model: str,
    writer_model: str,
) -> dict[str, Any]:
    block = format_snippets_block(snippets)
    miners = {
        "landscape": miner_region_landscape(block, region=bedrock_region, model_id=cheap_model),
        "ski_culture": miner_region_ski_culture(block, region=bedrock_region, model_id=cheap_model),
        "visit_why": miner_region_visit_why(block, region=bedrock_region, model_id=cheap_model),
    }
    curator = curator_region(
        miners, region_title=region_title, region=bedrock_region, model_id=writer_model
    )
    draft = writer_region(
        curator, region_title=region_title, region=bedrock_region, model_id=writer_model
    )
    final = editor_region(
        draft,
        region_title=region_title,
        curator=curator,
        region=bedrock_region,
        model_id=writer_model,
    )
    if _word_count(final) > REGION_WMAX:
        final = trim_region(final, region=bedrock_region, model_id=cheap_model)
    if _word_count(final) < REGION_WMIN:
        final = expand_region(
            final,
            curator=curator,
            region_title=region_title,
            region=bedrock_region,
            model_id=writer_model,
        )
    final = strip_region_boilerplate(final)
    return {
        "final": final,
        "final_word_count": _word_count(final),
        "curator": curator,
        "miners": miners,
        "draft": draft,
    }


def export_regions(
    *,
    parquet_path: Path,
    out_path: Path,
    bedrock_region: str,
    cheap_model: str,
    writer_model: str,
    brave_api_key: str | None,
    use_ddg: bool,
    delay_seconds: float,
    max_countries: int | None,
    max_state_pairs: int | None,
    countries_only: bool,
    states_only: bool,
) -> int:
    countries, state_pairs = load_unique_countries_and_states(parquet_path)
    cnt_c, cnt_sc = downhill_counts(parquet_path)
    if countries_only:
        state_pairs = []
    if states_only:
        countries = []
    if max_countries is not None:
        countries = countries[: max(0, max_countries)]
    if max_state_pairs is not None:
        state_pairs = state_pairs[: max(0, max_state_pairs)]
    total = len(countries) + len(state_pairs)
    if total == 0:
        print("Nothing to process.", file=sys.stderr)
        return 1

    items: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    cur = 0

    for country in countries:
        cur += 1
        try:
            rc = int(cnt_c.get(country, 0))
            names = resort_names_in_region(parquet_path, country=country, state=None)
            snippets = gather_snippets_country(
                country,
                resort_count=rc,
                resort_names=names,
                brave_api_key=brave_api_key,
                use_ddg=use_ddg,
            )
            r = run_region_pipeline(
                region_title=country,
                snippets=snippets,
                bedrock_region=bedrock_region,
                cheap_model=cheap_model,
                writer_model=writer_model,
            )
            body = str(r["final"]).strip()
            items.append(
                {
                    "pageId": wiki_country_page_id(country),
                    "winterSportsId": "",
                    "title": country,
                    "contentMarkdown": f"# {country}\n\n{body}",
                    "tier": REGION_TIER,
                    "pageKind": "country",
                    "finalWordCount": r["final_word_count"],
                }
            )
            print(f"[{cur}/{total}] country ok {country}", file=sys.stderr)
        except Exception as e:
            errors.append({"pageKind": "country", "country": country, "error": str(e)})
            print(f"[{cur}/{total}] country ERR {country}: {e}", file=sys.stderr)
        if delay_seconds > 0:
            time.sleep(delay_seconds)

    for state, country in state_pairs:
        cur += 1
        try:
            rc = int(cnt_sc.get((state, country), 0))
            names = resort_names_in_region(parquet_path, country=country, state=state)
            snippets = gather_snippets_state(
                state,
                country,
                resort_count=rc,
                resort_names=names,
                brave_api_key=brave_api_key,
                use_ddg=use_ddg,
            )
            title = state
            r = run_region_pipeline(
                region_title=f"{state}, {country}",
                snippets=snippets,
                bedrock_region=bedrock_region,
                cheap_model=cheap_model,
                writer_model=writer_model,
            )
            body = str(r["final"]).strip()
            items.append(
                {
                    "pageId": wiki_state_page_id(state, country),
                    "winterSportsId": "",
                    "title": title,
                    "contentMarkdown": f"# {title}\n\n{body}",
                    "tier": REGION_TIER,
                    "pageKind": "state",
                    "state": state,
                    "country": country,
                    "finalWordCount": r["final_word_count"],
                }
            )
            print(f"[{cur}/{total}] state ok {state} / {country}", file=sys.stderr)
        except Exception as e:
            errors.append(
                {"pageKind": "state", "state": state, "country": country, "error": str(e)}
            )
            print(f"[{cur}/{total}] state ERR {state}/{country}: {e}", file=sys.stderr)
        if delay_seconds > 0:
            time.sleep(delay_seconds)

    payload = {
        "format": "globalskiatlas.resort_wiki_content_v1",
        "pageScope": "wiki_regions_v2",
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "items": items,
        "errors": errors,
        "counts": {
            "countriesWritten": sum(1 for x in items if x.get("pageKind") == "country"),
            "statesWritten": sum(1 for x in items if x.get("pageKind") == "state"),
            "errors": len(errors),
        },
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {out_path} ({len(items)} items, {len(errors)} errors)", file=sys.stderr)
    return 1 if errors else 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-i", "--input", type=Path, required=True, help="ski_areas_analyzed.parquet")
    ap.add_argument("--out-json", type=Path, required=True, help="Combined export JSON path")
    ap.add_argument("--bedrock-region", default="us-east-1")
    ap.add_argument("--cheap-model", default=DEFAULT_CHEAP_MODEL)
    ap.add_argument("--writer-model", default=DEFAULT_WRITER_MODEL)
    ap.add_argument("--no-ddg", action="store_true")
    ap.add_argument("--brave-api-key", default=None)
    ap.add_argument("--delay-seconds", type=float, default=0.35)
    ap.add_argument("--max-countries", type=int, default=None)
    ap.add_argument("--max-state-pairs", type=int, default=None)
    ap.add_argument("--countries-only", action="store_true")
    ap.add_argument("--states-only", action="store_true")
    args = ap.parse_args()
    brave = (args.brave_api_key or os.environ.get("BRAVE_API_KEY") or "").strip() or None
    return export_regions(
        parquet_path=args.input,
        out_path=args.out_json,
        bedrock_region=args.bedrock_region,
        cheap_model=args.cheap_model,
        writer_model=args.writer_model,
        brave_api_key=brave,
        use_ddg=not args.no_ddg,
        delay_seconds=args.delay_seconds,
        max_countries=args.max_countries,
        max_state_pairs=args.max_state_pairs,
        countries_only=args.countries_only,
        states_only=args.states_only,
    )


if __name__ == "__main__":
    raise SystemExit(main())
