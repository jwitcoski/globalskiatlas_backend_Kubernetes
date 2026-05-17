#!/usr/bin/env python3
"""
Generate atlas-style resort prose via Amazon Bedrock (Nova), with multi-source web snippets
as grounding (Wikipedia when it exists, plus web search — not a substitute for editorial QA).

Research sources (merged into one SNIPPETS block for miners):
  - Wikipedia summaries (often empty for tiny hills; ids W1, W2, …)
  - DuckDuckGo web results via the `ddgs` package (no API key; ids D1, D2, …) — queries include passes,
    lift prices, airports/shuttles, season length, lodging/dining prices, and reviews
  - Optional Brave Web Search API if BRAVE_API_KEY is set (ids B1, B2, …)
  - If the first search round is thin, extra Wikipedia / DDG queries run; if still empty and a parquet row
    is available (batch / -i --name), a dataset stub snippet (source=dataset) supplies minimal factual grounding.

Pipeline: snippets -> parallel cheap miners (history, landform, ops, access, pass network, visitor economy,
season, review sentiment — subset depends on layout tier) -> curator -> draft -> critic -> editor ->
trim-to-max -> expand-to-min (when still short). All dollar amounts and pass claims must come from snippets.

Usage:
  # API smoke test (one short completion)
  python scripts/generate_resort_copy_bedrock.py --converse-smoke

  # Fetch research only (no Bedrock); good for Eagle Rock–style spots with no wiki article
  python scripts/generate_resort_copy_bedrock.py --research-only --demo-query "Eagle Rock ski resort Pennsylvania"

  # All four print plates (small → mega) in one run; optional JSON file
  python scripts/generate_resort_copy_bedrock.py --demo-query "Jay Peak" --demo-state Vermont --all-plate-tiers --out-json output/jay_peak_copy.json

  # One row from combined parquet: layout tier from resort size (--tier auto) or fixed --tier
  python scripts/generate_resort_copy_bedrock.py -i output/combined/ski_areas_analyzed.parquet --name "Killington" --tier auto --out-json output/killington.json

  # Batch: one JSON file for all rows (for wiki-bulk-update-resort-content.js); first 10 only
  python scripts/generate_resort_copy_bedrock.py -i output/combined/ski_areas_analyzed.parquet --batch-all \\
    --batch-out-combined output/resort_wiki_content_first10.json --batch-limit 10

  # Wiki country-* and state-*-* overview pages (dedicated regional pipeline — not resort run_pipeline):
  python scripts/generate_wiki_region_copy_bedrock.py -i output/combined/ski_areas_analyzed.parquet \\
    --out-json output/wiki_regions.json --max-countries 2 --max-state-pairs 3

Requires: boto3, ddgs (or legacy duckduckgo-search), pandas (parquet mode only). HTTP uses stdlib urllib. IAM: bedrock:InvokeModel on chosen models.
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
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

_DEFAULT_HTTP_UA = "globalskiatlas_data/1.0 (resort-copy-script; https://github.com)"


def _http_get_json(
    url: str,
    *,
    extra_headers: dict[str, str] | None = None,
    timeout: float = 25.0,
) -> Any:
    """GET URL and parse JSON (stdlib only)."""
    headers = {"User-Agent": _DEFAULT_HTTP_UA}
    if extra_headers:
        headers.update(extra_headers)
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    return json.loads(raw)

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    boto3 = None  # type: ignore

    class ClientError(Exception):  # type: ignore[misc, no-redef]
        """Placeholder when botocore is absent; must not alias to plain Exception."""

        pass

# ---------------------------------------------------------------------------
# Layout tier -> word targets (align with atlas print plates; see plan docs)
# ---------------------------------------------------------------------------

TIER_WORD_BANDS: dict[str, tuple[int, int]] = {
    "small": (90, 100),
    "medium": (140, 175),
    "large": (180, 220),
    "mega": (520, 680),
    "small_medium": (90, 100),
    "small_landscape": (90, 100),
    "medium_landscape": (140, 175),
    "large_landscape": (180, 220),
    "mega_landscape": (520, 680),
}

DEFAULT_CHEAP_MODEL = "amazon.nova-lite-v1:0"
DEFAULT_WRITER_MODEL = "amazon.nova-pro-v1:0"

# Ban school-essay / outline sentence openers (lazy metadiscourse before the real clause).
_ATLAS_PROSE_LEADIN_RULE = (
    "Atlas prose: never begin a sentence or paragraph with outline-style lead-ins such as "
    "\"In terms of\", \"When considering\", \"When it comes to\", \"For those planning\", "
    "\"For visitors\", \"Historically\", \"In conclusion\", \"Overall\", \"Additionally\", "
    "\"Furthermore\", \"Moreover\", \"In summary\", \"With regard to\", \"As for\", "
    "or similar topic-fronted fragments (including \"When considering the visitor economy,\"). "
    "Open with the place, resort, mountain, or a concrete image — not a section label."
)

_BANNED_TRAVEL_CLICHES_RULE = (
    "Hard ban — never use: nestled/nestle/Nestled, hidden gem, world-class, well-kept secret, "
    "picture-perfect, jewel-box, chalet-chic. Open with the proper name or map fact "
    '(e.g. "In Butha-Buthe, Lesotho, AfriSki…") not "Nestled in…".'
)


def strip_banned_travel_cliches(text: str) -> str:
    """Deterministic cleanup when models ignore soft bans (especially 'nestled')."""
    if not (text or "").strip():
        return text
    s = text
    s = re.sub(r"(?i)\bNestled,?\s+in\b", "In", s)
    s = re.sub(r"(?i)\bNestled,?\s+within\b", "Within", s)
    s = re.sub(r"(?i),?\s*nestled\s+in\b", ", in", s)
    s = re.sub(r"(?i)\bhidden gem\b", "strong pick", s)
    s = re.sub(r"(?i)\bworld-class\b", "notable", s)
    s = re.sub(r"(?i)\bwell-kept secret\b", "little-known area", s)
    s = re.sub(r"(?i)\bpicture-perfect\b", "sharp", s)
    s = re.sub(r"(?i)\bjewel-box\b", "compact", s)
    s = re.sub(r"(?i)\bnestled\b", "", s)
    s = re.sub(r"(?i)\bnestle\b", "", s)
    s = re.sub(r"[ \t]{2,}", " ", s)
    s = re.sub(r"\s+,", ",", s)
    s = re.sub(r",\s+,", ", ", s)
    s = re.sub(r"\(\s+", "(", s)
    s = re.sub(r"\s+\)", ")", s)
    return s.strip()


# Print plate tiers for multi-output runs (word bands from TIER_WORD_BANDS).
PLATE_TIERS: list[str] = ["small", "medium", "large", "mega"]

# Country / state wiki overview pages (~half a print page; same word band as "large" resort plates).
REGION_WIKI_LAYOUT_TIER: str = "large"

# Wiki ingest (trails/lifts/acres) -> one atlas layout tier for copy length / research depth.
SIZE_CATEGORY_TO_LAYOUT_TIER: dict[str, str] = {
    "small_hill": "small",
    "ski_mountain": "medium",
    "multiple_mountains": "large",
    "mega_resort": "mega",
    "unknown": "medium",
}

# More plate space => more parallel fact miners + higher per-miner caps + more curator bullets.
# pass_network = Epic / Ikon / Indy / etc.; visitor_economy = lift/lodging/transport/dining $ from snippets;
# season_calendar = typical months / year-round; reviews_sentiment = praise + critique from review-like snippets.
TIER_RESEARCH_SPEC: dict[str, dict[str, int]] = {
    "small": {
        "history_max": 1,
        "landform_max": 1,
        "ops_max": 0,
        "access_max": 0,
        "pass_max": 0,
        "economy_max": 0,
        "season_max": 0,
        "reviews_max": 0,
        "curator_max_bullets": 2,
    },
    "medium": {
        "history_max": 2,
        "landform_max": 2,
        "ops_max": 2,
        "access_max": 0,
        "pass_max": 1,
        "economy_max": 1,
        "season_max": 2,
        "reviews_max": 1,
        "curator_max_bullets": 6,
    },
    "large": {
        "history_max": 3,
        "landform_max": 3,
        "ops_max": 2,
        "access_max": 2,
        "pass_max": 2,
        "economy_max": 2,
        "season_max": 2,
        "reviews_max": 2,
        "curator_max_bullets": 8,
    },
    "mega": {
        "history_max": 4,
        "landform_max": 4,
        "ops_max": 3,
        "access_max": 3,
        "pass_max": 3,
        "economy_max": 3,
        "season_max": 3,
        "reviews_max": 3,
        "curator_max_bullets": 12,
    },
}


def research_spec_for_layout_tier(tier: str) -> dict[str, int]:
    k = str(tier).strip().lower()
    if k == "small_medium":
        k = "small"
    elif k.endswith("_landscape"):
        k = k.removesuffix("_landscape")
    if k not in TIER_RESEARCH_SPEC:
        k = "medium"
    return TIER_RESEARCH_SPEC[k].copy()


def _writer_max_tokens(tier: str) -> int:
    k = str(tier).strip().lower()
    if k.endswith("_landscape"):
        k = k.removesuffix("_landscape")
    return {"small": 420, "medium": 960, "large": 1500, "mega": 3600, "small_medium": 420}.get(k, 960)


def _word_count(text: str) -> int:
    return len(text.split())


def _strip_json_fence(raw: str) -> str:
    s = raw.strip()
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.IGNORECASE)
        s = re.sub(r"\s*```$", "", s)
    return s.strip()


def _parse_json_object(raw: str) -> dict[str, Any]:
    s = _strip_json_fence(raw)
    return json.loads(s)


try:
    from ddgs import DDGS
except ImportError:
    try:
        from duckduckgo_search import DDGS  # type: ignore[no-redef]
    except ImportError:
        DDGS = None  # type: ignore


def fetch_wikipedia_snippets(resort_query: str, max_chars: int = 8000) -> list[dict[str, Any]]:
    """Pull Wikipedia REST summaries; ids W1, W2, … (often empty for tiny ski areas)."""
    snippets: list[dict[str, Any]] = []
    params = urllib.parse.urlencode(
        {
            "action": "query",
            "list": "search",
            "srsearch": resort_query,
            "format": "json",
            "srlimit": "5",
        }
    )
    search_url = f"https://en.wikipedia.org/w/api.php?{params}"
    try:
        sr_data = _http_get_json(search_url, timeout=20.0)
    except (urllib.error.HTTPError, urllib.error.URLError, json.JSONDecodeError, OSError):
        return snippets
    hits = sr_data.get("query", {}).get("search", [])
    titles = [h["title"] for h in hits[:4]]
    if not titles:
        return snippets

    for i, title in enumerate(titles):
        safe = urllib.parse.quote(title.replace(" ", "_"))
        summary_url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{safe}"
        try:
            data = _http_get_json(summary_url, timeout=20.0)
        except (urllib.error.HTTPError, urllib.error.URLError, json.JSONDecodeError, OSError):
            continue
        extract = (data.get("extract") or "").strip()
        url = data.get("content_urls", {}).get("desktop", {}).get("page", "")
        if not extract:
            continue
        if len(extract) > max_chars // max(len(titles), 1):
            extract = extract[: max_chars // max(len(titles), 1)] + "…"
        snippets.append(
            {
                "id": f"W{i + 1}",
                "source": "wikipedia",
                "title": title,
                "url": url,
                "text": extract,
            }
        )
    return snippets


def fetch_duckduckgo_snippets(
    queries: list[str],
    *,
    max_per_query: int = 6,
    max_total: int = 14,
    per_snippet_max_chars: int = 900,
    id_start: int = 1,
) -> list[dict[str, Any]]:
    """Web search snippets via DuckDuckGo (no API key). ids D{id_start}, …"""
    out: list[dict[str, Any]] = []
    if DDGS is None:
        return out
    seen_urls: set[str] = set()
    n = 0
    seq = id_start
    for q in queries:
        if not q.strip() or n >= max_total:
            break
        try:
            with DDGS() as ddgs:
                rows = list(ddgs.text(q.strip(), max_results=max_per_query))  # type: ignore[misc]
        except Exception:
            continue
        for row in rows:
            if n >= max_total:
                break
            href = (row.get("href") or row.get("url") or "").strip()
            title = (row.get("title") or "").strip()
            body = (row.get("body") or "").strip()
            if not body and not title:
                continue
            key = href.split("#", 1)[0].rstrip("/").lower() if href else title.lower()
            if key in seen_urls:
                continue
            seen_urls.add(key)
            text = body or title
            if len(text) > per_snippet_max_chars:
                text = text[:per_snippet_max_chars] + "…"
            n += 1
            out.append(
                {
                    "id": f"D{seq}",
                    "source": "duckduckgo",
                    "title": title or href or q,
                    "url": href or "",
                    "text": text,
                }
            )
            seq += 1
    return out


def fetch_brave_web_snippets(
    query: str,
    api_key: str,
    *,
    count: int = 10,
    per_snippet_max_chars: int = 900,
) -> list[dict[str, Any]]:
    """Brave Web Search API (paid/free tier); ids B1, B2, …"""
    out: list[dict[str, Any]] = []
    if not api_key.strip() or not query.strip():
        return out
    params = urllib.parse.urlencode({"q": query.strip(), "count": str(min(count, 20))})
    brave_url = f"https://api.search.brave.com/res/v1/web/search?{params}"
    try:
        data = _http_get_json(
            brave_url,
            extra_headers={
                "Accept": "application/json",
                "X-Subscription-Token": api_key.strip(),
            },
            timeout=25.0,
        )
    except (urllib.error.HTTPError, urllib.error.URLError, json.JSONDecodeError, OSError):
        return out
    results = (data.get("web") or {}).get("results") or []
    for i, item in enumerate(results[:count]):
        title = (item.get("title") or "").strip()
        url = (item.get("url") or "").strip()
        desc = (item.get("description") or "").strip()
        text = desc or title
        if len(text) > per_snippet_max_chars:
            text = text[:per_snippet_max_chars] + "…"
        out.append(
            {
                "id": f"B{i + 1}",
                "source": "brave",
                "title": title or url,
                "url": url,
                "text": text,
            }
        )
    return out


def research_queries(name: str, state: str | None, country: str | None) -> list[str]:
    """Web search queries: core resort + passes, pricing, transport, season, reviews (deduped)."""
    name = (name or "").strip()
    loc = " ".join(x for x in (state or "", country or "") if x).strip()
    nl = name.lower()
    if "ski" in nl or "resort" in nl or "mountain" in nl:
        primary = name
    else:
        primary = f"{name} ski resort"
    qs: list[str] = []
    # Slovenia: prioritize local multi-area pass (not Epic/Ikon) in snippet set
    if country and country.strip().lower() in ("slovenia", "slovenija"):
        qs.append(f"{name} Julian Alps International Ski Pass")
    qs.extend(
        [
            primary,
            f"{primary} {loc}".strip() if loc else primary,
            f'"{name}" {loc} ski'.strip() if loc else f'"{name}" ski area',
            f"{primary} Epic Ikon Indy Mountain Collective ski pass",
            f"{primary} Indy Pass",
            f"{primary} lift ticket price {loc}".strip(),
            f"{primary} closest airport shuttle bus train",
            f"{primary} ski season opening closing months longest",
            f"{primary} hotel lodging price per night {loc}".strip(),
            f"{primary} dinner restaurant food cost",
            f"{primary} ski resort reviews pros cons",
        ]
    )
    if country:
        qs.append(f'"{name}" {country} ski pass'.strip())
    seen: set[str] = set()
    uniq: list[str] = []
    for q in qs:
        q2 = " ".join(q.split())
        if len(q2) < 4 or q2.lower() in seen:
            continue
        seen.add(q2.lower())
        uniq.append(q2)
    return uniq[:14]


def research_queries_fallback(name: str, state: str | None, country: str | None) -> list[str]:
    """Broader web queries when the primary round returns too little text."""
    name = (name or "").strip()
    loc = " ".join(x for x in (state or "", country or "") if x).strip()
    qs: list[str] = [
        f"{name} skiing",
        f"{name} ski area",
        f"{name} winter sports",
        f"{name} snow",
        f"{name} mountain resort",
        f"{name} Wikipedia",
        f"{loc} ski".strip(),
        f"{country} ski resorts".strip() if country else "",
        f"{country} skiing travel".strip() if country else "",
        f"{name} tourism winter",
        f'"{name}" ski',
    ]
    seen: set[str] = set()
    uniq: list[str] = []
    for q in qs:
        q2 = " ".join(q.split())
        if len(q2) < 4 or q2.lower() in seen:
            continue
        seen.add(q2.lower())
        uniq.append(q2)
    return uniq[:12]


def wiki_fallback_search_queries(name: str, state: str | None, country: str | None) -> list[str]:
    """Alternate Wikipedia OpenSearch strings when the first query returns no extracts."""
    name = (name or "").strip()
    loc = " ".join(x for x in (state or "", country or "") if x).strip()
    out = [
        f"{name} ski",
        f"{name} resort",
        name if len(name) > 4 else f"{name} mountain",
        f"{name} {loc}".strip() if loc else name,
        f"{country} {name}".strip() if country else "",
    ]
    return [q for q in out if q.strip()]


def wiki_search_query(name: str, state: str | None, country: str | None) -> str:
    """Bias Wikipedia OpenSearch toward the place name + region (reduces generic ski lists)."""
    name = (name or "").strip()
    loc = " ".join(x for x in (state or "", country or "") if x).strip()
    if loc:
        return f"{name} {loc}"
    return name if len(name) > 3 else f"{name} ski"


def _snippet_identity(s: dict[str, Any]) -> str:
    u = (s.get("url") or "").strip().split("#", 1)[0].rstrip("/").lower()
    t = (s.get("title") or "").strip().lower()
    src = (s.get("source") or "").strip().lower()
    return f"{src}|{u}|{t}"


def _snippets_meaningful(snippets: list[dict[str, Any]], *, min_chars: int = 200) -> bool:
    tot = 0
    for s in snippets:
        tot += len((s.get("text") or "").strip()) + len((s.get("title") or "").strip())
    return tot >= min_chars


def _append_unique_snippets(merged: list[dict[str, Any]], extra: list[dict[str, Any]]) -> None:
    seen = {_snippet_identity(s) for s in merged}
    for s in extra:
        if not (s.get("text") or "").strip() and not (s.get("title") or "").strip():
            continue
        key = _snippet_identity(s)
        if key not in seen:
            seen.add(key)
            merged.append(s)


def _renumber_snippet_ids(snippets: list[dict[str, Any]]) -> None:
    """Sequential ids per source in list order (W1…, B1…, D1…, P1…)."""
    counts: dict[str, int] = {}
    for s in snippets:
        src = (s.get("source") or "dataset").lower()
        if src not in ("wikipedia", "brave", "duckduckgo", "dataset"):
            src = "dataset"
        counts[src] = counts.get(src, 0) + 1
        prefix = {"wikipedia": "W", "brave": "B", "duckduckgo": "D", "dataset": "P"}[src]
        s["id"] = f"{prefix}{counts[src]}"


def parquet_row_stub_snippet(row: dict[str, Any]) -> dict[str, Any]:
    """Grounding from analysed parquet columns only when web/wiki return nothing."""
    name = str(row.get("english_name") or row.get("name") or "").strip() or "Ski area"
    country = str(row.get("country") or "").strip()
    state = str(row.get("state") or "").strip()
    trails = row.get("downhill_trails")
    lifts = row.get("total_lifts")
    ha = row.get("skiable_terrain_ha")
    ac = row.get("skiable_terrain_acres")
    loc = ", ".join(x for x in (state, country) if x)
    parts = [f"Dataset entry: downhill ski area “{name}”"]
    if loc:
        parts.append(f"in {loc}.")
    else:
        parts.append("(location fields blank in dataset).")
    if trails is not None and str(trails).strip() != "":
        parts.append(f"Downhill trails (dataset): {trails}.")
    if lifts is not None and str(lifts).strip() != "":
        parts.append(f"Lift count (dataset): {lifts}.")
    if ac is not None and str(ac).strip() != "":
        parts.append(f"Skiable terrain acres (dataset): {ac}.")
    elif ha is not None and str(ha).strip() != "":
        parts.append(f"Skiable terrain hectares (dataset): {ha}.")
    text = " ".join(parts)
    return {
        "id": "P1",
        "source": "dataset",
        "title": f"{name} (atlas dataset row)",
        "url": "",
        "text": text,
    }


def gather_research_snippets(
    *,
    name: str,
    state: str | None = None,
    country: str | None = None,
    brave_api_key: str | None = None,
    use_ddg: bool = True,
    wiki_query: str | None = None,
    parquet_row: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Merge Wikipedia + DuckDuckGo (+ optional Brave). Adds fallback queries and optional dataset stub."""
    queries = research_queries(name, state, country)
    wq = wiki_query if wiki_query else wiki_search_query(name, state, country)
    merged: list[dict[str, Any]] = []

    def _wiki() -> list[dict[str, Any]]:
        try:
            return fetch_wikipedia_snippets(wq)
        except Exception:
            return []

    def _brave() -> list[dict[str, Any]]:
        if not brave_api_key:
            return []
        try:
            return fetch_brave_web_snippets(queries[0], brave_api_key)
        except Exception:
            return []

    def _ddg() -> list[dict[str, Any]]:
        if not use_ddg or DDGS is None:
            return []
        try:
            return fetch_duckduckgo_snippets(queries, max_per_query=6, max_total=26, id_start=1)
        except Exception as e:
            print(f"duckduckgo search failed: {e}", file=sys.stderr)
            return []

    with ThreadPoolExecutor(max_workers=3) as ex:
        fut_w = ex.submit(_wiki)
        fut_b = ex.submit(_brave)
        fut_d = ex.submit(_ddg)
        wiki_snips = fut_w.result()
        brave_snips = fut_b.result()
        ddg_snips = fut_d.result()

    merged.extend(wiki_snips)
    merged.extend(brave_snips)
    merged.extend(ddg_snips)

    if not _snippets_meaningful(merged):
        for alt in wiki_fallback_search_queries(name, state, country):
            try:
                more_w = fetch_wikipedia_snippets(alt)
            except Exception:
                more_w = []
            before = len(merged)
            _append_unique_snippets(merged, more_w)
            if len(merged) > before and _snippets_meaningful(merged):
                break

    if not _snippets_meaningful(merged) and use_ddg and DDGS is not None:
        try:
            fb = research_queries_fallback(name, state, country)
            more_d = fetch_duckduckgo_snippets(fb, max_per_query=5, max_total=18, id_start=1)
            _append_unique_snippets(merged, more_d)
        except Exception as e:
            print(f"duckduckgo fallback search failed: {e}", file=sys.stderr)

    if not _snippets_meaningful(merged) and parquet_row is not None:
        merged.append(parquet_row_stub_snippet(parquet_row))

    _renumber_snippet_ids(merged)
    return merged


def format_snippets_for_prompt(snippets: list[dict[str, Any]]) -> str:
    lines = []
    for s in snippets:
        src = s.get("source", "?")
        lines.append(f"[{s['id']}] source={src} | {s['title']} ({s['url']})\n{s['text']}\n")
    return "\n".join(lines).strip()


def _snippet_text_by_id(snippets: list[dict[str, Any]]) -> dict[str, str]:
    """Title + body per snippet id (pass miners often cite titles)."""
    by: dict[str, str] = {}
    for s in snippets:
        sid = str(s.get("id") or "").strip()
        if not sid:
            continue
        title = (s.get("title") or "").strip()
        text = (s.get("text") or "").strip()
        by[sid] = f"{title}\n{text}"
    return by


def _pass_brand_grounding_needles(fact: str) -> list[str]:
    """If fact names a major multi-resort pass, these substrings must appear in cited snippet text."""
    fl = (fact or "").lower()
    needles: list[str] = []
    if "mountain collective" in fl:
        needles.append("collective")
    if "power pass" in fl:
        needles.append("power pass")
    if "ikon" in fl:
        needles.append("ikon")
    if re.search(r"\bepic\b", fl) and ("pass" in fl or "local" in fl):
        needles.append("epic")
    if "indy" in fl:
        needles.append("indy")
    out: list[str] = []
    seen: set[str] = set()
    for n in needles:
        if n not in seen:
            seen.add(n)
            out.append(n)
    return out


def _pass_network_fact_snippet_grounded(
    fact: str, snippet_ids: list[Any], by_id: dict[str, str]
) -> bool:
    ids = [str(x).strip() for x in snippet_ids if str(x).strip()]
    if not ids:
        return False
    combined = " ".join(by_id.get(i, "") for i in ids).lower()
    needles = _pass_brand_grounding_needles(fact)
    if not needles:
        return True
    return all(n in combined for n in needles)


def sanitize_miner_pass_network_against_snippets(
    miner_results: dict[str, Any], snippets: list[dict[str, Any]]
) -> None:
    """Drop pass_network facts not supported by cited snippet title+body (stops Jasna-lake → Ikon leaps)."""
    pn = miner_results.get("pass_network")
    if not isinstance(pn, dict):
        return
    facts_raw = pn.get("facts")
    if not isinstance(facts_raw, list):
        return
    by_id = _snippet_text_by_id(snippets)
    kept: list[dict[str, Any]] = []
    for item in facts_raw:
        if not isinstance(item, dict):
            continue
        fact = str(item.get("fact") or "")
        sids = item.get("snippet_ids")
        if not isinstance(sids, list):
            sids = []
        if _pass_network_fact_snippet_grounded(fact, sids, by_id):
            kept.append(item)
    pn["facts"] = kept
    if not kept:
        pn["insufficient"] = True
    miner_results["pass_network"] = pn


def bedrock_converse(
    *,
    region: str,
    model_id: str,
    system_text: str,
    user_text: str,
    max_tokens: int,
    temperature: float,
) -> tuple[str, dict[str, Any]]:
    if boto3 is None:
        raise RuntimeError("boto3 is required: pip install boto3")
    client = boto3.client("bedrock-runtime", region_name=region)
    kwargs: dict[str, Any] = {
        "modelId": model_id,
        "messages": [{"role": "user", "content": [{"text": user_text}]}],
        "inferenceConfig": {"maxTokens": max_tokens, "temperature": temperature},
    }
    if system_text.strip():
        kwargs["system"] = [{"text": system_text}]
    resp = client.converse(**kwargs)
    parts = resp.get("output", {}).get("message", {}).get("content") or []
    text = ""
    for p in parts:
        if "text" in p:
            text += p["text"]
    usage = {k: resp.get(k) for k in ("usage", "metrics") if k in resp}
    return text, usage


def converse_with_backoff(**kwargs: Any) -> tuple[str, dict[str, Any]]:
    delays = [1.0, 2.0, 4.0]
    last_err: Exception | None = None
    for attempt, delay in enumerate([0.0] + delays):
        if delay:
            time.sleep(delay)
        try:
            return bedrock_converse(**kwargs)
        except ClientError as e:
            last_err = e
            if not hasattr(e, "response") or e.response is None:
                raise
            code = (e.response.get("Error") or {}).get("Code", "")
            if code in ("ThrottlingException", "TooManyRequestsException") and attempt < len(delays):
                continue
            raise
    raise last_err  # type: ignore[misc]


def miner_history(
    snippets_block: str, *, max_facts: int, region: str, model_id: str
) -> dict[str, Any]:
    mf = max(1, min(int(max_facts), 6))
    need = mf
    system = (
        "You extract historical facts only from the SNIPPETS block. "
        "Output a single JSON object, no markdown. Schema: "
        '{"facts":[{"fact":"string<=28 words","snippet_ids":["W1","D3"]}],'
        '"insufficient":false,"gap":null}. '
        f"At most {mf} facts (aim for {need} distinct facts). "
        "Each fact must cite snippet_ids that appear in the SNIPPETS headers (e.g. W1, D2, B1). "
        f"If you cannot support {need} facts from snippets, set insufficient true and gap to a short string."
    )
    user = f"SNIPPETS:\n{snippets_block}\n\nReturn JSON only."
    raw, _ = converse_with_backoff(
        region=region,
        model_id=model_id,
        system_text=system,
        user_text=user,
        max_tokens=200 + 120 * mf,
        temperature=0.1,
    )
    return _parse_json_object(raw)


def miner_landform(
    snippets_block: str, *, max_facts: int, region: str, model_id: str
) -> dict[str, Any]:
    mf = max(1, min(int(max_facts), 6))
    need = mf
    system = (
        "You extract geology, landform, and regional physical setting only from SNIPPETS. "
        "Output JSON only, no markdown. Schema: "
        '{"facts":[{"fact":"string<=32 words","snippet_ids":["W2","D1"]}],'
        '"insufficient":false,"gap":null}. '
        f"At most {mf} facts (aim for {need}). No marketing fluff."
    )
    user = f"SNIPPETS:\n{snippets_block}\n\nReturn JSON only."
    raw, _ = converse_with_backoff(
        region=region,
        model_id=model_id,
        system_text=system,
        user_text=user,
        max_tokens=200 + 120 * mf,
        temperature=0.1,
    )
    return _parse_json_object(raw)


def miner_ops_culture(
    snippets_block: str, *, max_facts: int, region: str, model_id: str
) -> dict[str, Any]:
    mf = max(1, min(int(max_facts), 6))
    system = (
        "You extract mountain operations and resort-development facts only from SNIPPETS: "
        "snowmaking, night skiing, lift projects, ownership or expansion milestones, "
        "base village or real-estate phases if stated. "
        "Output JSON only. Schema: "
        '{"facts":[{"fact":"string<=30 words","snippet_ids":["D2"]}],'
        '"insufficient":false,"gap":null}. '
        f"At most {mf} facts. No generic praise; each fact must cite snippet_ids."
    )
    user = f"SNIPPETS:\n{snippets_block}\n\nReturn JSON only."
    raw, _ = converse_with_backoff(
        region=region,
        model_id=model_id,
        system_text=system,
        user_text=user,
        max_tokens=200 + 120 * mf,
        temperature=0.1,
    )
    return _parse_json_object(raw)


def miner_access_corridor(
    snippets_block: str, *, max_facts: int, region: str, model_id: str
) -> dict[str, Any]:
    mf = max(1, min(int(max_facts), 6))
    system = (
        "You extract access, travel corridor, and human-geography placement only from SNIPPETS: "
        "distance to major cities, highways, border crossings, airports, valley towns, "
        "orientation or views toward a named region if stated. "
        "Output JSON only. Schema: "
        '{"facts":[{"fact":"string<=30 words","snippet_ids":["D1"]}],'
        '"insufficient":false,"gap":null}. '
        f"At most {mf} facts. Each fact must cite snippet_ids."
    )
    user = f"SNIPPETS:\n{snippets_block}\n\nReturn JSON only."
    raw, _ = converse_with_backoff(
        region=region,
        model_id=model_id,
        system_text=system,
        user_text=user,
        max_tokens=200 + 120 * mf,
        temperature=0.1,
    )
    return _parse_json_object(raw)


def miner_pass_network(
    snippets_block: str, *, max_facts: int, region: str, model_id: str
) -> dict[str, Any]:
    mf = max(1, min(int(max_facts), 4))
    system = (
        "From SNIPPETS only: which multi-resort season pass PRODUCTS explicitly include THIS SAME ski resort "
        "(the mountain/resort named in the snippets — not generic regional ski marketing). "
        "Eligible names include Epic Pass, Ikon Pass, Indy Pass, Mountain Collective, Power Pass, etc. "
        "STRICT RULES: (1) Each fact must cite snippet_ids whose text BOTH names this resort (or clear context) "
        "AND names exactly one pass product, OR clearly states one pass includes this resort. "
        "(2) NEVER put Epic and Ikon in the same fact unless one snippet explicitly says THIS resort is on BOTH. "
        "(3) If snippets only mention Ikon for this resort, do not add Epic or Indy. If only Indy, do not add Epic or Ikon. "
        "(4) Do not infer from pages that compare 'Epic vs Ikon' nationally — only facts tied to this resort. "
        "(5) Prefer one pass product per fact; max facts still applies. "
        "(6) Do not confuse lakes, towns, or homonyms (e.g. Lake Jasna near Kranjska Gora vs other 'Jasna' ski areas) "
        "with pass participation — only if the snippet text explicitly ties THIS resort or named ski area to a pass. "
        "(7) The cited snippet title or body must literally contain the pass product wording you use (e.g. 'Ikon', 'Epic Pass'); "
        "do not infer passes from geography alone. "
        "Output JSON only. Schema: "
        '{"facts":[{"fact":"string<=30 words","snippet_ids":["D4"]}],'
        '"insufficient":false,"gap":null}. '
        f"At most {mf} facts. If no qualifying snippet, set insufficient true."
    )
    user = f"SNIPPETS:\n{snippets_block}\n\nReturn JSON only."
    raw, _ = converse_with_backoff(
        region=region,
        model_id=model_id,
        system_text=system,
        user_text=user,
        max_tokens=200 + 120 * mf,
        temperature=0.1,
    )
    return _parse_json_object(raw)


def miner_visitor_economy(
    snippets_block: str, *, max_facts: int, region: str, model_id: str
) -> dict[str, Any]:
    mf = max(1, min(int(max_facts), 4))
    system = (
        "From SNIPPETS only: visitor trip-economy signals — window lift-ticket pricing, lodging rates "
        "(e.g. 'from $X'), ground transport from a named airport or city, shuttle/bus, dinner or restaurant "
        "price hints if dollar amounts appear. "
        "Output JSON only. Schema: "
        '{"facts":[{"fact":"string<=32 words","snippet_ids":["D9"]}],'
        '"insufficient":false,"gap":null}. '
        f"At most {mf} facts. Do not invent prices; only facts explicitly supported by snippet text. "
        "If no $ or explicit fare language, insufficient true."
    )
    user = f"SNIPPETS:\n{snippets_block}\n\nReturn JSON only."
    raw, _ = converse_with_backoff(
        region=region,
        model_id=model_id,
        system_text=system,
        user_text=user,
        max_tokens=200 + 120 * mf,
        temperature=0.1,
    )
    return _parse_json_object(raw)


def miner_season_calendar(
    snippets_block: str, *, max_facts: int, region: str, model_id: str
) -> dict[str, Any]:
    mf = max(1, min(int(max_facts), 4))
    system = (
        "From SNIPPETS only: typical ski season opening/closing timing, longest-season claims, "
        "year-round or summer operations if stated, best snow months if stated. "
        "Output JSON only. Schema: "
        '{"facts":[{"fact":"string<=32 words","snippet_ids":["D5"]}],'
        '"insufficient":false,"gap":null}. '
        f"At most {mf} facts. Each fact must cite snippet_ids."
    )
    user = f"SNIPPETS:\n{snippets_block}\n\nReturn JSON only."
    raw, _ = converse_with_backoff(
        region=region,
        model_id=model_id,
        system_text=system,
        user_text=user,
        max_tokens=200 + 120 * mf,
        temperature=0.1,
    )
    return _parse_json_object(raw)


def miner_reviews_sentiment(
    snippets_block: str, *, max_facts: int, region: str, model_id: str
) -> dict[str, Any]:
    mf = max(1, min(int(max_facts), 4))
    system = (
        "From SNIPPETS only (reviews, TripAdvisor, forums, blog titles): extract balanced visitor sentiment. "
        "Output JSON only, no markdown. Schema: "
        '{"positives":[{"text":"string<=32 words","snippet_ids":["D9"]}],'
        '"negatives":[{"text":"string<=32 words","snippet_ids":["D9"]}],'
        '"insufficient":false,"gap":null}. '
        f"At most {mf} total items split across positives and negatives (at least one of each if snippets support). "
        "Negatives: fair paraphrase of complaints in text, not insults. If no review tone, insufficient true."
    )
    user = f"SNIPPETS:\n{snippets_block}\n\nReturn JSON only."
    raw, _ = converse_with_backoff(
        region=region,
        model_id=model_id,
        system_text=system,
        user_text=user,
        max_tokens=200 + 140 * mf,
        temperature=0.1,
    )
    return _parse_json_object(raw)


def curator_merge(
    miner_results: dict[str, Any],
    *,
    tier: str,
    wmin: int,
    wmax: int,
    curator_max_bullets: int,
    region: str,
    model_id: str,
) -> dict[str, Any]:
    cap = max(2, min(int(curator_max_bullets), 16))
    system = (
        "You are an atlas editor. Merge ALL miner JSON payloads into one curator bundle. "
        "Miners may include: history, landform, operations, access_corridor (each often has a facts[] array); "
        "pass_network, visitor_economy, season_calendar (facts[]); "
        "reviews_sentiment (positives[] and negatives[] with text + snippet_ids). "
        "Output JSON only. No new factual claims beyond miner inputs. "
        'Schema: {"bullets":[{"text":"string","snippet_ids":["W1","D2"]}],'
        '"use_in_lead_index":0,"coverage_gaps":["optional"],'
        f'"tier":"{tier}","word_min":{wmin},"word_max":{wmax}}}. '
        f"At most {cap} bullets total. Each bullet must introduce a distinct theme "
        "(setting, pass products, trip cost/transport, season timing, access, operations, "
        "terrain/snow, history, visitor praise, visitor caution); "
        "merge or drop bullets that restate the same idea. "
        "Pass_network: never merge two major pass brands (Epic vs Ikon vs Indy, etc.) into one bullet unless "
        "a miner fact explicitly states THIS resort participates in both; otherwise one pass product per bullet. "
        "If pass_network.facts is empty, do not mention Epic Pass, Ikon Pass, Indy Pass, or Mountain Collective in any bullet. "
        "use_in_lead_index is the index of the single strongest differentiation bullet."
    )
    user = json.dumps(miner_results, ensure_ascii=False)
    raw, _ = converse_with_backoff(
        region=region,
        model_id=model_id,
        system_text=system,
        user_text=user,
        max_tokens=min(4000, 400 + 110 * cap),
        temperature=0.2,
    )
    return _parse_json_object(raw)


def writer_draft(
    curator: dict[str, Any],
    *,
    resort_label: str,
    tier: str,
    region: str,
    model_id: str,
) -> str:
    wmin = int(curator.get("word_min", 140))
    wmax = int(curator.get("word_max", 175))
    plate_hi = TIER_WORD_BANDS.get(tier, (wmin, wmax))[1]
    system = (
        "You write differentiation-first ski atlas prose in a confident guidebook voice. "
        "Rules: Do not recap statistics (vertical, lift counts, acreage, trail counts). "
        "Use only ideas supported by curator bullets (paraphrase). "
        "Do not invent years, 'first in region', or ownership unless in bullet text. "
        "Multi-resort passes: name ONLY pass products explicitly stated in curator bullets for THIS resort. "
        "Never write 'Epic and Ikon' (or pair Epic with Ikon) unless one bullet explicitly says both include this resort; "
        "if bullets only name Ikon, say Ikon only; if only Indy, say Indy only; do not pad with a second pass brand. "
        f"Target {wmin}-{wmax} words inclusive — never exceed {wmax} words. "
        "Anti-padding: if evidence is thin, write a shorter honest piece inside the band "
        "rather than repeating the same theme in new sentences. "
        "Each sentence must add a new angle from the bullets; do not restate the same "
        "fact with different wording. Use any distinctive place phrase (e.g. a range name, "
        "'private', 'community') at most once unless the bullets explicitly require otherwise. "
        f"{_BANNED_TRAVEL_CLICHES_RULE} "
        "Also avoid: well-kept secret, hidden gem, world-class, cozy (unless one use is essential). "
        f"{_ATLAS_PROSE_LEADIN_RULE} "
    )
    if plate_hi >= 220:
        system += (
            "When bullets cover pass products, ground transport from airports or cities, "
            "lodging or lift price hints, season length, or visitor praise/caution, give each its own sentence or "
            "short paragraph using only that bullet language; add one brief clause that pass roster and ticket "
            "prices change by season and should be confirmed on official resort or pass sites. "
        )
    system += "Output plain paragraphs only, no title."
    user = f"Resort: {resort_label}\n\nCURATOR_JSON:\n{json.dumps(curator, ensure_ascii=False)}"
    raw, _ = converse_with_backoff(
        region=region,
        model_id=model_id,
        system_text=system,
        user_text=user,
        max_tokens=_writer_max_tokens(tier),
        temperature=0.42,
    )
    return raw.strip()


def trim_to_word_max(
    text: str,
    *,
    wmax: int,
    region: str,
    model_id: str,
) -> str:
    """Cut-only pass when prose exceeds layout tier maximum."""
    wc = _word_count(text)
    if wc <= wmax:
        return text
    system = (
        f"The following text is {wc} words. Hard maximum is {wmax} words. "
        "Delete whole sentences (or redundant clauses) only until at or below the maximum. "
        "Do not add new facts, synonyms for padding, or new sentences. "
        "Remove sentences that repeat the same theme before cutting unique detail. "
        f"{_ATLAS_PROSE_LEADIN_RULE} "
        f"{_BANNED_TRAVEL_CLICHES_RULE} "
        "Output plain paragraphs only."
    )
    raw, _ = converse_with_backoff(
        region=region,
        model_id=model_id,
        system_text=system,
        user_text=text,
        max_tokens=min(4000, max(700, wmax * 6)),
        temperature=0.15,
    )
    return raw.strip()


def expand_to_word_min(
    text: str,
    *,
    curator: dict[str, Any],
    resort_label: str,
    wmin: int,
    wmax: int,
    tier: str,
    region: str,
    model_id: str,
) -> str:
    """Add curator-grounded material when final copy is below layout word_min."""
    wc = _word_count(text)
    if wc >= wmin:
        return text
    system = (
        f"The following prose is {wc} words. You must reach at least {wmin} words and never exceed {wmax}. "
        "Add sentences and short paragraphs using ONLY distinct angles from CURATOR_JSON bullets that are "
        "not already expressed in the draft (re-read both; no thematic repetition). "
        "Prioritize unused bullets on pass products (only as each bullet states them), trip costs/transport/lodging/dining when present, "
        "season timing, review praise and cautions, access, operations, landform, history. "
        "If economy bullets contain dollar figures, keep them approximate as in snippets and add one clause "
        "that current lift, lodging, and pass prices change by season and must be verified on official sites. "
        "Do not recap trail counts, vertical, or lift totals as a stat block. "
        f"{_ATLAS_PROSE_LEADIN_RULE} "
        f"{_BANNED_TRAVEL_CLICHES_RULE} "
        "Plain paragraphs only."
    )
    user = (
        f"Resort: {resort_label}\n\nCURATOR_JSON:\n{json.dumps(curator, ensure_ascii=False)}\n\n"
        f"DRAFT:\n{text}\n"
    )
    raw, _ = converse_with_backoff(
        region=region,
        model_id=model_id,
        system_text=system,
        user_text=user,
        max_tokens=min(4500, _writer_max_tokens(tier) + 800),
        temperature=0.38,
    )
    out = raw.strip()
    if _word_count(out) > wmax:
        out = trim_to_word_max(out, wmax=wmax, region=region, model_id=model_id)
    return out


def critic_json(
    draft: str,
    *,
    wmin: int,
    wmax: int,
    region: str,
    model_id: str,
) -> dict[str, Any]:
    wc = _word_count(draft)
    system = (
        "You are a strict copy desk critic. Output JSON only, no prose rewrite. "
        "Schema: {"
        '"violated_rule_ids":["string"],'
        '"cliches":["string"],'
        '"stat_dump_suspected":false,'
        f'"word_count":{wc},'
        '"tier_band_ok":true,'
        '"repeated_phrases":[{"phrase":"string","count":2}],'
        '"theme_repetition":false,'
        '"suggested_fixes":["imperative bullet instructions"]'
        "}. "
        f"word_count is {wc} for this draft. Set tier_band_ok true only if {wmin} <= {wc} <= {wmax}; "
        "otherwise false (including when below word_min). "
        "Detect thematic stutter: set theme_repetition true if multiple sentences re-express "
        "the same idea (e.g. private/community/cozy/welcome repeated without new detail). "
        "List repeated_phrases for distinctive n-grams (2-4 words) or key words "
        "(e.g. Blue Mountain, private, cozy) used more than once without adding fact. "
        "Set violated_rule_ids to include pass_bundle_unsupported if the draft names two major pass products "
        "(Epic, Ikon, Indy, etc.) together without evidence both apply to this resort. "
        "Set violated_rule_ids to include essay_outline_leadin if the draft uses banned outline openers "
        "(e.g. 'In terms of', 'Historically', 'Overall', 'In conclusion', 'When considering', 'For those planning'). "
        "List those openers in cliches. "
        "Set violated_rule_ids to include travel_cliche if the draft contains nestled, nestle, hidden gem, "
        "world-class, well-kept secret, picture-perfect, or jewel-box (case-insensitive). "
        "List each hit in cliches. "
        "Flag marketing clichés in cliches even if not in travel_cliche list. "
        "Do not add new factual claims in suggested_fixes."
    )
    user = f"DRAFT:\n{draft}\n"
    raw, _ = converse_with_backoff(
        region=region,
        model_id=model_id,
        system_text=system,
        user_text=user,
        max_tokens=550,
        temperature=0.1,
    )
    return _parse_json_object(raw)


def editor_finalize(
    draft: str,
    curator: dict[str, Any],
    critic: dict[str, Any],
    *,
    resort_label: str,
    wmin: int,
    wmax: int,
    tier: str,
    region: str,
    model_id: str,
) -> str:
    if wmax >= 400:
        expand_rule = (
            "If the draft is shorter than word_min, add compact sentences that each introduce "
            "new detail from distinct CURATOR_JSON bullets until you reach at least word_min "
            "(cap roughly 10 new short sentences; never repeat an angle already stated)."
        )
    else:
        expand_rule = (
            "If the draft is shorter than word_min, add at most two sentences that pull "
            "only new detail from CURATOR_JSON bullets — never repeat an angle already stated."
        )
    system = (
        "You are the finishing editor. Apply critic fixes for style and structure only. "
        "Ignore any critic suggestion that would introduce new factual claims not in CURATOR_JSON. "
        f"Final word count must be between {wmin} and {wmax} inclusive — never above {wmax}. "
        "If over word_max: delete whole redundant sentences first (especially repeated themes), "
        "then tighten; do not swap repetition for new padding. "
        "If theme_repetition or repeated_phrases in CRITIC_JSON: merge to one mention each "
        "unless bullets require two distinct uses. "
        "Do not broaden pass claims: never add a second pass brand (e.g. Epic alongside Ikon) unless CURATOR_JSON explicitly bundles both for this resort. "
        f"{_ATLAS_PROSE_LEADIN_RULE} "
        f"{_BANNED_TRAVEL_CLICHES_RULE} "
        "Mandatory style pass: delete every occurrence of nestled/nestle/hidden gem/world-class/well-kept secret "
        "and rewrite sentences so grammar stays natural (do not substitute equally tacky synonyms like 'perched' unless earned). "
        "If CRITIC_JSON violated_rule_ids contains travel_cliche or cliches mention those words, remove them completely. "
        "Remove any essay-style section lead-ins from the draft (rewrite those sentences to start with concrete subject). "
        f"{expand_rule} "
        "Output plain paragraphs only, no markdown headings."
    )
    user = (
        f"Resort: {resort_label}\n\n"
        f"CURATOR_JSON:\n{json.dumps(curator, ensure_ascii=False)}\n\n"
        f"CRITIC_JSON:\n{json.dumps(critic, ensure_ascii=False)}\n\n"
        f"DRAFT:\n{draft}\n"
    )
    raw, _ = converse_with_backoff(
        region=region,
        model_id=model_id,
        system_text=system,
        user_text=user,
        max_tokens=min(4000, _writer_max_tokens(tier) + 400),
        temperature=0.35,
    )
    return raw.strip()


def load_resort_row(parquet_path: Path, name_substr: str) -> dict[str, Any]:
    import pandas as pd

    df = pd.read_parquet(parquet_path)
    if "name" not in df.columns:
        raise SystemExit("Parquet missing 'name' column")
    mask = df["name"].astype(str).str.contains(name_substr, case=False, na=False)
    if mask.sum() == 0:
        raise SystemExit(f"No row matching name contains {name_substr!r}")
    row = df.loc[mask].iloc[0].to_dict()
    return row


def row_to_label(row: dict[str, Any]) -> str:
    name = row.get("english_name") or row.get("name") or "Unknown"
    parts = [str(name)]
    if row.get("state"):
        parts.append(str(row["state"]))
    if row.get("country"):
        parts.append(str(row["country"]))
    return ", ".join(parts)


def _wiki_slug(value: Any) -> str:
    """Same rules as GlobalSkiAtlas_2/scripts/wiki-ingest-parquet.js slug()."""
    if value is None:
        return "unknown"
    s = str(value).strip()
    if not s:
        return "unknown"
    t = s.lower()
    t = re.sub(r"\s+", "-", t)
    t = re.sub(r"[^a-z0-9-]", "", t)
    t = re.sub(r"-+", "-", t).strip("-")
    return t or "unknown"


def wiki_page_id_from_row(row: dict[str, Any]) -> str:
    """DynamoDB WikiPages key pageId — must match wiki-ingest-parquet.js rowToItem()."""
    name_raw = row.get("english_name") or row.get("name") or row.get("Name")
    name = str(name_raw).strip() if name_raw is not None else ""
    if not name:
        name = "unknown"
    state_keys = (
        "state",
        "State",
        "addr:state",
        "province",
        "addr:province",
        "state_province",
        "region",
    )
    country_keys = ("country", "Country", "addr:country", "country_name")
    state_val = None
    for k in state_keys:
        v = row.get(k)
        if v is not None and str(v).strip():
            state_val = str(v).strip()
            break
    country_val = None
    for k in country_keys:
        v = row.get(k)
        if v is not None and str(v).strip():
            country_val = str(v).strip()
            break
    name_slug = _wiki_slug(name)
    state_slug = _wiki_slug(state_val) if state_val else ""
    country_slug = _wiki_slug(country_val) if country_val else ""
    if state_slug:
        page_id = f"{name_slug}-{state_slug}"
    elif country_slug:
        page_id = f"{name_slug}-{country_slug}"
    else:
        page_id = name_slug
    return page_id or "unknown"


def wiki_country_page_id(country: str) -> str:
    """DynamoDB pageId for country pages (wiki-ingest buildCountryItem)."""
    return f"country-{_wiki_slug(country)}"


def wiki_state_page_id(state: str, country: str) -> str:
    """DynamoDB pageId for state pages (wiki-ingest buildStateItem)."""
    return f"state-{_wiki_slug(state)}-{_wiki_slug(country)}"


def _is_not_downhill_row(row: dict[str, Any]) -> bool:
    v = row.get("resort_type")
    if v is None:
        v = row.get("Resort Type")
    if v is None:
        return False
    return str(v).lower().strip() == "not a downhill ski resort"


def _row_num(row: dict[str, Any], *keys: str) -> float | None:
    for k in keys:
        if k not in row or row[k] is None or row[k] == "":
            continue
        try:
            n = float(row[k])
            if n == n:  # not NaN
                return n
        except (TypeError, ValueError):
            continue
    return None


def resort_size_category(row: dict[str, Any]) -> str:
    """Same buckets as GlobalSkiAtlas_2/scripts/wiki-ingest-parquet.js resortSizeCategory()."""
    if _is_not_downhill_row(row):
        return "unknown"
    trails = _row_num(row, "downhill_trails", "Downhill trails")
    lifts = _row_num(row, "total_lifts", "Total lifts")
    acres = _row_num(row, "skiable_terrain_acres", "Skiable terrain acres")
    if acres is None:
        ha = _row_num(row, "skiable_terrain_ha")
        if ha is not None:
            acres = ha * 2.471
    has_trails = trails is not None and trails >= 0
    has_lifts = lifts is not None and lifts >= 0
    if not has_trails or not has_lifts:
        return "unknown"
    has_acres = acres is not None and acres >= 0
    t = float(trails) if trails is not None else 0.0
    a = float(acres) if has_acres else 0.0
    if t >= 200 or a >= 10000:
        return "mega_resort"
    if t >= 100 or a >= 5000:
        return "multiple_mountains"
    if t >= 50 or a >= 1000:
        return "ski_mountain"
    return "small_hill"


def layout_tier_for_parquet_row(row: dict[str, Any]) -> str:
    cat = resort_size_category(row)
    return SIZE_CATEGORY_TO_LAYOUT_TIER.get(cat, "medium")


def resolve_layout_tier(cli_tier: str, row: dict[str, Any] | None) -> str:
    if cli_tier != "auto":
        return cli_tier
    if row is None:
        raise SystemExit("--tier auto requires a parquet row (use -i/--name with --input)")
    return layout_tier_for_parquet_row(row)


def load_parquet_resorts_for_batch(
    parquet_path: Path,
    *,
    offset: int = 0,
    limit: int | None = None,
    skip_not_downhill: bool = False,
    shard: tuple[int, int] | None = None,
) -> list[dict[str, Any]]:
    """Load resort rows in display order; optional filters for batch generation."""
    import pandas as pd

    df = pd.read_parquet(parquet_path)
    if "name" not in df.columns:
        raise SystemExit("Parquet missing 'name' column")
    df = df.reset_index(drop=True)
    if skip_not_downhill and "resort_type" in df.columns:
        rt = df["resort_type"].fillna("").astype(str).str.lower().str.strip()
        df = df.loc[rt != "not a downhill ski resort"].reset_index(drop=True)
    if shard is not None:
        si, sn = shard
        keep = [j for j in range(len(df)) if j % sn == si]
        df = df.iloc[keep].reset_index(drop=True)
    if offset:
        df = df.iloc[offset:]
    if limit is not None:
        df = df.iloc[:limit]
    return [r.to_dict() for _, r in df.iterrows()]


def _parse_shard_arg(s: str) -> tuple[int, int]:
    parts = s.strip().split("/")
    if len(parts) != 2:
        raise SystemExit("--shard must be I/N with N >= 1 and 0 <= I < N, e.g. 0/4")
    try:
        i, n = int(parts[0]), int(parts[1])
    except ValueError as e:
        raise SystemExit("--shard must be I/N integers") from e
    if n < 1 or i < 0 or i >= n:
        raise SystemExit("--shard invalid: need 0 <= I < N and N >= 1")
    return i, n


def run_pipeline(
    *,
    resort_label: str,
    snippets: list[dict[str, Any]],
    tier: str,
    bedrock_region: str,
    cheap_model: str,
    writer_model: str,
    include_snippets_in_output: bool = True,
) -> dict[str, Any]:
    spec = research_spec_for_layout_tier(tier)
    band = TIER_WORD_BANDS.get(tier, TIER_WORD_BANDS["medium"])
    wmin, wmax = band
    block = format_snippets_for_prompt(snippets)
    if not block:
        raise SystemExit(
            "No research snippets. Wikipedia may have no article for this hill; "
            "install ddgs for web snippets (`pip install ddgs`) "
            "and/or set BRAVE_API_KEY for Brave Search."
        )

    miner_jobs: list[tuple[str, Any, int]] = [
        ("history", miner_history, spec["history_max"]),
        ("landform", miner_landform, spec["landform_max"]),
    ]
    for key, fn, mx in (
        ("operations", miner_ops_culture, spec.get("ops_max", 0)),
        ("access_corridor", miner_access_corridor, spec.get("access_max", 0)),
        ("pass_network", miner_pass_network, spec.get("pass_max", 0)),
        ("visitor_economy", miner_visitor_economy, spec.get("economy_max", 0)),
        ("season_calendar", miner_season_calendar, spec.get("season_max", 0)),
        ("reviews_sentiment", miner_reviews_sentiment, spec.get("reviews_max", 0)),
    ):
        if mx > 0:
            miner_jobs.append((key, fn, mx))

    n_jobs = len(miner_jobs)
    with ThreadPoolExecutor(max_workers=max(2, min(8, n_jobs))) as ex:
        futs = {
            key: ex.submit(fn, block, max_facts=mx, region=bedrock_region, model_id=cheap_model)
            for key, fn, mx in miner_jobs
        }
        miner_results = {key: futs[key].result() for key in futs}

    sanitize_miner_pass_network_against_snippets(miner_results, snippets)

    curator = curator_merge(
        miner_results,
        tier=tier,
        wmin=wmin,
        wmax=wmax,
        curator_max_bullets=spec["curator_max_bullets"],
        region=bedrock_region,
        model_id=writer_model,
    )
    draft = writer_draft(
        curator,
        resort_label=resort_label,
        tier=tier,
        region=bedrock_region,
        model_id=writer_model,
    )
    critic = critic_json(draft, wmin=wmin, wmax=wmax, region=bedrock_region, model_id=cheap_model)
    final_before = editor_finalize(
        draft,
        curator,
        critic,
        resort_label=resort_label,
        wmin=wmin,
        wmax=wmax,
        tier=tier,
        region=bedrock_region,
        model_id=writer_model,
    )
    trimmed = trim_to_word_max(
        final_before,
        wmax=wmax,
        region=bedrock_region,
        model_id=cheap_model,
    )
    if _word_count(trimmed) > wmax:
        trimmed = trim_to_word_max(
            trimmed,
            wmax=wmax,
            region=bedrock_region,
            model_id=cheap_model,
        )
    expanded = expand_to_word_min(
        trimmed,
        curator=curator,
        resort_label=resort_label,
        wmin=wmin,
        wmax=wmax,
        tier=tier,
        region=bedrock_region,
        model_id=writer_model,
    )
    if _word_count(expanded) > wmax:
        expanded = trim_to_word_max(
            expanded,
            wmax=wmax,
            region=bedrock_region,
            model_id=cheap_model,
        )
    expanded = strip_banned_travel_cliches(expanded)
    trim_applied = _word_count(final_before.strip()) > wmax or trimmed.strip() != final_before.strip()
    expand_applied = _word_count(expanded) > _word_count(trimmed) or _word_count(trimmed) < wmin
    out: dict[str, Any] = {
        "resort_label": resort_label,
        "tier": tier,
        "word_band": [wmin, wmax],
        "research_spec": spec,
        "miner_results": miner_results,
        "curator": curator,
        "draft": draft,
        "critic": critic,
        "final": expanded,
        "final_word_count": _word_count(expanded),
        "trim_pass_applied": trim_applied,
        "expand_pass_applied": expand_applied,
    }
    if include_snippets_in_output:
        out["snippets"] = snippets
    return out


def run_all_plate_tiers(
    *,
    resort_label: str,
    snippets: list[dict[str, Any]],
    bedrock_region: str,
    cheap_model: str,
    writer_model: str,
) -> dict[str, Any]:
    """Run small, medium, large, mega once each (shared snippets; escalating fact depth)."""
    by_tier: dict[str, Any] = {}
    for t in PLATE_TIERS:
        by_tier[t] = run_pipeline(
            resort_label=resort_label,
            snippets=snippets,
            tier=t,
            bedrock_region=bedrock_region,
            cheap_model=cheap_model,
            writer_model=writer_model,
            include_snippets_in_output=False,
        )
    return {
        "resort_label": resort_label,
        "snippet_count": len(snippets),
        "snippets": snippets,
        "by_tier": by_tier,
    }


def slim_pipeline_trace(r: dict[str, Any]) -> dict[str, Any]:
    """Drop snippet bodies from a pipeline result; keep ids/urls/titles for audit."""
    slim = {k: v for k, v in r.items() if k != "snippets"}
    if "snippets" in r:
        slim["snippets"] = [
            {"id": s["id"], "source": s.get("source"), "title": s["title"], "url": s["url"]}
            for s in r["snippets"]
        ]
    return slim


def wiki_content_markdown_from_row(row: dict[str, Any], final_prose: str) -> str:
    """Wiki page body: H1 title + atlas paragraphs (matches placeholder style from wiki-ingest)."""
    name = str(row.get("english_name") or row.get("name") or row.get("Name") or "Resort").strip() or "Resort"
    body = (final_prose or "").strip()
    return f"# {name}\n\n{body}" if body else f"# {name}\n"


def combined_wiki_export_item(
    row: dict[str, Any],
    r: dict[str, Any],
    *,
    tier: str,
    include_pipeline: bool,
) -> dict[str, Any]:
    page_id = wiki_page_id_from_row(row)
    wsid = str(row.get("winter_sports_id", "")).strip()
    name = str(row.get("english_name") or row.get("name") or row.get("Name") or "").strip() or "Resort"
    item: dict[str, Any] = {
        "pageId": page_id,
        "winterSportsId": wsid,
        "title": name,
        "contentMarkdown": wiki_content_markdown_from_row(row, str(r.get("final") or "")),
        "tier": tier,
        "resortSizeCategory": resort_size_category(row),
        "finalWordCount": r.get("final_word_count"),
    }
    if include_pipeline:
        item["pipeline"] = slim_pipeline_trace(r)
    return item


def research_queries_for_country(country: str) -> list[str]:
    c = (country or "").strip()
    return [
        f"{c} ski resorts",
        f"{c} skiing travel",
        f"{c} winter sports tourism",
        f"{c} mountain skiing",
        f"skiing in {c}",
    ]


def research_queries_for_state(state: str, country: str) -> list[str]:
    s, co = (state or "").strip(), (country or "").strip()
    return [
        f"{s} {co} ski resorts",
        f"{s} skiing {co}",
        f"ski areas {s} {co}",
        f"{s} winter sports",
    ]


def _region_dataset_stub(
    *, kind: str, country: str, state: str | None, resort_count: int
) -> dict[str, Any]:
    if kind == "country":
        title = f"{country} (atlas dataset)"
        text = (
            f"The Global Ski Atlas analysed parquet lists {resort_count} downhill ski areas "
            f"(excluding rows marked not downhill) in {country}. "
            "Use with web snippets; do not invent a different resort count."
        )
    else:
        title = f"{state}, {country} (atlas dataset)"
        text = (
            f"The dataset lists {resort_count} downhill ski areas in {state}, {country}. "
            "Use with web snippets for a regional overview."
        )
    return {"id": "P1", "source": "dataset", "title": title, "url": "", "text": text}


def gather_region_snippets_country(
    country: str, *, resort_count: int, brave_api_key: str | None, use_ddg: bool
) -> list[dict[str, Any]]:
    queries = research_queries_for_country(country)
    merged: list[dict[str, Any]] = []
    try:
        merged.extend(fetch_wikipedia_snippets(f"{country.strip()} skiing"))
    except Exception:
        pass
    if brave_api_key:
        try:
            merged.extend(fetch_brave_web_snippets(queries[0], brave_api_key))
        except Exception:
            pass
    if use_ddg and DDGS is not None:
        try:
            merged.extend(
                fetch_duckduckgo_snippets(queries, max_per_query=5, max_total=22, id_start=1)
            )
        except Exception as e:
            print(f"country ddg: {e}", file=sys.stderr)
    if not _snippets_meaningful(merged, min_chars=160) and use_ddg and DDGS is not None:
        fb = [f"{country} ski travel", f"{country} mountains winter", f"alpine skiing {country}"]
        try:
            _append_unique_snippets(
                merged, fetch_duckduckgo_snippets(fb, max_per_query=4, max_total=12, id_start=1)
            )
        except Exception:
            pass
    merged.append(
        _region_dataset_stub(kind="country", country=country, state=None, resort_count=resort_count)
    )
    _renumber_snippet_ids(merged)
    return merged


def gather_region_snippets_state(
    state: str, country: str, *, resort_count: int, brave_api_key: str | None, use_ddg: bool
) -> list[dict[str, Any]]:
    queries = research_queries_for_state(state, country)
    merged: list[dict[str, Any]] = []
    try:
        merged.extend(fetch_wikipedia_snippets(f"{state} {country} ski"))
    except Exception:
        pass
    if brave_api_key:
        try:
            merged.extend(fetch_brave_web_snippets(queries[0], brave_api_key))
        except Exception:
            pass
    if use_ddg and DDGS is not None:
        try:
            merged.extend(
                fetch_duckduckgo_snippets(queries, max_per_query=5, max_total=22, id_start=1)
            )
        except Exception as e:
            print(f"state ddg: {e}", file=sys.stderr)
    if not _snippets_meaningful(merged, min_chars=160) and use_ddg and DDGS is not None:
        fb = [f"{state} ski {country}", f"{state} winter tourism"]
        try:
            _append_unique_snippets(
                merged, fetch_duckduckgo_snippets(fb, max_per_query=4, max_total=12, id_start=1)
            )
        except Exception:
            pass
    merged.append(
        _region_dataset_stub(kind="state", country=country, state=state, resort_count=resort_count)
    )
    _renumber_snippet_ids(merged)
    return merged


def load_unique_countries_and_states(parquet_path: Path) -> tuple[list[str], list[tuple[str, str]]]:
    import pandas as pd

    df = pd.read_parquet(parquet_path)
    if "country" not in df.columns:
        raise SystemExit("Parquet missing country column")
    countries = sorted({str(x).strip() for x in df["country"].dropna() if str(x).strip()})
    pairs: list[tuple[str, str]] = []
    if "state" in df.columns:
        sub = df[["state", "country"]].dropna()
        for r in sub.to_dict("records"):
            s, c = str(r.get("state", "")).strip(), str(r.get("country", "")).strip()
            if s and c:
                pairs.append((s, c))
    pairs = sorted(set(pairs))
    return countries, pairs


def wiki_regions_downhill_counts(parquet_path: Path) -> tuple[dict[str, int], dict[tuple[str, str], int]]:
    import pandas as pd

    df = pd.read_parquet(parquet_path)
    if "resort_type" in df.columns:
        rt = df["resort_type"].fillna("").astype(str).str.lower().str.strip()
        df_h = df.loc[rt != "not a downhill ski resort"]
    else:
        df_h = df
    by_c: dict[str, int] = {}
    if len(df_h) and "country" in df_h.columns:
        by_c = {
            str(k).strip(): int(v)
            for k, v in df_h.groupby(df_h["country"].astype(str).str.strip()).size().items()
            if str(k).strip()
        }
    by_sc: dict[tuple[str, str], int] = {}
    if len(df_h) and "state" in df_h.columns and "country" in df_h.columns:
        g = df_h.groupby(
            [df_h["state"].astype(str).str.strip(), df_h["country"].astype(str).str.strip()]
        ).size()
        for (s, c), v in g.items():
            if s and c:
                by_sc[(s, c)] = int(v)
    return by_c, by_sc


def run_wiki_regions_export(
    *,
    parquet_path: Path,
    out_path: Path,
    bedrock_region: str,
    cheap_model: str,
    writer_model: str,
    brave_api_key: str | None,
    use_ddg: bool,
    tier: str,
    delay_seconds: float,
    max_countries: int | None,
    max_state_pairs: int | None,
    countries_only: bool,
    states_only: bool,
) -> int:
    tier_use = tier if tier in TIER_RESEARCH_SPEC else REGION_WIKI_LAYOUT_TIER
    countries, state_pairs = load_unique_countries_and_states(parquet_path)
    cnt_c, cnt_sc = wiki_regions_downhill_counts(parquet_path)
    skip_countries = states_only
    skip_states = countries_only
    if skip_countries and skip_states:
        print("Choose at most one of --wiki-regions-countries-only / --wiki-regions-states-only", file=sys.stderr)
        return 1
    if max_countries is not None:
        countries = countries[: max(0, max_countries)]
    if max_state_pairs is not None:
        state_pairs = state_pairs[: max(0, max_state_pairs)]
    total = (0 if skip_countries else len(countries)) + (0 if skip_states else len(state_pairs))
    if total == 0:
        print("No countries or state pairs to process.", file=sys.stderr)
        return 1
    items: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    cur = 0
    if not skip_countries:
        for country in countries:
            cur += 1
            try:
                rc = int(cnt_c.get(country, 0))
                snippets = gather_region_snippets_country(
                    country,
                    resort_count=rc,
                    brave_api_key=brave_api_key,
                    use_ddg=use_ddg,
                )
                label = (
                    f"Country ski overview — {country} (atlas wiki; {rc} downhill ski areas in dataset). "
                    "Write for readers planning ski travel across the country; synthesise from snippets, "
                    "not a single-resort profile."
                )
                r = run_pipeline(
                    resort_label=label,
                    snippets=snippets,
                    tier=tier_use,
                    bedrock_region=bedrock_region,
                    cheap_model=cheap_model,
                    writer_model=writer_model,
                    include_snippets_in_output=False,
                )
                md = f"# {country}\n\n" + str(r.get("final") or "").strip()
                items.append(
                    {
                        "pageId": wiki_country_page_id(country),
                        "winterSportsId": "",
                        "title": country,
                        "contentMarkdown": md,
                        "tier": tier_use,
                        "pageKind": "country",
                        "finalWordCount": r.get("final_word_count"),
                    }
                )
                print(f"[{cur}/{total}] country ok {country}", file=sys.stderr)
            except Exception as e:
                errors.append({"pageKind": "country", "country": country, "error": str(e)})
                print(f"[{cur}/{total}] country ERR {country}: {e}", file=sys.stderr)
            if delay_seconds > 0:
                time.sleep(delay_seconds)
    if not skip_states:
        for state, country in state_pairs:
            cur += 1
            try:
                rc = int(cnt_sc.get((state, country), 0))
                snippets = gather_region_snippets_state(
                    state,
                    country,
                    resort_count=rc,
                    brave_api_key=brave_api_key,
                    use_ddg=use_ddg,
                )
                label = (
                    f"State/province ski overview — {state}, {country} (atlas wiki; {rc} dataset ski areas). "
                    "Regional overview for trip planning; synthesise from snippets, not one resort page."
                )
                r = run_pipeline(
                    resort_label=label,
                    snippets=snippets,
                    tier=tier_use,
                    bedrock_region=bedrock_region,
                    cheap_model=cheap_model,
                    writer_model=writer_model,
                    include_snippets_in_output=False,
                )
                md = f"# {state}\n\n" + str(r.get("final") or "").strip()
                items.append(
                    {
                        "pageId": wiki_state_page_id(state, country),
                        "winterSportsId": "",
                        "title": state,
                        "contentMarkdown": md,
                        "tier": tier_use,
                        "pageKind": "state",
                        "state": state,
                        "country": country,
                        "finalWordCount": r.get("final_word_count"),
                    }
                )
                print(f"[{cur}/{total}] state ok {state} / {country}", file=sys.stderr)
            except Exception as e:
                errors.append(
                    {
                        "pageKind": "state",
                        "state": state,
                        "country": country,
                        "error": str(e),
                    }
                )
                print(f"[{cur}/{total}] state ERR {state}/{country}: {e}", file=sys.stderr)
            if delay_seconds > 0:
                time.sleep(delay_seconds)
    payload: dict[str, Any] = {
        "format": "globalskiatlas.resort_wiki_content_v1",
        "pageScope": "wiki_regions",
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
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
            sys.stderr.reconfigure(encoding="utf-8")
        except Exception:
            pass
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bedrock-region", default="us-east-1", help="AWS region for Bedrock Runtime")
    ap.add_argument("--cheap-model", default=DEFAULT_CHEAP_MODEL, help="Nova Lite/Micro for miners + critic")
    ap.add_argument("--writer-model", default=DEFAULT_WRITER_MODEL, help="Nova Pro (or Claude ID) for curator/draft/editor")
    ap.add_argument(
        "--tier",
        default="medium",
        choices=list(TIER_WORD_BANDS.keys()) + ["auto"],
        help="Layout tier word band, or 'auto' from parquet row size (-i/--name only). Ignored when --all-plate-tiers",
    )
    ap.add_argument("-i", "--input", type=Path, default=None, help="ski_areas_analyzed.parquet (combined)")
    ap.add_argument("--name", type=str, default=None, help="Substring match on name column when using --input")
    ap.add_argument("--converse-smoke", action="store_true", help="Single short Bedrock call and exit")
    ap.add_argument(
        "--demo-massanutten",
        action="store_true",
        help="Full pipeline: Massanutten (Wikipedia + web search + optional Brave)",
    )
    ap.add_argument(
        "--demo-query",
        type=str,
        default=None,
        metavar="TEXT",
        help="Resort name or search phrase; used with --research-only or full Bedrock pipeline",
    )
    ap.add_argument("--demo-state", type=str, default=None, help="State/province for --demo-query")
    ap.add_argument("--demo-country", type=str, default=None, help="Country for --demo-query")
    ap.add_argument(
        "--research-only",
        action="store_true",
        help="Fetch snippets only (no Bedrock); use with --demo-massanutten, --demo-query, or -i/--name",
    )
    ap.add_argument("--no-ddg", action="store_true", help="Disable DuckDuckGo web snippets")
    ap.add_argument(
        "--brave-api-key",
        type=str,
        default=None,
        help="Brave Search API key (default: env BRAVE_API_KEY)",
    )
    ap.add_argument("--json-out", action="store_true", help="Print full JSON trace to stdout")
    ap.add_argument(
        "--all-plate-tiers",
        action="store_true",
        help="Emit all four plate tiers (small→mega) in one run; batch writes by_tier. Default is one tier from resort size",
    )
    ap.add_argument(
        "--out-json",
        type=Path,
        default=None,
        metavar="PATH",
        help="Write JSON result to PATH (useful with --all-plate-tiers)",
    )
    ap.add_argument(
        "--batch-all",
        action="store_true",
        help="Process all rows from -i parquet; one sized layout tier per resort unless --batch-force-tier or --all-plate-tiers",
    )
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        metavar="DIR",
        help="With --batch-all: optional directory for per-resort {winter_sports_id}.json (omit if only --batch-out-combined)",
    )
    ap.add_argument(
        "--batch-offset",
        type=int,
        default=0,
        help="Batch: skip first N rows after filters (default 0)",
    )
    ap.add_argument(
        "--batch-limit",
        type=int,
        default=None,
        metavar="N",
        help="Batch: process at most N rows after offset",
    )
    ap.add_argument(
        "--batch-skip-not-downhill",
        action="store_true",
        help="Batch: skip rows where resort_type is 'not a downhill ski resort'",
    )
    ap.add_argument(
        "--skip-existing",
        action="store_true",
        help="Batch: skip if {winter_sports_id}.json already exists in --out-dir",
    )
    ap.add_argument(
        "--delay-seconds",
        type=float,
        default=0.0,
        metavar="SEC",
        help="Batch: sleep between resorts (throttle web search / Bedrock)",
    )
    ap.add_argument(
        "--shard",
        type=str,
        default=None,
        metavar="I/N",
        help="Batch: process rows where index %% N == I after downhill filter (e.g. 0/4 for worker 0 of 4)",
    )
    ap.add_argument(
        "--batch-force-tier",
        choices=PLATE_TIERS,
        default=None,
        metavar="TIER",
        help="Batch: use this layout tier for every row instead of size-derived small/medium/large/mega",
    )
    ap.add_argument(
        "--batch-out-combined",
        type=Path,
        default=None,
        metavar="PATH",
        help="Batch: write one JSON (items[] + errors[]) for wiki-bulk-update-resort-content.js; use without --out-dir for a single file only",
    )
    ap.add_argument(
        "--batch-combined-full",
        action="store_true",
        help="Batch: include full pipeline trace per item in combined JSON (large)",
    )
    ap.add_argument(
        "--wiki-regions-out",
        type=Path,
        default=None,
        metavar="PATH",
        help="Write one JSON of country-* and state-*-* wiki overviews from parquet (same format as bulk resort export)",
    )
    ap.add_argument(
        "--wiki-regions-tier",
        default=REGION_WIKI_LAYOUT_TIER,
        choices=["small", "medium", "large", "mega"],
        help="Word band / research depth for country & state pages (default: large ~ half page)",
    )
    ap.add_argument(
        "--wiki-regions-delay-seconds",
        type=float,
        default=0.0,
        help="Pause between Bedrock runs when generating region pages",
    )
    ap.add_argument(
        "--wiki-regions-max-countries",
        type=int,
        default=None,
        metavar="N",
        help="Process only the first N countries after sorting (smoke test)",
    )
    ap.add_argument(
        "--wiki-regions-max-state-pairs",
        type=int,
        default=None,
        metavar="N",
        help="Process only the first N state/country pairs after sorting (smoke test)",
    )
    ap.add_argument(
        "--wiki-regions-countries-only",
        action="store_true",
        help="Only emit country pages (skip state-*-*)",
    )
    ap.add_argument(
        "--wiki-regions-states-only",
        action="store_true",
        help="Only emit state pages (skip country-*)",
    )
    args = ap.parse_args()

    brave_key = (args.brave_api_key or os.environ.get("BRAVE_API_KEY") or "").strip() or None
    use_ddg = not args.no_ddg

    if args.wiki_regions_out:
        print(
            "Note: prefer scripts/generate_wiki_region_copy_bedrock.py for country/state pages "
            "(regional 'why visit' voice; avoids single-resort profiles).",
            file=sys.stderr,
        )
        if args.batch_all:
            print("Run --wiki-regions-out in a separate command from --batch-all", file=sys.stderr)
            return 1
        if args.research_only:
            print("--wiki-regions-out requires full Bedrock (omit --research-only)", file=sys.stderr)
            return 1
        if not args.input:
            print("--wiki-regions-out requires -i/--input parquet", file=sys.stderr)
            return 1
        return run_wiki_regions_export(
            parquet_path=args.input,
            out_path=args.wiki_regions_out,
            bedrock_region=args.bedrock_region,
            cheap_model=args.cheap_model,
            writer_model=args.writer_model,
            brave_api_key=brave_key,
            use_ddg=use_ddg,
            tier=args.wiki_regions_tier,
            delay_seconds=args.wiki_regions_delay_seconds,
            max_countries=args.wiki_regions_max_countries,
            max_state_pairs=args.wiki_regions_max_state_pairs,
            countries_only=args.wiki_regions_countries_only,
            states_only=args.wiki_regions_states_only,
        )

    if args.batch_all and args.research_only:
        print("--batch-all cannot be combined with --research-only", file=sys.stderr)
        return 1
    if args.batch_all:
        if not args.input:
            print("--batch-all requires -i/--input", file=sys.stderr)
            return 1
        if not args.out_dir and not args.batch_out_combined:
            print("--batch-all requires --out-dir and/or --batch-out-combined", file=sys.stderr)
            return 1
        if args.batch_out_combined and args.all_plate_tiers:
            print(
                "Note: --batch-out-combined uses one size-matched tier per row; ignoring --all-plate-tiers",
                file=sys.stderr,
            )
        run_all_plates = bool(args.all_plate_tiers and not args.batch_out_combined)
        shard_t = _parse_shard_arg(args.shard) if args.shard else None
        rows = load_parquet_resorts_for_batch(
            args.input,
            offset=args.batch_offset,
            limit=args.batch_limit,
            skip_not_downhill=args.batch_skip_not_downhill,
            shard=shard_t,
        )
        out_dir = args.out_dir
        write_per_resort = out_dir is not None
        if write_per_resort:
            out_dir.mkdir(parents=True, exist_ok=True)
        combined_items: list[dict[str, Any]] = []
        combined_errors: list[dict[str, Any]] = []
        n_ok = n_err = n_skip = n_no_snippets = 0
        for i, row in enumerate(rows):
            wsid = str(row.get("winter_sports_id", "")).strip() or f"row_{args.batch_offset + i}"
            page_id = wiki_page_id_from_row(row)
            out_path = (out_dir / f"{wsid}.json") if write_per_resort else None
            if write_per_resort and out_path is not None and args.skip_existing and out_path.is_file():
                n_skip += 1
                print(f"[{i + 1}/{len(rows)}] skip existing {wsid}", file=sys.stderr)
                continue
            label = row_to_label(row)
            nm = str(row.get("english_name") or row.get("name") or "").strip()
            envelope: dict[str, Any] = {
                "schema_version": 1,
                "winter_sports_id": wsid,
                "pageId": page_id,
                "resort_label": label,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "bedrock_region": args.bedrock_region,
                "cheap_model": args.cheap_model,
                "writer_model": args.writer_model,
            }
            try:
                snippets = gather_research_snippets(
                    name=nm or page_id,
                    state=str(row["state"]) if row.get("state") is not None else None,
                    country=str(row["country"]) if row.get("country") is not None else None,
                    brave_api_key=brave_key,
                    use_ddg=use_ddg,
                    parquet_row=row,
                )
                if not snippets:
                    envelope["error"] = "no_snippets"
                    if write_per_resort and out_path is not None:
                        out_path.write_text(
                            json.dumps(envelope, ensure_ascii=False, indent=2),
                            encoding="utf-8",
                        )
                    combined_errors.append(
                        {
                            "pageId": page_id,
                            "winterSportsId": wsid,
                            "resort_label": label,
                            "error": "no_snippets",
                        }
                    )
                    n_no_snippets += 1
                    print(f"[{i + 1}/{len(rows)}] {wsid} no_snippets", file=sys.stderr)
                    if args.delay_seconds > 0:
                        time.sleep(args.delay_seconds)
                    continue
                if run_all_plates:
                    bundle = run_all_plate_tiers(
                        resort_label=label,
                        snippets=snippets,
                        bedrock_region=args.bedrock_region,
                        cheap_model=args.cheap_model,
                        writer_model=args.writer_model,
                    )
                    envelope["schema_version"] = 1
                    envelope["resort_label"] = bundle["resort_label"]
                    envelope["resort_size_category"] = resort_size_category(row)
                    envelope["snippet_count"] = bundle["snippet_count"]
                    envelope["snippets"] = [
                        {"id": s["id"], "source": s.get("source"), "title": s["title"], "url": s["url"]}
                        for s in bundle["snippets"]
                    ]
                    envelope["by_tier"] = {
                        t: slim_pipeline_trace(bundle["by_tier"][t]) for t in PLATE_TIERS
                    }
                else:
                    tier = args.batch_force_tier or layout_tier_for_parquet_row(row)
                    r = run_pipeline(
                        resort_label=label,
                        snippets=snippets,
                        tier=tier,
                        bedrock_region=args.bedrock_region,
                        cheap_model=args.cheap_model,
                        writer_model=args.writer_model,
                        include_snippets_in_output=True,
                    )
                    envelope["schema_version"] = 2
                    envelope["resort_size_category"] = resort_size_category(row)
                    envelope["tier"] = tier
                    slim = slim_pipeline_trace(r)
                    envelope.update(slim)
                    if args.batch_out_combined:
                        combined_items.append(
                            combined_wiki_export_item(
                                row,
                                r,
                                tier=tier,
                                include_pipeline=args.batch_combined_full,
                            )
                        )
                if write_per_resort and out_path is not None:
                    out_path.write_text(
                        json.dumps(envelope, ensure_ascii=False, indent=2),
                        encoding="utf-8",
                    )
                n_ok += 1
                tier_note = (
                    envelope.get("tier")
                    or ("all_plates" if run_all_plates else "?")
                )
                print(
                    f"[{i + 1}/{len(rows)}] ok {wsid} pageId={page_id} tier={tier_note}",
                    file=sys.stderr,
                )
            except Exception as e:
                envelope["error"] = "exception"
                envelope["error_message"] = str(e)
                envelope["traceback"] = traceback.format_exc()
                if write_per_resort and out_path is not None:
                    out_path.write_text(
                        json.dumps(envelope, ensure_ascii=False, indent=2),
                        encoding="utf-8",
                    )
                combined_errors.append(
                    {
                        "pageId": page_id,
                        "winterSportsId": wsid,
                        "resort_label": label,
                        "error": "exception",
                        "errorMessage": str(e),
                    }
                )
                n_err += 1
                print(f"[{i + 1}/{len(rows)}] ERR {wsid}: {e}", file=sys.stderr)
            if args.delay_seconds > 0:
                time.sleep(args.delay_seconds)
        if args.batch_out_combined:
            payload: dict[str, Any] = {
                "format": "globalskiatlas.resort_wiki_content_v1",
                "generatedAt": datetime.now(timezone.utc).isoformat(),
                "items": combined_items,
                "errors": combined_errors,
                "counts": {
                    "ok": n_ok,
                    "exceptions": n_err,
                    "noSnippets": n_no_snippets,
                    "skipped": n_skip,
                    "rows": len(rows),
                },
            }
            args.batch_out_combined.parent.mkdir(parents=True, exist_ok=True)
            args.batch_out_combined.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            print(f"Wrote combined export {args.batch_out_combined}", file=sys.stderr)
        dest = args.batch_out_combined or out_dir
        print(
            f"Batch finished: ok={n_ok} exceptions={n_err} no_snippets={n_no_snippets} "
            f"skipped={n_skip} -> {dest}",
            file=sys.stderr,
        )
        return 1 if n_err else 0

    def _snippets_label_and_row() -> tuple[str, list[dict[str, Any]], dict[str, Any] | None]:
        if args.demo_massanutten:
            return (
                "Massanutten Resort, Virginia, United States",
                gather_research_snippets(
                    name="Massanutten Resort",
                    state="Virginia",
                    country="United States",
                    brave_api_key=brave_key,
                    use_ddg=use_ddg,
                ),
                None,
            )
        if args.demo_query:
            return (
                ", ".join(
                    x
                    for x in (args.demo_query, args.demo_state or "", args.demo_country or "")
                    if str(x).strip()
                ),
                gather_research_snippets(
                    name=args.demo_query.strip(),
                    state=args.demo_state,
                    country=args.demo_country,
                    brave_api_key=brave_key,
                    use_ddg=use_ddg,
                ),
                None,
            )
        if args.input and args.name:
            row = load_resort_row(args.input, args.name)
            label = row_to_label(row)
            nm = str(row.get("english_name") or row.get("name") or "").strip()
            return (
                label,
                gather_research_snippets(
                    name=nm or args.name,
                    state=str(row["state"]) if row.get("state") is not None else None,
                    country=str(row["country"]) if row.get("country") is not None else None,
                    brave_api_key=brave_key,
                    use_ddg=use_ddg,
                    parquet_row=row,
                ),
                row,
            )
        raise SystemExit("Set --demo-massanutten, or --demo-query, or --input with --name")

    def _snippets_and_label() -> tuple[str, list[dict[str, Any]]]:
        lab, sn, _ = _snippets_label_and_row()
        return lab, sn

    if args.research_only:
        try:
            label, snippets = _snippets_and_label()
        except SystemExit as e:
            print(str(e), file=sys.stderr)
            return 1
        summary = [
            {
                "id": s["id"],
                "source": s.get("source"),
                "title": s.get("title"),
                "url": s.get("url"),
                "text_preview": (s.get("text") or "")[:240],
            }
            for s in snippets
        ]
        print(
            json.dumps(
                {"resort_label": label, "snippet_count": len(snippets), "snippets": summary},
                ensure_ascii=False,
                indent=2,
            )
        )
        if not snippets:
            return 1
        return 0

    if args.converse_smoke:
        text, usage = converse_with_backoff(
            region=args.bedrock_region,
            model_id=args.cheap_model,
            system_text="Reply in one short sentence.",
            user_text="Say that Bedrock Converse is working.",
            max_tokens=80,
            temperature=0.2,
        )
        print(text)
        print("usage:", usage, file=sys.stderr)
        return 0

    # Full Bedrock pipeline (Massanutten, ad-hoc query, or parquet row)
    if args.demo_massanutten or args.demo_query or (args.input and args.name):
        try:
            resort_label, snippets, parquet_row = _snippets_label_and_row()
        except SystemExit as e:
            print(str(e), file=sys.stderr)
            return 1
        if not snippets:
            print(
                "No research snippets: no Wikipedia hits and no web results "
                "(install ddgs and avoid --no-ddg, or set BRAVE_API_KEY).",
                file=sys.stderr,
            )
            return 1

        try:
            eff_tier = resolve_layout_tier(args.tier, parquet_row)
        except SystemExit as e:
            print(str(e), file=sys.stderr)
            return 1

        if args.all_plate_tiers:
            bundle = run_all_plate_tiers(
                resort_label=resort_label,
                snippets=snippets,
                bedrock_region=args.bedrock_region,
                cheap_model=args.cheap_model,
                writer_model=args.writer_model,
            )
            if args.out_json:
                args.out_json.parent.mkdir(parents=True, exist_ok=True)
                args.out_json.write_text(
                    json.dumps(bundle, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                print(f"Wrote {args.out_json}", file=sys.stderr)
            if args.json_out:
                print(json.dumps(bundle, ensure_ascii=False, indent=2))
            else:
                for t in PLATE_TIERS:
                    r = bundle["by_tier"][t]
                    lo, hi = r["word_band"]
                    print(f"\n=== {t.upper()} ({lo}-{hi} words) ===")
                    print(r["final"])
                    print(f"--- word count: {r['final_word_count']}", file=sys.stderr)
            return 0

        result = run_pipeline(
            resort_label=resort_label,
            snippets=snippets,
            tier=eff_tier,
            bedrock_region=args.bedrock_region,
            cheap_model=args.cheap_model,
            writer_model=args.writer_model,
        )
        slim = slim_pipeline_trace(result)
        if parquet_row is not None:
            slim["resort_size_category"] = resort_size_category(parquet_row)
            slim["tier_resolved"] = eff_tier
        if args.out_json:
            args.out_json.parent.mkdir(parents=True, exist_ok=True)
            args.out_json.write_text(json.dumps(slim, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"Wrote {args.out_json}", file=sys.stderr)
        if args.json_out:
            print(json.dumps(slim, ensure_ascii=False, indent=2))
        elif not args.out_json:
            if args.demo_massanutten or args.demo_query:
                print("--- FINAL COPY ---")
            print(result["final"])
            print("--- word count:", result["final_word_count"], file=sys.stderr)
            if parquet_row is not None:
                print(
                    f"--- layout tier: {eff_tier} (resort_size_category={resort_size_category(parquet_row)})",
                    file=sys.stderr,
                )
        return 0

    ap.print_help()
    print(
        "\nExamples: --converse-smoke | --research-only --demo-query \"...\" | "
        "--demo-massanutten | --demo-query \"...\" | --all-plate-tiers --out-json path.json | "
        "-i .../ski_areas_analyzed.parquet --name ... [--tier auto] | "
        "--batch-all -i .../parquet --batch-out-combined PATH.json [--batch-limit N] | "
        "--batch-all -i .../parquet --out-dir DIR [--batch-out-combined PATH] | "
        "-i .../parquet --wiki-regions-out wiki_regions.json [--wiki-regions-max-countries N]",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
