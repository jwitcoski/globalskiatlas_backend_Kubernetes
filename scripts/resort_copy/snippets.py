"""Score, filter, and select search snippets before note extraction."""
from __future__ import annotations

import re
from typing import Any

from . import bedrock

from .config import (
    PAGE_EXCERPT_CHARS,
)


def _trim_snippets(snippets: list[dict[str, Any]], *, limit: int, chars: int) -> list[dict[str, Any]]:
    """Keep Wikipedia first, then the rest, short enough for an 8B context."""
    wiki = [s for s in snippets if s.get("source") == "wikipedia"]
    rest = [s for s in snippets if s.get("source") != "wikipedia"]
    chosen = (wiki + rest)[:limit]
    out: list[dict[str, Any]] = []
    for s in chosen:
        text = str(s.get("text") or "")
        if len(text) > chars:
            text = text[:chars].rsplit(" ", 1)[0] + "…"
        item = dict(s)
        item["text"] = text
        out.append(item)
    return out


def _bullets_from_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    raw = payload.get("bullets")
    if not isinstance(raw, list):
        return []
    bullets: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        text = str(item.get("text") or "").strip()
        if not text:
            continue
        ids = item.get("snippet_ids")
        if not isinstance(ids, list):
            ids = []
        bullets.append(
            {
                "topic": str(item.get("topic") or "other"),
                "text": text,
                "snippet_ids": [str(x).strip() for x in ids if str(x).strip()],
            }
        )
    return bullets


def _keep_grounded(
    bullets: list[dict[str, Any]], snippets: list[dict[str, Any]], *, resort_name: str
) -> list[dict[str, Any]]:
    known = {str(s.get("id")) for s in snippets}
    by_id = bedrock._snippet_text_by_id(snippets)
    kept: list[dict[str, Any]] = []
    for b in bullets:
        ids = [i for i in b["snippet_ids"] if i in known]
        if b["snippet_ids"] and not ids:
            continue
        if not bedrock._pass_network_fact_snippet_grounded(b["text"], ids, by_id):
            continue
        cited = " ".join(by_id.get(i, "") for i in ids)
        names_a_pass = bool(bedrock._pass_brand_grounding_needles(b["text"])) or b.get("topic") == "pass"
        if names_a_pass and not _mentions_resort(cited, resort_name):
            continue
        kept.append({"topic": b.get("topic") or "other", "text": b["text"], "snippet_ids": ids})
    return kept


_GENERIC_NAME_WORDS = frozenset(
    {
        "ski",
        "area",
        "resort",
        "mountain",
        "mountains",
        "mount",
        "club",
        "snow",
        "bowl",
        "college",
        "university",
        "hill",
        "hills",
        "valley",
        "basin",
        "peak",
        "ridge",
        "lake",
        "park",
        "recreation",
        "center",
        "centre",
        "winter",
        "the",
        "and",
        "of",
        "de",
        "la",
        "le",
    }
)


_DOSSIER_WORDS: tuple[str, ...] = (
    "open",
    "opened",
    "founded",
    "established",
    "pass",
    "private",
    "public",
    "municipal",
    "club",
    "nonprofit",
    "profit",
    "overlook",
    "lake",
    "valley",
    "famous",
    "racer",
    "champion",
    "lodging",
    "hotel",
    "apres",
    "nightlife",
    "family",
    "festival",
    "event",
    "village",
    "gondola",
    "world cup",
    "olympic",
    "glacier",
    "hut",
    "refuge",
    "airport",
    "train",
    "car-free",
    "crowded",
    "operated",
    "owned",
    "snowmaking",
    "school",
    "lesson",
    "powder",
    "piste",
    "parking",
    "nordic",
    "night",
    "history",
    "lunch",
    "weekend",
    "ticket",
    "childcare",
    "tubing",
    "instagram",
    "facebook",
    "alterra",
    "boyne",
    "powdr",
    "beginner",
    "expert",
)


def _name_needles(name: str) -> list[str]:
    """Phrases a page must contain before it can support a fact about this resort."""
    raw = re.sub(r"\s+", " ", (name or "").lower()).strip()
    needles: list[str] = []
    if len(raw) >= 4:
        needles.append(raw)
    for token in re.findall(r"[a-z0-9]+", raw):
        if token in _GENERIC_NAME_WORDS or len(token) < 4:
            continue
        if token not in needles:
            needles.append(token)
    return needles or ([raw] if raw else [])


def _mentions_resort(blob: str, name: str) -> bool:
    text = blob.lower()
    return any(needle in text for needle in _name_needles(name))


# A snippet that says ", California" is not evidence for a Pennsylvania hill.
_OTHER_ADMIN = (
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado", "connecticut",
    "delaware", "florida", "georgia", "hawaii", "idaho", "illinois", "indiana", "iowa",
    "kansas", "kentucky", "louisiana", "maine", "maryland", "massachusetts", "michigan",
    "minnesota", "mississippi", "missouri", "montana", "nebraska", "nevada",
    "new hampshire", "new jersey", "new mexico", "new york", "north carolina",
    "north dakota", "ohio", "oklahoma", "oregon", "pennsylvania", "rhode island",
    "south carolina", "south dakota", "tennessee", "texas", "utah", "vermont",
    "virginia", "washington", "west virginia", "wisconsin", "wyoming",
    "ontario", "quebec", "british columbia", "alberta", "manitoba", "saskatchewan",
    "nova scotia", "new brunswick",
)


def _names_another_state(blob: str, state: str | None) -> bool:
    if not state:
        return False
    text = blob.lower()
    ours = state.lower().strip()
    if ours and ours in text:
        return False
    for other in _OTHER_ADMIN:
        if other == ours:
            continue
        if re.search(rf"(?:,|\bin\b|\bof\b)\s+{re.escape(other)}\b", text):
            return True
    return False


def _page_is_this_resort(blob: str, name: str, state: str | None) -> bool:
    """'Bear' is not enough. The full name, two name words, or the name plus the state."""
    if _names_another_state(blob, state):
        return False
    text = blob.lower()
    full = re.sub(r"\s+", " ", (name or "").lower()).strip()
    if len(full) >= 4 and full in text:
        return True
    tokens = [needle for needle in _name_needles(name) if " " not in needle]
    if any(len(token) >= 8 and token in text for token in tokens):
        return True
    state_name = (state or "").lower().strip()
    if state_name and state_name in text and any(token in text for token in tokens):
        return True
    if len(tokens) >= 2 and sum(token in text for token in tokens) >= 2:
        return True
    return False


def _drop_pages_about_other_places(
    snippets: list[dict[str, Any]], name: str, state: str | None = None
) -> list[dict[str, Any]]:
    """A national Epic-vs-Ikon roundup is not evidence this hill is on either pass."""
    kept: list[dict[str, Any]] = []
    for snippet in snippets:
        blob = f"{snippet.get('title') or ''} {snippet.get('text') or ''}"
        # A fetched page was checked on its search blurb. Its excerpt can name other states.
        if snippet.get("fetched") or _page_is_this_resort(blob, name, state):
            kept.append(dict(snippet))
    if kept:
        bedrock._renumber_snippet_ids(kept)
    return kept


# One reserved slot each, so a late About page is not pushed out by the first ten hits.
_TOPIC_BUCKETS: tuple[tuple[str, str], ...] = (
    ("year", r"\b(opened|founded|established|since)\b|\b(18|19|20)\d{2}\b"),
    ("status", r"not-for-profit|not for profit|nonprofit|non-profit|ski club|municipal"),
    ("pass", r"\b(epic|ikon|indy|collective|magic)\b|\b[\w'’-]+\s+pass\b"),
    ("setting", r"overlook|\blake\b|\bbay\b|\bvalley\b|\bview"),
    ("review", r"\breview\b|learned to ski|worth the"),
    ("lodging", r"ski-in|ski out|slopeside|\blodge\b|where to stay"),
    ("scene", r"night ski|apr[eè]s|festival|snowshoe|terrain park|\bfamily\b"),
    ("story", r"founded by|named after|bring winter|history"),
    ("access", r"minutes from|km from|kilometres|kilometers|drive from|airport|southwest of"),
    ("price", r"day pass|lift ticket|day ticket|admission|\$|cad\s?\d|usd\s?\d|€\s?\d"),
    ("who", r"beginner|locals|members|learned to ski|ski club"),
    ("operator", r"operated by|owned by|vail resorts|alterra|boyne|powdr|compagnie des alpes"),
    ("season", r"opens in|closes in|season runs|snowmaking|glacier"),
    ("amenities", r"night skiing|terrain park|nordic|tubing|ski school|childcare|kids club"),
    ("parking", r"free parking|paid parking|parking fee|day trip"),
)


_DIRECTORY_PAGE = re.compile(
    r"booking\.com|skicentral|find the perfect|vacation rental|hotels\.com|expedia\.com",
    re.IGNORECASE,
)


_READER_NOTE = re.compile(
    r"day pass|lift ticket|day ticket|\$|cad\s?\d|minutes from|km from|kilomet|"
    r"drive from|beginner|members|learned to ski|not-for-profit|ski club",
    re.IGNORECASE,
)


_STAT_RECAP = re.compile(
    r"\b(skiable acres|acres|acreage|vertical|elevation of|elevation|chairlift|number of lifts|trail count)\b",
    re.IGNORECASE,
)


def _snippet_blob(snippet: dict[str, Any]) -> str:
    return f"{snippet.get('title') or ''} {snippet.get('text') or ''}".lower()


def _snippet_score(snippet: dict[str, Any], name: str) -> int:
    blob = _snippet_blob(snippet)
    url = str(snippet.get("url") or "").lower()
    score = sum(1 for word in _DOSSIER_WORDS if word in blob)
    if _DIRECTORY_PAGE.search(url) or "find the perfect" in blob:
        score -= 8
    if any(needle in url for needle in _name_needles(name)):
        score += 5
    title = str(snippet.get("title") or "").lower()
    if snippet.get("source") == "wikipedia" and "ski" in title:
        score += 2
    if _STAT_RECAP.search(blob):
        score -= 3
    return score


def _trim_snippet(snippet: dict[str, Any], chars: int) -> dict[str, Any]:
    text = str(snippet.get("text") or "")
    if len(text) > chars:
        text = text[:chars].rsplit(" ", 1)[0] + "…"
    item = dict(snippet)
    item["text"] = text
    return item


def _select_snippets(
    snippets: list[dict[str, Any]], *, limit: int, chars: int, name: str, state: str | None = None
) -> list[dict[str, Any]]:
    """Keep one strong page per fact, then fill. Fetch order must not decide."""
    snippets = _drop_pages_about_other_places(snippets, name, state)
    ranked = sorted(snippets, key=lambda snippet: _snippet_score(snippet, name), reverse=True)
    chosen: list[dict[str, Any]] = []
    seen: set[str] = set()

    def take(snippet: dict[str, Any]) -> None:
        key = str(snippet.get("url") or id(snippet))
        if key in seen:
            return
        seen.add(key)
        chosen.append(_trim_snippet(snippet, PAGE_EXCERPT_CHARS if snippet.get("fetched") else chars))

    for snippet in ranked:
        if snippet.get("fetched") and len(chosen) < limit:
            take(snippet)
    for _bucket, pattern in _TOPIC_BUCKETS:
        if len(chosen) >= limit:
            break
        best: dict[str, Any] | None = None
        best_score = 0
        for snippet in ranked:
            if str(snippet.get("url") or id(snippet)) in seen:
                continue
            if not re.search(pattern, _snippet_blob(snippet)):
                continue
            score = _snippet_score(snippet, name)
            if score > best_score:
                best = snippet
                best_score = score
        if best is not None:
            take(best)
    for snippet in ranked:
        if len(chosen) >= limit:
            break
        if _snippet_score(snippet, name) <= 0:
            continue
        take(snippet)
    return chosen[:limit]
