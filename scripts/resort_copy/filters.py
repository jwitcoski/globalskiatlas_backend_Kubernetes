"""Flat amenity table for filtering resorts.

A cell is true only when a note, key point, or snippet says the resort has that
amenity. It is false only when the source says the resort does not. Otherwise
the cell is null, which means unknown, not absent.
"""
from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Any

from .config import output_path

FILTERS_PATH = output_path("resort_filters", "filters.jsonl")

# Order is the column order in the CSV.
FILTER_COLUMNS: tuple[str, ...] = (
    "snowtubing",
    "ski_rentals",
    "ski_instructors",
    "night_skiing",
    "terrain_park",
    "nordic",
    "childcare",
    "snowmaking",
)

# Text columns. Blank means the run did not establish the value.
PROFILE_COLUMNS: tuple[str, ...] = ("website", "passes", "price_band")

# CSV headers. Each value is followed by a Source column holding the page URL.
COLUMN_LABELS: dict[str, str] = {
    "website": "Website",
    "passes": "Passes",
    "price_band": "Price",
    "snowtubing": "Snowtubing",
    "ski_rentals": "Ski Rentals",
    "ski_instructors": "Ski Instructors",
    "night_skiing": "Night Skiing",
    "terrain_park": "Terrain Park",
    "nordic": "Nordic",
    "childcare": "Childcare",
    "snowmaking": "Snowmaking",
}

# Approximate USD rates so a euro or yen day ticket lands in the same band.
_USD_RATES: dict[str, float] = {
    "USD": 1.0,
    "CAD": 0.73,
    "AUD": 0.66,
    "NZD": 0.60,
    "EUR": 1.08,
    "GBP": 1.27,
    "CHF": 1.13,
    "JPY": 0.0067,
    "SEK": 0.094,
    "NOK": 0.092,
}

_MEGA_PASSES = ("Epic", "Ikon", "Indy", "Mountain Collective", "Power Pass")
_PASS_PATTERNS: tuple[tuple[str, str], ...] = (
    ("Mountain Collective", r"mountain collective"),
    ("Power Pass", r"\bpower pass\b"),
    ("Nord Pass", r"\bnord pass\b"),
    ("Epic", r"\bepic(?:\s+pass)?\b"),
    ("Ikon", r"\bikon(?:\s+(?:pass|base))?\b"),
    ("Indy", r"\bindy(?:\s+pass)?\b"),
)
_PASS_JUNK = re.compile(
    r"not found|one pass this resort|local area pass|schema|ski course|lessons?",
    re.IGNORECASE,
)
_NOT_RESORT_SITE = (
    "wikipedia.",
    "wikidata.",
    "tripadvisor.",
    "booking.com",
    "liftopia.",
    "expedia.",
    "facebook.",
    "instagram.",
    "youtube.",
    "google.",
)


class _Spec:
    def __init__(self, key: str, yes: str, no: str, reject: str | None = None, key_point: str | None = None) -> None:
        self.key = key
        self.yes = re.compile(yes, re.IGNORECASE)
        self.no = re.compile(no, re.IGNORECASE)
        self.reject = re.compile(reject, re.IGNORECASE) if reject else None
        self.key_point = key_point or key


_SPECS: tuple[_Spec, ...] = (
    _Spec(
        "snowtubing",
        r"snow tubing|snowtubing|snowtube|tube park|tubing park|tubing hill",
        r"\b(?:no|not|without)\b[^.]{0,40}(?:snow tubing|snowtubing|tube park|tubing park)",
        key_point="tubing",
    ),
    _Spec(
        "ski_rentals",
        r"(?:ski|snowboard|equipment|gear) rentals?|rental (?:shop|center|centre|department)|"
        r"rent (?:skis|a snowboard|equipment|gear)",
        r"\b(?:no|not|without)\b[^.]{0,40}(?:ski|snowboard|equipment) rentals?",
        reject=r"vacation rental|condo rental|house rental|chalet rental|townhouse rental|hotel rental",
    ),
    _Spec(
        "ski_instructors",
        r"ski instructors?|snowboard instructors?|ski school|snowboard school|ski lessons?|"
        r"learn to ski|ski instruction",
        r"\b(?:no|not|without)\b[^.]{0,40}(?:ski school|ski instructors?|ski lessons?)",
        key_point="ski_school",
    ),
    _Spec(
        "night_skiing",
        r"night ski(?:ing)?|skiing at night",
        r"\b(?:no|not|without)\b[^.]{0,30}night ski",
    ),
    _Spec(
        "terrain_park",
        r"terrain parks?|snow parks?",
        r"\b(?:no|not|without)\b[^.]{0,30}terrain park",
    ),
    _Spec(
        "nordic",
        r"\bnordic\b|cross-country ski|cross country ski",
        r"\b(?:no|not|without)\b[^.]{0,30}(?:nordic|cross-country)",
    ),
    _Spec(
        "childcare",
        r"childcare|child care|kids'? club|day care|daycare",
        r"\b(?:no|not|without)\b[^.]{0,30}(?:childcare|child care|kids'? club)",
    ),
    _Spec(
        "snowmaking",
        r"snowmaking|snow making|snow guns|snow cannons",
        r"\b(?:no|not|without)\b[^.]{0,30}snow ?making",
    ),
)


def _sentences(text: str) -> list[str]:
    parts = re.split(r"(?<=[.!?])\s+|\n+", text or "")
    return [part.strip() for part in parts if part.strip()]


def _host(url: str) -> str:
    match = re.search(r"https?://([^/]+)", (url or "").lower())
    if not match:
        return ""
    return match.group(1).removeprefix("www.").split(":")[0]


def _snippet_index(snippets: list[dict[str, Any]]) -> dict[str, str]:
    index: dict[str, str] = {}
    for snippet in snippets:
        sid = str(snippet.get("id") or "")
        url = str(snippet.get("url") or "")
        if sid and url:
            index[sid] = url
    return index


def _urls_from_ids(ids: Any, index: dict[str, str]) -> list[str]:
    urls: list[str] = []
    for sid in ids or []:
        url = index.get(str(sid))
        if url and url not in urls:
            urls.append(url)
    return urls


def _point_urls(value: Any, index: dict[str, str]) -> list[str]:
    if isinstance(value, dict):
        urls = [str(url) for url in value.get("sources") or [] if url]
        if not urls:
            urls = _urls_from_ids(value.get("snippet_ids"), index)
        return urls
    if isinstance(value, list):
        urls = []
        for item in value:
            for url in _point_urls(item, index):
                if url not in urls:
                    urls.append(url)
        return urls
    return []


def classify_source(urls: list[str], official_host: str) -> dict[str, str]:
    """official when a cited page is on the resort's own host. Otherwise third_party or unknown."""
    cleaned = [url for url in urls if _host(url)]
    if official_host:
        for url in cleaned:
            host = _host(url)
            if host == official_host or host.endswith("." + official_host):
                return {"kind": "official", "url": url}
    if cleaned:
        return {"kind": "third_party", "url": cleaned[0]}
    return {"kind": "unknown", "url": ""}


def _evidence_bits(
    notes: list[dict[str, Any]],
    snippets: list[dict[str, Any]],
) -> list[tuple[str, list[str]]]:
    index = _snippet_index(snippets)
    bits: list[tuple[str, list[str]]] = []
    for note in notes:
        text = str(note.get("text") or "").strip()
        if text:
            bits.append((text, _urls_from_ids(note.get("snippet_ids"), index)))
    for snippet in snippets:
        text = f"{snippet.get('title') or ''} {snippet.get('text') or ''}".strip()
        url = str(snippet.get("url") or "")
        if text:
            bits.append((text, [url] if url else []))
    return bits


def _judge(spec: _Spec, text: str) -> str | None:
    """Return yes, no, or None for one chunk of text."""
    yes_hit = False
    no_hit = False
    for sentence in _sentences(text):
        if spec.reject and spec.reject.search(sentence) and spec.yes.search(sentence):
            continue
        if spec.no.search(sentence):
            no_hit = True
        elif spec.yes.search(sentence):
            yes_hit = True
    if yes_hit and no_hit:
        return None
    if yes_hit:
        return "yes"
    if no_hit:
        return "no"
    return None


def _from_key_point(
    spec: _Spec,
    key_points: dict[str, Any],
    index: dict[str, str],
) -> tuple[str | None, str, list[str]]:
    raw = key_points.get(spec.key_point)
    if not isinstance(raw, dict):
        return None, "", []
    text = str(raw.get("value") or "")
    urls = _point_urls(raw, index)
    value = text.strip().lower()
    if value in {"yes", "true"}:
        return "yes", text, urls
    if value in {"no", "false"}:
        return "no", text, urls
    return _judge(spec, text), text, urls


def extract_filters(
    *,
    notes: list[dict[str, Any]] | None = None,
    snippets: list[dict[str, Any]] | None = None,
    key_points: dict[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    """One entry per filter column. value is True, False, or None."""
    notes = notes or []
    snippets = snippets or []
    key_points = key_points or {}
    index = _snippet_index(snippets)
    bits = _evidence_bits(notes, snippets)
    found: dict[str, dict[str, Any]] = {}
    for spec in _SPECS:
        decision: str | None = None
        evidence = ""
        urls: list[str] = []
        point_decision, point_text, point_urls = _from_key_point(spec, key_points, index)
        if point_decision:
            decision = point_decision
            evidence = point_text
            urls = list(point_urls)
        for text, bit_urls in bits:
            judged = _judge(spec, text)
            if judged is None:
                continue
            if decision is None:
                decision, evidence, urls = judged, text[:240], list(bit_urls)
            elif judged != decision:
                decision = None
                evidence = ""
                urls = []
                break
            else:
                for url in bit_urls:
                    if url not in urls:
                        urls.append(url)
        found[spec.key] = {
            "value": {"yes": True, "no": False}.get(decision or "", None) if decision else None,
            "evidence": evidence,
            "source": urls[0] if urls else "",
            "sources": urls,
        }
    return found


def missing_amenity_topics(
    notes: list[dict[str, Any]] | None = None,
    snippets: list[dict[str, Any]] | None = None,
    key_points: dict[str, Any] | None = None,
) -> list[str]:
    """Amenities the user filters on that this resort has not confirmed or denied."""
    flags = extract_filters(notes=notes, snippets=snippets, key_points=key_points)
    wanted = ("snowtubing", "ski_rentals", "ski_instructors")
    return [key for key in wanted if flags[key]["value"] is None]


def _point_text(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("value") or "").strip()
    if isinstance(value, list):
        parts = []
        for item in value:
            if isinstance(item, dict) and item.get("value"):
                parts.append(str(item["value"]).strip())
            elif isinstance(item, str) and item.strip():
                parts.append(item.strip())
        return " | ".join(parts)
    return ""


def _website_cite(
    key_points: dict[str, Any] | None,
    notes: list[dict[str, Any]] | None = None,
    snippets: list[dict[str, Any]] | None = None,
) -> tuple[str | None, list[str]]:
    """Resort URL plus the pages that stated it."""
    index = _snippet_index(snippets or [])
    candidates: list[tuple[str, list[str]]] = []
    raw = (key_points or {}).get("website")
    site = _point_text(raw)
    if site:
        candidates.append((site, _point_urls(raw, index)))
    for note in notes or []:
        cites = _urls_from_ids(note.get("snippet_ids"), index)
        for found in re.findall(r"https?://[^\s)>\"]+", str(note.get("text") or "")):
            candidates.append((found, cites))
    for raw_url, cites in candidates:
        url = raw_url.split("?")[0].rstrip(".,")
        host = _host(url)
        if not host or any(blocked in host for blocked in _NOT_RESORT_SITE):
            continue
        if url.lower().startswith("http"):
            return url, cites
    return None, []


def resort_website(key_points: dict[str, Any] | None, notes: list[dict[str, Any]] | None = None) -> str | None:
    """Official-looking URL. Directory and social hosts are skipped."""
    url, _cites = _website_cite(key_points, notes)
    return url


def _named_passes(text: str) -> list[str]:
    if not text or _PASS_JUNK.search(text) or len(text) > 60:
        return []
    if re.search(r"\b(?:sits|from|hours|located|near|road|citizens|holders)\b", text, re.IGNORECASE):
        return []
    found: list[str] = []
    remainder = text
    for label, pattern in _PASS_PATTERNS:
        if re.search(pattern, remainder, re.IGNORECASE):
            found.append(label)
            remainder = re.sub(pattern, " ", remainder, flags=re.IGNORECASE)
    leftover = re.sub(r"\b(?:pass|ski|the|a|an|or)\b", " ", remainder, flags=re.IGNORECASE)
    leftover = re.sub(r"[^A-Za-z0-9'&+/ -]+", " ", leftover)
    leftover = re.sub(r"\s+", " ", leftover).strip(" -")
    if " " not in leftover and len(leftover) < 5:
        return found
    if leftover and leftover.lower() not in {"pass", "epic", "ikon", "snow"} and leftover not in found:
        found.append(leftover)
    return found


def _passes_cite(
    key_points: dict[str, Any] | None,
    notes: list[dict[str, Any]] | None = None,
    snippets: list[dict[str, Any]] | None = None,
) -> tuple[list[str], list[str]]:
    """Named passes and the pages that named them."""
    index = _snippet_index(snippets or [])
    points = key_points or {}
    chunks: list[tuple[str, list[str]]] = []
    for field in ("pass_affiliation", "pass_products"):
        raw = points.get(field)
        text = _point_text(raw)
        if text:
            chunks.append((text, _point_urls(raw, index)))
    for note in notes or []:
        if note.get("topic") == "pass":
            chunks.append((str(note.get("text") or ""), _urls_from_ids(note.get("snippet_ids"), index)))
    names: list[str] = []
    urls: list[str] = []
    for chunk, cites in chunks:
        added = False
        for piece in re.split(r"\s*\|\s*|,\s*", chunk):
            for name in _named_passes(piece):
                if name not in names:
                    names.append(name)
                    added = True
        if added:
            for url in cites:
                if url not in urls:
                    urls.append(url)
    mega = [name for name in names if name in _MEGA_PASSES]
    if len(mega) >= 2:
        names = [name for name in names if name not in _MEGA_PASSES]
        if not names:
            urls = []
    return names, urls


def resort_passes(key_points: dict[str, Any] | None, notes: list[dict[str, Any]] | None = None) -> list[str]:
    """Named passes. Two or more of Epic, Ikon, Indy, or Mountain Collective means a roundup, not this resort."""
    names, _urls = _passes_cite(key_points, notes)
    return names


def _amount(token: str) -> float | None:
    cleaned = token.replace(",", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


def day_price_usd(text: str) -> tuple[float, str] | None:
    """Adult day price in USD, plus the matched text. Multi-day prices are split per day."""
    if not text or re.search(r"per night|/night|\ba night\b", text, re.IGNORECASE):
        return None
    if re.search(r"season pass|membership", text, re.IGNORECASE) and not re.search(
        r"day ticket|day pass|lift ticket|daily", text, re.IGNORECASE
    ):
        return None
    days = 1
    day_count = re.search(r"(\d+)\s*-\s*days?|\b(\d+)\s+days?\b", text, re.IGNORECASE)
    if day_count:
        days = int(day_count.group(1) or day_count.group(2))
    patterns = (
        (r"¥\s?([\d,]+(?:\.\d+)?)", "JPY"),
        (r"€\s?([\d,]+(?:\.\d+)?)", "EUR"),
        (r"£\s?([\d,]+(?:\.\d+)?)", "GBP"),
        (r"\bC\$\s?([\d,]+(?:\.\d+)?)", "CAD"),
        (r"\bA\$\s?([\d,]+(?:\.\d+)?)", "AUD"),
        (r"\bNZD\s?\$?\s?([\d,]+(?:\.\d+)?)", "NZD"),
        (r"([\d,]+(?:\.\d+)?)\s?NZD\b", "NZD"),
        (r"\$\s?([\d,]+(?:\.\d+)?)\s*(CAD|USD|AUD|NZD|EUR|GBP|CHF)\b", None),
        (r"\b(CAD|USD|AUD|NZD|EUR|GBP|CHF|JPY)\s?\$?\s?([\d,]+(?:\.\d+)?)", None),
        (r"\$\s?([\d,]+(?:\.\d+)?)", "USD"),
    )
    for pattern, forced in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if not match:
            continue
        if forced is None and match.lastindex == 2 and match.group(1).isalpha():
            code, number = match.group(1).upper(), _amount(match.group(2))
        elif forced is None:
            number, code = _amount(match.group(1)), match.group(2).upper()
        else:
            number, code = _amount(match.group(1)), forced
        if number is None or number <= 0:
            continue
        per_day = number / days if days > 1 else number
        usd = per_day * _USD_RATES[code]
        if usd < 15:
            continue
        return round(usd, 2), match.group(0)
    return None


def price_band(usd: float) -> str:
    """$ is 50 USD or less, $$ is 50–100, $$$ is 100–200, $$$$ is above 200."""
    if usd <= 50:
        return "$"
    if usd <= 100:
        return "$$"
    if usd <= 200:
        return "$$$"
    return "$$$$"


def _price_cite(
    key_points: dict[str, Any] | None,
    notes: list[dict[str, Any]] | None = None,
    snippets: list[dict[str, Any]] | None = None,
) -> tuple[str | None, str, list[str]]:
    """Price band, the matched amount, and the pages that stated it."""
    index = _snippet_index(snippets or [])
    points = key_points or {}
    texts: list[tuple[str, list[str]]] = []
    for field in ("lift_ticket", "weekend_ticket", "night_ticket"):
        raw = points.get(field)
        texts.append((_point_text(raw), _point_urls(raw, index)))
    for note in notes or []:
        text = str(note.get("text") or "")
        if note.get("topic") == "price" or re.search(r"day pass|lift ticket|day ticket", text, re.I):
            texts.append((text, _urls_from_ids(note.get("snippet_ids"), index)))
    for text, cites in texts:
        if not re.search(r"\b(?:day|daily|adult|lift|tickets?)\b", text, re.IGNORECASE):
            continue
        parsed = day_price_usd(text)
        if parsed:
            usd, shown = parsed
            return price_band(usd), f"{shown} (${usd:.0f})", cites
    return None, "", []


def resort_price(key_points: dict[str, Any] | None, notes: list[dict[str, Any]] | None = None) -> tuple[str | None, str]:
    """Price band from a day ticket. Season passes and per-night rates are ignored."""
    band, shown, _urls = _price_cite(key_points, notes)
    return band, shown


def filter_row(
    *,
    resort: str,
    tier: str | None = None,
    country: str | None = None,
    state: str | None = None,
    winter_sports_id: str | None = None,
    page_id: str | None = None,
    notes: list[dict[str, Any]] | None = None,
    snippets: list[dict[str, Any]] | None = None,
    key_points: dict[str, Any] | None = None,
) -> dict[str, Any]:
    flags = extract_filters(notes=notes, snippets=snippets, key_points=key_points)
    row: dict[str, Any] = {
        "winterSportsId": winter_sports_id,
        "pageId": page_id,
        "resort": resort,
        "tier": tier,
        "country": country,
        "state": state,
    }
    evidence: dict[str, str] = {}
    for key in FILTER_COLUMNS:
        item = flags[key]
        row[key] = item["value"]
        if item["value"] is not None and item["evidence"]:
            evidence[key] = item["evidence"]
    website, website_urls = _website_cite(key_points, notes, snippets)
    passes, pass_urls = _passes_cite(key_points, notes, snippets)
    band, price_evidence, price_urls = _price_cite(key_points, notes, snippets)
    row["website"] = website
    row["passes"] = passes
    row["price_band"] = band
    official_host = _host(website or "")
    sources: dict[str, dict[str, str]] = {}
    if website:
        evidence["website"] = website
        sources["website"] = classify_source(website_urls, official_host)
    if passes:
        evidence["passes"] = ", ".join(passes)
        sources["passes"] = classify_source(pass_urls, official_host)
    if price_evidence:
        evidence["price_band"] = price_evidence
        sources["price_band"] = classify_source(price_urls, official_host)
    for key in FILTER_COLUMNS:
        if row.get(key) is None:
            continue
        sources[key] = classify_source(list(flags[key].get("sources") or []), official_host)
    row["evidence"] = evidence
    row["sources"] = sources
    return row


def filter_row_from_result(
    result: dict[str, Any],
    *,
    winter_sports_id: str | None = None,
    page_id: str | None = None,
    country: str | None = None,
    state: str | None = None,
) -> dict[str, Any]:
    if isinstance(result.get("filters"), dict) and "snowtubing" in result["filters"]:
        row = dict(result["filters"])
        row["winterSportsId"] = winter_sports_id or row.get("winterSportsId")
        row["pageId"] = page_id or row.get("pageId")
        return row
    label = str(result.get("resort_label") or result.get("resort") or "")
    return filter_row(
        resort=label,
        tier=result.get("tier"),
        country=country,
        state=state,
        winter_sports_id=winter_sports_id,
        page_id=page_id,
        notes=result.get("notes") or [],
        snippets=result.get("snippets") or [],
        key_points=result.get("key_points") or {},
    )


def upsert_filter_row(record: dict[str, Any], path: Path = FILTERS_PATH) -> None:
    """One row per resort. A later run replaces the earlier row."""
    path.parent.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            item = json.loads(line)
            same_id = record.get("winterSportsId") and item.get("winterSportsId") == record.get("winterSportsId")
            same_name = not record.get("winterSportsId") and item.get("resort") == record.get("resort")
            if same_id or same_name:
                continue
            rows.append(item)
    rows.append(record)
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def load_filter_rows(path: Path = FILTERS_PATH) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def rows_from_copy_jsonl(path: Path) -> list[dict[str, Any]]:
    """Build filter rows from an existing resort-copy JSONL."""
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        item = json.loads(line)
        row = filter_row_from_result(
            item,
            winter_sports_id=item.get("winterSportsId"),
            page_id=item.get("pageId"),
        )
        key = str(row.get("winterSportsId") or row.get("resort") or "")
        if not key or key in seen:
            continue
        seen.add(key)
        rows.append(row)
    return rows


def write_filter_csv(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = (*PROFILE_COLUMNS, *FILTER_COLUMNS)
    fieldnames = ["Resort"]
    for column in columns:
        label = COLUMN_LABELS[column]
        fieldnames.extend((label, f"{label} Source"))
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            cited = row.get("sources") if isinstance(row.get("sources"), dict) else {}
            flat = {"Resort": row.get("resort") or ""}
            for column in columns:
                label = COLUMN_LABELS[column]
                value = row.get(column)
                if isinstance(value, list):
                    shown = ", ".join(str(item) for item in value)
                elif isinstance(value, bool):
                    shown = "true" if value else "false"
                elif value is None:
                    shown = ""
                else:
                    shown = str(value)
                item = cited.get(column) if isinstance(cited, dict) else None
                source = ""
                if shown and isinstance(item, dict):
                    source = str(item.get("url") or "")
                flat[label] = shown
                flat[f"{label} Source"] = source
            writer.writerow(flat)


def select_rows(rows: list[dict[str, Any]], wanted: list[str], *, match_all: bool = True) -> list[dict[str, Any]]:
    """Keep resorts whose filter cells are true. Unknown and false stay out."""
    keys = [key for key in wanted if key in FILTER_COLUMNS]
    if not keys:
        return rows

    def hits(row: dict[str, Any]) -> bool:
        flags = [row.get(key) is True for key in keys]
        return all(flags) if match_all else any(flags)

    return [row for row in rows if hits(row)]
