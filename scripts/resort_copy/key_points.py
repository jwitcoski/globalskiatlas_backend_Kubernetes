"""Harvest and assemble the structured key-points record (prices, passes, links)."""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from .config import (
    KEY_POINTS_PATH,
)
from .snippets import (
    _GENERIC_NAME_WORDS,
    _mentions_resort,
    _name_needles,
)


# OSM does not have these. The search pass keeps each one only when a snippet says it.
_KEY_POINT_FIELDS = (
    "website",
    "owner",
    "operator_group",
    "pass_affiliation",
    "operator_status",
    "year_started",
    "lift_ticket",
    "weekend_ticket",
    "night_ticket",
    "season_pass",
    "drive",
    "parking",
    "visit_type",
    "ski_school",
    "childcare",
    "season_open",
    "season_close",
    "snowmaking",
    "glacier",
    "night_skiing",
    "terrain_park",
    "nordic",
    "tubing",
    "village_type",
    "sectors",
    "terrain",
    "famous_for",
    "on_hill_lodge",
)


_YES_NO_FIELDS = (
    "ski_school",
    "childcare",
    "snowmaking",
    "glacier",
    "night_skiing",
    "terrain_park",
    "nordic",
    "tubing",
)


_SOCIAL_HOSTS = {
    "instagram.com": "instagram",
    "facebook.com": "facebook",
    "twitter.com": "twitter",
    "x.com": "x",
    "youtube.com": "youtube",
    "tiktok.com": "tiktok",
}


_NOT_RESORT_SITE = (
    "wikipedia.org",
    "wikidata.org",
    "tripadvisor.",
    "booking.com",
    "expedia.",
    "snow-forecast.com",
    "j2ski.com",
    "onthesnow.",
    "skiresort.com",
    "snow-online.com",
    "skicentral.com",
    "yelp.com",
    "google.",
    "grokipedia.com",
    "youtube.com",
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "tiktok.com",
    "x.com",
)


def _cited_urls(ids: list[str], snippets: list[dict[str, Any]]) -> list[str]:
    urls: list[str] = []
    for sid in ids:
        for snippet in snippets:
            if str(snippet.get("id")) == sid and snippet.get("url"):
                url = str(snippet["url"])
                if url not in urls:
                    urls.append(url)
    return urls


def _host(url: str) -> str:
    match = re.search(r"https?://([^/]+)", url.lower())
    return match.group(1).removeprefix("www.") if match else ""


def _point_value(text: str, ids: list[str], snippets: list[dict[str, Any]]) -> dict[str, Any] | None:
    cleaned = text.strip()
    if not cleaned or cleaned.lower().startswith("not found"):
        return None
    return {"value": cleaned, "snippet_ids": ids, "sources": _cited_urls(ids, snippets)}


def _social_links(snippets: list[dict[str, Any]], name: str = "") -> list[dict[str, str]]:
    found: list[dict[str, str]] = []
    seen: set[str] = set()
    tokens = [needle for needle in _name_needles(name) if " " not in needle and len(needle) >= 4]
    for snippet in snippets:
        url = str(snippet.get("url") or "")
        low_url = url.lower()
        if "/watch" in low_url or "/shorts" in low_url:
            continue
        host = _host(url)
        if tokens and not any(token in low_url for token in tokens):
            continue
        for domain, network in _SOCIAL_HOSTS.items():
            if host == domain or host.endswith("." + domain):
                if url not in seen:
                    seen.add(url)
                    found.append({"network": network, "url": url, "snippet_id": str(snippet.get("id") or "")})
    return found


def _host_matches_resort(host: str, url: str, name: str) -> bool:
    """A short token such as 'bear' is not enough to claim another resort's site."""
    words = [
        word
        for word in re.findall(r"[a-z0-9]+", (name or "").lower())
        if word not in _GENERIC_NAME_WORDS and len(word) >= 3
    ]
    joined = "".join(words[:2])
    if len(joined) >= 6 and joined in host:
        return True
    return any(len(word) >= 6 and word in host for word in words)


def _official_website(snippets: list[dict[str, Any]], name: str) -> dict[str, Any] | None:
    best: dict[str, Any] | None = None
    for snippet in snippets:
        url = str(snippet.get("url") or "")
        host = _host(url)
        if not host or any(blocked in host for blocked in _NOT_RESORT_SITE):
            continue
        if not _host_matches_resort(host, url, name):
            continue
        path = url.split(host, 1)[-1]
        score = 2 if path in ("", "/", "/index.html") else 1
        if best is None or score > best["score"]:
            best = {
                "score": score,
                "value": url.split("?")[0],
                "snippet_ids": [str(snippet.get("id") or "")],
                "sources": [url],
            }
    if best is None:
        return None
    best.pop("score")
    return best


_MONEY = re.compile(
    r"(?:(?:CAD|USD|EUR|GBP|CHF|SEK|NOK|AUD|NZD)\s?\$?\s?\d[\d,]*(?:\.\d{2})?"
    r"|[$€£]\s?\d[\d,]*(?:\.\d{2})?(?:\s?(?:CAD|USD|EUR|GBP|CHF))?)",
    re.IGNORECASE,
)


_YES_PATTERNS = {
    "ski_school": r"ski school|ski lessons",
    "childcare": r"childcare|kids club|day care|daycare",
    "snowmaking": r"snowmaking|snow making|snow guns",
    "glacier": r"\bglacier\b",
    "night_skiing": r"night ski",
    "terrain_park": r"terrain park",
    "nordic": r"\bnordic\b|cross-country|cross country",
    "tubing": r"snow tubing|snowtubing|tube park|tubing park|tubing hill",
}


_CHAINS = (
    ("Vail Resorts", r"vail resorts"),
    ("Alterra", r"\balterra\b"),
    ("Boyne", r"\bboyne\b"),
    ("Powdr", r"\bpowdr\b"),
    ("Compagnie des Alpes", r"compagnie des alpes"),
)


_PASS_NAMES = (
    ("Epic", r"\bepic pass\b"),
    ("Ikon", r"\bikon pass\b"),
    ("Indy", r"\bindy pass\b"),
    ("Mountain Collective", r"mountain collective"),
    ("Power Pass", r"\bpower pass\b"),
)


_AUDIENCE = (
    ("club", r"ski club|members only|not-for-profit"),
    ("beginners", r"beginner|learned to ski|never-ever|first time"),
    ("families", r"\bfamil(?:y|ies)\b"),
    ("destination", r"destination resort|fly-?in|international visitors"),
    ("experts", r"\bexperts?\b|advanced terrain|off piste|off-piste"),
)


_MONTHS = (
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
)


def _snippet_text(snippet: dict[str, Any]) -> str:
    return f"{snippet.get('title') or ''} {snippet.get('text') or ''}"


def _as_yes_no(text: str) -> str | None:
    cleaned = text.strip().lower()
    if cleaned in {"yes", "true", "y"}:
        return "yes"
    if cleaned in {"no", "false", "n"}:
        return "no"
    if cleaned.startswith("not found"):
        return None
    if re.search(r"\b(no|not|without)\b", cleaned):
        return "no"
    if re.search(r"\b(yes|has|offers|open)\b", cleaned):
        return "yes"
    return None


def _price_in(text: str, label: str) -> str | None:
    for match in re.finditer(label, text, flags=re.IGNORECASE):
        window = text[max(0, match.start() - 50) : match.end() + 50]
        if re.search(r"per night|a night|/night", window, flags=re.IGNORECASE):
            continue
        money = _MONEY.search(window)
        if money:
            return re.sub(r"\s+", " ", money.group(0)).strip()
    return None


def _from_snippet(snippet: dict[str, Any], value: str) -> dict[str, Any]:
    sid = str(snippet.get("id") or "")
    url = str(snippet.get("url") or "")
    return {
        "value": value,
        "snippet_ids": [sid] if sid else [],
        "sources": [url] if url else [],
    }


def _harvest_key_points(snippets: list[dict[str, Any]]) -> dict[str, Any]:
    """Pull comparable fields straight from snippet text. Missing stays missing."""
    found: dict[str, Any] = {"pass_products": [], "audience": []}
    for snippet in snippets:
        text = _snippet_text(snippet)
        low = text.lower()
        for field, pattern in _YES_PATTERNS.items():
            if field not in found and re.search(pattern, low):
                if not re.search(r"\bno\b.{0,24}(?:" + pattern + ")", low):
                    found[field] = _from_snippet(snippet, "yes")
        for label, pattern in _CHAINS:
            if "operator_group" in found:
                break
            if re.search(pattern, low) and re.search(r"operated by|owned by|part of", low):
                found["operator_group"] = _from_snippet(snippet, label)
        if "operator_group" not in found and re.search(r"independently (owned|operated)", low):
            found["operator_group"] = _from_snippet(snippet, "independent")
        if "owner" not in found:
            owner = re.search(r"(?:owned|operated) by ([A-Z][^.]{2,80})", text)
            if owner:
                found["owner"] = _from_snippet(snippet, re.sub(r"\s+", " ", owner.group(1)).strip(" ."))
        if "operator_status" not in found and re.search(r"not-for-profit|nonprofit|non-profit", low):
            found["operator_status"] = _from_snippet(snippet, "not-for-profit")
        elif "operator_status" not in found and re.search(r"\bski club\b", low):
            found["operator_status"] = _from_snippet(snippet, "club")
        for label, pattern in _PASS_NAMES:
            if any(label.lower() in item["value"].lower() or item["value"].lower() in label.lower() for item in found["pass_products"]):
                continue
            if not re.search(pattern, low):
                continue
            if re.search(r"not on|is not on|isn't on", low):
                continue
            found["pass_products"].append(_from_snippet(snippet, label))
        for field, label in (
            ("lift_ticket", r"day pass|day ticket|lift ticket|adult (?:day )?ticket"),
            ("weekend_ticket", r"weekend (?:day )?(?:pass|ticket|rate)"),
            ("night_ticket", r"night (?:pass|ticket|rate)"),
            ("season_pass", r"season pass|membership (?:fee|cost|price)"),
        ):
            if field in found or _LISTICLE.search(f"{snippet.get('title') or ''} {snippet.get('url') or ''}".lower()):
                continue
            price = _price_in(text, label)
            if price:
                found[field] = _from_snippet(snippet, price)
        if "drive" not in found:
            drive = re.search(
                r"\d+\s*(?:minute|min|km|kilometre|kilometer|mile)s?\s+"
                r"(?:from|north of|south of|east of|west of|outside|"
                r"southwest of|northeast of|northwest of|southeast of)\s+"
                r"[A-Za-z][\w'-]+(?:\s+[A-Za-z][\w'-]+){0,2}",
                text,
                flags=re.IGNORECASE,
            )
            if drive:
                found["drive"] = _from_snippet(snippet, re.sub(r"\s+", " ", drive.group(0)).strip())
        if "parking" not in found:
            if re.search(r"free parking", low):
                found["parking"] = _from_snippet(snippet, "free")
            elif re.search(r"paid parking|parking fee|pay to park", low):
                found["parking"] = _from_snippet(snippet, "paid")
            elif re.search(r"parking reservation|reserve(?:d| a)? parking", low):
                found["parking"] = _from_snippet(snippet, "reservation")
        if "visit_type" not in found:
            day = re.search(r"day trip|day-trip", low)
            stay = re.search(r"stay over|overnight", low)
            if day and not stay:
                found["visit_type"] = _from_snippet(snippet, "day trip")
            elif stay and not day:
                found["visit_type"] = _from_snippet(snippet, "stay over")
        for label, pattern in _AUDIENCE:
            if any(item["value"] == label for item in found["audience"]):
                continue
            if re.search(pattern, low):
                found["audience"].append(_from_snippet(snippet, label))
        if "season_open" not in found:
            opened = re.search(
                r"(?:opens|opening|open from|from)\s+(?:in\s+)?(" + "|".join(_MONTHS) + r")",
                low,
            )
            if opened:
                found["season_open"] = _from_snippet(snippet, opened.group(1))
        if "season_close" not in found:
            closed = re.search(
                r"(?:closes|closing|until|through)\s+(?:in\s+)?(" + "|".join(_MONTHS) + r")",
                low,
            )
            if closed and (found.get("season_open") or {}).get("value") != closed.group(1):
                found["season_close"] = _from_snippet(snippet, closed.group(1))
        if "village_type" not in found:
            if re.search(r"ski-in|ski out|ski-out", low):
                found["village_type"] = _from_snippet(snippet, "ski-in ski-out")
            elif re.search(r"purpose-built|purpose built|car-free", low):
                found["village_type"] = _from_snippet(snippet, "purpose-built")
            elif re.search(r"old town", low):
                found["village_type"] = _from_snippet(snippet, "old town")
        if "sectors" not in found:
            sectors = re.search(r"\b(\d+)\s+(?:villages|sectors|areas)\b", low)
            if sectors:
                link = ""
                if re.search(r"gondola", low):
                    link = ", linked by gondola"
                elif re.search(r"ski bus", low):
                    link = ", linked by ski bus"
                found["sectors"] = _from_snippet(snippet, f"{sectors.group(1)}{link}")
        if "terrain" not in found:
            for label, pattern in (
                ("expert", r"\bexperts?\b|off piste|off-piste"),
                ("powder", r"\bpowder\b"),
                ("intermediate", r"\bintermediate\b"),
                ("beginner hill", r"\bbeginners?\b"),
            ):
                if re.search(pattern, low):
                    found["terrain"] = _from_snippet(snippet, label)
                    break
    return found


_PRICE_FIELDS = frozenset({"lift_ticket", "weekend_ticket", "night_ticket", "season_pass"})


_PRICE = re.compile(
    r"[$€£¥]\s?\d|\d[\d,.]*\s?(?:usd|eur|chf|cad|gbp|nok|sek|jpy|aud|nzd|dollars|euros|francs|kr)\b",
    re.IGNORECASE,
)


# A ranking of cheap resorts quotes an old price for a hundred hills at once.
_LISTICLE = re.compile(r"cheapest|\bbest\b|\btop \d+|\d+ best|ranked|ranking|compared|\bvs\b")


_ON_HILL = r"ski[- ]in|slopeside|slope-side|on[- ]mountain|on the mountain|at the base|base lodge|summit lodge"


_LODGING_PAGE = r"lodging|hotel|where to stay|where-to-stay|accommodation|places to stay|vacation rental"


def assemble_key_points(
    notes: list[dict[str, Any]],
    raw_points: dict[str, Any],
    snippets: list[dict[str, Any]],
    name: str,
) -> dict[str, Any]:
    """Facts OSM does not store. Each value keeps the snippet it came from."""
    points: dict[str, Any] = {field: None for field in _KEY_POINT_FIELDS}
    points["social"] = _social_links(snippets, name)
    points["pass_products"] = []
    points["audience"] = []

    def cited_blob(ids: list[str]) -> str:
        return " ".join(
            _snippet_text(snippet)
            for snippet in snippets
            if str(snippet.get("id")) in ids
        ).lower()

    def cited_pages(ids: list[str]) -> str:
        return " ".join(
            f"{snippet.get('title') or ''} {snippet.get('url') or ''}"
            for snippet in snippets
            if str(snippet.get("id")) in ids
        ).lower()

    def take(field: str, item: Any) -> None:
        if points.get(field) or not isinstance(item, dict):
            return
        ids = item.get("snippet_ids")
        if not isinstance(ids, list):
            ids = []
        ids = [str(x).strip() for x in ids if str(x).strip()]
        text = str(item.get("text") or "")
        if field in _PRICE_FIELDS:
            if re.search(r"per night|a night|/night", text, re.IGNORECASE):
                return
            if not _PRICE.search(text) or _LISTICLE.search(cited_pages(ids)):
                return
        if field == "famous_for" and re.search(r"owned|operated|inherited|donated", text, re.IGNORECASE):
            return
        if field in {"village_type", "on_hill_lodge"}:
            blob = cited_blob(ids)
            # A resort's lodging page lists inns in town. That is not a lodge on the hill.
            on_hill = re.search(_ON_HILL, blob) and not re.search(r"\bno\b[^.]{0,30}ski[- ]in", blob)
            if not on_hill and re.search(_LODGING_PAGE, cited_pages(ids)):
                return
            if field == "village_type" and "ski-in" in text.lower() and not on_hill:
                return
        if field in _YES_NO_FIELDS:
            yn = _as_yes_no(text)
            if yn is None:
                return
            text = yn
        if field == "website":
            host = _host(text)
            if not host or not _host_matches_resort(host, text, name):
                return
        if field == "operator_group":
            known = {label.lower() for label, _pattern in _CHAINS}
            if text.lower() not in known and text.lower() != "independent":
                return
            if text.lower() == "independent" and not re.search(
                r"independently (owned|operated)", cited_blob(ids), re.IGNORECASE
            ):
                return
        if field == "drive" and not re.search(
            r"\d+\s*(?:minute|min|km|kilometre|kilometer|mile)s?\s+"
            r"(?:from|north of|south of|east of|west of|outside)\b",
            text,
            re.IGNORECASE,
        ):
            return
        if field == "sectors" and not re.search(r"\b\d+\s+(?:villages|sectors|areas)\b", text, re.IGNORECASE):
            return
        if field == "terrain":
            mapped = next(
                (
                    label
                    for label, pattern in (
                        ("expert", r"\bexperts?\b|off piste|off-piste"),
                        ("powder", r"\bpowder\b"),
                        ("intermediate", r"\bintermediate\b"),
                        ("beginner hill", r"\bbeginners?\b"),
                    )
                    if re.search(pattern, text, re.IGNORECASE)
                ),
                None,
            )
            if mapped is None:
                return
            text = mapped
        if field == "visit_type":
            low_visit = text.lower()
            if "day trip" in low_visit and "stay" in low_visit:
                return
            if "day trip" in low_visit or "day-trip" in low_visit:
                text = "day trip"
            elif "stay over" in low_visit or "overnight" in low_visit:
                text = "stay over"
            else:
                return
        value = _point_value(text, ids, snippets)
        if value is None:
            return
        if ids and not value["sources"]:
            return
        blob = cited_blob(ids)
        if field == "pass_affiliation" and blob and not _mentions_resort(blob, name):
            return
        if field in _YES_NO_FIELDS and blob and not re.search(_YES_PATTERNS[field], blob):
            return
        if ids and blob:
            tokens = re.findall(r"[a-z0-9]{4,}|\d+", text.lower())
            if tokens and not any(token in blob for token in tokens):
                return
        points[field] = value

    def take_list(key: str, item: Any) -> None:
        if not isinstance(item, dict):
            return
        ids = [str(x).strip() for x in (item.get("snippet_ids") or []) if str(x).strip()]
        raw = str(item.get("text") or "")
        if key == "audience":
            labels = [label for label, pattern in _AUDIENCE if re.search(pattern, raw, re.IGNORECASE)]
        elif key == "pass_products":
            labels = [raw.strip()] if raw.strip() else []
        else:
            labels = [raw.strip()] if raw.strip() else []
        for label in labels:
            if key == "pass_products" and any(
                label.lower() in existing.get("value", "").lower()
                or existing.get("value", "").lower() in label.lower()
                for existing in points[key]
            ):
                continue
            value = _point_value(label, ids, snippets)
            if value is None:
                continue
            if any(existing.get("value") == value["value"] for existing in points[key]):
                continue
            points[key].append(value)

    for field in _KEY_POINT_FIELDS:
        take(field, raw_points.get(field))
    for item in raw_points.get("pass_products") or []:
        take_list("pass_products", item)
    for item in raw_points.get("audience") or []:
        take_list("audience", item)
    for item in raw_points.get("social") or []:
        if isinstance(item, dict):
            url = str(item.get("text") or "")
            host = _host(url)
            for domain, network in _SOCIAL_HOSTS.items():
                if host == domain or host.endswith("." + domain):
                    if not any(link["url"] == url for link in points["social"]):
                        points["social"].append(
                            {
                                "network": network,
                                "url": url,
                                "snippet_id": ",".join(str(x) for x in item.get("snippet_ids") or []),
                            }
                        )

    for note in notes:
        topic = note.get("topic")
        if topic == "year_started":
            take("year_started", note)
        elif topic == "pass":
            take("pass_affiliation", note)
            take_list("pass_products", note)
        elif topic == "public_or_private":
            take("operator_status", note)
            if re.search(r"owned by|operated by|ski club|not-for-profit|nonprofit", note.get("text") or "", re.I):
                take("owner", note)
        elif topic == "famous_people":
            take("famous_for", note)
        elif topic == "access":
            take("drive", note)
        elif topic == "lodging" and re.search(r"ski-in|ski out|slopeside|on the hill", note.get("text") or "", re.I):
            take("on_hill_lodge", note)
        elif topic in {"price", "other"}:
            text = note.get("text") or ""
            if re.search(r"season pass|membership", text, re.I):
                take("season_pass", note)
            elif re.search(r"weekend", text, re.I):
                take("weekend_ticket", note)
            elif re.search(r"night", text, re.I):
                take("night_ticket", note)
            elif re.search(r"day pass|lift ticket|day ticket", text, re.I):
                take("lift_ticket", note)
        elif topic == "other" and re.search(
            r"owned by|operated by|ski club|not-for-profit|nonprofit", note.get("text") or "", re.I
        ):
            take("owner", note)
        elif topic == "who":
            take_list("audience", note)
    if points["website"] is None:
        points["website"] = _official_website(snippets, name)
    harvested = _harvest_key_points(snippets)
    for field, value in harvested.items():
        if field in {"pass_products", "audience"}:
            for item in value:
                if any(
                    str(item.get("value") or "").lower() in str(existing.get("value") or "").lower()
                    or str(existing.get("value") or "").lower() in str(item.get("value") or "").lower()
                    for existing in points[field]
                ):
                    continue
                points[field].append(item)
        elif points.get(field) is None:
            points[field] = value
    if points["pass_affiliation"] is None and points["pass_products"]:
        points["pass_affiliation"] = points["pass_products"][0]
    return points


def upsert_key_points(record: dict[str, Any], path: Path = KEY_POINTS_PATH) -> None:
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
    path.write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )
