"""Field checks, entity and marketing filters, length, and the deterministic critic.

These rules run in ordinary Python. They do not invent resort facts.
"""
from __future__ import annotations

import re
from datetime import date
from typing import Any

from . import bedrock
from .entity import mentions_name

OWNERSHIP_ALLOWED = ("public", "private", "municipal", "not-for-profit", "unknown")

_OPENING = re.compile(
    r"\b(?:open(?:ed|ing|s)?|found(?:ed|ing)?|establish(?:ed|ment)?|built|began|"
    r"inaugurat\w*|first (?:ski|season|chairlift|lift))\b",
    re.IGNORECASE,
)
_YEAR_REJECT = re.compile(
    r"post office|postal|timestamp|scraped|schema\.org|dateModified|updated at|"
    r"railway station|church opened|school opened",
    re.IGNORECASE,
)
_ISO_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
_MARKETING = re.compile(
    r"most extraordinary|world-class|world class|hidden gem|\bnestled\b|"
    r"best ski|once-in-a-lifetime|unforgettable|bucket list|"
    r"download (?:the|our) app|\bsknowed\b|steak and malbec|finest ski|"
    r"premier destination|book now|limited time|you(?:'|’)ll love",
    re.IGNORECASE,
)
_PRICE_TOPIC = re.compile(r"day pass|lift ticket|day ticket|season pass|admission|ticket price", re.IGNORECASE)
_CURRENCY = re.compile(
    r"(?:[$€£¥])\s?\d|\d[\d.,]*\s?(?:USD|CAD|AUD|NZD|EUR|GBP|CHF|JPY|SEK|NOK|KRW)\b|"
    r"\b(?:USD|CAD|AUD|NZD|EUR|GBP|CHF|JPY|SEK|NOK)\s?\$?\s?\d",
    re.IGNORECASE,
)
_DISTANCE_TOPIC = re.compile(r"\b(?:from|drive|airport|away|kilomet|minutes from)\b", re.IGNORECASE)
_DISTANCE_UNIT = re.compile(
    r"\d[\d.,]*\s*(?:km|kilomet(?:er|re)s?|mi|miles?|minutes?|mins?|hours?|hrs?)\b",
    re.IGNORECASE,
)
_KM = re.compile(r"\b(\d{2,4})\s*(km|kilomet(?:er|re)s?)\b", re.IGNORECASE)
_RESELLER = ("liftopia.", "tripadvisor.", "booking.com", "sknowed", "expedia.", "hotels.com")
_PASS_OK = ("epicpass.com", "ikonpass.com", "indyskipass.com", "wikipedia.org", "mountaincollective.com")
_CLICHE = re.compile(r"\b(?:nestled|hidden gem|world-class|world class)\b", re.IGNORECASE)
_MOOD = re.compile(
    r"\b(?:fireplaces?|cozy|cosy|hot cocoa|crisp air|winter wonderland|breathtaking)\b",
    re.IGNORECASE,
)
_PLACEHOLDER = re.compile(
    r"^(?:a venir|à venir|a-venir|tbd|tba|todo|unknown|n/?a|test|placeholder|coming soon|"
    r"unnamed|no name|ski area|resort)$",
    re.IGNORECASE,
)
_PROPER = re.compile(r"\b([A-Z][A-Za-z0-9'’-]{3,}(?:\s+[A-Z][A-Za-z0-9'’-]{3,})*)\b")
_STOP_PROPER = frozenset(
    "The This That These Those When Where While After Before During January February March April "
    "May June July August September October November December Monday Tuesday Wednesday Thursday "
    "Friday Saturday Sunday Today Resort Mountain Mount Ski Area Club Pass Valley Lake River "
    "North South East West".split()
)

_COUNTRIES = (
    "afghanistan", "albania", "andorra", "argentina", "australia", "austria", "belgium", "bolivia",
    "bosnia", "bulgaria", "canada", "chile", "china", "croatia", "czechia", "czech republic",
    "denmark", "finland", "france", "georgia", "germany", "greece", "hungary", "iceland", "india",
    "iran", "ireland", "italy", "japan", "kazakhstan", "lesotho", "liechtenstein", "lithuania",
    "luxembourg", "mexico", "mongolia", "montenegro", "morocco", "nepal", "netherlands",
    "new zealand", "north macedonia", "norway", "pakistan", "poland", "portugal", "romania",
    "russia", "serbia", "slovakia", "slovenia", "south africa", "south korea", "spain", "sweden",
    "switzerland", "turkey", "ukraine", "united kingdom", "united states", "united states of america",
)

_COUNTRY_ALIASES = {
    "united states": ("united states", "usa", "u.s."),
    "united states of america": ("united states", "usa", "u.s."),
    "united kingdom": ("united kingdom", "uk", "u.k.", "scotland", "england", "wales"),
    "czech republic": ("czech", "czechia"),
    "czechia": ("czech", "czech republic"),
    "south korea": ("south korea", "korea"),
    "republic of korea": ("south korea", "korea"),
}

MEDIUM_SECTIONS = ("history", "access", "terrain", "prices", "lodging", "scene", "who")
LARGE_SECTIONS = (
    "history", "access", "terrain", "lifts", "prices", "passes",
    "lodging", "season", "school", "scene", "food", "who",
)


def current_year() -> int:
    return date.today().year


def is_placeholder_name(name: str) -> tuple[bool, str]:
    """True when the label is not a resort name and should be skipped."""
    text = re.sub(r"\s+", " ", (name or "").strip())
    if len(text) < 3:
        return True, "name_too_short"
    if _PLACEHOLDER.match(text):
        return True, "placeholder_name"
    if re.fullmatch(r"[\d\W_]+", text):
        return True, "name_not_words"
    return False, ""


def opening_year(text: str, *, now_year: int | None = None) -> int | None:
    """A 4-digit opening year, or None when the sentence is a timestamp or another event."""
    if not text or _ISO_DATE.search(text) or _YEAR_REJECT.search(text):
        return None
    if not _OPENING.search(text):
        return None
    hi = now_year if now_year is not None else current_year()
    found = [int(y) for y in re.findall(r"\b(18[5-9]\d|19\d{2}|20\d{2})\b", text)]
    found = [y for y in found if 1850 <= y <= hi]
    if not found:
        return None
    return found[0]


def ownership_kind(text: str) -> str | None:
    """Map a sentence onto the allowed ownership set, or None if it is not about ownership."""
    low = (text or "").lower()
    if re.search(r"not-for-profit|nonprofit|non-profit|not for profit|ski club", low):
        return "not-for-profit"
    if re.search(r"\bmunicipal\b|town-owned|city-owned|publicly owned", low):
        return "municipal"
    if re.search(r"\bpublic\b", low) and "public transport" not in low and "republic" not in low:
        return "public"
    if re.search(r"\bprivate\b", low):
        return "private"
    if re.search(r"\bunknown\b", low) and re.search(r"owner|operator|private|public", low):
        return "unknown"
    return None


def price_has_currency(text: str) -> bool:
    return bool(_CURRENCY.search(text or ""))


def distance_has_unit(text: str) -> bool:
    return bool(_DISTANCE_UNIT.search(text or ""))


def is_marketing(text: str) -> bool:
    return bool(_MARKETING.search(text or ""))


def _country_mentioned(country: str, text: str) -> bool:
    low = text.lower()
    key = (country or "").strip().lower()
    if not key:
        return True
    if key in low:
        return True
    return any(alias in low for alias in _COUNTRY_ALIASES.get(key, ()))


def country_conflict(text: str, country: str | None, state: str | None = None) -> bool:
    """True when the snippet names some other country and never this resort's country."""
    if not country or _country_mentioned(country, text):
        return False
    low = text.lower()
    state_l = (state or "").strip().lower()
    for other in _COUNTRIES:
        if other == country.strip().lower() or other == state_l:
            continue
        if re.search(rf"\b{re.escape(other)}\b", low):
            return True
    return False


def snippet_entity_decision(
    blob: str,
    name: str,
    *,
    state: str | None = None,
    country: str | None = None,
    aliases: list[str] | None = None,
) -> tuple[bool, str]:
    """Keep a snippet only when it names this resort and stays in its country."""
    if not (blob or "").strip():
        return False, "empty"
    names = [name, *(aliases or [])]
    if not any(mentions_name(blob, candidate) for candidate in names if candidate):
        return False, "name_missing"
    if country_conflict(blob, country, state):
        return False, "country_mismatch"
    return True, "ok"


def _cited_urls(note: dict[str, Any], snippets: list[dict[str, Any]]) -> str:
    by_id = {str(s.get("id")): s for s in snippets}
    return " ".join(
        str((by_id.get(i) or {}).get("url") or "") for i in (note.get("snippet_ids") or [])
    ).lower()


def sanitize_notes(
    notes: list[dict[str, Any]],
    *,
    name: str,
    snippets: list[dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    """Drop notes whose year, ownership, price, distance, pass source, or tone fails."""
    snippets = snippets or []
    kept: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    for note in notes:
        topic = str(note.get("topic") or "")
        text = str(note.get("text") or "")
        if is_marketing(text):
            errors.append({"topic": topic, "reason": "marketing", "text": text[:180]})
            continue
        if topic == "year_started":
            year = opening_year(text)
            if year is None:
                errors.append({"topic": topic, "reason": "year_rejected", "text": text[:180]})
                continue
            note = {**note, "value": year}
        elif topic == "public_or_private":
            kind = ownership_kind(text)
            if kind is None:
                errors.append({"topic": topic, "reason": "ownership_rejected", "text": text[:180]})
                continue
            note = {**note, "value": kind}
        elif topic == "price" or _PRICE_TOPIC.search(text):
            if re.search(r"\d", text) and not price_has_currency(text):
                errors.append({"topic": topic or "price", "reason": "price_missing_currency", "text": text[:180]})
                continue
        elif topic == "access" or (_DISTANCE_TOPIC.search(text) and re.search(r"\d", text)):
            if re.search(r"\d", text) and not distance_has_unit(text):
                errors.append({"topic": topic or "access", "reason": "distance_missing_unit", "text": text[:180]})
                continue
        if topic == "pass":
            urls = _cited_urls(note, snippets)
            if urls and any(host in urls for host in _RESELLER) and not any(host in urls for host in _PASS_OK):
                errors.append({"topic": "pass", "reason": "pass_from_reseller", "text": text[:180]})
                continue
        kept.append(note)
    return kept, errors


def section_for_note(note: dict[str, Any]) -> str:
    topic = str(note.get("topic") or "")
    text = str(note.get("text") or "")
    mapped = {
        "year_started": "history",
        "famous_people": "history",
        "setting": "access",
        "access": "access",
        "price": "prices",
        "pass": "passes",
        "lodging": "lodging",
        "scene": "scene",
        "review": "scene",
        "who": "who",
        "public_or_private": "who",
    }
    if topic in mapped:
        return mapped[topic]
    if re.search(r"ski school|childcare|kids club|lesson", text, re.IGNORECASE):
        return "school"
    if re.search(r"restaurant|dining|lunch|food", text, re.IGNORECASE):
        return "food"
    if re.search(r"season|opens in|closes|snowmaking|night ski", text, re.IGNORECASE):
        return "season"
    if re.search(r"lift|gondola|chair|vertical|trail|piste|terrain", text, re.IGNORECASE):
        return "terrain"
    return "other"


def paragraph_budgets(tier: str) -> list[tuple[str, int]]:
    """Per-paragraph character budgets that add up to the middle of layout_goal."""
    key = bedrock.canonical_tier(tier)
    goal = bedrock.copy_goal_for_tier(key)
    target = (goal.characters[0] + goal.characters[1]) // 2
    if key == "small":
        return [("all", target)]
    keys = list(MEDIUM_SECTIONS if key == "medium" else LARGE_SECTIONS)
    keys = keys[: goal.paragraphs[1]]
    each = max(280, target // len(keys))
    return [(name, each) for name in keys]


def assign_sections(notes: list[dict[str, Any]], wanted: list[str]) -> dict[str, list[dict[str, Any]]]:
    buckets: dict[str, list[dict[str, Any]]] = {key: [] for key in wanted}
    fallback = "scene" if "scene" in buckets else (wanted[-1] if wanted else "other")
    for note in notes:
        section = section_for_note(note)
        if section == "terrain" and "lifts" in buckets and re.search(
            r"lift|gondola|chair", str(note.get("text") or ""), re.IGNORECASE
        ):
            section = "lifts"
        if section not in buckets:
            section = fallback
        buckets[section].append(note)
    return buckets


def _norm_num(token: str) -> str:
    return token.replace(",", "").rstrip(".")


def numbers_in(text: str) -> set[str]:
    return {_norm_num(tok) for tok in re.findall(r"\d[\d,.]*", text or "")}


def unsupported_numbers(text: str, notes: list[dict[str, Any]], extra: str = "") -> list[str]:
    allowed = numbers_in(" ".join(str(n.get("text") or "") for n in notes) + " " + extra)
    bad: list[str] = []
    for tok in re.findall(r"\d[\d,.]*", text or ""):
        if _norm_num(tok) not in allowed and tok not in bad:
            bad.append(tok)
    return bad


def distance_contradictions(text: str, notes: list[dict[str, Any]]) -> list[str]:
    note_blob = " ".join(str(n.get("text") or "") for n in notes)
    note_km = {int(n) for n, _u in _KM.findall(note_blob)}
    text_km = {int(n) for n, _u in _KM.findall(text or "")}
    if note_km and text_km and note_km.isdisjoint(text_km):
        return [f"distance mismatch: notes {sorted(note_km)} km vs text {sorted(text_km)} km"]
    return []


def reconcile_distances(text: str, notes: list[dict[str, Any]]) -> str:
    """Replace a lone conflicting kilometre figure with the figure in the notes."""
    note_blob = " ".join(str(n.get("text") or "") for n in notes)
    note_km = [int(n) for n, _u in _KM.findall(note_blob)]
    text_km = [int(n) for n, _u in _KM.findall(text or "")]
    if len(set(note_km)) == 1 and len(set(text_km)) == 1 and set(note_km) != set(text_km):
        return re.sub(
            rf"\b{text_km[0]}\s*(km|kilomet(?:er|re)s?)\b",
            lambda m: f"{note_km[0]} {m.group(1)}",
            text,
            count=1,
            flags=re.IGNORECASE,
        )
    return text


def unused_notes(text: str, notes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    low = (text or "").lower()
    unused: list[dict[str, Any]] = []
    for note in notes:
        words = [w for w in re.findall(r"[a-z0-9]+", str(note.get("text") or "").lower()) if len(w) >= 5]
        if not words:
            continue
        hits = sum(1 for w in words if w in low)
        if hits < max(2, int(0.25 * len(words))):
            unused.append(note)
    return unused


def proper_nouns_missing(sentence: str, allowed_blob: str) -> list[str]:
    allowed = allowed_blob.lower()
    missing: list[str] = []
    for match in _PROPER.findall(sentence or ""):
        if match in _STOP_PROPER or match.lower() in allowed:
            continue
        if all(part.lower() in allowed or part in _STOP_PROPER for part in match.split()):
            continue
        missing.append(match)
    return missing


def drop_foreign_sentences(text: str, notes: list[dict[str, Any]], *, name: str, country: str | None = None, state: str | None = None) -> str:
    """Drop a sentence that names a place or brand absent from the notes."""
    allowed = " ".join(
        [name or "", state or "", country or ""] + [str(n.get("text") or "") for n in notes]
    )
    paras: list[str] = []
    for para in bedrock._paragraphs(text):
        kept: list[str] = []
        for sentence in re.split(r"(?<=[.!?])\s+", para):
            if proper_nouns_missing(sentence, allowed):
                continue
            kept.append(sentence)
        if kept:
            paras.append(" ".join(kept))
    return "\n\n".join(paras)


def length_report(text: str, tier: str) -> dict[str, Any]:
    measured = bedrock.measure_copy(text or "", tier)
    goal = bedrock.copy_goal_for_tier(tier)
    short = bedrock._copy_under_min(measured, goal)
    return {
        **measured,
        "short": short,
        "characters_target": list(goal.characters),
        "paragraphs_target": list(goal.paragraphs),
        "lines_target": list(goal.lines),
        "character_pct": round(100 * measured["characters"] / goal.characters[0], 1) if goal.characters[0] else 0,
    }


def deterministic_critique(
    draft: str,
    notes: list[dict[str, Any]],
    *,
    name: str = "",
    country: str | None = None,
    state: str | None = None,
    tier: str = "small",
) -> dict[str, Any]:
    """Numbers, names, clichés, and contradictions. No model call."""
    allowed = " ".join(
        [name or "", state or "", country or ""] + [str(n.get("text") or "") for n in notes]
    )
    unsupported: list[str] = []
    for tok in unsupported_numbers(draft, notes, f"{name} {state or ''} {country or ''}"):
        unsupported.append(f"number {tok} is not in NOTES")
    for para in bedrock._paragraphs(draft):
        for sentence in re.split(r"(?<=[.!?])\s+", para):
            missing = proper_nouns_missing(sentence, allowed)
            if missing:
                unsupported.append(f"names not in NOTES: {', '.join(missing)}")
            if _MOOD.search(sentence):
                unsupported.append(sentence.strip())
    cliches = [m.group(0) for m in _CLICHE.finditer(draft or "")]
    contradictions = distance_contradictions(draft, notes)
    fixes: list[str] = []
    if name and (draft or "").lstrip().lower().startswith(name.lower()):
        fixes.append("The page title is the resort name. Rewrite the opening so it does not start with that name.")
    for item in contradictions:
        fixes.append(f"Fix contradiction using NOTES only: {item}")
    for claim in unsupported:
        if claim.startswith("names"):
            fixes.append(f"Delete the sentence that introduces {claim}.")
    unused = unused_notes(draft, notes)
    report = length_report(draft, tier)
    if report["short"] and unused:
        fixes.append("Work unused NOTES into the page. Do not add facts that are not in NOTES.")
    return {
        "checker": "deterministic",
        "unsupported": unsupported,
        "cliches": cliches,
        "contradictions": contradictions,
        "fixes": fixes,
        "unused_notes": [n.get("text") for n in unused],
        "length": report,
    }


def note_chars(notes: list[dict[str, Any]]) -> int:
    """Characters of note text the writer can actually draw on."""
    return sum(len(str(note.get("text") or "").strip()) for note in notes)


def note_char_target(tier: str) -> int:
    """Twice the upper print size. Search keeps going until the notes reach it.

    The printed page stays on the plate. The extra notes are the pile the writer chooses from.
    """
    return int(bedrock.copy_goal_for_tier(tier).characters[1]) * 2


def short_reason_for(text: str, notes: list[dict[str, Any]], tier: str) -> str | None:
    report = length_report(text, tier)
    if not report["short"]:
        return None
    if unused_notes(text, notes):
        return "writer_underused_notes"
    return "facts_exhausted"
