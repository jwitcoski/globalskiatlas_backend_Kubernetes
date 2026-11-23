"""Turn the sticky pile into a page brief and a typed fact card."""
from __future__ import annotations

import re
from typing import Any

from .filters import _host, _point_text, pass_url_trusted, resort_passes
from .key_points import _KEY_POINT_FIELDS, _YES_NO_FIELDS
from .validators import note_chars, opening_year, section_for_note


_FILLER = re.compile(
    r"no specific (?:information|details)|not provided in the indexed|"
    r"top destination|unforgettable|world-class|"
    r"vegetation types?|average snowiest",
    re.IGNORECASE,
)
_DB_STATS = re.compile(
    r"[^.]*\b(?:vertical drop|summit(?: elevation)?|acres? of (?:skiable )?terrain|"
    r"\d+\s+(?:trails?|lifts?|acres))\b[^.]*\.?",
    re.IGNORECASE,
)
_GENERIC = re.compile(
    r"\b(?:resort|mountain|ski|area|vermont|trail|skier|skiing|lodge|hotel)\b",
    re.IGNORECASE,
)


BRIEF_SECTIONS = (
    "history",
    "access",
    "town",
    "who",
    "school",
    "lodging",
    "food",
    "prices",
    "passes",
    "season",
    "scene",
    "terrain",
)


def is_filler_note(note: dict[str, Any]) -> bool:
    text = str(note.get("text") or "").strip()
    topic = str(note.get("topic") or "")
    if topic in {"pass", "price"}:
        return False
    if len(text) < 40:
        return True
    if _FILLER.search(text):
        return True
    if text.lower().startswith("yes, ") and len(text) < 80:
        return True
    return False


def _strip_db_stats(text: str) -> str:
    cleaned = _DB_STATS.sub("", text)
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()
    return cleaned or text


def usable_notes(notes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    kept: list[dict[str, Any]] = []
    seen: set[str] = set()
    for note in notes:
        if is_filler_note(note):
            continue
        text = _strip_db_stats(str(note.get("text") or "").strip())
        key = re.sub(r"[^a-z0-9]+", " ", text.lower())[:120]
        if key in seen:
            continue
        seen.add(key)
        kept.append({**note, "text": text})
    return kept


def usable_note_chars(notes: list[dict[str, Any]]) -> int:
    return note_chars(usable_notes(notes))


def compact_key_points(points: dict[str, Any]) -> dict[str, Any]:
    """Short typed cells for the facts table. Paragraphs do not belong here."""
    out = dict(points or {})
    official = _host(_point_text(out.get("website")))
    year_raw = _point_text(out.get("year_started"))
    year = opening_year(year_raw)
    if year:
        out["year_started"] = _keep_meta(out.get("year_started"), str(year))
    status = _point_text(out.get("operator_status"))
    kind = _status_kind(status)
    if kind:
        out["operator_status"] = _keep_meta(out.get("operator_status"), kind)
    drive = _point_text(out.get("drive"))
    drive_short = _first_match(
        drive,
        r"[^.]*\d+\s*(?:minute|min|km|mile)s?[^.]*\.",
    )
    if drive_short:
        out["drive"] = _keep_meta(out.get("drive"), drive_short.strip())
    famous = _point_text(out.get("famous_for"))
    if famous and len(famous) > 140:
        first = famous.split(".")[0].strip()
        out["famous_for"] = _keep_meta(out.get("famous_for"), first[:140] if first else None)
    lodge = _point_text(out.get("on_hill_lodge"))
    if lodge and len(lodge) > 160:
        first = lodge.split(".")[0].strip()
        out["on_hill_lodge"] = _keep_meta(out.get("on_hill_lodge"), first[:160] if first else None)
    terrain = _point_text(out.get("terrain"))
    blob = " ".join(_point_text(out.get(field)) for field in ("terrain", "famous_for") if out.get(field))
    kinds = [k for k in ("expert", "powder", "intermediate", "beginner") if re.search(rf"\b{k}", blob, re.I)]
    if len(kinds) != 1:
        out["terrain"] = None
    elif terrain and "beginner" in terrain and "expert" in blob.lower():
        out["terrain"] = None
    products: list[dict[str, Any]] = []
    for item in out.get("pass_products") or []:
        if not isinstance(item, dict):
            continue
        name = str(item.get("value") or "").strip()
        urls = [str(u) for u in (item.get("sources") or []) if u]
        if name and any(pass_url_trusted(url, official, name) for url in urls):
            products.append(item)
    out["pass_products"] = products
    if products and not _point_text(out.get("pass_affiliation")):
        out["pass_affiliation"] = products[0]
    elif products:
        aff = _point_text(out.get("pass_affiliation"))
        if aff and not any(pass_url_trusted(u, official, aff) for u in (out.get("pass_affiliation") or {}).get("sources") or []):
            out["pass_affiliation"] = products[0]
    for field in _KEY_POINT_FIELDS:
        if field in _YES_NO_FIELDS:
            continue
        value = _point_text(out.get(field))
        if value and len(value) > 220:
            out[field] = _keep_meta(out.get(field), value.split(".")[0].strip()[:180] or None)
    return out


def _keep_meta(raw: Any, value: str | None) -> Any:
    if value is None:
        return None
    if isinstance(raw, dict):
        return {**raw, "value": value}
    return {"value": value}


def _status_kind(text: str) -> str | None:
    low = (text or "").lower()
    if re.search(r"not-for-profit|nonprofit", low):
        return "not-for-profit"
    if re.search(r"\bclub\b", low):
        return "club"
    if re.search(r"\bmunicipal\b", low):
        return "municipal"
    if re.search(r"\bprivate\b", low):
        return "private"
    if re.search(r"\bpublic\b", low):
        return "public"
    return None


def _first_match(text: str, pattern: str) -> str:
    match = re.search(pattern, text or "", re.IGNORECASE)
    return match.group(0) if match else ""


def inject_fact_notes(notes: list[dict[str, Any]], key_points: dict[str, Any]) -> list[dict[str, Any]]:
    """Put typed table facts into the page pile so the writer can use them."""
    out = list(notes)
    blob = " ".join(str(n.get("text") or "") for n in notes).lower()
    passes = resort_passes(key_points, notes)
    if passes and not re.search(r"\bikon\b|\bepic\b|\bindy\b|mountain collective", blob):
        ids: list[str] = []
        aff = key_points.get("pass_affiliation")
        if isinstance(aff, dict):
            ids = [str(x) for x in (aff.get("snippet_ids") or []) if x]
        if not ids:
            for product in key_points.get("pass_products") or []:
                if isinstance(product, dict) and product.get("snippet_ids"):
                    ids = [str(x) for x in product["snippet_ids"] if x]
                    break
        out.append(
            {
                "topic": "pass",
                "text": f"This resort is on the {', '.join(passes)} pass.",
                "snippet_ids": ids,
            }
        )
    ticket = _point_text(key_points.get("lift_ticket")) or _point_text(key_points.get("weekend_ticket"))
    if ticket and re.search(r"[$€£¥]|\b(?:usd|eur|cad)\b|\d", ticket, re.I) and "lift ticket" not in blob:
        out.append({"topic": "price", "text": ticket, "snippet_ids": []})
    return out


def page_brief(notes: list[dict[str, Any]], *, name: str = "", key_points: dict[str, Any] | None = None) -> str:
    """Plain sectioned material for outline + writer. No JSON, no snippet ids."""
    writing = usable_notes(notes)
    buckets: dict[str, list[str]] = {key: [] for key in BRIEF_SECTIONS}
    for note in writing:
        section = section_for_note(note)
        if section == "other":
            section = "scene"
        if section not in buckets:
            buckets.setdefault(section, [])
        text = str(note.get("text") or "").strip()
        if text and text not in buckets[section]:
            buckets[section].append(text)
    passes = resort_passes(key_points or {}, notes)
    if passes and not buckets.get("passes"):
        buckets["passes"] = [f"This resort is on the {', '.join(passes)} pass."]
    lines = [f"Resort: {name}".strip()] if name else []
    for section in BRIEF_SECTIONS:
        items = buckets.get(section) or []
        if not items:
            continue
        lines.append(f"[{section}]")
        for item in items:
            lines.append(item)
        lines.append("")
    return "\n".join(lines).strip()


def writing_notes(notes: list[dict[str, Any]], key_points: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    pile = inject_fact_notes(usable_notes(notes), key_points or {})
    for note in pile:
        topic = str(note.get("topic") or "")
        if topic in {"year_started", "famous_people"}:
            note["topic"] = "history"
        elif topic == "setting":
            note["topic"] = "access"
        elif topic == "public_or_private":
            note["topic"] = "who"
    return pile
