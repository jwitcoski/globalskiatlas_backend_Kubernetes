"""Render wiki-style stats and markdown body for Scribus (ported from wiki/js/script.js)."""

from __future__ import annotations

import json
import re
from typing import Any, Optional

from atlas.book_gen.constants import RESORT_FACT_MAX_RANK, RESORT_TEXT_LIMIT

M_TO_FT = 3.28084
HA_TO_ACRES = 2.471054
MI_TO_KM = 1.60934


def format_elevation(m: float) -> str:
    ft = round(m * M_TO_FT)
    return f"{m:,.0f} m ({ft:,} ft)"


def format_area_acres(acres: float) -> str:
    ha = acres / HA_TO_ACRES
    ha_s = f"{ha:.1f}".rstrip("0").rstrip(".")
    return f"{acres:,.0f} acres ({ha_s} ha)"


def format_area_ha(ha: float) -> str:
    acres = round(ha * HA_TO_ACRES)
    return f"{ha:,.0f} ha ({acres:,} acres)"


def format_distance_mi(mi: float) -> str:
    km = mi * MI_TO_KM
    km_s = f"{km:.1f}".rstrip("0").rstrip(".")
    return f"{mi} mi ({km_s} km)"


def _category(page: dict[str, Any]) -> str:
    cat = page.get("resortSizeCategory")
    if cat:
        return str(cat)
    cat_obj = page.get("categorization") or {}
    if isinstance(cat_obj, dict) and cat_obj.get("size"):
        return str(cat_obj["size"])
    return "unknown"


def _allowed_ranks(page: dict[str, Any]) -> set[int]:
    category = _category(page)
    max_rank = RESORT_FACT_MAX_RANK.get(category, 4)
    use_ranks = page.get("visibleFactRanks")
    if isinstance(use_ranks, list) and use_ranks:
        return {int(r) for r in use_ranks}

    ranks: set[int] = set()
    for r in range(1, min(max_rank, 14) + 1):
        ranks.add(r)
    if max_rank < 12:
        ranks.update({12, 13, 14})
    if max_rank >= 9:
        ranks.add(15)
    return ranks


def _rank_allowed(ranks: set[int], r: int, max_rank: int) -> bool:
    if r in ranks:
        return True
    if r >= 16:
        return False
    if r >= 12:
        return True
    if r == 15:
        return max_rank >= 9
    return r <= max_rank


def render_stats_block(page: dict[str, Any]) -> str:
    """Plain-text stats grid for Scribus."""
    category = _category(page)
    max_rank = RESORT_FACT_MAX_RANK.get(category, 4)
    ranks = _allowed_ranks(page)

    def ok(r: int) -> bool:
        return _rank_allowed(ranks, r, max_rank)

    lines: list[str] = []

    def add(label: str, value: str) -> None:
        if value:
            lines.append(f"{label}: {value}")

    if ok(2):
        acres = page.get("skiableTerrainAcres")
        ha = page.get("skiableTerrainHa")
        if acres is not None:
            try:
                add("SKIABLE TERRAIN", format_area_acres(float(acres)))
            except (TypeError, ValueError):
                add("SKIABLE TERRAIN", f"{acres} acres")
        elif ha is not None:
            try:
                add("SKIABLE TERRAIN", format_area_ha(float(ha)))
            except (TypeError, ValueError):
                add("SKIABLE TERRAIN", f"{ha} ha")

    vertical_drop = page.get("verticalDropM")
    high = page.get("highElevationM")
    low = page.get("lowElevationM")
    if vertical_drop is None and high is not None and low is not None:
        try:
            if float(high) >= float(low):
                vertical_drop = float(high) - float(low)
        except (TypeError, ValueError):
            pass
    if ok(15) and vertical_drop is not None:
        try:
            vd = float(vertical_drop)
            if vd >= 0:
                add("VERTICAL DROP", format_elevation(vd))
        except (TypeError, ValueError):
            pass

    if ok(3) and page.get("downhillTrails"):
        add("TRAILS", str(page["downhillTrails"]))
    if ok(4) and page.get("totalLifts"):
        add("LIFTS", str(page["totalLifts"]))
    if ok(5) and page.get("longestTrailMi") is not None:
        try:
            add("LONGEST TRAIL", format_distance_mi(float(page["longestTrailMi"])))
        except (TypeError, ValueError):
            add("LONGEST TRAIL", str(page["longestTrailMi"]))
    if ok(6) and page.get("longestLiftMi") is not None:
        try:
            add("LONGEST LIFT", format_distance_mi(float(page["longestLiftMi"])))
        except (TypeError, ValueError):
            add("LONGEST LIFT", str(page["longestLiftMi"]))
    if ok(7):
        if page.get("totalAreaAcres") is not None:
            try:
                add("TOTAL AREA", format_area_acres(float(page["totalAreaAcres"])))
            except (TypeError, ValueError):
                add("TOTAL AREA", str(page["totalAreaAcres"]))
        elif page.get("totalAreaHa") is not None:
            try:
                add("TOTAL AREA", format_area_ha(float(page["totalAreaHa"])))
            except (TypeError, ValueError):
                add("TOTAL AREA", str(page["totalAreaHa"]))
    if ok(9) and page.get("highElevationM") is not None:
        try:
            add("ELEVATION (HIGH)", format_elevation(float(page["highElevationM"])))
        except (TypeError, ValueError):
            pass
    if ok(10) and page.get("lowElevationM") is not None:
        try:
            add("ELEVATION (LOW)", format_elevation(float(page["lowElevationM"])))
        except (TypeError, ValueError):
            pass

    return "\n".join(lines)


def render_stats_pairs(page: dict[str, Any]) -> list[tuple[str, str]]:
    """Label/value pairs for wiki-style stats grid (same facts as render_stats_block)."""
    category = _category(page)
    max_rank = RESORT_FACT_MAX_RANK.get(category, 4)
    ranks = _allowed_ranks(page)

    def ok(r: int) -> bool:
        return _rank_allowed(ranks, r, max_rank)

    pairs: list[tuple[str, str]] = []

    def add(label: str, value: str) -> None:
        if value:
            pairs.append((label, value))

    if ok(2):
        acres = page.get("skiableTerrainAcres")
        ha = page.get("skiableTerrainHa")
        if acres is not None:
            try:
                add("SKIABLE TERRAIN", format_area_acres(float(acres)))
            except (TypeError, ValueError):
                add("SKIABLE TERRAIN", f"{acres} acres")
        elif ha is not None:
            try:
                add("SKIABLE TERRAIN", format_area_ha(float(ha)))
            except (TypeError, ValueError):
                add("SKIABLE TERRAIN", f"{ha} ha")

    vertical_drop = page.get("verticalDropM")
    high = page.get("highElevationM")
    low = page.get("lowElevationM")
    if vertical_drop is None and high is not None and low is not None:
        try:
            if float(high) >= float(low):
                vertical_drop = float(high) - float(low)
        except (TypeError, ValueError):
            pass
    if ok(15) and vertical_drop is not None:
        try:
            vd = float(vertical_drop)
            if vd >= 0:
                add("VERTICAL DROP", format_elevation(vd))
        except (TypeError, ValueError):
            pass

    if ok(3) and page.get("downhillTrails"):
        add("TRAILS", str(page["downhillTrails"]))
    if ok(4) and page.get("totalLifts"):
        add("LIFTS", str(page["totalLifts"]))
    if ok(5) and page.get("longestTrailMi") is not None:
        try:
            add("LONGEST TRAIL", format_distance_mi(float(page["longestTrailMi"])))
        except (TypeError, ValueError):
            add("LONGEST TRAIL", str(page["longestTrailMi"]))
    if ok(6) and page.get("longestLiftMi") is not None:
        try:
            add("LONGEST LIFT", format_distance_mi(float(page["longestLiftMi"])))
        except (TypeError, ValueError):
            add("LONGEST LIFT", str(page["longestLiftMi"]))
    if ok(7):
        if page.get("totalAreaAcres") is not None:
            try:
                add("TOTAL AREA", format_area_acres(float(page["totalAreaAcres"])))
            except (TypeError, ValueError):
                add("TOTAL AREA", str(page["totalAreaAcres"]))
        elif page.get("totalAreaHa") is not None:
            try:
                add("TOTAL AREA", format_area_ha(float(page["totalAreaHa"])))
            except (TypeError, ValueError):
                add("TOTAL AREA", str(page["totalAreaHa"]))
    if ok(9) and page.get("highElevationM") is not None:
        try:
            add("ELEVATION (HIGH)", format_elevation(float(page["highElevationM"])))
        except (TypeError, ValueError):
            pass
    if ok(10) and page.get("lowElevationM") is not None:
        try:
            add("ELEVATION (LOW)", format_elevation(float(page["lowElevationM"])))
        except (TypeError, ValueError):
            pass

    return pairs


def render_trail_breakdown(page: dict[str, Any]) -> str:
    category = _category(page)
    max_rank = RESORT_FACT_MAX_RANK.get(category, 4)
    ranks = _allowed_ranks(page)
    if not _rank_allowed(ranks, 8, max_rank):
        return ""
    parts: list[str] = []
    for key, label in (
        ("trailsNovice", "Novice"),
        ("trailsEasy", "Easy"),
        ("trailsIntermediate", "Intermediate"),
        ("trailsAdvanced", "Advanced"),
        ("trailsExpert", "Expert"),
        ("trailsFreeride", "Freeride"),
        ("trailsExtreme", "Extreme"),
    ):
        v = page.get(key)
        if v:
            parts.append(f"{label} {v}")
    if not parts:
        return ""
    return "Trail breakdown: " + " · ".join(parts)


def render_subtitle(page: dict[str, Any]) -> str:
    category = _category(page)
    if category == "unknown":
        return "Not a downhill ski hill"
    parts: list[str] = []
    if page.get("resortType") and category != "unknown":
        parts.append(str(page["resortType"]))
    if page.get("totalLifts"):
        parts.append(f"{page['totalLifts']} lifts")
    if page.get("downhillTrails"):
        parts.append(f"{page['downhillTrails']} trails")
    if category == "mega_resort":
        parts.append("Mega resort")
    return " · ".join(parts)


def resort_display_name(page: dict[str, Any]) -> str:
    en = (page.get("englishName") or "").strip() if page.get("englishName") else ""
    title = (page.get("title") or "").strip() if page.get("title") else ""
    if en and title and en != title:
        return f"{en} ({title})"
    return en or title or str(page.get("pageId") or "")


def markdown_to_plain(text: str) -> str:
    """Minimal markdown → plain text for Scribus."""
    if not text:
        return ""
    t = text.replace("\r\n", "\n")
    t = re.sub(r"^#+\s+", "", t, flags=re.MULTILINE)
    t = re.sub(r"\*\*(.+?)\*\*", r"\1", t)
    t = re.sub(r"\*(.+?)\*", r"\1", t)
    t = re.sub(r"__(.+?)__", r"\1", t)
    t = re.sub(r"_(.+?)_", r"\1", t)
    t = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", t)
    t = re.sub(r"`([^`]+)`", r"\1", t)
    return t.strip()


def split_drop_cap(body_plain: str) -> tuple[str, str]:
    """First character for drop cap + remainder of first paragraph."""
    body_plain = body_plain.strip()
    if not body_plain:
        return "", ""
    for i, ch in enumerate(body_plain):
        if ch.isalpha():
            return ch, body_plain[i + 1 :]
    return body_plain[:1], body_plain[1:]


def render_body(page: dict[str, Any], *, truncate: bool = True) -> dict[str, str]:
    category = _category(page)
    limit = RESORT_TEXT_LIMIT.get(category, 1500)
    raw = page.get("content") or ""
    plain = markdown_to_plain(raw)
    if truncate and len(plain) > limit:
        plain = plain[: limit - 1].rstrip() + "…"
    drop, rest = split_drop_cap(plain)
    return {"body": plain, "drop_cap": drop, "body_after_cap": rest}


def render_location(page: dict[str, Any]) -> str:
    parts = [p for p in (page.get("state"), page.get("country")) if p]
    return ", ".join(parts)


def render_footer(page_num: int) -> str:
    return f"— {page_num}  Ski Atlas"


def build_scribus_fields(page: dict[str, Any], *, page_num: int = 0) -> dict[str, str]:
    body_parts = render_body(page)
    pairs = render_stats_pairs(page)
    return {
        "pageId": str(page.get("pageId") or ""),
        "location": render_location(page).upper(),
        "title": resort_display_name(page),
        "subtitle": render_subtitle(page),
        "stats_block": render_stats_block(page),
        "stats_pairs_json": json.dumps(pairs, ensure_ascii=False),
        "trail_breakdown": render_trail_breakdown(page),
        "body": body_parts["body"],
        "drop_cap": body_parts["drop_cap"],
        "body_after_cap": body_parts["body_after_cap"],
        "footer_line": render_footer(page_num) if page_num else "—  Ski Atlas",
    }
