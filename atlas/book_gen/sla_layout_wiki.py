"""Wiki-styled resort blocks for Scribus layout (header, stats panel, body)."""

from __future__ import annotations

import json
from typing import Any

from atlas.book_gen.render_resort_fields import split_drop_cap
from atlas.book_gen.wiki_style import (
    C_STATS_BG,
    TextRun,
    runs_body_plain,
    runs_drop_cap_body,
    runs_footer,
    runs_location,
    runs_stats_pairs,
    runs_subtitle,
    runs_title,
    runs_trail_breakdown,
    type_scale_for_slot,
)

# Layout tuple: kind, x, y, w, h, payload, linesp (optional on text)
# kind: "text" | "image" | "shape"
# text payload: str | list[TextRun]


def _text(
    x: float,
    y: float,
    w: float,
    h: float,
    runs: list[TextRun],
    linesp: float,
) -> tuple:
    return ("text", x, y, w, h, runs, linesp)


def _shape(x: float, y: float, w: float, h: float, fill: str) -> tuple:
    return ("shape", x, y, w, h, fill)


def _image(x: float, y: float, w: float, h: float, path: str) -> tuple:
    return ("image", x, y, w, h, path, 0)


def _pairs_from_fields(fields: dict[str, str]) -> list[tuple[str, str]]:
    raw = fields.get("stats_pairs_json") or "[]"
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            return [(str(a), str(b)) for a, b in data]
    except json.JSONDecodeError:
        pass
    return []


def append_wiki_text_column(
    lines: list[tuple],
    *,
    text_x: float,
    y: float,
    text_w: float,
    inner_h: float,
    fields: dict[str, str],
    slot: str,
    include_body: bool,
    footer_in_column: bool = True,
    body_char_limit: int | None = None,
) -> float:
    """Stack wiki header + stats + optional body; return y after last element."""
    scale = type_scale_for_slot(slot)
    pairs = _pairs_from_fields(fields)
    y_cursor = y

    loc = (fields.get("location") or "").strip()
    if loc:
        h = max(scale.location * 1.35, inner_h * 0.05)
        lines.append(_text(text_x, y_cursor, text_w, h, runs_location(loc, scale), scale.linesp))
        y_cursor += h

    title = (fields.get("title") or "").strip()
    if title:
        h = max(scale.title * 1.25, inner_h * 0.08 if slot != "quarter" else 0.12)
        lines.append(_text(text_x, y_cursor, text_w, h, runs_title(title, scale), scale.linesp))
        y_cursor += h

    sub = (fields.get("subtitle") or "").strip()
    if sub:
        h = max(scale.subtitle * 1.3, inner_h * 0.05)
        lines.append(_text(text_x, y_cursor, text_w, h, runs_subtitle(sub, scale), scale.linesp))
        y_cursor += h

    if pairs:
        n_lines = max(1, (len(pairs) + 1) // 2)
        stats_h = max(
            scale.stats_value * 1.5 * n_lines + scale.stats_pad * 2,
            inner_h * (0.14 if slot == "quarter" else 0.10),
        )
        pad = scale.stats_pad
        lines.append(_shape(text_x, y_cursor, text_w, stats_h, C_STATS_BG))
        if len(pairs) <= 2 or slot == "quarter":
            lines.append(
                _text(
                    text_x + pad,
                    y_cursor + pad,
                    text_w - 2 * pad,
                    stats_h - 2 * pad,
                    runs_stats_pairs(pairs, scale),
                    scale.linesp * 0.92,
                )
            )
        else:
            mid = text_w / 2.0
            col_w = mid - pad * 1.5
            left = pairs[0::2]
            right = pairs[1::2]
            lines.append(
                _text(
                    text_x + pad,
                    y_cursor + pad,
                    col_w,
                    stats_h - 2 * pad,
                    runs_stats_pairs(left, scale),
                    scale.linesp * 0.92,
                )
            )
            lines.append(
                _text(
                    text_x + mid + pad * 0.5,
                    y_cursor + pad,
                    col_w,
                    stats_h - 2 * pad,
                    runs_stats_pairs(right, scale),
                    scale.linesp * 0.92,
                )
            )
        y_cursor += stats_h

    trail = (fields.get("trail_breakdown") or "").strip()
    if trail and slot != "quarter":
        h = max(scale.trail * 1.4, inner_h * 0.03)
        lines.append(
            _text(text_x, y_cursor, text_w, h, runs_trail_breakdown(trail, scale), scale.linesp)
        )
        y_cursor += h

    if include_body:
        drop = (fields.get("drop_cap") or "").strip()
        body_after = (fields.get("body_after_cap") or fields.get("body") or "").strip()
        body_plain = (fields.get("body") or "").strip()
        footer_h = scale.footer * 1.4 if footer_in_column else 0.0
        body_h = max(12.0, inner_h - (y_cursor - y) - footer_h)
        if body_char_limit and body_plain and len(body_plain) > body_char_limit:
            body_plain = body_plain[: body_char_limit - 1].rstrip() + "…"
        if body_char_limit and body_plain:
            if slot == "quarter":
                drop = ""
                body_after = body_plain
            else:
                drop, body_after = split_drop_cap(body_plain)
        if drop and body_after and slot != "quarter":
            cap_w = scale.drop_cap * 0.65
            lines.append(
                _text(
                    text_x,
                    y_cursor,
                    cap_w,
                    min(scale.drop_cap * 1.1, body_h),
                    [TextRun(drop, scale.drop_cap, "AtlasTitle")],
                    scale.linesp,
                )
            )
            lines.append(
                _text(
                    text_x + cap_w,
                    y_cursor,
                    text_w - cap_w,
                    body_h,
                    runs_body_plain(body_after, scale),
                    scale.linesp,
                )
            )
        elif body_plain:
            lines.append(
                _text(text_x, y_cursor, text_w, body_h, runs_body_plain(body_plain, scale), scale.linesp)
            )
        elif drop:
            lines.append(
                _text(
                    text_x,
                    y_cursor,
                    text_w,
                    body_h,
                    runs_drop_cap_body(drop, body_after, scale),
                    scale.linesp,
                )
            )

    return y_cursor


def _estimate_body_chars(text_w: float, body_h: float, scale) -> int:
    """Rough capacity for a Scribus text frame (chars before overflow)."""
    if body_h <= 0 or text_w <= 0:
        return 0
    line_h = max(scale.body * 1.15, 8.0)
    chars_per_line = max(8, int(text_w / max(scale.body * 0.52, 4.0)))
    lines = max(1, int(body_h / line_h))
    return lines * chars_per_line


def _append_body_runs(
    lines: list[tuple],
    *,
    text_x: float,
    y: float,
    text_w: float,
    body_h: float,
    body_plain: str,
    scale,
    slot: str,
    use_drop_cap: bool,
) -> None:
    if not body_plain or body_h <= 0:
        return
    drop = ""
    body_after = body_plain
    if use_drop_cap and slot != "quarter":
        drop, body_after = split_drop_cap(body_plain)
    if drop and body_after:
        cap_w = scale.drop_cap * 0.65
        lines.append(
            _text(
                text_x,
                y,
                cap_w,
                min(scale.drop_cap * 1.1, body_h),
                [TextRun(drop, scale.drop_cap, "AtlasTitle")],
                scale.linesp,
            )
        )
        lines.append(
            _text(
                text_x + cap_w,
                y,
                text_w - cap_w,
                body_h,
                runs_body_plain(body_after, scale),
                scale.linesp,
            )
        )
    else:
        lines.append(
            _text(text_x, y, text_w, body_h, runs_body_plain(body_plain, scale), scale.linesp)
        )


def layout_wiki_around_native_map(
    inner_x: float,
    inner_y: float,
    inner_w: float,
    inner_h: float,
    fields: dict[str, str],
    map_path: str | None,
    *,
    slot: str,
    regions: dict[str, float],
    include_body: bool,
    body_char_limit: int | None = None,
    footer_in_column: bool = True,
) -> list[tuple]:
    """
    Full/half layout matching hand-tuned page geometry:

    - Map top-right at native scale (height capped)
    - Header stack in left column, snapped to top
    - Side body ends at split_y (does not enter bottom band)
    - Bottom body full-width band above footer
    """
    lines: list[tuple] = []
    scale = type_scale_for_slot(slot)
    footer_h = regions.get("footer_h") or (scale.footer * 1.5 if footer_in_column else 0.0)

    side_x = regions["side_x"]
    side_w = regions["side_w"]
    split_y = regions["split_y"]
    top_zone_h = regions.get("top_zone_h") or max(40.0, split_y - inner_y)

    y_after_header = append_wiki_text_column(
        lines,
        text_x=side_x,
        y=inner_y,
        text_w=side_w,
        inner_h=top_zone_h,
        fields=fields,
        slot=slot,
        include_body=False,
        footer_in_column=False,
    )

    if map_path:
        lines.append(
            _image(
                regions["map_x"],
                regions["map_y"],
                regions["map_w"],
                regions["map_h"],
                map_path,
            )
        )

    if include_body:
        body_plain = (fields.get("body") or "").strip()
        if body_char_limit and len(body_plain) > body_char_limit:
            body_plain = body_plain[: body_char_limit - 1].rstrip() + "…"

        side_body_h = max(12.0, split_y - y_after_header)
        bottom_h = max(12.0, regions.get("bottom_h") or 0.0)

        side_cap = _estimate_body_chars(side_w, side_body_h, scale)
        if bottom_h >= scale.body * 2.0 and len(body_plain) > side_cap:
            side_text = body_plain[:side_cap].rstrip()
            bottom_text = body_plain[side_cap:].lstrip()
            _append_body_runs(
                lines,
                text_x=side_x,
                y=y_after_header,
                text_w=side_w,
                body_h=side_body_h,
                body_plain=side_text,
                scale=scale,
                slot=slot,
                use_drop_cap=True,
            )
            if bottom_text:
                _append_body_runs(
                    lines,
                    text_x=regions["bottom_x"],
                    y=regions["bottom_y"],
                    text_w=regions["bottom_w"],
                    body_h=bottom_h,
                    body_plain=bottom_text,
                    scale=scale,
                    slot=slot,
                    use_drop_cap=False,
                )
        else:
            _append_body_runs(
                lines,
                text_x=side_x,
                y=y_after_header,
                text_w=side_w,
                body_h=side_body_h,
                body_plain=body_plain,
                scale=scale,
                slot=slot,
                use_drop_cap=True,
            )

    if footer_in_column:
        footer = (fields.get("footer_line") or "").strip()
        if footer:
            lines.append(
                _text(
                    inner_x,
                    inner_y + inner_h - footer_h,
                    inner_w,
                    footer_h,
                    runs_footer(footer, scale),
                    scale.linesp,
                )
            )
    return lines


def layout_wiki_slot(
    inner_x: float,
    inner_y: float,
    inner_w: float,
    inner_h: float,
    fields: dict[str, str],
    map_path: str | None,
    *,
    slot: str,
    map_x: float,
    map_y: float,
    map_w: float,
    map_h: float,
    text_x: float,
    text_w: float,
    include_body: bool,
    body_char_limit: int | None = None,
    footer_in_column: bool = True,
    text_y: float | None = None,
    text_col_h: float | None = None,
) -> list[tuple]:
    lines: list[tuple] = []
    scale = type_scale_for_slot(slot)
    col_y = inner_y if text_y is None else text_y
    col_h = inner_h if text_col_h is None else text_col_h
    append_wiki_text_column(
        lines,
        text_x=text_x,
        y=col_y,
        text_w=text_w,
        inner_h=col_h,
        fields=fields,
        slot=slot,
        include_body=include_body,
        footer_in_column=False,
        body_char_limit=body_char_limit,
    )
    if map_path:
        lines.append(_image(map_x, map_y, map_w, map_h, map_path))
    if footer_in_column:
        footer = (fields.get("footer_line") or "").strip()
        if footer:
            fh = scale.footer * 1.5
            lines.append(
                _text(
                    inner_x,
                    inner_y + inner_h - fh,
                    inner_w,
                    fh,
                    runs_footer(footer, scale),
                    scale.linesp,
                )
            )
    return lines
