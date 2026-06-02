"""Compose Scribus SLA documents from layout_plan + manifest (valid PAGEOBJECT XML)."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any
from xml.dom import minidom

from atlas.book_gen.log_util import log
from atlas.book_gen.map_sizes import TEXT_MAP_GAP_PT, map_dimensions_pt
from atlas.book_gen.sla_layout_wiki import append_wiki_text_column, layout_wiki_around_native_map, layout_wiki_slot
from atlas.book_gen.sla_prototypes import (
    build_document_root,
    make_image_frame,
    make_page,
    make_shape_frame,
    make_text_frame,
)
from atlas.book_gen.wiki_style import (
    C_BODY,
    C_TITLE,
    TextRun,
    runs_drop_cap_body,
    runs_footer,
    runs_title,
    type_scale_for_slot,
)
from atlas.book_gen.render_resort_fields import split_drop_cap

PT_PER_IN = 72.0
# Scribus canvas: first page sits at ScratchLeft/ScratchTop (see saved chapter.sla).
SCRIBUS_CANVAS_X = 100.0
SCRIBUS_CANVAS_Y = 20.0
PAGE_GAP_PT = 40.0

# Full/half pages: landscape map top-right at 100% export scale; bottom body band + footer.
MIN_SIDE_TEXT_PT = 40.0
# Full-page bottom body band (from hand-tuned Arapahoe Basin page 10 SLA).
FULL_BOTTOM_BODY_FRAC = 107.33 / 720.0
HALF_BOTTOM_BODY_FRAC = FULL_BOTTOM_BODY_FRAC * 0.55


def scribus_page_origin(page_num: int, page_height_pt: float) -> tuple[float, float]:
    """Absolute canvas position for page NUM (objects use page_x/y + margin offsets)."""
    return (
        SCRIBUS_CANVAS_X,
        SCRIBUS_CANVAS_Y + page_num * (page_height_pt + PAGE_GAP_PT),
    )

_item_id = 1000


def _in_to_pt(v: float) -> float:
    return v * PT_PER_IN


def _next_id() -> int:
    global _item_id
    _item_id += 1
    return _item_id


def _compact_stats(stats: str, *, max_lines: int = 3) -> str:
    rows = [ln.strip() for ln in (stats or "").splitlines() if ln.strip()]
    return "\n".join(rows[:max_lines])


def _truncate_text(text: str, max_chars: int) -> str:
    text = (text or "").strip()
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 1].rstrip() + "…"


def _map_and_text_widths(
    inner_x: float,
    inner_y: float,
    inner_w: float,
    inner_h: float,
    map_path: str | None,
    map_tier: str,
    *,
    map_export_dpi: int,
    map_on_right: bool = True,
) -> tuple[float, float, float, float, float, float]:
    """Map frame (x, y, w, h) and text column (text_x, text_w) for compact quarter rows."""
    map_w, map_h = map_dimensions_pt(map_path, map_tier, default_dpi=map_export_dpi)
    map_w = min(map_w, max(40.0, inner_w - TEXT_MAP_GAP_PT - 60.0))
    map_h = min(map_h, inner_h)
    map_y = inner_y + max(0.0, (inner_h - map_h) * 0.5)
    if map_on_right:
        map_x = inner_x + inner_w - map_w
        text_x = inner_x
        text_w = max(40.0, map_x - inner_x - TEXT_MAP_GAP_PT)
    else:
        map_x = inner_x
        text_x = inner_x + map_w + TEXT_MAP_GAP_PT
        text_w = max(40.0, inner_x + inner_w - text_x)
    return map_x, map_y, map_w, map_h, text_x, text_w


def _native_map_layout(
    inner_x: float,
    inner_y: float,
    inner_w: float,
    inner_h: float,
    map_path: str | None,
    map_tier: str,
    *,
    map_export_dpi: int,
    map_on_right: bool,
    slot: str = "full",
) -> dict[str, float]:
    """
    Map pinned top-right at 100% native export scale (shrink only if taller/wider than slot).

    Reserves a bottom body band and footer snapped to the slot edges. The side
    column (header + body) ends at split_y — it does not extend into the bottom band.
    """
    from atlas.book_gen.wiki_style import type_scale_for_slot

    gap = TEXT_MAP_GAP_PT
    scale = type_scale_for_slot(slot)
    footer_h = scale.footer * 1.5
    bottom_frac = FULL_BOTTOM_BODY_FRAC if slot == "full" else HALF_BOTTOM_BODY_FRAC
    bottom_body_h = inner_h * bottom_frac
    split_y = inner_y + inner_h - footer_h - bottom_body_h
    top_zone_h = max(40.0, split_y - inner_y)

    nat_w, nat_h = map_dimensions_pt(map_path, map_tier, default_dpi=map_export_dpi)
    if nat_w <= 0 or nat_h <= 0:
        nat_w = inner_w * 0.55
        nat_h = inner_h * 0.45

    plate_w = min(nat_w, inner_w - MIN_SIDE_TEXT_PT - gap)
    display_scale = 1.0
    if nat_h > top_zone_h:
        display_scale = min(display_scale, top_zone_h / nat_h)
    if nat_w > plate_w:
        display_scale = min(display_scale, plate_w / nat_w)
    map_w = nat_w * display_scale
    map_h = nat_h * display_scale

    if map_on_right:
        side_w = max(MIN_SIDE_TEXT_PT, inner_w - plate_w - gap)
        map_x = inner_x + inner_w - map_w
        side_x = inner_x
    else:
        side_w = max(MIN_SIDE_TEXT_PT, inner_w - plate_w - gap)
        map_x = inner_x
        side_x = inner_x + plate_w + gap

    map_y = inner_y
    return {
        "map_x": map_x,
        "map_y": map_y,
        "map_w": map_w,
        "map_h": map_h,
        "side_x": side_x,
        "side_y": inner_y,
        "side_w": side_w,
        "split_y": split_y,
        "top_zone_h": top_zone_h,
        "bottom_x": inner_x,
        "bottom_y": split_y,
        "bottom_w": inner_w,
        "bottom_h": bottom_body_h,
        "footer_h": footer_h,
    }


def _layout_quarter_slot(
    inner_x: float,
    inner_y: float,
    inner_w: float,
    inner_h: float,
    fields: dict[str, str],
    map_path: str | None,
    *,
    map_tier: str = "small",
    map_export_dpi: int = 300,
    map_on_right: bool = True,
    body_char_limit: int | None = 400,
) -> list[tuple]:
    """Small tier row: wiki header + stats panel + body blurb, map at export plate size."""
    map_x, map_y, map_w, map_h, text_x, text_w = _map_and_text_widths(
        inner_x,
        inner_y,
        inner_w,
        inner_h,
        map_path,
        map_tier,
        map_export_dpi=map_export_dpi,
        map_on_right=map_on_right,
    )
    return layout_wiki_slot(
        inner_x,
        inner_y,
        inner_w,
        inner_h,
        fields,
        map_path,
        slot="quarter",
        map_x=map_x,
        map_y=map_y,
        map_w=map_w,
        map_h=map_h,
        text_x=text_x,
        text_w=text_w,
        include_body=True,
        body_char_limit=body_char_limit,
        footer_in_column=False,
    )


def _layout_half_slot(
    inner_x: float,
    inner_y: float,
    inner_w: float,
    inner_h: float,
    fields: dict[str, str],
    map_path: str | None,
    *,
    map_tier: str = "medium",
    map_export_dpi: int = 300,
    map_on_right: bool = True,
    body_char_limit: int | None = 900,
) -> list[tuple]:
    regions = _native_map_layout(
        inner_x,
        inner_y,
        inner_w,
        inner_h,
        map_path,
        map_tier,
        map_export_dpi=map_export_dpi,
        map_on_right=map_on_right,
        slot="half",
    )
    return layout_wiki_around_native_map(
        inner_x,
        inner_y,
        inner_w,
        inner_h,
        fields,
        map_path,
        slot="half",
        regions=regions,
        include_body=True,
        body_char_limit=body_char_limit,
        footer_in_column=True,
    )


def _layout_full_slot(
    inner_x: float,
    inner_y: float,
    inner_w: float,
    inner_h: float,
    fields: dict[str, str],
    map_path: str | None,
    *,
    map_tier: str = "large",
    map_export_dpi: int = 300,
    map_on_right: bool = True,
) -> list[tuple]:
    regions = _native_map_layout(
        inner_x,
        inner_y,
        inner_w,
        inner_h,
        map_path,
        map_tier,
        map_export_dpi=map_export_dpi,
        map_on_right=map_on_right,
        slot="full",
    )
    return layout_wiki_around_native_map(
        inner_x,
        inner_y,
        inner_w,
        inner_h,
        fields,
        map_path,
        slot="full",
        regions=regions,
        include_body=True,
        footer_in_column=True,
    )


def _entry_layout(
    slot_w: float,
    slot_h: float,
    slot_x: float,
    slot_y: float,
    fields: dict[str, str],
    map_path: str | None,
    *,
    slot: str = "full",
    spread_page: int | None = None,
    map_tier: str = "small",
    map_export_dpi: int = 300,
    map_on_right: bool = True,
    slot_body_char_limits: dict[str, int | None] | None = None,
) -> list[tuple]:
    pad = 2.0 if slot == "quarter" else 4.0
    limits = slot_body_char_limits or {}
    inner_x = slot_x + pad
    inner_y = slot_y + pad
    inner_w = max(20.0, slot_w - 2 * pad)
    inner_h = max(20.0, slot_h - 2 * pad)

    if spread_page == 1:
        scale = type_scale_for_slot("spread")
        footer_h = scale.footer * 1.6
        lines: list[tuple] = []
        append_wiki_text_column(
            lines,
            text_x=inner_x,
            y=inner_y,
            text_w=inner_w,
            inner_h=inner_h - footer_h,
            fields=fields,
            slot="spread",
            include_body=True,
        )
        footer = (fields.get("footer_line") or "").strip()
        if footer:
            lines.append(
                (
                    "text",
                    inner_x,
                    inner_y + inner_h - footer_h,
                    inner_w,
                    footer_h,
                    runs_footer(footer, scale),
                    scale.linesp,
                )
            )
        return lines

    if spread_page == 0:
        lines: list[tuple] = []
        if map_path:
            map_w, map_h = map_dimensions_pt(
                map_path, map_tier, default_dpi=map_export_dpi
            )
            if map_w > 0 and map_h > 0:
                fit = min(inner_w / map_w, inner_h / map_h)
                map_w *= fit
                map_h *= fit
            map_x = inner_x + max(0.0, (inner_w - map_w) * 0.5)
            map_y = inner_y + max(0.0, (inner_h - map_h) * 0.5)
            lines.append(("image", map_x, map_y, map_w, map_h, map_path, 0))
        return lines

    if slot == "quarter":
        return _layout_quarter_slot(
            inner_x,
            inner_y,
            inner_w,
            inner_h,
            fields,
            map_path,
            map_tier=map_tier,
            map_export_dpi=map_export_dpi,
            map_on_right=map_on_right,
            body_char_limit=limits.get("quarter"),
        )
    if slot == "half":
        return _layout_half_slot(
            inner_x,
            inner_y,
            inner_w,
            inner_h,
            fields,
            map_path,
            map_tier=map_tier,
            map_export_dpi=map_export_dpi,
            map_on_right=map_on_right,
            body_char_limit=limits.get("half"),
        )
    return _layout_full_slot(
        inner_x,
        inner_y,
        inner_w,
        inner_h,
        fields,
        map_path,
        map_tier=map_tier,
        map_export_dpi=map_export_dpi,
        map_on_right=map_on_right,
    )


def _append_placement_objects(
    object_els: list[ET.Element],
    *,
    placements: list[dict[str, Any]],
    manifest_by_id: dict[str, dict[str, Any]],
    own_page: int,
    page_x: float,
    page_y: float,
    content_x: float,
    content_y: float,
    content_w: float,
    content_h: float,
    sla_output_path: Path | None,
    map_export_dpi: int = 300,
    slot_body_char_limits: dict[str, int | None] | None = None,
) -> None:
    for pl in placements:
        pid = pl["pageId"]
        entry = manifest_by_id.get(pid)
        if not entry:
            continue
        fields = entry.get("scribusFields") or {}
        map_path = entry.get("mapPath")
        map_tier = str(entry.get("mapTier") or "small")
        fx = page_x + content_x + float(pl["x"]) * content_w
        fy = page_y + content_y + float(pl["y"]) * content_h
        fw = float(pl["w"]) * content_w
        fh = float(pl["h"]) * content_h
        slot = str(pl.get("slot") or "full")
        if slot == "quarter" and float(pl.get("h") or 0) >= 0.99:
            slot = "full"
        spread_page = pl.get("spread_page")
        map_on_right = bool(pl.get("map_on_right", True))
        for item in _entry_layout(
            fw,
            fh,
            fx,
            fy,
            fields,
            map_path,
            slot=slot,
            spread_page=spread_page,
            map_tier=map_tier,
            map_export_dpi=map_export_dpi,
            map_on_right=map_on_right,
            slot_body_char_limits=slot_body_char_limits,
        ):
            kind = item[0]
            if kind == "shape":
                object_els.append(
                    make_shape_frame(
                        x=item[1],
                        y=item[2],
                        w=item[3],
                        h=item[4],
                        fill_color=item[5],
                        own_page=own_page,
                        item_id=_next_id(),
                    )
                )
            elif kind == "image":
                object_els.append(
                    make_image_frame(
                        x=item[1],
                        y=item[2],
                        w=item[3],
                        h=item[4],
                        image_path=item[5],
                        own_page=own_page,
                        item_id=_next_id(),
                        sla_output=sla_output_path,
                    )
                )
            else:
                content = item[5]
                linesp = item[6] if len(item) > 6 else None
                fontsize = 10.0
                if isinstance(content, list) and content:
                    fontsize = content[0].fontsize
                object_els.append(
                    make_text_frame(
                        x=item[1],
                        y=item[2],
                        w=item[3],
                        h=item[4],
                        text=content,
                        fontsize=fontsize,
                        own_page=own_page,
                        item_id=_next_id(),
                        linesp=linesp,
                    )
                )


def compose_single_page_sla(
    *,
    is_title: bool,
    manifest_by_id: dict[str, dict[str, Any]],
    trim_in: tuple[float, float] = (8.5, 11.0),
    margin_in: tuple[float, float, float, float] = (0.5, 0.5, 0.5, 0.5),
    chapter_title: str = "",
    overview_image_path: str | None = None,
    overview_body: str | None = None,
    page_info: dict[str, Any] | None = None,
    sla_output_path: Path | None = None,
    map_export_dpi: int = 300,
    slot_body_char_limits: dict[str, int | None] | None = None,
) -> ET.ElementTree:
    """One physical page (ANZPAGES=1); used for reliable PDF export."""
    global _item_id
    _item_id = 1000

    pw = _in_to_pt(trim_in[0])
    ph = _in_to_pt(trim_in[1])
    mt, mr, mb, ml = (_in_to_pt(x) for x in margin_in)
    margin_avg = (ml + mr + mt + mb) / 4.0
    content_x = ml
    content_y = mt
    content_w = pw - ml - mr
    content_h = ph - mt - mb

    root = build_document_root(
        width_pt=pw,
        height_pt=ph,
        margin_pt=margin_avg,
        page_count=1,
    )
    doc = root.find("DOCUMENT")
    assert doc is not None
    px, py = scribus_page_origin(0, ph)
    doc.append(
        make_page(
            num=0,
            width_pt=pw,
            height_pt=ph,
            margin_pt=margin_avg,
            page_xpos=px,
            page_ypos=py,
        )
    )
    object_els: list[ET.Element] = []
    if overview_image_path:
        # Overview page: map on top half, text below (like "large" rule: one page).
        gap = 12.0
        img_h = max(120.0, content_h * 0.58)
        img_h = min(img_h, content_h - 80.0)
        text_y = py + content_y + img_h + gap
        text_h = max(40.0, (py + content_y + content_h) - text_y)
        object_els.append(
            make_image_frame(
                x=px + content_x,
                y=py + content_y,
                w=content_w,
                h=img_h,
                image_path=overview_image_path,
                own_page=0,
                item_id=_next_id(),
            )
        )
        scale = type_scale_for_slot("full")
        overview_runs = runs_title(f"{chapter_title}\nRegional Overview", scale)
        body = (overview_body or "").strip()
        if body:
            drop, rest = split_drop_cap(body)
            overview_runs.append(TextRun("\n\n", scale.body, C_BODY))
            overview_runs.extend(runs_drop_cap_body(drop, rest, scale))
        object_els.append(
            make_text_frame(
                x=px + content_x,
                y=text_y,
                w=content_w,
                h=text_h,
                text=overview_runs,
                fontsize=scale.title * 1.1,
                own_page=0,
                item_id=_next_id(),
                fcolor=C_TITLE,
            )
        )
    elif is_title:
        scale = type_scale_for_slot("full")
        object_els.append(
            make_text_frame(
                x=px + content_x + 40,
                y=py + content_y + content_h * 0.4,
                w=content_w - 80,
                h=80,
                text=runs_title(chapter_title, scale),
                fontsize=scale.title * 1.4,
                own_page=0,
                item_id=_next_id(),
                fcolor=C_TITLE,
            )
        )
    elif page_info:
        _append_placement_objects(
            object_els,
            placements=page_info.get("placements") or [],
            manifest_by_id=manifest_by_id,
            own_page=0,
            page_x=px,
            page_y=py,
            content_x=content_x,
            content_y=content_y,
            content_w=content_w,
            content_h=content_h,
            sla_output_path=sla_output_path,
            map_export_dpi=map_export_dpi,
            slot_body_char_limits=slot_body_char_limits,
        )
    for po in object_els:
        doc.append(po)
    _fix_document_sections(doc, 1)
    return ET.ElementTree(root)


def compose_chapter_sla(
    layout_plan: dict[str, Any],
    manifest_by_id: dict[str, dict[str, Any]],
    *,
    trim_in: tuple[float, float] = (8.5, 11.0),
    margin_in: tuple[float, float, float, float] = (0.5, 0.5, 0.5, 0.5),
    chapter_title: str = "",
    sla_output_path: Path | None = None,
    map_export_dpi: int = 300,
    slot_body_char_limits: dict[str, int | None] | None = None,
) -> ET.ElementTree:
    global _item_id
    _item_id = 1000

    pw = _in_to_pt(trim_in[0])
    ph = _in_to_pt(trim_in[1])
    mt, mr, mb, ml = (_in_to_pt(x) for x in margin_in)
    margin_avg = (ml + mr + mt + mb) / 4.0
    content_x = ml
    content_y = mt
    content_w = pw - ml - mr
    content_h = ph - mt - mb

    pages = layout_plan.get("pages") or []
    page_count = len(pages) + (1 if chapter_title else 0)
    log(f"sla_compose: writing {len(pages)} content page(s) (+ title={bool(chapter_title)}) ...")

    root = build_document_root(
        width_pt=pw,
        height_pt=ph,
        margin_pt=margin_avg,
        page_count=page_count,
    )
    doc = root.find("DOCUMENT")
    assert doc is not None

    page_els: list[ET.Element] = []
    object_els: list[ET.Element] = []

    own = 0
    if chapter_title:
        px, py = scribus_page_origin(own, ph)
        page_els.append(
            make_page(
                num=own,
                width_pt=pw,
                height_pt=ph,
                margin_pt=margin_avg,
                page_xpos=px,
                page_ypos=py,
            )
        )
        object_els.append(
            make_text_frame(
                x=px + content_x + 40,
                y=py + content_y + content_h * 0.4,
                w=content_w - 80,
                h=80,
                text=chapter_title,
                fontsize=28,
                own_page=own,
                item_id=_next_id(),
            )
        )
        own += 1

    for page_info in pages:
        n_pl = len(page_info.get("placements") or [])
        log(f"  sla page {own + 1}: type={page_info.get('page_type')} placements={n_pl}")
        px, py = scribus_page_origin(own, ph)
        page_els.append(
            make_page(
                num=own,
                width_pt=pw,
                height_pt=ph,
                margin_pt=margin_avg,
                page_xpos=px,
                page_ypos=py,
            )
        )
        _append_placement_objects(
            object_els,
            placements=page_info.get("placements") or [],
            manifest_by_id=manifest_by_id,
            own_page=own,
            page_x=px,
            page_y=py,
            content_x=content_x,
            content_y=content_y,
            content_w=content_w,
            content_h=content_h,
            sla_output_path=sla_output_path,
            map_export_dpi=map_export_dpi,
            slot_body_char_limits=slot_body_char_limits,
        )
        own += 1

    # Scribus expects empty <PAGE/> then <PAGEOBJECT/> siblings (not nested in PAGE).
    for page_el in page_els:
        doc.append(page_el)
    for po in object_els:
        doc.append(po)

    _fix_document_sections(doc, page_count)

    return ET.ElementTree(root)


def _fix_document_sections(doc: ET.Element, page_count: int) -> None:
    """Scribus template only lists page 0 in <Sections>; fix for multi-page docs."""
    if page_count < 1:
        return
    for sections in doc.findall("Sections"):
        for sec in list(sections):
            sections.remove(sec)
        last = page_count - 1
        ET.SubElement(
            sections,
            "Section",
            Number="0",
            Name="0",
            From="0",
            To=str(last),
            Type="Type_1_2_3",
            Start="1",
            Reversed="0",
            Active="1",
        )


def write_sla(tree: ET.ElementTree, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    log(f"sla_compose: serializing XML -> {path} ...")
    rough = ET.tostring(tree.getroot(), encoding="unicode")
    parsed = minidom.parseString(rough)
    pretty = parsed.toprettyxml(indent="  ")
    lines = [ln for ln in pretty.splitlines() if ln.strip()]
    if lines and lines[0].startswith("<?xml"):
        xml_decl = lines[0]
        body = "\n".join(lines[1:])
        out = xml_decl + "\n" + body + "\n"
    else:
        out = pretty
    path.write_text(out, encoding="utf-8")
    log(f"sla_compose: wrote {path.stat().st_size / 1024:.1f} KB")
