#!/usr/bin/env python3
"""
Build eight Ski Atlas print-layout templates: four **portrait-aspect** plates (existing
sizes) plus four **landscape** variants with width and height swapped.

Exports are sized for **Adobe InDesign**: page = map plate only (no extra white for
placeholder text). Portrait dimensions (mm):

  small:   105 × 74.25
  medium:  105 × 148.5
  large:   210 × 148.5
  mega:    210 × 297

Landscape templates use the opposite dimensions (e.g. small → 74.25 × 105).

Globe inset + north arrow use **fixed mm** (small-layout measurements) on all tiers.
Inset **upper-right**, north arrow **bottom-right**. Portrait **small** keeps legacy
scale-bar Y positions; **small landscape** stacks scale bars at the bottom-left from
measured bar heights.

Run from repo root:
  python -m atlas.map_gen.build_atlas_layout_templates
"""

from __future__ import annotations

import re
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC_QGZ = ROOT / "atlas/map_gen/templates/ski_atlas_small_medium_template.qgz"
TEMPLATE_QGS = "wintergreen_map.qgs"
LAYOUT_NAME = "Ski Atlas Export"

BASE_W = 107.95
BASE_H = 139.7

OUT_DIR = ROOT / "atlas/map_gen/templates"

# White page under layout (type 65638).
PAPER_UUID = "{479e093f-add1-4669-92fb-180404d9bf6a}"
MAIN_MAP_UUID = "{843e891d-8334-43c6-988a-58fbb894dec9}"

# Globe inset cluster — same width+height → circular clip in export (was oval after non-uniform scale).
GLOBE_INSET_IDS = frozenset(
    ("overview_inset_map", "overview_inset_dot", "globe_clip_shape")
)

# Globe inset LayoutItems (template UUIDs) — move as a group.
GLOBE_INSET_UUIDS = (
    "{1b475dac-151b-405e-9cb8-8cc3b41c33fc}",  # overview_inset_dot
    "{2f49388b-470e-4183-9877-579f75ca1b41}",  # overview_inset_map
    "{eb3dbbea-2d98-4fe4-a099-7b457733bdb0}",  # globe_clip_shape
)

# Scale bars (type 65646): km = higher on page in template; mi below.
SCALE_BAR_KM_UUID = "{ab260a4b-b995-4ced-b6d7-0bfd8f8bce21}"
SCALE_BAR_MI_UUID = "{f5b30f05-0fab-452f-a87c-e618f9cd812f}"

# North arrow (picture item) — keep physical mm same on every tier (measured from small export).
NORTH_ARROW_UUID = "{f9b8c7a9-b853-42bc-b510-668808a3931b}"

# Globe inset: fixed square side (mm) = small-layout size so medium/large/mega do not scale up.
# User small export ~11.470 × 11.251 mm → square for circular clip.
INSET_GLOBE_SIDE_MM = min(11.470, 11.251)
NORTH_ARROW_W_MM = 6.128
NORTH_ARROW_H_MM = 4.464

# Page-edge inset for corner-placed furniture (mm from printable edge).
PAGE_CORNER_MARGIN_MM = 0.5
# Vertical gap between stacked km / mi scale bars when bottom-aligning (mm).
SCALE_STACK_GAP_MM = 0.5


def _layout_block(qgs: str) -> tuple[int, int, str]:
    token = f'name="{LAYOUT_NAME}"'
    idx = qgs.find(token)
    if idx < 0:
        raise SystemExit(f"Layout {LAYOUT_NAME!r} not found")
    start = qgs.rfind("<Layout", 0, idx)
    end = qgs.find("</Layout>", idx)
    if start < 0 or end < 0:
        raise SystemExit("Malformed Layout XML")
    end += len("</Layout>")
    return start, end, qgs[start:end]


def _scale_mm_attr(tag: str, attr: str, sx: float, sy: float) -> str:
    pat = re.compile(rf'{attr}="([^"]*)"')

    def repl(m: re.Match[str]) -> str:
        raw = m.group(1)
        parts = raw.split(",")
        if len(parts) < 3:
            return m.group(0)
        try:
            x = float(parts[0]) * sx
            y = float(parts[1]) * sy
        except ValueError:
            return m.group(0)
        unit = parts[2]
        return f'{attr}="{x},{y},{unit}"'

    return pat.sub(repl, tag)


def scale_layout_items(layout_xml: str, sx: float, sy: float) -> str:
    def patch_tag(m: re.Match[str]) -> str:
        tag = m.group(0)
        tag = _scale_mm_attr(tag, "positionOnPage", sx, sy)
        tag = _scale_mm_attr(tag, "position", sx, sy)
        tag = _scale_mm_attr(tag, "size", sx, sy)
        return tag

    return re.sub(r"<LayoutItem\b[^>]*>", patch_tag, layout_xml)


def set_paper_and_main_map(layout_xml: str, page_w: float, page_h: float) -> str:
    """Paper + main trail map exactly fill the export page (no half-page gutter)."""

    def patch_tag(m: re.Match[str]) -> str:
        tag = m.group(0)
        if PAPER_UUID in tag:
            tag = re.sub(r'size="[^"]*"', f'size="{page_w},{page_h},mm"', tag, count=1)
            return tag
        if MAIN_MAP_UUID in tag and 'type="65639"' in tag:
            tag = re.sub(r'size="[^"]*"', f'size="{page_w},{page_h},mm"', tag, count=1)
            tag = re.sub(r'positionOnPage="[^"]*"', 'positionOnPage="0,0,mm"', tag, count=1)
            tag = re.sub(r'position="[^"]*"', 'position="0,0,mm"', tag, count=1)
        return tag

    return re.sub(r"<LayoutItem\b[^>]*>", patch_tag, layout_xml)


def square_globe_inset_cluster(layout_xml: str) -> str:
    """Force overview inset map/dot/clip to a square frame so the globe exports as a circle."""

    def patch_tag(m: re.Match[str]) -> str:
        tag = m.group(0)
        im = re.search(r'\bid="([^"]*)"', tag)
        if not im or im.group(1) not in GLOBE_INSET_IDS:
            return tag
        sm = re.search(r'size="([^"]*)"', tag)
        if not sm:
            return tag
        parts = sm.group(1).split(",")
        if len(parts) < 3:
            return tag
        try:
            w, h = float(parts[0]), float(parts[1])
        except ValueError:
            return tag
        side = min(w, h)
        unit = parts[2]
        tag = re.sub(r'size="[^"]*"', f'size="{side},{side},{unit}"', tag, count=1)
        return tag

    return re.sub(r"<LayoutItem\b[^>]*>", patch_tag, layout_xml)


def lock_inset_and_north_arrow_sizes(layout_xml: str) -> str:
    """Globe inset + north arrow stay at small-export mm on all tiers (no upscaling)."""

    def patch_tag(m: re.Match[str]) -> str:
        tag = m.group(0)
        im = re.search(r'\bid="([^"]*)"', tag)
        if im and im.group(1) in GLOBE_INSET_IDS:
            tag = re.sub(
                r'size="[^"]*"',
                f'size="{INSET_GLOBE_SIDE_MM},{INSET_GLOBE_SIDE_MM},mm"',
                tag,
                count=1,
            )
            return tag
        if NORTH_ARROW_UUID in tag and 'type="65640"' in tag:
            tag = re.sub(
                r'size="[^"]*"',
                f'size="{NORTH_ARROW_W_MM},{NORTH_ARROW_H_MM},mm"',
                tag,
                count=1,
            )
            return tag
        return tag

    return re.sub(r"<LayoutItem\b[^>]*>", patch_tag, layout_xml)


def _set_position_mm(layout_xml: str, uuid: str, x_mm: float, y_mm: float) -> str:
    """Set position + positionOnPage on a LayoutItem matched by uuid substring."""
    pos = f"{x_mm},{y_mm},mm"

    def patch_tag(m: re.Match[str]) -> str:
        tag = m.group(0)
        if uuid not in tag:
            return tag
        tag = re.sub(r'positionOnPage="[^"]*"', f'positionOnPage="{pos}"', tag, count=1)
        tag = re.sub(r'position="[^"]*"', f'position="{pos}"', tag, count=1)
        return tag

    return re.sub(r"<LayoutItem\b[^>]*>", patch_tag, layout_xml)


def move_globe_inset_cluster(layout_xml: str, x_mm: float, y_mm: float) -> str:
    for uid in GLOBE_INSET_UUIDS:
        layout_xml = _set_position_mm(layout_xml, uid, x_mm, y_mm)
    return layout_xml


def move_small_scale_bars(layout_xml: str, km_y_mm: float, mi_y_mm: float) -> str:
    """Small portrait layout: left-aligned scale bars (x=0); km above mi."""
    layout_xml = _set_position_mm(layout_xml, SCALE_BAR_KM_UUID, 0.0, km_y_mm)
    layout_xml = _set_position_mm(layout_xml, SCALE_BAR_MI_UUID, 0.0, mi_y_mm)
    return layout_xml


def _get_item_size_mm(layout_xml: str, uuid: str) -> tuple[float, float] | None:
    """Read width,height from the first LayoutItem open tag containing uuid."""
    for m in re.finditer(r"<LayoutItem\b[^>]*>", layout_xml):
        tag = m.group(0)
        if uuid not in tag:
            continue
        sm = re.search(r'size="([^"]*)"', tag)
        if not sm:
            return None
        parts = sm.group(1).split(",")
        if len(parts) < 3:
            return None
        try:
            return float(parts[0]), float(parts[1])
        except ValueError:
            return None
    return None


def position_scale_bars_bottom_left(
    layout_xml: str, page_h_mm: float, *, left_x_mm: float
) -> str:
    """Stack km above mi, flush to bottom edge; left edge at left_x_mm."""
    km_sz = _get_item_size_mm(layout_xml, SCALE_BAR_KM_UUID)
    mi_sz = _get_item_size_mm(layout_xml, SCALE_BAR_MI_UUID)
    if not km_sz or not mi_sz:
        return layout_xml
    _, km_h = km_sz
    _, mi_h = mi_sz
    m = PAGE_CORNER_MARGIN_MM
    y_mi = page_h_mm - m - mi_h
    y_km = y_mi - SCALE_STACK_GAP_MM - km_h
    layout_xml = _set_position_mm(layout_xml, SCALE_BAR_KM_UUID, left_x_mm, y_km)
    layout_xml = _set_position_mm(layout_xml, SCALE_BAR_MI_UUID, left_x_mm, y_mi)
    return layout_xml


def position_inset_top_right(layout_xml: str, page_w_mm: float) -> str:
    """Upper-right corner (referencePoint=0 / upper-left of item)."""
    x = page_w_mm - INSET_GLOBE_SIDE_MM - PAGE_CORNER_MARGIN_MM
    y = PAGE_CORNER_MARGIN_MM
    return move_globe_inset_cluster(layout_xml, x, y)


def position_north_arrow_bottom_right(
    layout_xml: str, page_w_mm: float, page_h_mm: float
) -> str:
    """Bottom-right corner; position is top-left of item (referencePoint=0)."""
    x = page_w_mm - NORTH_ARROW_W_MM - PAGE_CORNER_MARGIN_MM
    y = page_h_mm - NORTH_ARROW_H_MM - PAGE_CORNER_MARGIN_MM
    return _set_position_mm(layout_xml, NORTH_ARROW_UUID, x, y)


def write_qgz(dest_qgz: Path, qgs_bytes: bytes) -> None:
    dest_qgz.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(SRC_QGZ, "r") as zin:
        with zipfile.ZipFile(dest_qgz, "w", zipfile.ZIP_DEFLATED) as zout:
            for info in zin.infolist():
                data = zin.read(info.filename)
                if info.filename == TEMPLATE_QGS:
                    data = qgs_bytes
                zout.writestr(info, data)


def main() -> None:
    if not SRC_QGZ.exists():
        raise SystemExit(f"Missing source template: {SRC_QGZ}")

    qgs = zipfile.ZipFile(SRC_QGZ).read(TEMPLATE_QGS).decode("utf-8")
    ls, le, blk = _layout_block(qgs)

    tiers: dict[str, tuple[float, float]] = {
        "ski_atlas_small_template.qgz": (105.0, 74.25),
        "ski_atlas_medium_template.qgz": (105.0, 148.5),
        "ski_atlas_large_template.qgz": (210.0, 148.5),
        "ski_atlas_mega_template.qgz": (210.0, 297.0),
        "ski_atlas_small_landscape_template.qgz": (74.25, 105.0),
        "ski_atlas_medium_landscape_template.qgz": (148.5, 105.0),
        "ski_atlas_large_landscape_template.qgz": (148.5, 210.0),
        "ski_atlas_mega_landscape_template.qgz": (297.0, 210.0),
    }

    for fname, (pw, ph) in tiers.items():
        sx = pw / BASE_W
        sy = ph / BASE_H
        new_blk = scale_layout_items(blk, sx, sy)
        new_blk = set_paper_and_main_map(new_blk, pw, ph)
        new_blk = square_globe_inset_cluster(new_blk)
        new_blk = lock_inset_and_north_arrow_sizes(new_blk)
        new_blk = position_inset_top_right(new_blk, pw)
        new_blk = position_north_arrow_bottom_right(new_blk, pw, ph)
        if fname == "ski_atlas_small_template.qgz":
            new_blk = move_small_scale_bars(new_blk, km_y_mm=65.0, mi_y_mm=69.0)
        elif fname == "ski_atlas_small_landscape_template.qgz":
            new_blk = position_scale_bars_bottom_left(
                new_blk, page_h_mm=ph, left_x_mm=0.0
            )

        out_qgs = qgs[:ls] + new_blk + qgs[le:]
        dest = OUT_DIR / fname
        write_qgz(dest, out_qgs.encode("utf-8"))
        print(f"Wrote {dest.relative_to(ROOT)}  ({pw}×{ph} mm)")


if __name__ == "__main__":
    main()
