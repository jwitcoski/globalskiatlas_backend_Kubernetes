"""Pack resort entries onto 8.5x11 physical pages (quarter / half / full / spread)."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from atlas.book_gen.constants import CATEGORY_BOOK_ORDER
from atlas.book_gen.log_util import log
from atlas.book_gen.map_sizes import (
    QUARTER_ROW_GAP_FRAC,
    map_dimensions_pt,
    slot_height_fraction,
)
from atlas.book_gen.resort_category import page_fraction, slot_for_fraction

PT_PER_IN = 72.0


@dataclass
class Placement:
    pageId: str
    slot: str
    x: float
    y: float
    w: float
    h: float
    page_index: int
    spread_page: int | None = None  # 0=left, 1=right for spread entries
    map_on_right: bool = True  # alternate per slot tier: right, left, right, …


@dataclass
class PhysicalPage:
    page_index: int
    page_type: str  # composite | full | spread_left | spread_right
    placements: list[Placement]


def content_area_pt(
    trim_in: tuple[float, float] = (8.5, 11.0),
    margin_in: tuple[float, float, float, float] = (0.5, 0.5, 0.5, 0.5),
) -> tuple[float, float]:
    pw = trim_in[0] * PT_PER_IN
    ph = trim_in[1] * PT_PER_IN
    mt, mr, mb, ml = (m * PT_PER_IN for m in margin_in)
    return pw - ml - mr, ph - mt - mb


def pack_manifest_entries(
    manifest: list[dict[str, Any]],
    *,
    quarters_per_sheet: int = 4,
    content_h_pt: float | None = None,
    map_export_dpi: int = 300,
) -> dict[str, Any]:
    """
    Pack resorts onto pages. Returns layout_plan dict.

    Sorting: size tier (small → medium → large → mega), then title A–Z within tier.
    Row heights for small/medium follow QGIS map plate size (not equal ¼ slices).
    """
    entries = [e for e in manifest if e.get("slot") not in (None, "skip")]
    qps = max(1, min(4, int(quarters_per_sheet)))
    ch = content_h_pt if content_h_pt and content_h_pt > 0 else 720.0
    log(
        f"pack_pages: laying out {len(entries)} resort entries "
        f"(quarters_per_sheet={qps}, map_export_dpi={map_export_dpi}) ..."
    )
    entries.sort(
        key=lambda e: (
            CATEGORY_BOOK_ORDER.get(e.get("resortSizeCategory") or "unknown", 99),
            (e.get("title") or e.get("pageId") or "").casefold(),
        )
    )

    pages: list[PhysicalPage] = []
    page_index = 0

    composite_capacity = 0.0
    composite_placements: list[Placement] = []
    composite_y = 0.0
    half_top_used = False
    # Alternation resets at each tier (quarter / half / full) when sort order changes tier.
    flip_index: dict[str, int] = {"quarter": 0, "half": 0, "full": 0}

    def map_on_right_for_slot(slot: str) -> bool:
        if slot not in flip_index:
            return True
        idx = flip_index[slot]
        flip_index[slot] = idx + 1
        return (idx % 2) == 0

    def flush_composite() -> None:
        nonlocal page_index, composite_capacity, composite_placements, composite_y
        nonlocal half_top_used
        if not composite_placements:
            return
        _expand_quarter_placements_to_fill_page(composite_placements)
        pages.append(
            PhysicalPage(
                page_index=page_index,
                page_type="composite",
                placements=list(composite_placements),
            )
        )
        page_index += 1
        composite_capacity = 0.0
        composite_placements = []
        composite_y = 0.0
        half_top_used = False

    def _map_row_frac(entry: dict[str, Any]) -> float:
        _, mh = map_dimensions_pt(
            entry.get("mapPath"),
            entry.get("mapTier") or "small",
            default_dpi=map_export_dpi,
        )
        return slot_height_fraction(mh, ch)

    def add_to_composite(entry: dict[str, Any], frac: float) -> bool:
        nonlocal composite_capacity, composite_y, half_top_used
        slot = entry["slot"]
        pid = entry["pageId"]

        if frac <= 0.25:
            if qps == 1:
                flush_composite()
                composite_placements.append(
                    Placement(
                        pageId=pid,
                        slot=slot,
                        x=0.0,
                        y=0.0,
                        w=1.0,
                        h=1.0,
                        page_index=page_index,
                        map_on_right=map_on_right_for_slot(slot),
                    )
                )
                flush_composite()
                return True
            h_frac = _map_row_frac(entry)
            gap = QUARTER_ROW_GAP_FRAC
            need = h_frac + (gap if composite_placements else 0.0)
            if composite_capacity + need > 1.0 + 1e-6:
                return False
            if composite_placements:
                composite_y += gap
                composite_capacity += gap
            composite_placements.append(
                Placement(
                    pageId=pid,
                    slot=slot,
                    x=0.0,
                    y=composite_y,
                    w=1.0,
                    h=h_frac,
                    page_index=page_index,
                    map_on_right=map_on_right_for_slot(slot),
                )
            )
            composite_y += h_frac
            composite_capacity += h_frac
            return True

        if frac <= 0.5:
            h_frac = _map_row_frac(entry)
            if h_frac > 0.5 + 1e-6:
                h_frac = 0.5
            if composite_capacity + h_frac > 1.0 + 1e-6:
                return False
            if half_top_used:
                y = 0.5
                h = min(0.5, 1.0 - composite_capacity)
            else:
                y = 0.0
                h = h_frac
                half_top_used = True
            composite_placements.append(
                Placement(
                    pageId=pid,
                    slot=slot,
                    x=0.0,
                    y=y,
                    w=1.0,
                    h=h,
                    page_index=page_index,
                    map_on_right=map_on_right_for_slot(slot),
                )
            )
            composite_capacity += h
            return True
        return False

    for entry in entries:
        frac = float(entry.get("pageFraction") or page_fraction(entry.get("resortSizeCategory", "unknown")))
        slot = slot_for_fraction(frac)
        entry["slot"] = slot
        entry["pageFraction"] = frac

        if slot == "skip":
            continue

        if slot == "spread":
            flush_composite()
            pages.append(
                PhysicalPage(
                    page_index=page_index,
                    page_type="spread_left",
                    placements=[
                        Placement(
                            pageId=entry["pageId"],
                            slot=slot,
                            x=0.0,
                            y=0.0,
                            w=1.0,
                            h=1.0,
                            page_index=page_index,
                            spread_page=0,
                        )
                    ],
                )
            )
            page_index += 1
            pages.append(
                PhysicalPage(
                    page_index=page_index,
                    page_type="spread_right",
                    placements=[
                        Placement(
                            pageId=entry["pageId"],
                            slot=slot,
                            x=0.0,
                            y=0.0,
                            w=1.0,
                            h=1.0,
                            page_index=page_index,
                            spread_page=1,
                        )
                    ],
                )
            )
            page_index += 1
            continue

        if slot == "full":
            flush_composite()
            pages.append(
                PhysicalPage(
                    page_index=page_index,
                    page_type="full",
                    placements=[
                        Placement(
                            pageId=entry["pageId"],
                            slot=slot,
                            x=0.0,
                            y=0.0,
                            w=1.0,
                            h=1.0,
                            page_index=page_index,
                            map_on_right=map_on_right_for_slot(slot),
                        )
                    ],
                )
            )
            page_index += 1
            continue

        if not add_to_composite(entry, frac):
            flush_composite()
            if not add_to_composite(entry, frac):
                pages.append(
                    PhysicalPage(
                        page_index=page_index,
                        page_type=entry["slot"],
                        placements=[
                            Placement(
                                pageId=entry["pageId"],
                                slot=entry["slot"],
                                x=0.0,
                                y=0.0,
                                w=1.0,
                                h=1.0,
                                page_index=page_index,
                                map_on_right=map_on_right_for_slot(entry["slot"]),
                            )
                        ],
                    )
                )
                page_index += 1

    flush_composite()

    by_type: dict[str, int] = {}
    for p in pages:
        by_type[p.page_type] = by_type.get(p.page_type, 0) + 1
    log(
        f"pack_pages: {len(pages)} physical page(s) "
        f"({', '.join(f'{k}={v}' for k, v in sorted(by_type.items()))})"
    )

    return {
        "pages": [
            {
                "page_index": p.page_index,
                "page_type": p.page_type,
                "placements": [asdict(pl) for pl in p.placements],
            }
            for p in pages
        ],
        "physical_page_count": len(pages),
    }


def write_layout_plan(path: Path, plan: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(plan, indent=2), encoding="utf-8")


def load_layout_plan(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _expand_quarter_placements_to_fill_page(
    placements: list[Placement],
    *,
    gap_frac: float = QUARTER_ROW_GAP_FRAC,
) -> None:
    """
    When fewer than four small hills fit on a composite page, grow each row's
    normalized height so the stack uses the full content area (no bottom gap).
    """
    if not placements:
        return
    if any(p.slot != "quarter" for p in placements):
        return
    n = len(placements)
    total_gaps = gap_frac * (n - 1) if n > 1 else 0.0
    avail = 1.0 - total_gaps
    sum_h = sum(p.h for p in placements)
    if sum_h <= 0 or sum_h >= avail - 1e-6:
        return
    scale = avail / sum_h
    y = 0.0
    for i, p in enumerate(placements):
        p.y = y
        p.h = p.h * scale
        y += p.h
        if i < n - 1:
            y += gap_frac
    log(
        f"  pack_pages: expanded {n} small-hill row(s) to fill page "
        f"(height scale {scale:.2f})"
    )
