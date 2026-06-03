"""Matplotlib charts for regional facts pages (mirrors GlobalSkiAtlas_2 Ski*Facts)."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd

from atlas.book_gen.constants import RESORT_CATEGORY_CRITERIA, RESORT_CATEGORY_LABEL
from atlas.book_gen.regional_facts import _aggregate_lift_types, _name_col, _num_series
from atlas.book_gen.resort_category import resort_size_category

# Website palette (SkiResortFacts / tailwind teal)
C_TEAL = "#0d9488"
C_TEAL_LIGHT = "#5eead4"
C_SLATE = "#94a3b8"
C_SLATE_DARK = "#1e293b"
C_LABEL = "#374151"
C_BG = "#f8fafc"
C_GRID = "#e5e7eb"
M_TO_FT = 3.28084

# Floating elevation bars (reference: Colorado Ski Area Elevation/Vertical)
C_ELEV_BAR = "#8B1E2A"
C_ELEV_BAR_EDGE = "#ffffff"
C_ELEV_BASE_LINE = "#1e3a5f"
C_ELEV_SUMMIT_LINE = "#0f766e"

# Reference-style elevation mountains (outline triangles, Québec chart look).
C_ELEV_REF_BG = "#d4e8f2"
C_ELEV_REF_LINE = "#173554"
C_ELEV_REF_TEXT = "#173554"
_ELEV_LINE_RAMP = ()  # unused; kept so older imports do not break

TIER_COLORS = {
    "small_hill": "#5b8fb4",
    "ski_mountain": "#0d9488",
    "multiple_mountains": "#2563eb",
    "mega_resort": "#1a365d",
}

# Pastel fills for trails-vs-acres scatter tier backgrounds (RGBA).
_TIER_SCATTER_BG: dict[str, tuple[float, float, float, float]] = {
    "small_hill": (0.90, 0.92, 0.96, 0.70),
    "ski_mountain": (0.55, 0.72, 0.86, 0.45),
    "multiple_mountains": (0.45, 0.62, 0.94, 0.40),
    "mega_resort": (0.28, 0.42, 0.72, 0.42),
}

_SCATTER_TIER_ORDER = (
    "small_hill",
    "ski_mountain",
    "multiple_mountains",
    "mega_resort",
)

TRAIL_DIFF_COLORS = {
    "Novice": "#22c55e",
    "Easy": "#4ade80",
    "Intermediate": "#2563eb",
    "Advanced": "#1a1a1a",
    "Expert": "#991b1b",
    "Freeride": "#7f1d1d",
    "Extreme": "#450a0a",
    "Terrain parks": "#ea580c",
}

LIFT_TYPE_COLORS = {
    "chair lift": "#7c3aed",
    "gondola": "#0d9488",
    "magic carpet": "#0891b2",
    "platter": "#2563eb",
    "t-bar": "#92400e",
    "rope tow": "#475569",
    "cable car": "#1d4ed8",
    "detachable": "#6366f1",
}

# Same artwork as QGIS Lines.qml markers (atlas/map_gen/icons).
_MAP_ICONS_DIR = Path(__file__).resolve().parents[1] / "map_gen" / "icons"
_LIFT_TYPE_ICON: dict[str, tuple[str, ...]] = {
    # PNG first (QGIS map markers); SVG fallback when cairosvg is installed.
    "chair lift": ("ski-lift.png", "noun-ski-lift-8803.svg"),
    "detachable": ("ski-lift.png", "noun-ski-lift-8803.svg"),
    "mixed lift": ("ski-lift.png", "noun-ski-lift-8803.svg"),
    "magic carpet": ("Magic_Carpet.png", "Magic_Carpet.svg"),
    "gondola": ("cablecar.png", "cable-car-svgrepo-com.svg"),
    "cable car": ("cablecar.png", "cable-car-svgrepo-com.svg"),
    # tbar.png is a duplicate of ski-lift.png; tow/surface lifts use drawn icons below.
}
_DRAWN_LIFT_KINDS = frozenset(
    {"t-bar", "j-bar", "rope tow", "platter", "drag lift"}
)


def _mpl_setup() -> None:
    import matplotlib

    matplotlib.use("Agg")


def _save_fig(fig, path: Path, dpi: int = 180) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=dpi, bbox_inches="tight", facecolor=fig.get_facecolor())
    import matplotlib.pyplot as plt

    plt.close(fig)
    return path


def _style_axes(ax) -> None:
    ax.set_facecolor(C_BG)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(C_GRID)
    ax.spines["bottom"].set_color(C_GRID)
    ax.tick_params(colors=C_LABEL, labelsize=8)
    ax.grid(True, axis="both", color=C_GRID, linewidth=0.6, alpha=0.8)


_RESORT_NAME_SUFFIXES = (
    " ski resort",
    " ski area",
    " recreation area",
    " resort",
    " hill",
)


def _clean_resort_display_name(name: str) -> str:
    """Drop trailing generic suffixes (Ski Resort, Recreation Area, etc.)."""
    name = " ".join(str(name).split())
    while name:
        low = name.casefold()
        stripped = False
        for suf in _RESORT_NAME_SUFFIXES:
            if low.endswith(suf):
                name = name[: -len(suf)].rstrip(" -–—,")
                stripped = True
                break
        if not stripped:
            break
    return name.strip()


def _short_name(name: str, max_len: int = 18) -> str:
    name = _clean_resort_display_name(name.strip())
    if len(name) <= max_len:
        return name
    return name[: max_len - 1].rstrip() + "…"


HA_TO_ACRES = 2.471054
# Base half-width in data units (packed left-to-right; no overlap).
_ELEV_MOUNTAIN_MIN_HW = 0.10
_ELEV_MOUNTAIN_MAX_HW = 1.15
_ELEV_MOUNTAIN_GAP = 0.14
_ELEV_MIN_ASPECT = 4.2  # min vertical/width — steep peaks, not tents
# Overlapping layout (alphabetical, x = 0..n-1): acreage → base width vs chart span.
_ELEV_OVERLAP_MIN_BASE_FRAC = 0.05
_ELEV_OVERLAP_MAX_BASE_FRAC = 0.50
_ELEV_CAP_FRAC = 0.91  # chord height on slopes (tip above)
_ELEV_PEAK_LW = 2.0


def _skiable_acres_series(df: pd.DataFrame) -> pd.Series:
    acres = _num_series(
        df,
        "skiable_terrain_acres",
        "skiableTerrainAcres",
        "Skiable Terrain Acres",
    )
    ha = _num_series(df, "skiable_terrain_ha", "skiableTerrainHa", "Skiable Terrain Ha")
    if ha.notna().any():
        acres = acres.fillna(ha * HA_TO_ACRES)
    if acres.notna().any():
        return acres
    return _num_series(df, "total_area_acres", "totalAreaAcres", "total_area_ha")


def _fmt_elev_label(y: float, y_unit: str) -> str:
    v = int(round(y))
    if y_unit == "ft":
        return f"{v:,} ft".replace(",", "\u202f")
    return f"{v} m"


def _steep_half_width(half_w: float, vert: float) -> float:
    """Keep peaks narrow/steep like the reference (not wide tents)."""
    if vert <= 0:
        return half_w
    return min(half_w, vert / _ELEV_MIN_ASPECT)


def _cap_chord(
    cx: float,
    base_y: float,
    summit_y: float,
    half_w: float,
) -> tuple[float, float, float, float, float]:
    """Return x_left, x_right, y_chord, tip_x, tip_y on the triangle."""
    vert = max(summit_y - base_y, 1.0)
    y_chord = base_y + vert * _ELEV_CAP_FRAC
    frac = (y_chord - base_y) / vert
    x_left = (cx - half_w) + frac * half_w
    x_right = (cx + half_w) - frac * half_w
    return x_left, x_right, y_chord, cx, summit_y


def _draw_reference_peak(
    ax,
    cx: float,
    base_y: float,
    summit_y: float,
    half_w: float,
) -> tuple[float, float]:
    """
    Steep outlined triangle + downward cap arc on the upper slopes (reference look).
    Returns summit x,y for the elevation label.
    """
    import numpy as np
    from matplotlib.patches import Polygon

    y_span = ax.get_ylim()[1] - ax.get_ylim()[0]
    min_rise = max(80.0, y_span * 0.012)
    if summit_y - base_y < min_rise:
        summit_y = base_y + min_rise
    half_w = _steep_half_width(half_w, summit_y - base_y)
    vert = summit_y - base_y

    x_left, x_right, y_chord, tip_x, tip_y = _cap_chord(
        cx, base_y, summit_y, half_w
    )

    # Main triangle (sharp tip above the cap chord).
    ax.add_patch(
        Polygon(
            [
                (cx - half_w, base_y),
                (x_left, y_chord),
                (tip_x, tip_y),
                (x_right, y_chord),
                (cx + half_w, base_y),
            ],
            closed=True,
            facecolor="none",
            edgecolor=C_ELEV_REF_LINE,
            linewidth=_ELEV_PEAK_LW,
            joinstyle="round",
            zorder=2,
        )
    )

    # Cap arc: chord endpoints on the slopes, bowing downward (reference ∩).
    bulge = max(vert * 0.075, y_span * 0.004)
    mid_x = (x_left + x_right) / 2.0
    ctrl_y = y_chord - bulge
    t = np.linspace(0.0, 1.0, 32)
    arc_x = (1 - t) ** 2 * x_left + 2 * (1 - t) * t * mid_x + t**2 * x_right
    arc_y = (1 - t) ** 2 * y_chord + 2 * (1 - t) * t * ctrl_y + t**2 * y_chord
    ax.plot(
        arc_x,
        arc_y,
        color=C_ELEV_REF_LINE,
        linewidth=_ELEV_PEAK_LW,
        solid_capstyle="round",
        zorder=4,
    )
    return tip_x, tip_y


def _elevation_figure_inches() -> tuple[float, float]:
    """Match book facts page: full content width × upper half of content height."""
    from atlas.book_gen.pack_pages import content_area_pt

    cw_pt, ch_pt = content_area_pt()
    return cw_pt / 72.0, (ch_pt / 2.0) / 72.0


def _mountain_half_width_from_acres(
    acres: float,
    *,
    acres_min: float,
    acres_max: float,
    x_chart_span: float | None = None,
) -> float:
    """Base half-width from skiable acres (packed or overlapping layout)."""
    if x_chart_span is not None and x_chart_span > 0:
        min_hw = (_ELEV_OVERLAP_MIN_BASE_FRAC / 2.0) * x_chart_span
        max_hw = (_ELEV_OVERLAP_MAX_BASE_FRAC / 2.0) * x_chart_span
        if acres > 0 and acres_max > acres_min:
            rel = float((acres - acres_min) / (acres_max - acres_min))
            rel = rel**0.88
            return min_hw + rel * (max_hw - min_hw)
        return min_hw
    if acres > 0 and acres_max > acres_min:
        rel = float((acres - acres_min) / (acres_max - acres_min))
        rel = rel**0.88
        return _ELEV_MOUNTAIN_MIN_HW + rel * (
            _ELEV_MOUNTAIN_MAX_HW - _ELEV_MOUNTAIN_MIN_HW
        )
    return _ELEV_MOUNTAIN_MIN_HW


def _pack_mountain_centers(half_widths: list[float], *, gap: float = _ELEV_MOUNTAIN_GAP) -> list[float]:
    """Place mountain centers so bases do not overlap (row order preserved)."""
    if not half_widths:
        return []
    centers: list[float] = [half_widths[0]]
    for i in range(1, len(half_widths)):
        prev_hw = half_widths[i - 1]
        hw = half_widths[i]
        centers.append(centers[-1] + prev_hw + gap + hw)
    return centers


_ELEV_LABEL_FONT: dict[str, tuple[float, float, str]] = {
    "mega_resort": (9.5, 7.5, "bold"),
    "multiple_mountains": (7.5, 6.5, "bold"),
    "ski_mountain": (6.0, 5.25, "bold"),
    "small_hill": (3.8, 3.2, "normal"),
    "unknown": (3.8, 3.2, "normal"),
}


def _elevation_label_fonts(tier: str) -> tuple[float, float, str]:
    return _ELEV_LABEL_FONT.get(tier, _ELEV_LABEL_FONT["small_hill"])


_ELEV_MARKER_FS = 7.0


def _elevation_axis_round(val: float, *, use_feet: bool, up: bool) -> float:
    """Snap axis limits to a fixed grid (100 ft or 50 m)."""
    import math

    unit = 100.0 if use_feet else 50.0
    if up:
        return math.ceil(val / unit) * unit
    return math.floor(val / unit) * unit


def _elevation_tick_step(span: float, *, use_feet: bool) -> float:
    """Readable Y tick interval from the plotted elevation span."""
    if span <= 0:
        return 500.0 if use_feet else 100.0
    if use_feet:
        for step in (100.0, 250.0, 500.0, 1000.0):
            if span / step <= 8:
                return step
        return 1000.0
    for step in (25.0, 50.0, 100.0, 200.0, 500.0):
        if span / step <= 8:
            return step
    return 500.0

_ELEV_SUMMIT_LABEL_CANDIDATES: list[tuple[float, float, str, str, bool]] = [
    (0, 0, "center", "bottom", False),
    (0, 10, "center", "bottom", True),
    (-22, 5, "right", "bottom", True),
    (22, 5, "left", "bottom", True),
    (0, 18, "center", "bottom", True),
    (-30, 12, "right", "bottom", True),
    (30, 12, "left", "bottom", True),
    (0, 26, "center", "bottom", True),
]


def _rects_overlap_px(
    a: tuple[float, float, float, float],
    b: tuple[float, float, float, float],
    *,
    pad: float = 2.0,
) -> bool:
    al, ab, ar, at = a[0] - pad, a[1] - pad, a[2] + pad, a[3] + pad
    bl, bb, br, bt = b[0] - pad, b[1] - pad, b[2] + pad, b[3] + pad
    return not (ar < bl or al > br or at < bb or ab > bt)


def _text_bbox_px(
    px: float,
    py: float,
    *,
    width_px: float,
    height_px: float,
    ha: str,
    va: str,
) -> tuple[float, float, float, float]:
    if ha == "center":
        left, right = px - width_px / 2.0, px + width_px / 2.0
    elif ha == "left":
        left, right = px, px + width_px
    else:
        left, right = px - width_px, px
    if va == "top":
        bottom, top = py - height_px, py
    elif va == "bottom":
        bottom, top = py, py + height_px
    else:
        bottom, top = py - height_px / 2.0, py + height_px / 2.0
    return (left, bottom, right, top)


def _text_size_px(fig, fontsize: float, text: str, weight: str) -> tuple[float, float]:
    bold = 1.1 if weight == "bold" else 1.0
    w = 0.62 * bold * (fontsize / 72.0) * fig.dpi * max(len(text), 1)
    h = 1.22 * (fontsize / 72.0) * fig.dpi
    return w, h


def _elevation_letter_code(index: int) -> str:
    """0 → a, 1 → b, …, 26 → aa (alphabetical order matches chart x)."""
    n = index + 1
    code = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        code = chr(ord("a") + rem) + code
    return code


def _legend_display_name(raw: str) -> str:
    return _clean_resort_display_name(raw).title()


def _legend_short_name(raw: str) -> str:
    """Compact label for the elevation chart key (fits narrow table columns)."""
    s = _legend_display_name(raw)
    for suffix in (
        " Ski Resort",
        " Ski Area",
        " Recreation Area",
        " Resort",
        " Mountain",
        " Nordic Center",
        " Outdoor Center",
        " Ski Club",
        " Center",
        " Hill",
    ):
        if s.endswith(suffix):
            s = s[: -len(suffix)].strip()
    return s


def _mountain_obstacle_rects_px(
    ax, mountains: list[dict[str, Any]]
) -> list[tuple[float, float, float, float]]:
    rects: list[tuple[float, float, float, float]] = []
    for m in mountains:
        vert = float(m["summit_y"]) - float(m["base_y"])
        x0 = float(m["cx"]) - float(m["half_w"]) * 1.08
        x1 = float(m["cx"]) + float(m["half_w"]) * 1.08
        y0 = float(m["base_y"])
        y1 = float(m["summit_y"]) + vert * 0.06
        p0 = ax.transData.transform((x0, y0))
        p1 = ax.transData.transform((x1, y1))
        rects.append(
            (
                min(p0[0], p1[0]),
                min(p0[1], p1[1]),
                max(p0[0], p1[0]),
                max(p0[1], p1[1]),
            )
        )
    return rects


def _place_elevation_label(
    ax,
    fig,
    *,
    anchor_xy: tuple[float, float],
    text: str,
    fontsize: float,
    weight: str,
    candidates: list[tuple[float, float, str, str, bool]],
    placed: list[tuple[float, float, float, float]],
    obstacles: list[tuple[float, float, float, float]],
    fig_bbox_px: tuple[float, float, float, float],
) -> bool:
    """Place one label; use a leader line when the default spot collides."""
    x, y = anchor_xy
    scale = fig.dpi / 72.0
    w_px, h_px = _text_size_px(fig, fontsize, text, weight)
    fl, fb, fr, ft = fig_bbox_px

    for dx, dy, ha, va, leader in candidates:
        ax_px, ay_px = ax.transData.transform((x, y))
        tx_px = ax_px + dx * scale
        ty_px = ay_px + dy * scale
        bb = _text_bbox_px(
            tx_px, ty_px, width_px=w_px, height_px=h_px, ha=ha, va=va
        )
        if bb[0] < fl or bb[2] > fr or bb[1] < fb or bb[3] > ft:
            continue
        if any(_rects_overlap_px(bb, p) for p in placed):
            continue
        if any(_rects_overlap_px(bb, o, pad=1.0) for o in obstacles):
            continue

        kw: dict[str, Any] = dict(
            fontsize=fontsize,
            fontweight=weight,
            color=C_ELEV_REF_TEXT,
            ha=ha,
            va=va,
            zorder=6,
            clip_on=False,
        )
        if leader or dx != 0 or dy != 0:
            ax.annotate(
                text,
                (x, y),
                xytext=(dx, dy),
                textcoords="offset points",
                arrowprops={
                    "arrowstyle": "-",
                    "color": C_ELEV_REF_TEXT,
                    "lw": 0.75,
                    "shrinkA": 2,
                    "shrinkB": 3,
                    "alpha": 0.9,
                },
                **kw,
            )
        else:
            ax.text(x, y, text, **kw)
        placed.append(bb)
        return True

    # Last resort: callout with no collision check.
    dx, dy, ha, va, _ = candidates[-1]
    ax.annotate(
        text,
        (x, y),
        xytext=(dx, dy),
        textcoords="offset points",
        fontsize=fontsize,
        fontweight=weight,
        color=C_ELEV_REF_TEXT,
        ha=ha,
        va=va,
        zorder=6,
        clip_on=False,
        arrowprops={
            "arrowstyle": "-",
            "color": C_ELEV_REF_TEXT,
            "lw": 0.75,
            "shrinkA": 2,
            "shrinkB": 3,
            "alpha": 0.9,
        },
    )
    ax_px, ay_px = ax.transData.transform((x, y))
    tx_px = ax_px + dx * scale
    ty_px = ay_px + dy * scale
    placed.append(
        _text_bbox_px(tx_px, ty_px, width_px=w_px, height_px=h_px, ha=ha, va=va)
    )
    return False


def _elevation_legend_grid(n: int) -> tuple[int, int, float]:
    """Columns, rows, and figure height fraction for the name key panel."""
    # Tall columns (many rows per column) — matches A→Z peak order down each column.
    if n <= 12:
        max_rows = 6
    elif n <= 24:
        max_rows = 6
    else:
        max_rows = 6
    ncol = min(n, max(6, -(-n // max_rows)))
    nrow = (n + ncol - 1) // ncol
    # Panel height tracks row count; table is top-aligned so rows are not stretched.
    leg_frac = min(0.30, 0.028 + nrow * 0.019)
    return ncol, nrow, leg_frac


def _elevation_legend_label(code: str, name: str, *, max_chars: int) -> str:
    text = f"{code}. {name}"
    if len(text) <= max_chars:
        return text
    return text[: max(4, max_chars - 1)].rstrip() + "…"


def _elevation_legend_max_chars(
    ncol: int, *, fig_width_in: float, fontsize: float
) -> int:
    """Characters that fit in one table column (conservative for proportional font)."""
    col_in = (fig_width_in * 0.94) / max(ncol, 1)
    char_in = (fontsize / 72.0) * 0.55
    return max(9, int(col_in / char_in * 0.92))


def _draw_elevation_legend_panel(
    ax,
    entries: list[tuple[str, str]],
    *,
    fig_width_in: float,
    fig_height_in: float,
    leg_frac: float,
    ncol: int,
    nrow: int,
) -> None:
    """Name key in a fixed-width table (column-major: a↓b↓c, then next column)."""
    n = len(entries)
    if not n:
        return
    ax.set_axis_off()
    fs = 4.8 if n > 36 else 5.2 if n > 24 else 5.6 if n > 14 else 6.0
    max_chars = _elevation_legend_max_chars(
        ncol, fig_width_in=fig_width_in, fontsize=fs
    )

    rows: list[list[str]] = []
    for r in range(nrow):
        row: list[str] = []
        for c in range(ncol):
            i = c * nrow + r
            if i < n:
                code, name = entries[i]
                row.append(
                    _elevation_legend_label(code, name, max_chars=max_chars)
                )
            else:
                row.append("")
        rows.append(row)

    line_in = (fs / 72.0) * 1.1
    content_in = nrow * line_in
    panel_in = max(fig_height_in * leg_frac, line_in)
    bbox_h = min(0.98, content_in / panel_in)
    table_bbox = [0.01, 1.0 - bbox_h, 0.98, bbox_h]

    table = ax.table(
        cellText=rows,
        colWidths=[1.0 / ncol] * ncol,
        loc="upper left",
        cellLoc="left",
        bbox=table_bbox,
    )
    table.auto_set_font_size(False)
    table.set_fontsize(fs)
    bg = ax.get_facecolor()
    for cell in table.get_celld().values():
        cell.set_linewidth(0)
        cell.set_edgecolor("none")
        cell.set_facecolor(bg)
        cell.get_text().set_color(C_ELEV_REF_TEXT)
        cell.PAD = 0.03
        cell.get_text().set_ha("left")
        cell.get_text().set_va("center")


def _label_elevation_lettered(
    ax,
    mountains: list[dict[str, Any]],
    *,
    y_unit: str,
) -> None:
    """Letter marker on each peak; summit elevation on highest only."""
    if not mountains:
        return

    fig = ax.figure
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    fig_bbox_px = fig.get_window_extent(renderer=renderer)
    fig_bbox_px = (fig_bbox_px.x0, fig_bbox_px.y0, fig_bbox_px.x1, fig_bbox_px.y1)

    y_span = ax.get_ylim()[1] - ax.get_ylim()[0]
    pad = y_span * 0.012
    mountain_obs = _mountain_obstacle_rects_px(ax, mountains)
    placed: list[tuple[float, float, float, float]] = []
    max_summit = max(float(m["summit_y"]) for m in mountains)

    marker_bbox = dict(
        boxstyle="round,pad=0.2",
        facecolor="white",
        edgecolor=C_ELEV_REF_TEXT,
        linewidth=0.7,
        alpha=0.92,
    )

    for m in mountains:
        base_y = float(m["base_y"])
        summit_y = float(m["summit_y"])
        cy = base_y + (summit_y - base_y) * 0.2
        ax.text(
            float(m["cx"]),
            cy,
            str(m["code"]),
            ha="center",
            va="center",
            fontsize=_ELEV_MARKER_FS,
            fontweight="bold",
            color=C_ELEV_REF_TEXT,
            zorder=7,
            clip_on=False,
            bbox=marker_bbox,
        )
        w_px, h_px = _text_size_px(fig, _ELEV_MARKER_FS, str(m["code"]), "bold")
        cx_px, cy_px = ax.transData.transform((float(m["cx"]), cy))
        placed.append(
            _text_bbox_px(
                cx_px, cy_px, width_px=w_px, height_px=h_px, ha="center", va="center"
            )
        )

    for m in mountains:
        if float(m["summit_y"]) < max_summit:
            continue
        tier = str(m.get("tier", "small_hill"))
        _, fs_elev, weight = _elevation_label_fonts(tier)
        obs_idx = mountains.index(m)
        other_obs = [r for i, r in enumerate(mountain_obs) if i != obs_idx]
        _place_elevation_label(
            ax,
            fig,
            anchor_xy=(float(m["tip_x"]), float(m["tip_y"]) + pad * 0.25),
            text=_fmt_elev_label(float(m["summit_y"]), y_unit),
            fontsize=fs_elev,
            weight=weight,
            candidates=_ELEV_SUMMIT_LABEL_CANDIDATES,
            placed=placed,
            obstacles=other_obs,
            fig_bbox_px=fig_bbox_px,
        )


def _lift_icon_path(kind: str) -> Path | None:
    """Resolve map icon file for a lift type label (OSM aerialway → display name)."""
    key = kind.strip().lower()
    for name, files in _LIFT_TYPE_ICON.items():
        if key == name or key.replace("_", " ") == name:
            for fname in files:
                path = _MAP_ICONS_DIR / fname
                if path.is_file():
                    return path
    fallback = _MAP_ICONS_DIR / "ski-lift.png"
    return fallback if fallback.is_file() else None


def _load_icon_rgba(path: Path, *, size_px: int = 96) -> Any:
    """Load PNG or rasterize SVG for matplotlib OffsetImage."""
    import numpy as np

    if path.suffix.lower() == ".svg":
        try:
            import cairosvg
            from io import BytesIO

            from PIL import Image

            buf = BytesIO()
            cairosvg.svg2png(
                url=str(path),
                write_to=buf,
                output_width=size_px,
                output_height=size_px,
            )
            return np.asarray(Image.open(buf).convert("RGBA")) / 255.0
        except ImportError:
            pass
    import matplotlib.image as mpimg

    if path.suffix.lower() == ".svg":
        raise ValueError(f"SVG rasterize unavailable: {path.name}")
    img = mpimg.imread(path)
    if img.ndim == 2:
        return img
    if img.shape[2] == 4:
        return img
    # RGB → RGBA
    import numpy as np

    rgba = np.ones((*img.shape[:2], 4), dtype=img.dtype)
    rgba[..., :3] = img[..., :3]
    return rgba


def _lift_display_name(kind: str) -> str:
    return kind.replace("_", " ").strip().title()


def _draw_tbar_icon(ax, *, edge: str, cable: str, j_bar: bool = False) -> None:
    """T-bar / J-bar: cable, hanger, crossbar with end pads, two skiers."""
    from matplotlib.patches import Circle, FancyBboxPatch

    bar_color = "#b45309"
    bar_edge = "#78350f"
    snow = "#e2e8f0"

    # Ground / snow line
    ax.plot([0.08, 0.92], [0.24, 0.24], color=snow, lw=3, solid_capstyle="round", zorder=1)

    # Overhead cable
    ax.plot([0.06, 0.94], [0.9, 0.9], color=cable, lw=2.8, solid_capstyle="round", zorder=2)
    ax.add_patch(Circle((0.06, 0.9), 0.018, facecolor=cable, edgecolor="none", zorder=2))
    ax.add_patch(Circle((0.94, 0.9), 0.018, facecolor=cable, edgecolor="none", zorder=2))

    cx, cable_y = 0.5, 0.9
    hang_bottom = 0.68
    ax.plot([cx, cx], [hang_bottom, cable_y], color=cable, lw=2.4, solid_capstyle="round", zorder=3)

    # Vertical stem
    stem_top = hang_bottom - 0.02
    stem_bot = 0.36
    ax.plot([cx, cx], [stem_bot, stem_top], color=bar_edge, lw=5.5, solid_capstyle="round", zorder=4)

    # Horizontal crossbar (J-bar: longer on one side)
    bar_y = 0.54
    if j_bar:
        ax.plot([0.2, 0.5], [bar_y, bar_y], color=bar_color, lw=6, solid_capstyle="round", zorder=5)
        ax.plot([0.5, 0.82], [bar_y, bar_y], color=bar_color, lw=4.5, solid_capstyle="round", zorder=5)
        end_x = (0.2, 0.82)
    else:
        ax.plot([0.18, 0.82], [bar_y, bar_y], color=bar_color, lw=6.5, solid_capstyle="round", zorder=5)
        end_x = (0.18, 0.82)

    for x in end_x:
        ax.add_patch(
            FancyBboxPatch(
                (x - 0.055, bar_y - 0.055),
                0.11,
                0.11,
                boxstyle="round,pad=0.01",
                facecolor=bar_color,
                edgecolor=bar_edge,
                linewidth=1.4,
                zorder=6,
            )
        )

    def _skier(sx: float, facing: float) -> None:
        """Small skier straddling the bar (facing: +1 right, -1 left)."""
        head_y = 0.48
        ax.add_patch(Circle((sx, head_y), 0.038, facecolor=edge, edgecolor="none", zorder=7))
        # shoulders on bar
        ax.plot([sx - 0.05 * facing, sx + 0.08 * facing], [bar_y, bar_y], color=edge, lw=3, zorder=7)
        # legs
        ax.plot(
            [sx, sx - 0.05 * facing],
            [head_y - 0.04, 0.28],
            color=edge,
            lw=2.8,
            solid_capstyle="round",
            zorder=7,
        )
        ax.plot(
            [sx, sx + 0.04 * facing],
            [head_y - 0.04, 0.28],
            color=edge,
            lw=2.8,
            solid_capstyle="round",
            zorder=7,
        )

    _skier(0.36, 1.0)
    _skier(0.64, -1.0)


def _draw_lift_icon_rgba(kind: str, *, size_px: int = 128) -> Any:
    """Simple line-art icons for surface lifts (tbar.png duplicates chair-lift art)."""
    import numpy as np
    from matplotlib.backends.backend_agg import FigureCanvasAgg
    from matplotlib.figure import Figure
    from matplotlib.patches import Circle, Ellipse

    key = kind.strip().lower()
    fig = Figure(figsize=(1, 1), dpi=size_px)
    canvas = FigureCanvasAgg(fig)
    ax = fig.add_axes([0.06, 0.06, 0.88, 0.88])
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    fig.patch.set_alpha(0)
    edge = "#0f172a"
    cable = "#64748b"

    if key in ("t-bar", "j-bar"):
        _draw_tbar_icon(ax, edge=edge, cable=cable, j_bar=(key == "j-bar"))
    elif key == "rope tow":
        ax.plot([0.12, 0.88], [0.9, 0.9], color=cable, lw=2.2, solid_capstyle="round")
        ax.plot([0.2, 0.78], [0.22, 0.82], color="#92400e", lw=3.2, solid_capstyle="round")
        ax.add_patch(
            Circle(
                (0.78, 0.8),
                0.075,
                facecolor="none",
                edgecolor="#92400e",
                linewidth=2.8,
            )
        )
    elif key in ("platter", "drag lift"):
        # Poma / platter: disk towed between the legs.
        ax.plot([0.12, 0.88], [0.9, 0.9], color=cable, lw=2.2, solid_capstyle="round")
        ax.plot([0.5, 0.5], [0.52, 0.88], color=cable, lw=2.2, solid_capstyle="round")
        ax.add_patch(
            Ellipse(
                (0.5, 0.4),
                0.34,
                0.12,
                facecolor="#475569",
                edgecolor=edge,
                linewidth=2,
            )
        )
        ax.add_patch(Circle((0.5, 0.5), 0.05, facecolor=edge, edgecolor="none"))
    else:
        import matplotlib.pyplot as plt

        plt.close(fig)
        return None

    canvas.draw()
    w, h = canvas.get_width_height()
    buf = np.asarray(canvas.buffer_rgba(), dtype=float).reshape((h, w, 4)) / 255.0
    import matplotlib.pyplot as plt

    plt.close(fig)
    return buf


def _lift_icon_rgba(kind: str) -> Any | None:
    """Raster icon for chart row: drawn surface lifts or map PNG."""
    key = kind.strip().lower()
    if key in _DRAWN_LIFT_KINDS:
        return _draw_lift_icon_rgba(kind)
    path = _lift_icon_path(kind)
    if path is None:
        return None
    return _load_icon_rgba(path, size_px=96)


def lift_type_counts(df: pd.DataFrame) -> dict[str, int]:
    if "lift_types" not in df.columns:
        return {}
    counts: dict[str, int] = {}
    pat = re.compile(r"([^:,]+):\s*(\d+)")
    for raw in df["lift_types"].dropna().astype(str):
        for m in pat.finditer(raw):
            kind = m.group(1).strip().lower()
            counts[kind] = counts.get(kind, 0) + int(m.group(2))
    return counts


def trail_difficulty_totals(df: pd.DataFrame) -> dict[str, int]:
    mapping = (
        ("trails_novice", "Novice"),
        ("trails_easy", "Easy"),
        ("trails_intermediate", "Intermediate"),
        ("trails_advanced", "Advanced"),
        ("trails_expert", "Expert"),
        ("trails_freeride", "Freeride"),
        ("trails_extreme", "Extreme"),
        ("trails_snow_park", "Terrain parks"),
        ("trails_snowpark", "Terrain parks"),
        ("terrain_park_trails", "Terrain parks"),
    )
    totals: dict[str, int] = {}
    for col, label in mapping:
        if col not in df.columns:
            continue
        if label in totals:
            continue
        s = pd.to_numeric(df[col], errors="coerce").fillna(0)
        v = int(s.sum())
        if v > 0:
            totals[label] = v
    return totals


def _scatter_tier_index(trails: float, acres: float) -> int:
    """Tier index 0–3 from the same rules as resort_size_category (trails + acres)."""
    cat = resort_size_category(
        {
            "downhill_trails": trails,
            "total_lifts": 1,
            "skiable_terrain_acres": acres,
        }
    )
    return {
        "small_hill": 0,
        "ski_mountain": 1,
        "multiple_mountains": 2,
        "mega_resort": 3,
    }.get(cat, 0)


def _draw_scatter_tier_legend_fig(
    fig,
    handles: list[Any],
) -> None:
    """Tier labels below plot; column width follows label length (no overlap)."""
    from matplotlib.patches import Rectangle

    leg_ax = fig.add_axes([0.06, 0.02, 0.88, 0.10])
    leg_ax.set_axis_off()
    n = len(handles)
    if not n:
        return
    labels = [str(h.get_label()) for h in handles]
    weights = [max(len(lab), 10) for lab in labels]
    gap = 0.010 if n > 1 else 0.0
    usable = 1.0 - gap * (n - 1)
    swatch_w = 0.028
    x = 0.0
    for handle, lab, wt in zip(handles, labels, weights):
        col_w = (wt / sum(weights)) * usable
        leg_ax.add_patch(
            Rectangle(
                (x, 0.62),
                swatch_w,
                0.22,
                transform=leg_ax.transAxes,
                facecolor=handle.get_facecolor(),
                edgecolor=handle.get_edgecolor(),
                linewidth=handle.get_linewidth() or 0.4,
                clip_on=False,
            )
        )
        leg_ax.text(
            x + swatch_w + 0.006,
            0.72,
            lab,
            transform=leg_ax.transAxes,
            ha="left",
            va="center",
            fontsize=6.2,
            color=C_SLATE_DARK,
            clip_on=True,
        )
        x += col_w + gap


def _draw_trails_acres_tier_background(
    ax,
    *,
    xmax: float,
    ymax: float,
    grid: int = 220,
) -> None:
    """Color the plot by size tier (small / ski mountain / multiple / mega)."""
    import numpy as np
    from matplotlib.colors import BoundaryNorm, ListedColormap

    ts = np.linspace(0, xmax, grid)
    ys = np.linspace(0, ymax, grid)
    t_grid, a_grid = np.meshgrid(ts, ys)
    z = np.zeros(t_grid.shape, dtype=int)
    for iy in range(t_grid.shape[0]):
        for ix in range(t_grid.shape[1]):
            z[iy, ix] = _scatter_tier_index(float(t_grid[iy, ix]), float(a_grid[iy, ix]))

    cmap = ListedColormap(
        [_TIER_SCATTER_BG[cat] for cat in _SCATTER_TIER_ORDER]
    )
    norm = BoundaryNorm([-0.5, 0.5, 1.5, 2.5, 3.5], cmap.N)
    ax.pcolormesh(
        t_grid,
        a_grid,
        z,
        cmap=cmap,
        norm=norm,
        shading="auto",
        zorder=0,
    )


def _scatter_tier_legend_label(cat: str, count: int) -> str:
    name = RESORT_CATEGORY_LABEL.get(cat, cat)
    return f"({count}) {name}"


def _tier_counts_from_df(df: pd.DataFrame) -> dict[str, int]:
    counts: dict[str, int] = {}
    for _, row in df.iterrows():
        cat = resort_size_category(row.to_dict())
        if cat == "unknown":
            continue
        counts[cat] = counts.get(cat, 0) + 1
    return counts


def render_trails_vs_acres_scatter(
    df: pd.DataFrame,
    path: Path,
    *,
    region_title: str = "",
    tier_counts: dict[str, int] | None = None,
) -> Path | None:
    _mpl_setup()
    import matplotlib.pyplot as plt

    name_col = _name_col(df)
    trails = _num_series(df, "downhill_trails", "downhillTrails")
    acres = _num_series(
        df,
        "skiable_terrain_acres",
        "skiableTerrainAcres",
        "total_area_acres",
    )
    mask = trails.notna() & (trails > 0) & acres.notna() & (acres > 0)
    if not mask.any():
        return None

    names = df.loc[mask, name_col].astype(str)
    t = trails.loc[mask]
    a = acres.loc[mask]

    fig, ax = plt.subplots(figsize=(4.8, 3.2), facecolor="white")
    _style_axes(ax)

    max_t = float(t.max()) or 1.0
    max_a = float(a.max()) or 1.0
    x_hi = max_t * 1.06
    y_hi = max_a * 1.06
    ax.set_xlim(0, x_hi)
    ax.set_ylim(0, y_hi)
    _draw_trails_acres_tier_background(ax, xmax=x_hi, ymax=y_hi)

    top_trail_names = set(names.loc[t.sort_values(ascending=False).head(10).index])
    top_area_names = set(names.loc[a.sort_values(ascending=False).head(10).index])
    outlier_names = top_trail_names | top_area_names

    for idx in t.index:
        nm = names.loc[idx]
        is_out = nm in outlier_names
        ax.scatter(
            t.loc[idx],
            a.loc[idx],
            s=36 if is_out else 18,
            c=C_TEAL if is_out else C_SLATE,
            alpha=0.92 if is_out else 0.45,
            edgecolors="white" if is_out else "none",
            linewidths=0.5,
            zorder=3 if is_out else 2,
        )

    ax.set_xlabel("Number of trails", fontsize=7, color=C_LABEL, labelpad=3)
    ax.set_ylabel("Skiable acres", fontsize=7, color=C_LABEL, labelpad=2)
    title = "Trails vs acreage"
    if region_title:
        title = f"{region_title}: trails vs acreage"
    ax.set_title(title, fontsize=10, fontweight="600", color=C_SLATE_DARK, pad=8)

    from matplotlib.patches import Patch

    counts = tier_counts if tier_counts is not None else _tier_counts_from_df(df)
    tier_handles = [
        Patch(
            facecolor=_TIER_SCATTER_BG[cat],
            edgecolor="#cbd5e1",
            linewidth=0.4,
            label=_scatter_tier_legend_label(cat, counts.get(cat, 0)),
        )
        for cat in _SCATTER_TIER_ORDER
    ]

    outliers = [
        (names.loc[i], t.loc[i], a.loc[i])
        for i in t.index
        if names.loc[i] in outlier_names
    ]
    outliers.sort(key=lambda x: (x[1] / max_t) + (x[2] / max_a), reverse=True)
    # Greedy label placement using rendered pixel bboxes to avoid overlaps.
    fig.canvas.draw()  # ensure transforms/renderer available
    renderer = fig.canvas.get_renderer()
    placed_px: list[tuple[float, float, float, float]] = []

    def overlaps(b: tuple[float, float, float, float]) -> bool:
        # Allow a tiny pad to reduce near-touches.
        pad = 2.0
        bl, bb, br, bt = b[0] - pad, b[1] - pad, b[2] + pad, b[3] + pad
        for r in placed_px:
            if not (br < r[0] or bl > r[2] or bt < r[1] or bb > r[3]):
                return True
        return False

    # Avoid labels covering any plotted points (in pixel space).
    pts_px = [ax.transData.transform((float(tx), float(ay))) for tx, ay in zip(t.values, a.values)]
    point_pad_px = 2.0

    def covers_any_point(
        b: tuple[float, float, float, float],
        *,
        exclude_px: tuple[float, float] | None = None,
    ) -> bool:
        l, bb, r, tt = b
        for px, py in pts_px:
            if (
                exclude_px is not None
                and abs(px - exclude_px[0]) <= 0.5
                and abs(py - exclude_px[1]) <= 0.5
            ):
                continue
            if (l - point_pad_px) <= px <= (r + point_pad_px) and (bb - point_pad_px) <= py <= (tt + point_pad_px):
                return True
        return False

    ax_bbox = ax.get_window_extent(renderer=renderer)
    fig_bbox = fig.get_window_extent(renderer=renderer)
    # Try more offsets; allow labels to spill outside the axes (like the website),
    # but keep them inside the overall figure canvas.
    candidates: list[tuple[int, int, str]] = [
        (6, 4, "left"),    # right of point
        (10, 6, "left"),
        (14, 2, "left"),
        (6, -6, "left"),   # right/below
        (12, -10, "left"),
        (-6, 4, "right"),  # left of point
        (-10, 6, "right"),
        (-14, 2, "right"),
        (-6, -6, "right"),
        (-12, -10, "right"),
        (0, 10, "center"),
        (0, -12, "center"),
    ]

    for nm, tx, ay in outliers[:12]:
        label = _short_name(nm, 22)
        placed = False
        for dx, dy, ha in candidates:
            # Leader line from point -> label.
            txt = ax.annotate(
                label,
                (tx, ay),
                xytext=(dx, dy),
                textcoords="offset points",
                fontsize=7,
                fontweight="600",
                color=C_SLATE_DARK,
                ha=ha,
                va="bottom" if dy >= 0 else "top",
                zorder=5,
                clip_on=False,
                arrowprops={
                    "arrowstyle": "-",
                    "color": C_SLATE,
                    "lw": 0.8,
                    "shrinkA": 2,
                    "shrinkB": 4,
                    "alpha": 0.85,
                },
            )
            fig.canvas.draw()
            # Annotation window extent can include the arrow; we want to test collisions
            # based on the text box only (otherwise every label \"covers\" its own dot).
            arrow = getattr(txt, "arrow_patch", None)
            if arrow is not None:
                arrow.set_visible(False)
                fig.canvas.draw()
            bb = txt.get_window_extent(renderer=renderer)
            if arrow is not None:
                arrow.set_visible(True)
                fig.canvas.draw()
            b = (bb.x0, bb.y0, bb.x1, bb.y1)
            # Keep labels on-canvas (figure bounds) and non-overlapping.
            # Prefer to keep them inside the plot when possible.
            on_canvas = (
                b[0] >= fig_bbox.x0
                and b[2] <= fig_bbox.x1
                and b[1] >= fig_bbox.y0
                and b[3] <= fig_bbox.y1
            )
            in_axes = (
                b[0] >= ax_bbox.x0
                and b[2] <= ax_bbox.x1
                and b[1] >= ax_bbox.y0
                and b[3] <= ax_bbox.y1
            )
            if on_canvas and not overlaps(b) and not covers_any_point(b, exclude_px=ax.transData.transform((float(tx), float(ay)))):
                placed_px.append(b)
                placed = True
                break
            txt.remove()
        if not placed:
            # If nothing fits cleanly, skip the label rather than clutter.
            continue

    # X-axis label on the axes; tier legend drawn below.
    fig.subplots_adjust(left=0.11, right=0.98, bottom=0.24, top=0.90)
    _draw_scatter_tier_legend_fig(fig, tier_handles)
    return _save_fig(fig, path)


def _draw_tier_silhouette(
    ax,
    tier: str,
    cx: float,
    base: float,
    *,
    width: float,
    height: float,
    color: str,
) -> float:
    """
    Draw a tier-sized mountain silhouette at cx. Returns y of the highest peak (for count label).
    """
    from matplotlib.patches import Polygon

    w, h = width, height
    snow = "#f8fafc"
    edge = "#0f172a"
    lw = 1.1

    def _add(verts: list[tuple[float, float]], *, z: int = 2) -> None:
        ax.add_patch(
            Polygon(
                verts,
                closed=True,
                facecolor=color,
                edgecolor=edge,
                linewidth=lw,
                joinstyle="round",
                zorder=z,
            )
        )

    def _snow_cap(peak_x: float, peak_y: float, half_w: float) -> None:
        cap_h = max(h * 0.22, 0.04)
        cap_w = half_w * 0.55
        ax.add_patch(
            Polygon(
                [
                    (peak_x - cap_w, peak_y - cap_h * 0.35),
                    (peak_x, peak_y + cap_h * 0.15),
                    (peak_x + cap_w, peak_y - cap_h * 0.35),
                ],
                closed=True,
                facecolor=snow,
                edgecolor=edge,
                linewidth=0.6,
                zorder=4,
            )
        )

    peak_y = base + h

    if tier == "small_hill":
        # Single rounded bump (half-ellipse).
        import numpy as np

        t = np.linspace(0, np.pi, 36)
        rx, ry = w, h * 0.9
        xs = cx + rx * np.cos(t)
        ys = base + ry * np.sin(t)
        verts = [(float(xs[0]), base)] + [
            (float(x), float(y)) for x, y in zip(xs, ys)
        ] + [(float(xs[-1]), base)]
        _add(verts)
        peak_y = base + ry
        _snow_cap(cx, peak_y, w * 0.24)

    elif tier == "ski_mountain":
        verts = [
            (cx - w, base),
            (cx - w * 0.12, base + h * 0.42),
            (cx, base + h),
            (cx + w * 0.15, base + h * 0.38),
            (cx + w, base),
        ]
        _add(verts)
        _snow_cap(cx, peak_y, w * 0.22)

    elif tier == "multiple_mountains":
        peaks = [
            (cx - w * 0.52, base + h * 0.52, w * 0.18),
            (cx - w * 0.08, base + h * 0.88, w * 0.2),
            (cx + w * 0.48, base + h * 0.62, w * 0.17),
        ]
        ridge = [
            (cx - w, base),
            (cx - w * 0.68, base + h * 0.18),
            (cx - w * 0.52, base + h * 0.52),
            (cx - w * 0.28, base + h * 0.38),
            (cx - w * 0.08, base + h * 0.88),
            (cx + w * 0.18, base + h * 0.48),
            (cx + w * 0.48, base + h * 0.62),
            (cx + w * 0.72, base + h * 0.28),
            (cx + w, base),
        ]
        _add(ridge)
        peak_y = base + h * 0.88
        for px, py, hw in peaks:
            _snow_cap(px, py, hw)

    else:  # mega_resort
        ridge = [
            (cx - w, base),
            (cx - w * 0.78, base + h * 0.22),
            (cx - w * 0.58, base + h * 0.58),
            (cx - w * 0.38, base + h * 0.42),
            (cx - w * 0.18, base + h * 0.78),
            (cx, base + h),
            (cx + w * 0.12, base + h * 0.72),
            (cx + w * 0.32, base + h * 0.85),
            (cx + w * 0.52, base + h * 0.55),
            (cx + w * 0.72, base + h * 0.68),
            (cx + w * 0.88, base + h * 0.35),
            (cx + w, base),
        ]
        _add(ridge)
        peak_y = base + h
        for px, py, hw in [
            (cx - w * 0.58, base + h * 0.58, w * 0.14),
            (cx - w * 0.18, base + h * 0.78, w * 0.13),
            (cx, base + h, w * 0.16),
            (cx + w * 0.32, base + h * 0.85, w * 0.14),
            (cx + w * 0.72, base + h * 0.68, w * 0.12),
        ]:
            _snow_cap(px, py, hw)

    # Tiny base trees for scale (small_hill gets more trees).
    tree_n = 3 if tier == "small_hill" else 2 if tier == "ski_mountain" else 1
    tree_color = "#166534"
    for j in range(tree_n):
        tx = cx - w * 0.65 + j * (w * 1.3 / max(tree_n - 1, 1))
        tri_h = h * (0.14 if tier != "mega_resort" else 0.1)
        tri_w = w * 0.07
        ax.add_patch(
            Polygon(
                [
                    (tx, base),
                    (tx - tri_w, base + tri_h),
                    (tx + tri_w, base + tri_h),
                ],
                closed=True,
                facecolor=tree_color,
                edgecolor="none",
                zorder=5,
            )
        )

    return peak_y


_TIER_VISUAL = {
    "small_hill": {"width": 0.26, "height": 0.14},
    "ski_mountain": {"width": 0.30, "height": 0.36},
    "multiple_mountains": {"width": 0.44, "height": 0.44},
    "mega_resort": {"width": 0.48, "height": 0.50},
}


def render_tier_bar_chart(
    tier_counts: dict[str, int],
    path: Path,
    *,
    region_title: str = "",
) -> Path | None:
    """Illustrated resort-size graphic (mountain silhouettes + counts)."""
    if not tier_counts:
        return None
    _mpl_setup()
    import matplotlib.pyplot as plt
    from matplotlib.patches import FancyBboxPatch

    order = ("small_hill", "ski_mountain", "multiple_mountains", "mega_resort")
    items: list[tuple[str, int, str, str]] = []
    for cat in order:
        n = tier_counts.get(cat, 0)
        if n:
            items.append(
                (cat, n, RESORT_CATEGORY_LABEL.get(cat, cat), TIER_COLORS.get(cat, C_TEAL))
            )

    if not items:
        return None

    fig, ax = plt.subplots(figsize=(3.4, 3.2), facecolor=C_BG)
    ax.set_facecolor(C_BG)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    title = "Resorts by size"
    if region_title:
        title = f"{region_title}: by size"
    ax.text(
        0.5,
        0.96,
        title,
        ha="center",
        va="top",
        fontsize=10,
        fontweight="600",
        color=C_SLATE_DARK,
    )

    n_cols = len(items)
    col_w = 1.0 / n_cols
    ground_y = 0.24

    # Ground strip across the graphic.
    ax.add_patch(
        FancyBboxPatch(
            (0.04, ground_y - 0.018),
            0.92,
            0.028,
            boxstyle="round,pad=0.002",
            facecolor="#cbd5e1",
            edgecolor="none",
            zorder=0,
        )
    )
    ax.plot([0.04, 0.96], [ground_y, ground_y], color="#64748b", lw=1.0, zorder=1)

    for i, (cat, count, label, color) in enumerate(items):
        cx = (i + 0.5) * col_w
        spec = _TIER_VISUAL.get(cat, _TIER_VISUAL["small_hill"])
        peak_y = _draw_tier_silhouette(
            ax,
            cat,
            cx,
            ground_y,
            width=spec["width"] * col_w * 0.92,
            height=spec["height"],
            color=color,
        )

        # Bold count badge floating above the peak.
        badge_y = min(peak_y + 0.06, 0.88)
        ax.text(
            cx,
            badge_y,
            str(count),
            ha="center",
            va="center",
            fontsize=16 if count < 10 else 14,
            fontweight="800",
            color=C_SLATE_DARK,
            zorder=6,
            bbox={
                "boxstyle": "round,pad=0.35",
                "facecolor": "white",
                "edgecolor": color,
                "linewidth": 2.0,
                "alpha": 0.96,
            },
        )

        # Wrap long labels on two lines if needed.
        label_lines = label.replace(" mountains", "\nmountains") if " " in label else label
        label_bottom = 0.13
        n_label_lines = label_lines.count("\n") + 1
        label_line_h = 0.030 if n_cols >= 4 else 0.034
        ax.text(
            cx,
            label_bottom,
            label_lines,
            ha="center",
            va="bottom",
            fontsize=7,
            fontweight="600",
            color=C_LABEL,
            linespacing=1.1,
            zorder=6,
        )
        criteria = RESORT_CATEGORY_CRITERIA.get(cat, "")
        if criteria:
            crit_top = label_bottom - n_label_lines * label_line_h - 0.014
            ax.text(
                cx,
                crit_top,
                criteria,
                ha="center",
                va="top",
                fontsize=4.5 if n_cols >= 4 else 4.9,
                color=C_SLATE,
                linespacing=1.08,
                zorder=6,
            )

    fig.tight_layout(pad=0.4)
    return _save_fig(fig, path)


# North American on-trail symbols (green circle, blue square, black diamond).
C_TRAIL_GREEN = "#22c55e"
C_TRAIL_GREEN_EDGE = "#15803d"
C_TRAIL_BLUE = "#2563eb"
C_TRAIL_BLUE_EDGE = "#1e40af"
C_TRAIL_BLACK = "#1a1a1a"
C_TRAIL_BLACK_EDGE = "#0f172a"
C_TRAIL_PARK = "#ea580c"
C_TRAIL_PARK_EDGE = "#c2410c"


def _na_trail_groups(totals: dict[str, int]) -> list[tuple[str, str, int, str]]:
    """NA symbols: green circle, blue square, black diamond, double black diamond."""
    groups: list[tuple[str, str, int, str]] = []

    green = sum(totals.get(k, 0) for k in ("Novice", "Easy"))
    if green:
        groups.append(("circle", "Green circle", green, C_TRAIL_GREEN))

    inter = totals.get("Intermediate", 0)
    if inter:
        groups.append(("square", "Blue square", inter, C_TRAIL_BLUE))

    advanced = totals.get("Advanced", 0)
    if advanced:
        groups.append(("diamond", "Black diamond", advanced, C_TRAIL_BLACK))

    double = sum(
        totals.get(k, 0) for k in ("Expert", "Freeride", "Extreme")
    )
    if double:
        groups.append(
            ("double_diamond", "Double black\ndiamond", double, C_TRAIL_BLACK)
        )

    return groups


def _na_trail_subtitle(symbol: str, totals: dict[str, int]) -> str:
    if symbol == "circle":
        parts = [k for k in ("Novice", "Easy") if totals.get(k, 0)]
        return " · ".join(parts) if parts else ""
    if symbol == "square":
        return "Intermediate"
    if symbol == "diamond":
        return "Advanced"
    if symbol == "double_diamond":
        parts = [k for k in ("Expert", "Freeride", "Extreme") if totals.get(k, 0)]
        if len(parts) >= 3:
            return f"{parts[0]} · {parts[1]}\n{parts[2]}"
        return " · ".join(parts) if parts else ""
    return ""


def _draw_single_diamond(
    ax, cx: float, cy: float, size: float, *, edge: str, lw: float, z: int
) -> None:
    from matplotlib.patches import Polygon

    s = size * 1.1
    ax.add_patch(
        Polygon(
            [(cx, cy + s), (cx + s, cy), (cx, cy - s), (cx - s, cy)],
            closed=True,
            facecolor=C_TRAIL_BLACK,
            edgecolor=edge,
            linewidth=lw,
            joinstyle="round",
            zorder=z,
        )
    )


def _draw_trail_symbol(ax, kind: str, cx: float, cy: float, size: float, color: str) -> None:
    """Draw NA trail rating shape centered at (cx, cy)."""
    from matplotlib.patches import Circle, Rectangle

    edge = {
        "circle": C_TRAIL_GREEN_EDGE,
        "square": C_TRAIL_BLUE_EDGE,
        "diamond": C_TRAIL_BLACK_EDGE,
        "double_diamond": C_TRAIL_BLACK_EDGE,
    }.get(kind, C_SLATE_DARK)
    lw = 1.8

    if kind == "circle":
        ax.add_patch(
            Circle(
                (cx, cy),
                size,
                facecolor=color,
                edgecolor=edge,
                linewidth=lw,
                zorder=3,
            )
        )
    elif kind == "square":
        ax.add_patch(
            Rectangle(
                (cx - size, cy - size),
                size * 2,
                size * 2,
                facecolor=color,
                edgecolor=edge,
                linewidth=lw,
                zorder=3,
            )
        )
    elif kind == "double_diamond":
        # Stacked pair (standard NA double-black on trail maps).
        gap = size * 0.08
        half = size * 0.52
        _draw_single_diamond(
            ax, cx, cy + half + gap, half, edge=edge, lw=lw, z=3
        )
        _draw_single_diamond(
            ax, cx, cy - half - gap, half, edge=edge, lw=lw, z=4
        )
    else:  # single diamond
        _draw_single_diamond(ax, cx, cy, size * 1.15, edge=edge, lw=lw, z=3)


def _draw_terrain_park_symbol(ax, cx: float, cy: float, size: float) -> None:
    """Orange park marker (rounded box + rail line)."""
    from matplotlib.patches import FancyBboxPatch

    ax.add_patch(
        FancyBboxPatch(
            (cx - size * 1.1, cy - size * 0.65),
            size * 2.2,
            size * 1.3,
            boxstyle="round,pad=0.02",
            facecolor=C_TRAIL_PARK,
            edgecolor=C_TRAIL_PARK_EDGE,
            linewidth=1.6,
            zorder=3,
        )
    )
    ax.plot(
        [cx - size * 0.7, cx + size * 0.7],
        [cy, cy],
        color="white",
        lw=2.2,
        solid_capstyle="round",
        zorder=4,
    )


def render_trail_difficulty_chart(
    totals: dict[str, int],
    path: Path,
    *,
    region_title: str = "",
) -> Path | None:
    """North American trail-rating infographic (circle, square, diamond, double diamond)."""
    if not totals:
        return None
    groups = _na_trail_groups(totals)
    park_count = totals.get("Terrain parks", 0)
    if not groups and not park_count:
        return None

    _mpl_setup()
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(3.4, 3.2), facecolor=C_BG)
    ax.set_facecolor(C_BG)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    title = "Trail difficulty & terrain parks"
    if region_title:
        title = f"{region_title}: trail difficulty & terrain parks"
    ax.text(
        0.5,
        0.96,
        title,
        ha="center",
        va="top",
        fontsize=9.5,
        fontweight="600",
        color=C_SLATE_DARK,
    )

    n_slots = len(groups) + (1 if park_count else 0)
    sym_y = 0.52
    if n_slots >= 5:
        sym_size = 0.038
        label_fs = 5.4
        count_fs = 9
    elif n_slots >= 4:
        sym_size = 0.048
        label_fs = 5.8
        count_fs = 10
    else:
        sym_size = 0.062
        label_fs = 6.2
        count_fs = 11

    for i, (kind, rating_label, count, color) in enumerate(groups):
        cx = (i + 0.5) / n_slots
        _draw_trail_symbol(ax, kind, cx, sym_y, sym_size, color)

        sym_pad = sym_size * (2.4 if kind == "double_diamond" else 1.35)
        ax.text(
            cx,
            sym_y + sym_pad + 0.06,
            f"{count:,}",
            ha="center",
            va="bottom",
            fontsize=count_fs,
            fontweight="800",
            color=C_SLATE_DARK,
            zorder=5,
        )
        label_y = sym_y - sym_pad - 0.05
        n_label_lines = rating_label.count("\n") + 1
        line_h = 0.034 if n_slots >= 5 else 0.038
        ax.text(
            cx,
            label_y,
            rating_label,
            ha="center",
            va="top",
            fontsize=label_fs,
            fontweight="700",
            color=C_SLATE_DARK,
            linespacing=0.95,
            zorder=5,
        )
        sub = _na_trail_subtitle(kind, totals)
        if sub:
            sub_y = label_y - n_label_lines * line_h - 0.020
            ax.text(
                cx,
                sub_y,
                sub,
                ha="center",
                va="top",
                fontsize=4.6 if n_slots >= 5 else 5.0,
                color=C_SLATE,
                linespacing=1.05,
                zorder=5,
            )

    if park_count:
        cx = (len(groups) + 0.5) / n_slots
        _draw_terrain_park_symbol(ax, cx, sym_y, sym_size * 0.95)
        ax.text(
            cx,
            sym_y + sym_size * 1.35 + 0.06,
            f"{park_count:,}",
            ha="center",
            va="bottom",
            fontsize=count_fs,
            fontweight="800",
            color=C_SLATE_DARK,
        )
        ax.text(
            cx,
            sym_y - sym_size * 1.2 - 0.04,
            "Terrain parks",
            ha="center",
            va="top",
            fontsize=label_fs,
            fontweight="700",
            color=C_TRAIL_PARK,
        )

    fig.tight_layout(pad=0.35)
    return _save_fig(fig, path)


def render_lift_types_chart(
    counts: dict[str, int],
    path: Path,
    *,
    region_title: str = "",
    top_n: int = 6,
) -> Path | None:
    """Lift counts with map icons (atlas/map_gen/icons, same as QGIS resort maps)."""
    if not counts:
        return None
    _mpl_setup()
    import matplotlib.pyplot as plt

    items = sorted(counts.items(), key=lambda x: (-x[1], x[0]))[:top_n]
    if not items:
        return None

    n = len(items)
    max_v = max(v for _, v in items) or 1

    fig, ax = plt.subplots(figsize=(3.4, 3.2), facecolor=C_BG)
    ax.set_facecolor(C_BG)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    title = "Lift types"
    if region_title:
        title = f"{region_title}: lift types"
    ax.text(
        0.5,
        0.97,
        title,
        ha="center",
        va="top",
        fontsize=9.5,
        fontweight="600",
        color=C_SLATE_DARK,
        transform=ax.transAxes,
    )

    icon_x = 0.10
    bar_left = 0.22
    bar_right = 0.76
    bar_span = bar_right - bar_left
    row_h = 0.10
    y_top, y_bot = 0.84, 0.14
    y_step = (y_top - y_bot) / max(n - 1, 1)

    from matplotlib.patches import FancyBboxPatch

    for i, (kind, val) in enumerate(items):
        y = y_top - i * y_step if n > 1 else (y_top + y_bot) / 2
        color = LIFT_TYPE_COLORS.get(kind, C_TEAL)
        frac = val / max_v

        def _bar(w: float, *, face: str, alpha: float, z: int) -> None:
            ax.add_patch(
                FancyBboxPatch(
                    (bar_left, y - row_h / 2),
                    w,
                    row_h,
                    boxstyle="round,pad=0.004",
                    facecolor=face,
                    edgecolor="white" if alpha > 0.5 else "none",
                    linewidth=0.6,
                    alpha=alpha,
                    transform=ax.transAxes,
                    zorder=z,
                )
            )

        _bar(bar_span, face=C_GRID, alpha=0.4, z=1)
        _bar(bar_span * frac, face=color, alpha=0.9, z=2)

        rgba = _lift_icon_rgba(kind)
        if rgba is not None:
            half = 0.042 if n <= 5 else 0.036
            ax.imshow(
                rgba,
                extent=(icon_x - half, icon_x + half, y - half, y + half),
                transform=ax.transAxes,
                aspect="equal",
                interpolation="bilinear",
                zorder=4,
            )

        ax.text(
            bar_left + 0.015,
            y,
            _lift_display_name(kind),
            transform=ax.transAxes,
            ha="left",
            va="center",
            fontsize=6.0 if n > 5 else 6.6,
            fontweight="600",
            color="white" if frac > 0.42 else C_LABEL,
            zorder=5,
        )
        ax.text(
            bar_right + 0.02,
            y,
            f"{val:,}",
            transform=ax.transAxes,
            ha="left",
            va="center",
            fontsize=9 if n > 5 else 10,
            fontweight="800",
            color=C_SLATE_DARK,
            zorder=5,
        )

    fig.tight_layout(pad=0.35)
    return _save_fig(fig, path)


def render_elevation_range_chart(
    df: pd.DataFrame,
    path: Path,
    *,
    region_title: str = "",
    top_n: int | None = None,
    use_feet: bool = True,
) -> Path | None:
    """
    Reference-style outlined peaks per resort (base → summit); acreage sets base width.
    Requires elevation_low_m / elevation_high_m from regional elevation outputs.
    """
    _mpl_setup()
    import matplotlib.pyplot as plt
    import numpy as np

    if "elevation_low_m" not in df.columns or "elevation_high_m" not in df.columns:
        return None
    lo = pd.to_numeric(df["elevation_low_m"], errors="coerce")
    hi = pd.to_numeric(df["elevation_high_m"], errors="coerce")
    mask = lo.notna() & hi.notna() & (hi >= lo)
    if not mask.any():
        return None

    scale = M_TO_FT if use_feet else 1.0
    y_unit = "ft" if use_feet else "m"

    name_col = _name_col(df)
    d = df.loc[mask].copy()
    d["_lo"] = lo.loc[mask] * scale
    d["_hi"] = hi.loc[mask] * scale
    d = d.sort_values(
        name_col,
        key=lambda s: s.astype(str).str.strip().str.casefold(),
    ).reset_index(drop=True)
    if top_n is not None:
        d = d.head(int(top_n))
    if d.empty:
        return None

    n = len(d)
    bases = d["_lo"].astype(float).values
    tops = d["_hi"].astype(float).values
    acres_s = _skiable_acres_series(d)
    pos_acres = acres_s[acres_s > 0]
    acres_min = float(pos_acres.min()) if len(pos_acres) else 0.0
    acres_max = float(pos_acres.max()) if len(pos_acres) else 0.0
    x = np.arange(n)
    x_chart_span = max(float(n), 1.0) - 0.3
    half_widths = []
    for i in range(n):
        a = float(acres_s.iloc[i]) if pd.notna(acres_s.iloc[i]) else 0.0
        hw = _mountain_half_width_from_acres(
            a,
            acres_min=acres_min,
            acres_max=acres_max,
            x_chart_span=x_chart_span,
        )
        vert = float(tops[i]) - float(bases[i])
        half_widths.append(_steep_half_width(hw, vert))
    fig_w, fig_h = _elevation_figure_inches()
    leg_ncol, leg_nrow, leg_frac = _elevation_legend_grid(n)
    fig = plt.figure(figsize=(fig_w, fig_h), facecolor=C_ELEV_REF_BG)
    gs = fig.add_gridspec(
        2,
        1,
        height_ratios=[1.0 - leg_frac, leg_frac],
        hspace=0.05,
    )
    ax = fig.add_subplot(gs[0])
    leg_ax = fig.add_subplot(gs[1])
    ax.set_facecolor(C_ELEV_REF_BG)
    leg_ax.set_facecolor(C_ELEV_REF_BG)

    sorted_bases = np.sort(bases.astype(float))
    min_base = float(sorted_bases[0])
    max_top = float(tops.max())
    gap_pad = 120.0 if y_unit == "ft" else 40.0
    top_pad = max(80.0 if y_unit == "ft" else 25.0, (max_top - min_base) * 0.05)

    # Y limits on a 100 ft (or 50 m) grid; tick step is chosen separately for readability.
    if len(sorted_bases) >= 2 and sorted_bases[1] - sorted_bases[0] > gap_pad * 3:
        y_plot_floor = _elevation_axis_round(
            float(sorted_bases[0]) - gap_pad,
            use_feet=use_feet,
            up=False,
        )
    else:
        y_plot_floor = _elevation_axis_round(
            min_base - gap_pad,
            use_feet=use_feet,
            up=False,
        )
    y_plot_floor = max(0.0, y_plot_floor)
    y_max = _elevation_axis_round(max_top + top_pad, use_feet=use_feet, up=True)
    step = _elevation_tick_step(y_max - y_plot_floor, use_feet=use_feet)

    ax.set_ylim(y_plot_floor, y_max)
    ax.set_yticks(np.arange(y_plot_floor, y_max + step * 0.5, step))
    ax.set_ylabel(f"Elevation ({y_unit})", fontsize=9, color=C_LABEL)
    ax.set_xticks([])
    ax.yaxis.grid(True, color="#b8d4e6", linewidth=0.8, alpha=0.9, zorder=0)
    ax.xaxis.grid(False)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(C_GRID)
    ax.spines["bottom"].set_color(C_GRID)
    ax.tick_params(colors=C_LABEL, labelsize=8)

    if region_title:
        title = f"{region_title} Ski Area Elevation/Vertical"
    else:
        title = "Ski Area Elevation/Vertical"
    ax.set_title(title, fontsize=11, fontweight="600", color=C_SLATE_DARK, pad=10)
    ax.set_xlim(-0.35, n - 0.65)
    ax.margins(x=0)

    mountains: list[dict[str, Any]] = []
    for i in range(n):
        lo_y, hi_y = float(bases[i]), float(tops[i])
        hw = half_widths[i]
        cx = float(x[i])
        row = d.iloc[i]
        nm = str(row[name_col]).strip()
        tier = resort_size_category(row.to_dict())
        tip_x, tip_y = _draw_reference_peak(ax, cx, lo_y, hi_y, hw)
        mountains.append(
            {
                "name": nm,
                "code": _elevation_letter_code(i),
                "tier": tier,
                "cx": cx,
                "base_y": lo_y,
                "summit_y": hi_y,
                "half_w": hw,
                "tip_x": tip_x,
                "tip_y": tip_y,
            }
        )

    legend_entries = [
        (str(m["code"]), _legend_short_name(str(m["name"]))) for m in mountains
    ]
    _label_elevation_lettered(ax, mountains, y_unit=y_unit)
    _draw_elevation_legend_panel(
        leg_ax,
        legend_entries,
        fig_width_in=fig_w,
        fig_height_in=fig_h,
        leg_frac=leg_frac,
        ncol=leg_ncol,
        nrow=leg_nrow,
    )

    fig.subplots_adjust(left=0.08, right=0.98, top=0.92, bottom=0.02)
    return _save_fig(fig, path, dpi=300)


def generate_regional_facts_charts(
    df: pd.DataFrame,
    out_dir: Path,
    *,
    region_title: str = "",
    tier_counts: dict[str, int] | None = None,
) -> dict[str, str]:
    """Write chart PNGs; return kind -> absolute path string."""
    out_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, str] = {}

    scatter = render_trails_vs_acres_scatter(
        df,
        out_dir / "chart_trails_vs_acres.png",
        region_title=region_title,
        tier_counts=tier_counts,
    )
    if scatter:
        paths["scatter"] = str(scatter.resolve()).replace("\\", "/")

    tier = render_tier_bar_chart(
        tier_counts or {},
        out_dir / "chart_resorts_by_size.png",
        region_title=region_title,
    )
    if tier:
        paths["tiers"] = str(tier.resolve()).replace("\\", "/")

    diff = render_trail_difficulty_chart(
        trail_difficulty_totals(df),
        out_dir / "chart_trail_difficulty.png",
        region_title=region_title,
    )
    if diff:
        paths["trail_difficulty"] = str(diff.resolve()).replace("\\", "/")

    lifts = render_lift_types_chart(
        lift_type_counts(df),
        out_dir / "chart_lift_types.png",
        region_title=region_title,
    )
    if lifts:
        paths["lift_types"] = str(lifts.resolve()).replace("\\", "/")

    elev = render_elevation_range_chart(
        df, out_dir / "chart_elevation_range.png", region_title=region_title
    )
    if elev:
        paths["elevation_range"] = str(elev.resolve()).replace("\\", "/")

    return paths
