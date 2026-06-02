"""Matplotlib charts for regional facts pages (mirrors GlobalSkiAtlas_2 Ski*Facts)."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd

from atlas.book_gen.constants import RESORT_CATEGORY_LABEL
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

TRAIL_DIFF_COLORS = {
    "Novice": "#22c55e",
    "Easy": "#4ade80",
    "Intermediate": "#2563eb",
    "Advanced": "#1a1a1a",
    "Expert": "#991b1b",
    "Freeride": "#7f1d1d",
    "Extreme": "#450a0a",
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


_ELEV_Y_FLOOR_FT = 4500.0  # axis base — leaves room below lowest resort for name key
_ELEV_Y_FLOOR_M = 1370.0
_ELEV_MARKER_FS = 7.0

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


def _draw_elevation_legend(
    ax,
    entries: list[tuple[str, str]],
    *,
    y_bottom: float,
    y_top: float,
    xlim: tuple[float, float],
) -> None:
    """Name key in the empty band below mountains (between y floor and lowest base)."""
    n = len(entries)
    if not n or y_top <= y_bottom:
        return
    ncol = 6 if n > 32 else 5 if n > 24 else 4 if n > 16 else 3
    nrow = (n + ncol - 1) // ncol
    fs = 5.6 if n > 32 else 6.0 if n > 22 else 6.4
    x0, x1 = xlim
    span = x1 - x0
    col_w = span / ncol
    y_step = (y_top - y_bottom) / max(nrow, 1)

    for c in range(ncol):
        x_left = x0 + c * col_w + col_w * 0.04
        for r in range(nrow):
            i = c * nrow + r
            if i >= n:
                continue
            code, name = entries[i]
            y = y_top - (r + 0.55) * y_step
            ax.text(
                x_left,
                y,
                f"{code}. {name}",
                fontsize=fs,
                color=C_ELEV_REF_TEXT,
                ha="left",
                va="top",
                zorder=5,
                clip_on=False,
            )


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
    )
    totals: dict[str, int] = {}
    for col, label in mapping:
        if col not in df.columns:
            continue
        s = pd.to_numeric(df[col], errors="coerce").fillna(0)
        v = int(s.sum())
        if v > 0:
            totals[label] = v
    return totals


def render_trails_vs_acres_scatter(
    df: pd.DataFrame,
    path: Path,
    *,
    region_title: str = "",
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

    top_trail_names = set(names.loc[t.sort_values(ascending=False).head(10).index])
    top_area_names = set(names.loc[a.sort_values(ascending=False).head(10).index])
    outlier_names = top_trail_names | top_area_names

    max_t = float(t.max()) or 1.0
    max_a = float(a.max()) or 1.0

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

    ax.set_xlabel("Number of trails", fontsize=9, color=C_LABEL)
    ax.set_ylabel("Skiable acres", fontsize=9, color=C_LABEL)
    title = "Trails vs acreage"
    if region_title:
        title = f"{region_title}: trails vs acreage"
    ax.set_title(title, fontsize=10, fontweight="600", color=C_SLATE_DARK, pad=8)
    ax.set_xlim(left=0)
    ax.set_ylim(bottom=0)

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

    fig.tight_layout()
    return _save_fig(fig, path)


def render_tier_bar_chart(
    tier_counts: dict[str, int],
    path: Path,
    *,
    region_title: str = "",
) -> Path | None:
    if not tier_counts:
        return None
    _mpl_setup()
    import matplotlib.pyplot as plt

    order = ("small_hill", "ski_mountain", "multiple_mountains", "mega_resort")
    labels: list[str] = []
    values: list[int] = []
    colors: list[str] = []
    for cat in order:
        n = tier_counts.get(cat, 0)
        if n:
            labels.append(RESORT_CATEGORY_LABEL.get(cat, cat))
            values.append(n)
            colors.append(TIER_COLORS.get(cat, C_TEAL))

    if not values:
        return None

    fig, ax = plt.subplots(figsize=(3.4, 3.2), facecolor="white")
    _style_axes(ax)
    y_pos = range(len(labels))
    ax.barh(list(y_pos), values, color=colors, height=0.55, edgecolor="white", linewidth=0.8)
    ax.set_yticks(list(y_pos), labels, fontsize=8)
    ax.invert_yaxis()
    for i, v in enumerate(values):
        ax.text(v + max(values) * 0.02, i, str(v), va="center", fontsize=8, color=C_LABEL)
    ax.set_xlabel("Resorts", fontsize=9, color=C_LABEL)
    title = "Resorts by size"
    if region_title:
        title = f"{region_title}: by size"
    ax.set_title(title, fontsize=10, fontweight="600", color=C_SLATE_DARK, pad=8)
    ax.set_xlim(0, max(values) * 1.18)
    fig.tight_layout()
    return _save_fig(fig, path)


def render_trail_difficulty_chart(
    totals: dict[str, int],
    path: Path,
    *,
    region_title: str = "",
) -> Path | None:
    if not totals:
        return None
    _mpl_setup()
    import matplotlib.pyplot as plt

    order = ("Novice", "Easy", "Intermediate", "Advanced", "Expert", "Freeride", "Extreme")
    labels = [k for k in order if k in totals]
    values = [totals[k] for k in labels]
    colors = [TRAIL_DIFF_COLORS.get(k, C_SLATE) for k in labels]

    fig, ax = plt.subplots(figsize=(3.4, 3.2), facecolor="white")
    _style_axes(ax)
    left = 0.0
    for label, val, color in zip(labels, values, colors):
        ax.barh(0, val, left=left, height=0.45, color=color, edgecolor="white", linewidth=0.8)
        if val >= max(values) * 0.08:
            ax.text(
                left + val / 2,
                0,
                f"{val:,}",
                ha="center",
                va="center",
                fontsize=7,
                color="white",
                fontweight="600",
            )
        left += val

    ax.set_yticks([])
    ax.set_xlabel("Trail count by difficulty", fontsize=9, color=C_LABEL)
    title = "Trail difficulty"
    if region_title:
        title = f"{region_title}: trail difficulty"
    ax.set_title(title, fontsize=10, fontweight="600", color=C_SLATE_DARK, pad=8)

    from matplotlib.patches import Patch

    legend = [
        Patch(facecolor=TRAIL_DIFF_COLORS.get(l, C_SLATE), label=l) for l in labels
    ]
    ax.legend(
        handles=legend,
        loc="upper center",
        bbox_to_anchor=(0.5, -0.22),
        ncol=min(4, len(labels)),
        fontsize=6.5,
        frameon=False,
    )
    fig.tight_layout()
    return _save_fig(fig, path)


def render_lift_types_chart(
    counts: dict[str, int],
    path: Path,
    *,
    region_title: str = "",
    top_n: int = 6,
) -> Path | None:
    if not counts:
        return None
    _mpl_setup()
    import matplotlib.pyplot as plt

    items = sorted(counts.items(), key=lambda x: (-x[1], x[0]))[:top_n]
    labels = [k.title() for k, _ in items]
    values = [v for _, v in items]
    colors = [LIFT_TYPE_COLORS.get(k, C_TEAL) for k, _ in items]

    fig, ax = plt.subplots(figsize=(3.4, 3.2), facecolor="white")
    _style_axes(ax)
    y_pos = range(len(labels))
    ax.barh(list(y_pos), values, color=colors, height=0.55, edgecolor="white", linewidth=0.8)
    ax.set_yticks(list(y_pos), labels, fontsize=8)
    ax.invert_yaxis()
    for i, v in enumerate(values):
        ax.text(v + max(values) * 0.02, i, f"{v:,}", va="center", fontsize=8, color=C_LABEL)
    ax.set_xlabel("Lifts", fontsize=9, color=C_LABEL)
    title = "Lift types"
    if region_title:
        title = f"{region_title}: lift types"
    ax.set_title(title, fontsize=10, fontweight="600", color=C_SLATE_DARK, pad=8)
    ax.set_xlim(0, max(values) * 1.22)
    fig.tight_layout()
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
    fig, ax = plt.subplots(figsize=(fig_w, fig_h), facecolor=C_ELEV_REF_BG)
    ax.set_facecolor(C_ELEV_REF_BG)

    sorted_bases = np.sort(bases.astype(float))
    min_base = float(sorted_bases[0])
    y_max = float(np.ceil(tops.max() / 1000.0) * 1000.0)
    if y_unit == "ft":
        y_plot_floor = _ELEV_Y_FLOOR_FT
        y_max = y_max + 500.0
        gap_pad = 180.0
        step = 1000.0
    else:
        y_plot_floor = _ELEV_Y_FLOOR_M
        y_max = y_max + 200.0
        gap_pad = 55.0
        step = 500.0
    # Reserve the empty band below the main cluster of bases (e.g. CO: Hoedown ~4.9k, rest ~6.4k+).
    if len(sorted_bases) >= 2 and sorted_bases[1] - sorted_bases[0] > gap_pad * 3:
        legend_top = float(sorted_bases[1]) - gap_pad
    else:
        legend_top = min_base - gap_pad
    legend_bottom = y_plot_floor + gap_pad * 0.55
    legend_top = max(legend_top, legend_bottom + (y_max - y_plot_floor) * 0.1)

    ax.set_ylim(y_plot_floor, y_max)
    ax.set_yticks(np.arange(y_plot_floor, y_max + 1, step))
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
        (str(m["code"]), _legend_display_name(str(m["name"]))) for m in mountains
    ]
    _label_elevation_lettered(ax, mountains, y_unit=y_unit)
    _draw_elevation_legend(
        ax,
        legend_entries,
        y_bottom=legend_bottom,
        y_top=legend_top,
        xlim=ax.get_xlim(),
    )

    fig.tight_layout()
    return _save_fig(fig, path, dpi=300)


def render_base_elevation_dotplot(
    df: pd.DataFrame,
    path: Path,
    *,
    region_title: str = "",
    top_n: int = 28,
) -> Path | None:
    """Dot plot of base elevations (low point) by resort."""
    _mpl_setup()
    import matplotlib.pyplot as plt

    if "elevation_low_m" not in df.columns:
        return None
    lo = pd.to_numeric(df["elevation_low_m"], errors="coerce")
    if not lo.notna().any():
        return None

    name_col = _name_col(df)
    d = df.loc[lo.notna()].copy()
    d["_lo"] = lo.loc[d.index]
    d = d.sort_values("_lo", ascending=True).head(int(top_n))
    if d.empty:
        return None

    labels = [_short_name(str(v), 18) for v in d[name_col].astype(str).tolist()]
    x = list(range(len(d)))

    fig, ax = plt.subplots(figsize=(6.2, 3.2), facecolor="white")
    _style_axes(ax)
    ax.grid(True, axis="y", color=C_GRID, linewidth=0.6, alpha=0.8)
    ax.grid(False, axis="x")

    ax.scatter(x, d["_lo"].astype(float), c=C_TEAL, s=28, zorder=3, edgecolors="white", linewidths=0.6)

    ax.set_xticks(x, labels, fontsize=7, rotation=45, ha="right")
    ax.set_ylabel("Base elevation (m)", fontsize=9, color=C_LABEL)
    title = "Lowest elevations (base)"
    if region_title:
        title = f"{region_title}: lowest base elevations"
    ax.set_title(title, fontsize=10, fontweight="600", color=C_SLATE_DARK, pad=8)
    ax.margins(x=0.02)

    fig.tight_layout()
    return _save_fig(fig, path)
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
        df, out_dir / "chart_trails_vs_acres.png", region_title=region_title
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

    base = render_base_elevation_dotplot(
        df, out_dir / "chart_base_elevations.png", region_title=region_title
    )
    if base:
        paths["base_elevations"] = str(base.resolve()).replace("\\", "/")

    return paths
