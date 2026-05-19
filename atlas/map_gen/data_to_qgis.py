#!/usr/bin/env python3
"""
Data-to-map script: ski_areas.parquet → per-resort QGIS project + layout PNG export.

For each resort, copies ski_atlas_small_medium_template.qgz, patches filters and paths,
writes resort_inset_point.geojson, copies supporting icons, then exports the print layout
to PNG via QgsLayoutExporter (same engine as QGIS — this is the authoritative map image).

For each resort (default trail-based tier), writes **portrait and landscape** print
layouts at the same logical size so users can compare: portrait in
``atlas_work/{slug}/``, landscape in ``atlas_work/{slug}-layout-{tier}-landscape/``.
``--all-layout-tiers`` still emits all eight size/orientation folders.

Output (typical):
  atlas_work/{slug}/{slug}_map.qgz
  atlas_work/{slug}/{slug}_export.png
  atlas_work/{slug}-layout-{tier}-landscape/{slug}-layout-{tier}-landscape_map.qgz
  atlas_work/{slug}-layout-{tier}-landscape/{slug}-layout-{tier}-landscape_export.png

Optional: --matplotlib-preview writes {slug}_preview.png using matplotlib only (rough QA,
not the styled layout).

Usage:
  python -m atlas.map_gen.data_to_qgis --all-resorts --region north-america/us/virginia
  python -m atlas.map_gen.data_to_qgis --all-resorts
  python -m atlas.map_gen.data_to_qgis --all-resorts --limit 3
  python -m atlas.map_gen.data_to_qgis --all-resorts --no-export-layout   # QGZ only (no QGIS)
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
import zipfile
from pathlib import Path
from typing import Any, Optional

import geopandas as gpd
import yaml

from atlas.map_gen.layout_constants import (
    expand_bounds_for_rotation,
    expand_bounds_to_main_map_aspect,
    main_map_frame_mm,
)

# ── Template constants ────────────────────────────────────────────────────────
# These must match whatever is inside ski_atlas_small_medium_template.qgz
TEMPLATE_RESORT_NAME = "Wintergreen"
TEMPLATE_STATE_NAME = "Virginia"
TEMPLATE_STATE_POSTAL = "VA"
TEMPLATE_STATE_ISO = "US-VA"
TEMPLATE_QGS_NAME = "wintergreen_map.qgs"
# Rotation baked into the template (Wintergreen ski_north_angle=271.8 → 360-271.8=88.2)
TEMPLATE_ROTATION = 91.80000000000001137  # stored value in template XML

# Layout page / main map frame dimensions (mm) — matches ski_atlas_small_medium_template
# Template layout item UUIDs (from the template QGS).
_TEMPLATE_MAIN_MAP_UUID = "{843e891d-8334-43c6-988a-58fbb894dec9}"
# overview_resort_point memory-layer identifiers embedded in the template
_INSET_MEM_DS_SIMPLE = "memory?geometry=Point&amp;crs=EPSG:4326"
_INSET_MEM_DS_UID = (
    "memory?geometry=Point&amp;crs=EPSG:4326"
    "&amp;uid={adfb6c6b-d707-43bf-8b88-029a02dc96c1}"
)
_INSET_GEOJSON_DS = "./resort_inset_point.geojson"

# US state → (postal, iso_3166_2)
US_STATE_CODES: dict[str, tuple[str, str]] = {
    "Alabama": ("AL", "US-AL"), "Alaska": ("AK", "US-AK"),
    "Arizona": ("AZ", "US-AZ"), "Arkansas": ("AR", "US-AR"),
    "California": ("CA", "US-CA"), "Colorado": ("CO", "US-CO"),
    "Connecticut": ("CT", "US-CT"), "Delaware": ("DE", "US-DE"),
    "Florida": ("FL", "US-FL"), "Georgia": ("GA", "US-GA"),
    "Hawaii": ("HI", "US-HI"), "Idaho": ("ID", "US-ID"),
    "Illinois": ("IL", "US-IL"), "Indiana": ("IN", "US-IN"),
    "Iowa": ("IA", "US-IA"), "Kansas": ("KS", "US-KS"),
    "Kentucky": ("KY", "US-KY"), "Louisiana": ("LA", "US-LA"),
    "Maine": ("ME", "US-ME"), "Maryland": ("MD", "US-MD"),
    "Massachusetts": ("MA", "US-MA"), "Michigan": ("MI", "US-MI"),
    "Minnesota": ("MN", "US-MN"), "Mississippi": ("MS", "US-MS"),
    "Missouri": ("MO", "US-MO"), "Montana": ("MT", "US-MT"),
    "Nebraska": ("NE", "US-NE"), "Nevada": ("NV", "US-NV"),
    "New Hampshire": ("NH", "US-NH"), "New Jersey": ("NJ", "US-NJ"),
    "New Mexico": ("NM", "US-NM"), "New York": ("NY", "US-NY"),
    "North Carolina": ("NC", "US-NC"), "North Dakota": ("ND", "US-ND"),
    "Ohio": ("OH", "US-OH"), "Oklahoma": ("OK", "US-OK"),
    "Oregon": ("OR", "US-OR"), "Pennsylvania": ("PA", "US-PA"),
    "Rhode Island": ("RI", "US-RI"), "South Carolina": ("SC", "US-SC"),
    "South Dakota": ("SD", "US-SD"), "Tennessee": ("TN", "US-TN"),
    "Texas": ("TX", "US-TX"), "Utah": ("UT", "US-UT"),
    "Vermont": ("VT", "US-VT"), "Virginia": ("VA", "US-VA"),
    "Washington": ("WA", "US-WA"), "West Virginia": ("WV", "US-WV"),
    "Wisconsin": ("WI", "US-WI"), "Wyoming": ("WY", "US-WY"),
    "District of Columbia": ("DC", "US-DC"),
}

# Applied by patch_qgs() so regenerated projects inherit corrected subset/symbology.
_LIFT_TYPE_FIELD_OLD = '''<field name="lift_type" subType="0" length="0" expression="lower(replace(replace(regexp_substr(&quot;tags&quot;, '&quot;aerialway&quot;: &quot;[^&quot;]+&quot;'), '&quot;aerialway&quot;: &quot;', ''), '&quot;', ''))" typeName="" precision="0" comment="" type="10"/>'''

_LIFT_TYPE_FIELD_NEW = '''<field name="lift_type" subType="0" length="0" expression="CASE WHEN lower(trim(replace(replace(regexp_substr(&quot;tags&quot;, '&quot;aerialway&quot;: &quot;[^&quot;]+&quot;'), '&quot;aerialway&quot;: &quot;', ''), '&quot;', ''))) IN ('chair_lift','magic_carpet','cable_car','drag_lift','t-bar','rope_tow') THEN lower(trim(replace(replace(regexp_substr(&quot;tags&quot;, '&quot;aerialway&quot;: &quot;[^&quot;]+&quot;'), '&quot;aerialway&quot;: &quot;', ''), '&quot;', ''))) WHEN lower(trim(replace(replace(regexp_substr(&quot;tags&quot;, '&quot;aerialway&quot;: &quot;[^&quot;]+&quot;'), '&quot;aerialway&quot;: &quot;', ''), '&quot;', ''))) IN ('','pylon','station','mast') THEN '' ELSE 'else' END" typeName="" precision="0" comment="" type="10"/>'''
_LIFT_FALLBACK_RULE_OLD = (
    '<rule filter="ELSE" key="{10ccbc89-87bd-4e51-91c6-9a8fe50a36bc}" '
    'symbol="6" label="fallback"/>'
)
_LIFT_FALLBACK_RULE_NEW = (
    '<rule filter="&quot;lift_type&quot; = \'else\'" key="{10ccbc89-87bd-4e51-91c6-9a8fe50a36bc}" '
    'symbol="6" label="fallback"/>'
)
_PARKING_SUBSET_OR_OLD = (
    ' AND ("tags" ILIKE \'%"amenity": "parking"%\' OR "tags" ILIKE '
    '\'%"parking": "surface"%\')'
)
_PARKING_SUBSET_ONLY_AMENITY = ' AND "tags" ILIKE \'%"amenity": "parking"%\''


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_config(config_path: Optional[Path] = None) -> dict[str, Any]:
    if config_path is None:
        config_path = _repo_root() / "config" / "atlas.yaml"
    if not config_path.exists():
        return {}
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def slugify(name: str) -> str:
    slug = name.lower()
    slug = re.sub(r"[^a-z0-9]+", "-", slug)
    return slug.strip("-")


def _safe_slug_fallback(row: Any) -> str:
    """Fallback slug when the resort name is non-ASCII (e.g. Japanese) and slugify() becomes empty."""
    for k in ("winter_sports_id", "osm_way_id", "osm_id"):
        v = getattr(row, "get", lambda _k, _d=None: None)(k, None)
        if v is None:
            continue
        s = str(v).strip()
        if s and s.casefold() not in {"nan", "none"}:
            s = re.sub(r"[^0-9a-zA-Z]+", "-", s).strip("-").lower()
            if s:
                return f"resort-{s}"
    return "resort"


def _normalize_winter_sports_id(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.casefold() in {"nan", "none"}:
        return None
    try:
        return str(int(float(s)))
    except (TypeError, ValueError):
        return s


def _winter_sports_id_from_row(row: Any) -> Optional[str]:
    for id_col in ("winter_sports_id", "osm_way_id", "osm_id"):
        if id_col not in getattr(row, "index", row):
            continue
        wid = _normalize_winter_sports_id(row.get(id_col))
        if wid:
            return wid
    return None


def _ski_north_angle_for_row(
    input_dir: Path,
    row: Any,
    resort_name: str,
) -> Optional[float]:
    """Per-resort ski_north_angle; prefers regional analyzed parquet, then combined."""
    import pandas as pd

    wid = _winter_sports_id_from_row(row)
    region = row.get("region") if hasattr(row, "get") else None
    paths: list[Path] = []
    if region is not None and str(region).strip():
        reg_path = _repo_root() / "output" / str(region).strip() / "ski_areas_analyzed.parquet"
        if reg_path.exists():
            paths.append(reg_path)
    for name in ("ski_areas_analyzed.parquet", "ski_areas_elevation.parquet"):
        p = input_dir / name
        if p.exists():
            paths.append(p)

    for path in paths:
        df = pd.read_parquet(path)
        if "ski_north_angle" not in df.columns:
            continue
        sub = df
        if wid and "winter_sports_id" in df.columns:
            sub = df[df["winter_sports_id"].astype(str) == wid]
        elif resort_name and "name" in df.columns:
            sub = df[df["name"].astype(str).str.strip() == resort_name.strip()]
        if sub.empty:
            continue
        val = sub.iloc[0]["ski_north_angle"]
        if val is not None and not (isinstance(val, float) and pd.isna(val)):
            return float(val)
    return None


def _regional_output_path(region: str, filename: str) -> Optional[Path]:
    """Per-region pipeline output (accurate contours/elev); combined can have bad name joins."""
    region = str(region or "").strip()
    if not region or region.casefold() in {"nan", "none"}:
        return None
    p = _repo_root() / "output" / region / filename
    return p if p.exists() else None


def _load_regional_layer_cache(
    regional_cache: dict[str, dict[str, Optional[gpd.GeoDataFrame]]],
    region: str,
    filename: str,
) -> Optional[gpd.GeoDataFrame]:
    region = str(region or "").strip()
    if not region:
        return None
    if region not in regional_cache:
        regional_cache[region] = {}
    if filename not in regional_cache[region]:
        p = _regional_output_path(region, filename)
        if p is None:
            regional_cache[region][filename] = None
        else:
            gdf = gpd.read_parquet(p)
            if gdf.crs is None:
                gdf = gdf.set_crs("EPSG:4326")
            regional_cache[region][filename] = gdf
    return regional_cache[region][filename]


def _layer_gdf_for_resort(
    combined: Optional[gpd.GeoDataFrame],
    regional_cache: dict[str, dict[str, Optional[gpd.GeoDataFrame]]],
    row: Any,
    resort_name: str,
    *,
    filename: str,
    name_col: str = "name",
    buffer_bounds: Optional[tuple[float, float, float, float]] = None,
) -> gpd.GeoDataFrame:
    """Prefer regional parquet for this row's region; fall back to combined."""
    region = str(row.get("region") or "").strip()
    reg = _load_regional_layer_cache(regional_cache, region, filename)
    if reg is not None and not reg.empty:
        hit = _filter_gdf_to_resort(reg, row, resort_name, name_col=name_col)
        if not hit.empty:
            return hit
    if combined is None:
        return gpd.GeoDataFrame()
    hit = _filter_gdf_to_resort(combined, row, resort_name, name_col=name_col)
    if (
        hit.empty
        or buffer_bounds is None
        or _bounds_intersect(buffer_bounds, tuple(hit.total_bounds))
    ):
        return hit
    # Combined slice has the right name but wrong location (known combined-parquet issue).
    return gpd.GeoDataFrame()


def _bounds_intersect(
    a: tuple[float, float, float, float],
    b: tuple[float, float, float, float],
    margin_deg: float = 0.05,
) -> bool:
    ax1, ay1, ax2, ay2 = a
    bx1, by1, bx2, by2 = b
    return not (
        ax2 + margin_deg < bx1
        or bx2 + margin_deg < ax1
        or ay2 + margin_deg < by1
        or by2 + margin_deg < ay1
    )


def _filter_gdf_to_resort(
    gdf: Optional[gpd.GeoDataFrame],
    row: Any,
    resort_name: str,
    *,
    name_col: str = "name",
) -> gpd.GeoDataFrame:
    if gdf is None or gdf.empty:
        return gpd.GeoDataFrame()

    wid = _winter_sports_id_from_row(row)
    if wid and "winter_sports_id" in gdf.columns:
        hit = gdf[gdf["winter_sports_id"].astype(str) == wid]
        if not hit.empty:
            return hit.copy()

    for col in (name_col, "name", "Ski Area"):
        if col in gdf.columns:
            hit = gdf[gdf[col].astype(str).str.strip() == resort_name.strip()]
            if not hit.empty:
                return hit.copy()
    return gpd.GeoDataFrame()


def _copy_template_icons(resort_dir: Path, template_path: Path, work_dir: Path) -> None:
    """Copy lift/badge icons next to the QGZ so relative ./icons/ paths resolve."""
    candidates = [
        template_path.parent / "icons",
        work_dir / "wintergreen-ski-resort" / "icons",
        work_dir / "wintergreen" / "icons",
        _repo_root() / "atlas_work" / "wintergreen-ski-resort" / "icons",
        _repo_root() / "atlas_work" / "wintergreen" / "icons",
    ]
    src_dir = next((c for c in candidates if c.is_dir() and any(c.iterdir())), None)
    if src_dir is None:
        return
    dst = resort_dir / "icons"
    dst.mkdir(parents=True, exist_ok=True)
    for f in src_dir.iterdir():
        if f.is_file() and f.suffix.lower() in {".svg", ".png", ".jpg", ".jpeg"}:
            shutil.copy2(f, dst / f.name)


def _rewrite_icon_paths(content: str) -> str:
    """Point symbology image paths at this resort's ./icons/ folder."""
    return re.sub(
        r'(?:[A-Za-z]:|\.{1,2})[^"]*[/\\]atlas_work[/\\][^"\\]+[/\\]icons[/\\]',
        "./icons/",
        content,
    )


def _ensure_map_layers_checked(content: str) -> str:
    for layer_name in ("ski_area_contours", "ski_area_elevation_points"):
        content = re.sub(
            rf'(<layer-tree-layer\b[^>]*\bname="{re.escape(layer_name)}"[^>]*\bchecked=")'
            r'Qt::Unchecked\("',
            r'\1Qt::Checked\("',
            content,
            count=1,
        )
    return content


def _state_subset(state_name: str) -> str:
    """Build the QGIS subset string for the state boundary layer."""
    codes = US_STATE_CODES.get(state_name)
    if codes:
        postal, iso = codes
        return (
            f'"name" = \'{state_name}\' OR "name_en" = \'{state_name}\' '
            f'OR "postal" = \'{postal}\' OR "iso_3166_2" = \'{iso}\''
        )
    return f'"name" = \'{state_name}\' OR "name_en" = \'{state_name}\''


def _compute_map_extent(
    buffer_bounds: tuple[float, float, float, float],
    rotation_deg,
    pad: float = 0.05,
) -> tuple[float, float, float, float]:
    """
    Return the 1000ft buffer bounds with a small padding.

    This intentionally does not try to fit a portrait frame/aspect ratio.
    """
    xmin, ymin, xmax, ymax = buffer_bounds
    pw = (xmax - xmin) * pad
    ph = (ymax - ymin) * pad
    return (xmin - pw, ymin - ph, xmax + pw, ymax + ph)


def _replace_local_extents(
    content: str,
    bounds: tuple[float, float, float, float],
    pad: float = 0.0,
) -> str:
    """
    Replace all resort-sized extent references with the given bounds.

    Handles two formats used by QGIS:
      1. Child-element:  <extent><xmin>…</xmin>…</extent>   (mapcanvas)
      2. Attribute:      <Extent xmin="…" … />               (print layout map items)
         Also: <DefaultViewExtent xmin="…" … >               (default view)

    "Local" = width and height both < 5 degrees; global/continental extents are left alone.
    """
    xmin, ymin, xmax, ymax = bounds
    pw = (xmax - xmin) * pad
    ph = (ymax - ymin) * pad
    nx1, ny1, nx2, ny2 = xmin - pw, ymin - ph, xmax + pw, ymax + ph

    # ── Format 1: <extent> and <wgs84extent> child-element blocks ─────────────
    def _replace_any_extent(tag: str, inner: str) -> str:
        """Core replacement: returns updated inner XML or original if not local."""
        xm = re.search(r"<xmin>([^<]+)</xmin>", inner)
        ym = re.search(r"<ymin>([^<]+)</ymin>", inner)
        xx = re.search(r"<xmax>([^<]+)</xmax>", inner)
        yx = re.search(r"<ymax>([^<]+)</ymax>", inner)
        if not (xm and ym and xx and yx):
            return f"<{tag}>{inner}</{tag}>"
        w = float(xx.group(1)) - float(xm.group(1))
        h = float(yx.group(1)) - float(ym.group(1))
        if w > 5 or h > 5:
            return f"<{tag}>{inner}</{tag}>"

        def _sub(t: str, val: float, s: str) -> str:
            return re.sub(rf"<{t}>[^<]+</{t}>", f"<{t}>{val:.16f}</{t}>", s)

        ni = _sub("xmin", nx1, inner)
        ni = _sub("ymin", ny1, ni)
        ni = _sub("xmax", nx2, ni)
        ni = _sub("ymax", ny2, ni)
        return f"<{tag}>{ni}</{tag}>"

    for etag in ("extent", "wgs84extent"):
        content = re.compile(rf"<{etag}>(.*?)</{etag}>", re.DOTALL).sub(
            lambda m, t=etag: _replace_any_extent(t, m.group(1)), content
        )

    # ── Format 2: <Extent .../> and <DefaultViewExtent .../>  ──────────────────
    # Attribute order varies across QGIS versions/templates, so parse attributes
    # by name instead of relying on a fixed order.
    tag_re = re.compile(r"<(Extent|DefaultViewExtent)\b[^>]*?/?>", re.DOTALL)

    def _replace_extent_tag(m: re.Match) -> str:
        tag = m.group(0)
        try:
            xm = re.search(r'xmin="([^"]+)"', tag)
            ym = re.search(r'ymin="([^"]+)"', tag)
            xx = re.search(r'xmax="([^"]+)"', tag)
            yx = re.search(r'ymax="([^"]+)"', tag)
            if not (xm and ym and xx and yx):
                return tag
            x1, y1, x2, y2 = float(xm.group(1)), float(ym.group(1)), float(xx.group(1)), float(yx.group(1))
        except ValueError:
            return tag

        # Skip projected (metres-scale) or global extents
        if abs(x1) > 360 or abs(y1) > 360 or (x2 - x1) > 5 or (y2 - y1) > 5:
            return tag

        tag2 = re.sub(r'xmin="[^"]+"', f'xmin="{nx1:.16f}"', tag, count=1)
        tag2 = re.sub(r'ymin="[^"]+"', f'ymin="{ny1:.16f}"', tag2, count=1)
        tag2 = re.sub(r'xmax="[^"]+"', f'xmax="{nx2:.16f}"', tag2, count=1)
        tag2 = re.sub(r'ymax="[^"]+"', f'ymax="{ny2:.16f}"', tag2, count=1)
        return tag2

    content = tag_re.sub(_replace_extent_tag, content)

    return content


def _localize_datasources(content: str) -> str:
    """Replace ../../output/combined/{file}.parquet|...subset=... with ./data/{file}.parquet.

    The per-resort data files are written by _write_resort_data() so QGIS only
    loads a small per-resort parquet rather than scanning the 500 MB combined one.
    The |geometrytype= option is preserved; all |subset= clauses are dropped.

    Two passes are needed:
    1. <datasource> text nodes: subset filter uses literal " chars, so stop at <
    2. source="" attributes: stop at " (attribute delimiter)
    """
    def _make_local(filename: str, options: str) -> str:
        gm = re.search(r'\|geometrytype=(\w+)', options)
        suffix = f'|geometrytype={gm.group(1)}' if gm else ''
        return f'./data/{filename}{suffix}'

    # Pass 1: <datasource>...</datasource> elements.
    # Text nodes may contain literal " so we match up to the closing tag (<).
    def _replace_element(m: re.Match) -> str:
        inner = m.group(1)
        fm = re.match(r'\.\./\.\./output/combined/([\w.]+\.parquet)(.*)', inner, re.DOTALL)
        if not fm:
            return m.group(0)
        return f'<datasource>{_make_local(fm.group(1), fm.group(2))}</datasource>'

    content = re.sub(
        r'<datasource>(\.\./\.\./output/combined/[\w.]+\.parquet[^<]*)</datasource>',
        _replace_element,
        content,
    )

    # Pass 2: source="..." attributes.
    # Attribute values cannot contain unescaped ", so [^<"]* is safe here.
    def _replace_attr(m: re.Match) -> str:
        return _make_local(m.group(1), m.group(2))

    content = re.sub(
        r'\.\./\.\./output/combined/([\w.]+\.parquet)([^<"]*)',
        _replace_attr,
        content,
    )

    return content


_PARKING_LAYER_DATASOURCE_LOCAL = "./data/osm_parking.parquet|geometrytype=Polygon"


def _replace_maplayer_datasource(content: str, layer_name: str, new_uri: str) -> str:
    """Replace the first <datasource> inside the <maplayer> whose <layername> matches."""
    token = f"<layername>{layer_name}</layername>"
    li = content.find(token)
    if li < 0:
        return content
    bs = content.rfind("<maplayer", 0, li)
    be = content.find("</maplayer>", li)
    if bs < 0 or be < 0:
        return content
    block = content[bs:be]
    new_block, n = re.subn(
        r"<datasource>[^<]*</datasource>",
        f"<datasource>{new_uri}</datasource>",
        block,
        count=1,
    )
    if not n:
        return content
    return content[:bs] + new_block + content[be:]


def _set_layoutitem_attr_by_uuid(content: str, uuid_str: str, attr: str, value: str) -> str:
    """Set an attribute on the <LayoutItem ...> opening tag matching uuid="{...}"."""
    token = f'uuid="{uuid_str}"'
    i = content.find(token)
    if i < 0:
        return content
    tag_start = content.rfind("<LayoutItem", 0, i)
    tag_end = content.find(">", i)
    if tag_start < 0 or tag_end < 0:
        return content
    tag = content[tag_start : tag_end + 1]
    if f'{attr}="' not in tag:
        new_tag = tag[:-1] + f' {attr}="{value}">'
    else:
        new_tag = re.sub(rf'{attr}="[^"]*"', f'{attr}="{value}"', tag, count=1)
    return content[:tag_start] + new_tag + content[tag_end + 1 :]


def _set_first_layoutitem_attr_by_type(content: str, type_code: str, attr: str, value: str) -> str:
    """Set an attribute on the first <LayoutItem ... type="X" ...> opening tag."""
    pat = rf"<LayoutItem\b[^>]*\btype=\"{re.escape(type_code)}\"[^>]*>"
    m = re.search(pat, content)
    if not m:
        return content
    tag = m.group(0)
    if f'{attr}="' not in tag:
        new_tag = tag[:-1] + f' {attr}="{value}">'
    else:
        new_tag = re.sub(rf'{attr}="[^"]*"', f'{attr}="{value}"', tag, count=1)
    return content[: m.start()] + new_tag + content[m.end() :]


def _set_layout_attr_by_name(content: str, layout_name: str, attr: str, value: str) -> str:
    """Set an attribute on the <Layout ...> opening tag matching name="...".

    QGIS may rewrite/normalize layout XML on open; this lets us clear template-linked
    behavior (e.g. worldFileMap) deterministically.
    """
    token = f'name="{layout_name}"'
    i = content.find(token)
    if i < 0:
        return content
    tag_start = content.rfind("<Layout", 0, i)
    tag_end = content.find(">", i)
    if tag_start < 0 or tag_end < 0:
        return content
    tag = content[tag_start : tag_end + 1]
    if f'{attr}="' not in tag:
        new_tag = tag[:-1] + f' {attr}="{value}">'
    else:
        new_tag = re.sub(rf'{attr}="[^"]*"', f'{attr}="{value}"', tag, count=1)
    return content[:tag_start] + new_tag + content[tag_end + 1 :]


def _strip_layoutitem_attr_in_layout(content: str, layout_name: str, attr: str) -> str:
    """Remove an attribute from all <LayoutItem ...> tags inside a named <Layout> block."""
    token = f'name="{layout_name}"'
    i = content.find(token)
    if i < 0:
        return content
    layout_start = content.rfind("<Layout", 0, i)
    layout_end = content.find("</Layout>", i)
    if layout_start < 0 or layout_end < 0:
        return content
    layout_end += len("</Layout>")
    block = content[layout_start:layout_end]

    # Strip `attr="..."` from opening tags only (keep whitespace sane).
    def _strip(m: re.Match[str]) -> str:
        tag = m.group(0)
        tag2 = re.sub(rf"\s+{re.escape(attr)}=\"[^\"]*\"", "", tag)
        return tag2

    block2 = re.sub(r"<LayoutItem\b[^>]*>", _strip, block)
    if block2 == block:
        return content
    return content[:layout_start] + block2 + content[layout_end:]


def _force_layoutitem_ddp_width_height_by_uuid(
    content: str, uuid_str: str, width_mm: float, height_mm: float
) -> str:
    """Force data-defined Width/Height on a layout item.

    QGIS serializes data-defined overrides via QgsPropertyCollection under
    <LayoutObject><dataDefinedProperties>. Setting dataDefinedWidth/Height here
    prevents template-linked normalization from changing item size on open/save.
    """
    token = f'uuid="{uuid_str}"'
    i = content.find(token)
    if i < 0:
        return content
    item_start = content.rfind("<LayoutItem", 0, i)
    item_end = content.find("</LayoutItem>", i)
    if item_start < 0 or item_end < 0:
        return content
    item_end += len("</LayoutItem>")
    block = content[item_start:item_end]

    # Locate the dataDefinedProperties block within this LayoutItem.
    m = re.search(r"<dataDefinedProperties>[\s\S]*?</dataDefinedProperties>", block)
    if not m:
        return content

    # Qgis::PropertyType::Static is typically enum value 1.
    ddp = (
        "<dataDefinedProperties>\n"
        "          <Option type=\"Map\">\n"
        "            <Option type=\"QString\" value=\"\" name=\"name\"/>\n"
        "            <Option type=\"Map\" name=\"properties\">\n"
        "              <Option type=\"Map\" name=\"dataDefinedWidth\">\n"
        "                <Option type=\"bool\" value=\"true\" name=\"active\"/>\n"
        "                <Option type=\"int\" value=\"1\" name=\"type\"/>\n"
        f"                <Option type=\"QString\" value=\"{width_mm}\" name=\"val\"/>\n"
        "              </Option>\n"
        "              <Option type=\"Map\" name=\"dataDefinedHeight\">\n"
        "                <Option type=\"bool\" value=\"true\" name=\"active\"/>\n"
        "                <Option type=\"int\" value=\"1\" name=\"type\"/>\n"
        f"                <Option type=\"QString\" value=\"{height_mm}\" name=\"val\"/>\n"
        "              </Option>\n"
        "            </Option>\n"
        "            <Option type=\"QString\" value=\"collection\" name=\"type\"/>\n"
        "          </Option>\n"
        "        </dataDefinedProperties>"
    )
    block2 = block[: m.start()] + ddp + block[m.end() :]
    return content[:item_start] + block2 + content[item_end:]


def _set_layoutitem_child_tag_attr_by_uuid(
    content: str,
    uuid_str: str,
    child_tag: str,
    attr: str,
    value: str,
) -> str:
    """Set an attribute on a child tag inside a specific LayoutItem block.

    Example: set <AtlasMap scalingMode="..."> inside the main map LayoutItem.
    """
    token = f'uuid="{uuid_str}"'
    i = content.find(token)
    if i < 0:
        return content
    item_start = content.rfind("<LayoutItem", 0, i)
    item_end = content.find("</LayoutItem>", i)
    if item_start < 0 or item_end < 0:
        return content
    item_end += len("</LayoutItem>")
    block = content[item_start:item_end]

    # Find child tag (self-closing is typical in QGS XML).
    m = re.search(rf"<{re.escape(child_tag)}\b[^>]*/>", block)
    if not m:
        return content
    tag = m.group(0)
    if f'{attr}="' not in tag:
        new_tag = tag[:-2] + f' {attr}="{value}"/>'
    else:
        new_tag = re.sub(rf'{attr}="[^"]*"', f'{attr}="{value}"', tag, count=1)
    block2 = block[: m.start()] + new_tag + block[m.end() :]
    return content[:item_start] + block2 + content[item_end:]


def _fmt_proj_angle(v: float) -> str:
    s = f"{v:.8f}".rstrip("0").rstrip(".")
    return "0" if s in {"", "-0"} else s


def _recenter_overview_inset_ortho(content: str, lon0: float, lat0: float) -> str:
    """Globe-style locator: orthographic CRS centered on the resort (both inset map items)."""

    lat_s = _fmt_proj_angle(float(lat0))
    lon_s = _fmt_proj_angle(float(lon0))
    proj4 = (
        f"+proj=ortho +lat_0={lat_s} +lon_0={lon_s} "
        "+x_0=0 +y_0=0 +ellps=WGS84 +units=m +no_defs +type=crs"
    )
    crs_xml = f"""        <crs>
          <spatialrefsys nativeFormat="Wkt">
            <wkt></wkt>
            <proj4>{proj4}</proj4>
            <srsid>0</srsid>
            <srid>0</srid>
            <authid></authid>
            <description>Orthographic</description>
            <projectionacronym></projectionacronym>
            <ellipsoidacronym>EPSG:7030</ellipsoidacronym>
            <geographicflag>false</geographicflag>
          </spatialrefsys>
        </crs>"""

    def _patch_layoutitem(item_id: str, xml: str) -> str:
        tok = f'id="{item_id}"'
        ii = xml.find(tok)
        if ii < 0:
            return xml
        ss = xml.rfind("<LayoutItem", 0, ii)
        ee = xml.find("</LayoutItem>", ii)
        if ss < 0 or ee < 0:
            return xml
        ee += len("</LayoutItem>")
        blk = xml[ss:ee]
        blk = re.sub(
            r"<crs>\s*<spatialrefsys[\s\S]*?</spatialrefsys>\s*</crs>",
            crs_xml.strip(),
            blk,
            count=1,
        )
        return xml[:ss] + blk + xml[ee:]

    for item in ("overview_inset_map", "overview_inset_dot"):
        content = _patch_layoutitem(item, content)
    return content


def _boost_overview_resort_point_marker(content: str) -> str:
    """Inset resort star: black fill, slightly larger + heavier outline (readable over country tint)."""
    tok = "<layername>overview_resort_point</layername>"
    li = content.find(tok)
    if li < 0:
        return content
    bs = content.rfind("<maplayer", 0, li)
    be = content.find("</maplayer>", li)
    if bs < 0 or be < 0:
        return content
    block = content[bs:be]
    sm = block.find('class="SimpleMarker"')
    if sm < 0:
        return content
    sm_end = block.find("</layer>", sm)
    if sm_end < 0:
        return content
    frag = block[sm:sm_end]
    frag = re.sub(
        r'<Option type="QString" value="[^"]*" name="color"/>',
        '<Option type="QString" value="0,0,0,255,rgb:0,0,0,1" name="color"/>',
        frag,
        count=1,
    )
    frag = re.sub(
        r'<Option type="QString" value="[^"]*" name="size"/>',
        '<Option type="QString" value="4" name="size"/>',
        frag,
        count=1,
    )
    frag = re.sub(
        r'<Option type="QString" value="[^"]*" name="outline_width"/>',
        '<Option type="QString" value="0.35" name="outline_width"/>',
        frag,
        count=1,
    )
    block = block[:sm] + frag + block[sm_end:]
    return content[:bs] + block + content[be:]


def patch_qgs(
    content: str,
    resort_name: str,
    state_name: str,
    buffer_bounds: Optional[tuple[float, float, float, float]] = None,
    ski_north_angle: Optional[float] = None,
    centroid_lon: Optional[float] = None,
    centroid_lat: Optional[float] = None,
    inset_country_raw: Optional[str] = None,
    layout_tier: str = "small_medium",
) -> str:
    """Substitute resort name, state, map extents, rotation, and inset point into QGS XML."""
    map_w_mm, map_h_mm = main_map_frame_mm(layout_tier)
    # Replace resort name everywhere it appears in subset filters
    content = content.replace(TEMPLATE_RESORT_NAME, resort_name)

    # ski_atlas_small_medium_template: lift fallback only when lift_type = else;
    # parking layer subset only amenity=parking (not generic parking=* tags).
    content = content.replace(_LIFT_TYPE_FIELD_OLD, _LIFT_TYPE_FIELD_NEW)
    content = content.replace(_LIFT_FALLBACK_RULE_OLD, _LIFT_FALLBACK_RULE_NEW)
    content = content.replace(_PARKING_SUBSET_OR_OLD, _PARKING_SUBSET_ONLY_AMENITY)

    # Replace state subset string (appears in <datasource> and source= attributes)
    old_subset = (
        f'"name" = \'{TEMPLATE_STATE_NAME}\' OR "name_en" = \'{TEMPLATE_STATE_NAME}\' '
        f'OR "postal" = \'{TEMPLATE_STATE_POSTAL}\' OR "iso_3166_2" = \'{TEMPLATE_STATE_ISO}\''
    )
    content = content.replace(old_subset, _state_subset(state_name))

    # Replace the rule filter attribute
    content = content.replace(
        f"filter=\"&quot;name&quot; = '{TEMPLATE_STATE_NAME}'\"",
        f"filter=\"&quot;name&quot; = '{state_name}'\"",
    )

    # Replace highlight rule description
    content = content.replace(
        f'description="Highlight {TEMPLATE_STATE_NAME}"',
        f'description="Highlight {state_name}"',
    )

    # ── Overview inset: dot + host country highlight ─────────────────────────
    # Ensure inset dot and host-country highlight are driven by the current resort
    # (not an absolute path or USA-specific filters baked into the template).
    #
    # 1) overview_inset_dot LayerSet <Layer> element: replace any stale absolute path.
    content = re.sub(
        r'(id="overview_inset_dot"[\s\S]*?<Layer\s+source=")[^"]+(" provider="ogr" name="overview_resort_point")',
        rf"\1{_INSET_GEOJSON_DS}\2",
        content,
        count=1,
    )

    # 2) Host country highlight: update any USA-specific subset/rule filters.
    # Prefer the parquet Country field; fall back to centroid bbox heuristics.
    country = (inset_country_raw or "").strip()
    if not country or country.casefold() in {"nan", "none"}:
        if centroid_lon is not None and centroid_lat is not None:
            lon = float(centroid_lon)
            lat = float(centroid_lat)
            if -50 <= lat <= -5 and 108 <= lon <= 160:
                country = "Australia"
            elif -52 <= lat <= -28 and (160 <= lon <= 180 or -180 <= lon <= -165):
                country = "New Zealand"
            else:
                country = "United States"
        else:
            country = "United States"

    # Template uses both substring subsets ('United States%') and exact-name rules
    # ('United States of America') across overview layers.
    content = content.replace("United States%", f"{country}%")
    full = "United States of America" if country == "United States" else country
    content = content.replace("United States of America", full)

    # 3) Locator inset: orthographic globe centered on resort (template framing / extent unchanged).
    if centroid_lon is not None and centroid_lat is not None:
        content = _recenter_overview_inset_ortho(
            content, lon0=float(centroid_lon), lat0=float(centroid_lat)
        )

    # ── Inset point layer: switch memory → GeoJSON ─────────────────────────────
    # The template's overview_resort_point layer is an empty memory layer.
    # We write resort_inset_point.geojson next to the QGZ; point the layer at it.
    #
    # 1. maplayer <datasource> (simple form, no uid)
    content = content.replace(
        f"<datasource>{_INSET_MEM_DS_SIMPLE}</datasource>",
        f"<datasource>{_INSET_GEOJSON_DS}</datasource>",
    )
    # 2. <provider> tag within the same maplayer block as the new datasource.
    #    The provider is ~2200 chars after the datasource, so we locate the
    #    enclosing <maplayer> ... </maplayer> block and replace within it only.
    geojson_ds_str = f"<datasource>{_INSET_GEOJSON_DS}</datasource>"
    geojson_pos = content.find(geojson_ds_str)
    if geojson_pos >= 0:
        block_start = content.rfind("<maplayer", 0, geojson_pos)
        block_end = content.find("</maplayer>", geojson_pos) + len("</maplayer>")
        if block_start >= 0 and block_end > block_start:
            block = content[block_start:block_end]
            block = block.replace(
                '<provider encoding="">memory</provider>',
                '<provider encoding="">ogr</provider>',
                1,
            )
            content = content[:block_start] + block + content[block_end:]
    # 3. layer-tree-layer source attribute (uid form)
    content = content.replace(
        f'source="{_INSET_MEM_DS_UID}" providerKey="memory"',
        f'source="{_INSET_GEOJSON_DS}" providerKey="ogr"',
    )
    # 4. overview_inset_dot LayerSet <Layer> element (uid form)
    content = content.replace(
        f'provider="memory" source="{_INSET_MEM_DS_UID}"',
        f'provider="ogr" source="{_INSET_GEOJSON_DS}"',
    )

    content = _boost_overview_resort_point_marker(content)

    # ── Map extents and rotation ───────────────────────────────────────────────
    # ski_north_angle = bearing from base→summit (0=N, 90=E).
    # QGIS rotation R puts geographic bearing (360-R) at screen-top.
    # To put the summit at the top: R = (360 - ski_north_angle) % 360
    #
    # When ski_north_angle is missing (typical outside US elevation parquet), we must
    # still override the template rotation (~Wintergreen 88°); otherwise worldwide maps
    # stay wrongly tilted and feel like the wrong projection.
    if ski_north_angle is not None:
        rotation = (360.0 - ski_north_angle) % 360.0
    else:
        rotation = 0.0

    if buffer_bounds is not None:
        # Padded buffer bounds, then expand symmetrically so lon/lat span ratio matches
        # the fixed print frame (avoids letterboxing inside the map item).
        map_extent = _compute_map_extent(buffer_bounds, rotation, pad=0.05)
        map_extent = expand_bounds_to_main_map_aspect(
            map_extent,
            frame_width_mm=map_w_mm,
            frame_height_mm=map_h_mm,
        )
        map_extent = expand_bounds_for_rotation(map_extent, rotation)
        content = _replace_local_extents(content, map_extent)

    # Legacy contour styles in some templates reference ELEV; parquet uses elevation_m.
    content = content.replace("&quot;ELEV&quot;", "&quot;elevation_m&quot;")

    # Update map canvas rotation (do not touch label/shape rotation attributes).
    content = re.sub(
        r"(<mapcanvas>[\s\S]*?<rotation>)[^<]+(</rotation>)",
        rf"\g<1>{rotation}\g<2>",
        content,
        count=1,
    )
    # Update main map LayoutItem mapRotation attribute by UUID (robust to template edits).
    content = _set_layoutitem_attr_by_uuid(
        content, _TEMPLATE_MAIN_MAP_UUID, "mapRotation", str(rotation)
    )
    # North arrow matches map orientation (first north arrow item).
    content = _set_first_layoutitem_attr_by_type(
        content, "65640", "pictureRotation", str(rotation)
    )

    # Main map uses empty LayerSet + project layers; inset dot layer was template-unchecked.
    content = re.sub(
        r'(<layer-tree-layer\b[^>]*\bname="overview_resort_point"[^>]*\bchecked=")Qt::Unchecked(")',
        r"\1Qt::Checked\2",
        content,
        count=1,
    )

    # Point all combined-parquet datasources at local per-resort data files.
    content = _localize_datasources(content)
    content = _rewrite_icon_paths(content)
    content = _ensure_map_layers_checked(content)

    # Parking layer uses single-symbol symbology; after localization it would share the same
    # osm_near_winter_sports Polygon slice as forests/buildings and hatch every polygon.
    # Point it at a tiny parquet written by _write_resort_data() instead.
    content = _replace_maplayer_datasource(
        content, "parking", _PARKING_LAYER_DATASOURCE_LOCAL
    )

    # Main map: atlas scalingMode="2" (predefined scale / auto) can let QGIS resize the
    # map frame on load to match the geographic extent — combined with aspect-ratio lock
    # in the UI this shows up as width stuck at 107.95 mm but height collapsing (~110 mm).
    # Always pin fixed scaling + canonical mm frame (even when extent/rotation branches
    # above did not run — e.g. missing buffer row).
    content = _set_layoutitem_child_tag_attr_by_uuid(
        content, _TEMPLATE_MAIN_MAP_UUID, "AtlasMap", "scalingMode", "0"
    )
    content = _set_layoutitem_attr_by_uuid(
        content,
        _TEMPLATE_MAIN_MAP_UUID,
        "size",
        f"{map_w_mm},{map_h_mm},mm",
    )
    # Lock the item in the layout to prevent QGIS from auto-resizing it on load
    # (observed: height collapses to ~110 mm and then gets saved back into the QGZ).
    content = _set_layoutitem_attr_by_uuid(
        content,
        _TEMPLATE_MAIN_MAP_UUID,
        "positionLock",
        "true",
    )
    # QGIS sometimes re-applies template item geometry on load when templateUuid is set.
    # Clearing it prevents the map item from being treated as a template-linked item.
    content = _set_layoutitem_attr_by_uuid(
        content,
        _TEMPLATE_MAIN_MAP_UUID,
        "templateUuid",
        "",
    )
    # QGIS layout can store a "worldFileMap" pointer to the map item; on open it may
    # normalize the map item's geometry. Clear the pointer to avoid any special casing.
    content = _set_layout_attr_by_name(content, "Ski Atlas Export", "worldFileMap", "")

    # Critical: QGIS uses templateUuid linkage to "normalize" layout items on open/save.
    # This was causing the main map to collapse to height=110.208 mm. Strip templateUuid
    # from all layout items so the layout is no longer treated as template-linked.
    content = _strip_layoutitem_attr_in_layout(content, "Ski Atlas Export", "templateUuid")

    # Force data-defined Width/Height for the main map item so QGIS cannot
    # normalize its geometry during open/save.
    content = _force_layoutitem_ddp_width_height_by_uuid(
        content, _TEMPLATE_MAIN_MAP_UUID, map_w_mm, map_h_mm
    )

    return content


def make_inset_point_geojson(lon: float, lat: float) -> str:
    return json.dumps(
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"role": "globe_inset"},
                    "geometry": {
                        "type": "Point",
                        "coordinates": [round(lon, 6), round(lat, 6)],
                    },
                }
            ],
        },
        indent=2,
    )


def render_resort_preview(
    resort_name: str,
    resort_dir: Path,
    buffer_bounds: Optional[tuple[float, float, float, float]],
    ski_area_gdf: gpd.GeoDataFrame,
    buffer_gdf: gpd.GeoDataFrame,
    contours_gdf: gpd.GeoDataFrame,
    pistes_gdf: gpd.GeoDataFrame,
) -> None:
    """Render a quick PNG map of the resort for visual QA without opening QGIS."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.patheffects as pe

    slug = slugify(resort_name)
    out_path = resort_dir / f"{slug}_preview.png"

    fig, ax = plt.subplots(figsize=(10, 10), facecolor="#f5f5f0")
    ax.set_facecolor("#f5f5f0")
    ax.set_aspect("equal")

    # 1 — Buffer (light blue fill)
    if not buffer_gdf.empty:
        buffer_gdf.plot(ax=ax, facecolor="#ddeeff", edgecolor="#8aaabb",
                        linewidth=0.8, zorder=1)

    # 2 — Contours (thin brown, major every 100 m bolder)
    if not contours_gdf.empty:
        minor = contours_gdf[contours_gdf["elevation_m"] % 100 != 0]
        major = contours_gdf[contours_gdf["elevation_m"] % 100 == 0]
        if not minor.empty:
            minor.plot(ax=ax, color="#b09070", linewidth=0.4, zorder=2)
        if not major.empty:
            major.plot(ax=ax, color="#8a6040", linewidth=0.9, zorder=3)
            # Label major contours
            for _, row in major.iterrows():
                try:
                    pt = row.geometry.interpolate(0.5, normalized=True)
                    ax.annotate(
                        f"{int(row['elevation_m'])} m",
                        xy=(pt.x, pt.y),
                        fontsize=5,
                        color="#5a3010",
                        ha="center",
                        path_effects=[pe.withStroke(linewidth=1.5, foreground="white")],
                        zorder=4,
                    )
                except Exception:
                    pass

    # 3 — Piste runs (green)
    if not pistes_gdf.empty:
        pistes_lines = pistes_gdf[pistes_gdf.geometry.type.isin(
            ["LineString", "MultiLineString"])]
        pistes_polys = pistes_gdf[pistes_gdf.geometry.type.isin(
            ["Polygon", "MultiPolygon"])]
        if not pistes_polys.empty:
            pistes_polys.plot(ax=ax, facecolor="#a8d8a8", edgecolor="#5a9a5a",
                              linewidth=0.5, zorder=5)
        if not pistes_lines.empty:
            pistes_lines.plot(ax=ax, color="#3a8a3a", linewidth=0.8, zorder=6)

    # 4 — Resort boundary (bold black outline)
    if not ski_area_gdf.empty:
        ski_area_gdf.plot(ax=ax, facecolor="none", edgecolor="#111111",
                          linewidth=2.0, zorder=7)

    # Set extent to buffer bounds + 10 % padding
    if buffer_bounds:
        xmin, ymin, xmax, ymax = buffer_bounds
        pw = (xmax - xmin) * 0.15
        ph = (ymax - ymin) * 0.15
        ax.set_xlim(xmin - pw, xmax + pw)
        ax.set_ylim(ymin - ph, ymax + ph)

    ax.set_title(resort_name, fontsize=14, fontweight="bold", pad=10)
    ax.set_xlabel("Longitude", fontsize=8)
    ax.set_ylabel("Latitude", fontsize=8)
    ax.tick_params(labelsize=7)
    ax.grid(True, alpha=0.25, linewidth=0.4)

    # Legend
    from matplotlib.patches import Patch
    from matplotlib.lines import Line2D
    legend_elements = [
        Patch(facecolor="#ddeeff", edgecolor="#8aaabb", label="1000 ft buffer"),
        Line2D([0], [0], color="#8a6040", linewidth=1.5, label="Major contour (100 m)"),
        Line2D([0], [0], color="#b09070", linewidth=0.6, label="Minor contour"),
        Patch(facecolor="#a8d8a8", edgecolor="#5a9a5a", label="Piste runs"),
        Line2D([0], [0], color="#111111", linewidth=2, label="Resort boundary"),
    ]
    ax.legend(handles=legend_elements, loc="lower right", fontsize=7,
              framealpha=0.85)

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def _write_resort_data(
    resort_name: str,
    resort_dir: Path,
    ski_area_gdf: gpd.GeoDataFrame,
    buffer_gdf: gpd.GeoDataFrame,
    contours_gdf: gpd.GeoDataFrame,
    pistes_gdf: gpd.GeoDataFrame,
    osm_all: Optional[gpd.GeoDataFrame],
    elev_points_gdf: Optional[gpd.GeoDataFrame] = None,
    row: Any = None,
) -> None:
    """Write per-resort clipped parquet files to resort_dir/data/.

    QGIS QGZ datasources point to ./data/{file}.parquet so the project loads
    only the small per-resort slice rather than the full combined dataset.
    """
    resort_dir.mkdir(parents=True, exist_ok=True)
    data_dir = resort_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    if not ski_area_gdf.empty:
        ski_area_gdf.to_parquet(data_dir / "ski_areas.parquet")
    if not buffer_gdf.empty:
        buffer_gdf.to_parquet(data_dir / "ski_areas_1000ft_buffer.parquet")
    if not contours_gdf.empty:
        contours_gdf.to_parquet(data_dir / "ski_area_contours.parquet")
    if not pistes_gdf.empty:
        pistes_gdf.to_parquet(data_dir / "pistes.parquet")

    def _filter_osm(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        if row is not None:
            hit = _filter_gdf_to_resort(gdf, row, resort_name, name_col="name")
            if not hit.empty:
                return hit
        needle = resort_name.casefold()
        for col in ("Ski Area", "name"):
            if col in gdf.columns:
                mask = gdf[col].astype(str).str.casefold().str.contains(
                    needle, na=False, regex=False
                )
                return gdf[mask].copy()
        return gpd.GeoDataFrame()

    if osm_all is not None and not osm_all.empty:
        filtered = _filter_osm(osm_all)
        if not filtered.empty:
            filtered.to_parquet(data_dir / "osm_near_winter_sports.parquet")
            gt = filtered.geometry.geom_type.astype(str)
            polys = filtered[gt.isin(("Polygon", "MultiPolygon"))].copy()
            # JSON tags use "amenity": "parking" — excludes parking_space / parking_entrance
            # where the closing quote does not immediately follow "parking".
            pk_mask = polys["tags"].map(
                lambda t: bool(
                    re.search(r'"amenity"\s*:\s*"parking"', str(t), flags=re.IGNORECASE)
                )
            )
            parking_only = polys[pk_mask].copy()
            if parking_only.empty:
                parking_only = polys.iloc[:0].copy()
            parking_only.to_parquet(data_dir / "osm_parking.parquet")

    if elev_points_gdf is not None and not elev_points_gdf.empty:
        elev_points_gdf.to_parquet(data_dir / "ski_area_elevation_points.parquet")


def process_resort(
    resort_name: str,
    state_name: str,
    centroid_lon: float,
    centroid_lat: float,
    resort_dir: Path,
    template_path: Path,
    icon_src: Optional[Path],
    work_dir: Optional[Path] = None,
    buffer_bounds: Optional[tuple[float, float, float, float]] = None,
    ski_north_angle: Optional[float] = None,
    inset_country_raw: Optional[str] = None,
    ski_area_gdf: Optional[gpd.GeoDataFrame] = None,
    buffer_gdf: Optional[gpd.GeoDataFrame] = None,
    contours_gdf: Optional[gpd.GeoDataFrame] = None,
    pistes_gdf: Optional[gpd.GeoDataFrame] = None,
    preview: bool = False,
    layout_tier: str = "small_medium",
    project_slug: Optional[str] = None,
) -> bool:
    resort_dir.mkdir(parents=True, exist_ok=True)
    # For non-ASCII resort names (e.g. Japanese), slugify() can become empty.
    # Use the resort directory name (already stabilized in run_resorts) as fallback.
    # project_slug: folder basename for *_map.qgz (e.g. camelback-mountain-resort-layout-large).
    file_slug = project_slug or slugify(resort_name) or resort_dir.name
    out_qgz = resort_dir / f"{file_slug}_map.qgz"
    internal_qgs = f"{file_slug}_map.qgs"

    with zipfile.ZipFile(template_path, "r") as zin:
        items = [(info, zin.read(info.filename)) for info in zin.infolist()]

    with zipfile.ZipFile(out_qgz, "w", zipfile.ZIP_DEFLATED) as zout:
        for info, data in items:
            if info.filename == TEMPLATE_QGS_NAME:
                text = patch_qgs(
                    data.decode("utf-8"),
                    resort_name,
                    state_name,
                    buffer_bounds,
                    ski_north_angle,
                    centroid_lon=centroid_lon,
                    centroid_lat=centroid_lat,
                    inset_country_raw=inset_country_raw,
                    layout_tier=layout_tier,
                )
                data = text.encode("utf-8")
                info.filename = internal_qgs
            zout.writestr(info, data)

    (resort_dir / "resort_inset_point.geojson").write_text(
        make_inset_point_geojson(centroid_lon, centroid_lat), encoding="utf-8"
    )

    if template_path is not None and work_dir is not None:
        _copy_template_icons(resort_dir, template_path, work_dir)
    elif icon_src and icon_src.exists():
        icons_dir = resort_dir / "icons"
        icons_dir.mkdir(exist_ok=True)
        dst_icon = icons_dir / icon_src.name
        if dst_icon.resolve() != icon_src.resolve():
            shutil.copy2(icon_src, dst_icon)

    if preview:
        try:
            render_resort_preview(
                resort_name=resort_name,
                resort_dir=resort_dir,
                buffer_bounds=buffer_bounds,
                ski_area_gdf=ski_area_gdf if ski_area_gdf is not None else gpd.GeoDataFrame(),
                buffer_gdf=buffer_gdf if buffer_gdf is not None else gpd.GeoDataFrame(),
                contours_gdf=contours_gdf if contours_gdf is not None else gpd.GeoDataFrame(),
                pistes_gdf=pistes_gdf if pistes_gdf is not None else gpd.GeoDataFrame(),
            )
        except Exception as e:
            print(f"    Preview failed: {e}", file=sys.stderr)

    return True


def _find_icon(work_dir: Path) -> Optional[Path]:
    for svg in work_dir.rglob("icons/*.svg"):
        return svg
    return None


def _resolve_layout_tier(
    resort_name: str,
    n_trails: int,
    tiers_cfg: dict[str, Any],
    override: Optional[str],
) -> str:
    allowed = {
        "small",
        "medium",
        "large",
        "mega",
        "small_medium",
        "small_landscape",
        "medium_landscape",
        "large_landscape",
        "mega_landscape",
    }
    if override:
        o = override.strip().lower()
        if o not in allowed:
            raise ValueError(
                f"--layout-tier must be one of {sorted(allowed)}, got {override!r}"
            )
        return o
    mega_names = {
        str(x).strip().casefold()
        for x in (tiers_cfg.get("mega_resorts") or [])
        if str(x).strip()
    }
    if resort_name.strip().casefold() in mega_names:
        return "mega"
    if n_trails < 0:
        # Unknown trail count: use small tier + landscape twin (not legacy small_medium-only).
        return "small"
    smax = int(tiers_cfg.get("small_max", 9))
    mmax = int(tiers_cfg.get("medium_max", 30))
    if n_trails <= smax:
        return "small"
    if n_trails <= mmax:
        return "medium"
    return "large"


def _landscape_pair_for_print_tier(tier: str) -> Optional[str]:
    """If *tier* is a portrait print size, return the matching *_landscape tier id."""
    t = tier.strip().lower()
    if t in ("small", "medium", "large", "mega"):
        return f"{t}_landscape"
    return None


def _default_tier_runs(
    slug: str,
    lt: str,
    *,
    config: dict[str, Any],
) -> list[tuple[str, str, Optional[str]]]:
    """Single-resort mode: portrait + landscape when templates exist; else one run.

    Portrait stays in ``work_dir/{slug}/`` (legacy path). Landscape twin uses
    ``{slug}-layout-{tier}-landscape/`` so both exports are easy to find.
    """
    ls = _landscape_pair_for_print_tier(lt)
    if ls is None:
        # Legacy Wintergreen geometry / no paired landscape template.
        return [(lt, slug, None)]
    ls_path = _template_qgz_for_tier(config, ls)
    if not ls_path.exists():
        print(
            f"  WARNING: missing landscape template for {lt!r}: {ls_path} — "
            f"only portrait will be generated for {slug}",
            file=sys.stderr,
        )
        return [(lt, slug, None)]
    base = f"{slug}-layout-{lt}"
    return [
        (lt, slug, None),
        (ls, f"{base}-landscape", f"{base}-landscape"),
    ]


def _template_qgz_for_tier(config: dict[str, Any], tier: str) -> Path:
    tc = config.get("template") or {}
    fallback_rel = tc.get("fallback") or tc.get(
        "small_medium",
        "atlas/map_gen/templates/ski_atlas_small_medium_template.qgz",
    )
    rel_by_tier = {
        "small": tc.get("small", "atlas/map_gen/templates/ski_atlas_small_template.qgz"),
        "medium": tc.get("medium", "atlas/map_gen/templates/ski_atlas_medium_template.qgz"),
        "large": tc.get("large", "atlas/map_gen/templates/ski_atlas_large_template.qgz"),
        "mega": tc.get("mega", "atlas/map_gen/templates/ski_atlas_mega_template.qgz"),
        "small_landscape": tc.get(
            "small_landscape",
            "atlas/map_gen/templates/ski_atlas_small_landscape_template.qgz",
        ),
        "medium_landscape": tc.get(
            "medium_landscape",
            "atlas/map_gen/templates/ski_atlas_medium_landscape_template.qgz",
        ),
        "large_landscape": tc.get(
            "large_landscape",
            "atlas/map_gen/templates/ski_atlas_large_landscape_template.qgz",
        ),
        "mega_landscape": tc.get(
            "mega_landscape",
            "atlas/map_gen/templates/ski_atlas_mega_landscape_template.qgz",
        ),
        "small_medium": fallback_rel,
    }
    rel = rel_by_tier.get(tier) or fallback_rel
    return _repo_root() / rel


def run_resorts(
    input_dir: Path,
    work_dir: Path,
    config: dict[str, Any],
    resort_id: Optional[str] = None,
    region_filter: Optional[str] = None,
    limit: Optional[int] = None,
    preview: bool = False,
    export_layout: bool = False,
    export_dpi: int = 150,
    layout_tier_override: Optional[str] = None,
    all_layout_tiers: bool = False,
) -> int:
    # Windows console defaults can be cp1252; ensure we can print non-ASCII resort names.
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    except Exception:
        pass

    ski_areas_path = input_dir / "ski_areas.parquet"
    if not ski_areas_path.exists():
        print(f"Missing {ski_areas_path}", file=sys.stderr)
        return 0

    gdf = gpd.read_parquet(ski_areas_path)
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs("EPSG:4326")

    if "region" not in gdf.columns:
        gdf["region"] = ""

    if region_filter:
        gdf = gdf[gdf["region"] == region_filter].copy()

    if resort_id:
        for id_col in ("winter_sports_id", "osm_way_id", "osm_id"):
            if id_col in gdf.columns:
                gdf = gdf[gdf[id_col].astype(str) == resort_id].copy()
                break

    if gdf.empty:
        print("No resorts matched.", file=sys.stderr)
        return 0

    if limit:
        gdf = gdf.head(limit)

    name_col = "Ski Area" if "Ski Area" in gdf.columns else "name"
    state_col = "State" if "State" in gdf.columns else None

    # Duplicate rows can appear for the same OSM way in combined parquet; --resort should yield one map.
    if resort_id and len(gdf) > 1 and name_col in gdf.columns:
        gdf = gdf.drop_duplicates(subset=[name_col], keep="first").copy()

    # Load 1000ft buffer for extent replacement + preview
    buffer_gdf: Optional[gpd.GeoDataFrame] = None
    buffer_path = input_dir / "ski_areas_1000ft_buffer.parquet"
    if buffer_path.exists():
        buffer_gdf = gpd.read_parquet(buffer_path)
        if buffer_gdf.crs is None:
            buffer_gdf = buffer_gdf.set_crs("EPSG:4326")
        elif buffer_gdf.crs.to_epsg() != 4326:
            buffer_gdf = buffer_gdf.to_crs("EPSG:4326")

    # Load contours + pistes (once, filtered per resort for both preview and local data files)
    contours_all: Optional[gpd.GeoDataFrame] = None
    pistes_all: Optional[gpd.GeoDataFrame] = None
    contours_path = input_dir / "ski_area_contours.parquet"
    if contours_path.exists():
        print("  Loading contours...")
        contours_all = gpd.read_parquet(contours_path)
        if contours_all.crs is None:
            contours_all = contours_all.set_crs("EPSG:4326")
    pistes_path = input_dir / "pistes.parquet"
    if pistes_path.exists():
        print("  Loading pistes...")
        pistes_all = gpd.read_parquet(pistes_path)

    # Load OSM + elevation-points for local data files (so QGZ datasources use ./data/
    # instead of scanning the 500 MB combined parquet on every project open/export).
    osm_all: Optional[gpd.GeoDataFrame] = None
    osm_path = input_dir / "osm_near_winter_sports.parquet"
    if osm_path.exists():
        print("  Loading OSM data (large file — one-time load)...")
        osm_all = gpd.read_parquet(osm_path)
        if osm_all.crs is None:
            osm_all = osm_all.set_crs("EPSG:4326")
    elev_points_all: Optional[gpd.GeoDataFrame] = None
    elev_pts_path = input_dir / "ski_area_elevation_points.parquet"
    if elev_pts_path.exists():
        elev_points_all = gpd.read_parquet(elev_pts_path)

    tiers_cfg = config.get("trail_tiers") or {}

    icon_src = _find_icon(work_dir)

    count = 0
    seen_slugs: dict[str, int] = {}
    regional_cache: dict[str, dict[str, Optional[gpd.GeoDataFrame]]] = {}

    for _, row in gdf.iterrows():
        resort_name = str(row.get(name_col) or "").strip()
        if not resort_name:
            continue
        state_name = str(row.get(state_col) or "").strip() if state_col else ""
        centroid = row.geometry.centroid
        slug = slugify(resort_name) or _safe_slug_fallback(row)

        if slug in seen_slugs:
            seen_slugs[slug] += 1
            slug = f"{slug}-{seen_slugs[slug]}"
        else:
            seen_slugs[slug] = 0

        # Look up 1000ft buffer for this resort (bounds + filtered GDF for preview)
        buffer_bounds: Optional[tuple[float, float, float, float]] = None
        resort_buffer_gdf = gpd.GeoDataFrame()
        if buffer_gdf is not None:
            # Prefer matching on the same name column as ski_areas.parquet, but do it
            # case-insensitively and trimmed to avoid whitespace/case drift across sources.
            def _norm(s: str) -> str:
                return str(s).strip().casefold()

            # 1) Try stable IDs first (avoids name mismatches / duplicates)
            id_pairs = []
            for id_col in ("osm_way_id", "osm_id"):
                if id_col in gdf.columns and id_col in buffer_gdf.columns:
                    v = row.get(id_col)
                    if v is not None and str(v).strip() != "" and str(v).lower() != "nan":
                        id_pairs.append((id_col, str(v)))

            for id_col, v in id_pairs:
                try:
                    mask = buffer_gdf[id_col].astype(str) == v
                except Exception:
                    continue
                resort_buffer_gdf = buffer_gdf[mask].copy()
                if not resort_buffer_gdf.empty:
                    break

            # 2) Fallback to name matching (casefold + trim)
            if resort_buffer_gdf.empty:
                candidates: list[str] = []
                if name_col in buffer_gdf.columns:
                    candidates.append(name_col)
                if "name" in buffer_gdf.columns and "name" != name_col:
                    candidates.append("name")
                if "Ski Area" in buffer_gdf.columns and "Ski Area" != name_col:
                    candidates.append("Ski Area")

                for col in candidates:
                    try:
                        mask = buffer_gdf[col].astype(str).map(_norm) == _norm(resort_name)
                    except Exception:
                        continue
                    resort_buffer_gdf = buffer_gdf[mask].copy()
                    if not resort_buffer_gdf.empty:
                        break

            if not resort_buffer_gdf.empty:
                geom = resort_buffer_gdf.geometry.union_all()
                buffer_bounds = geom.bounds  # (minx, miny, maxx, maxy)

        # Filter ski_area row for preview
        resort_ski_gdf = gdf[gdf[name_col] == resort_name][["geometry"]].copy()

        resort_contours = _layer_gdf_for_resort(
            contours_all,
            regional_cache,
            row,
            resort_name,
            filename="ski_area_contours.parquet",
            name_col="name",
            buffer_bounds=buffer_bounds,
        )

        resort_pistes = _filter_gdf_to_resort(
            pistes_all, row, resort_name, name_col=name_col
        )

        resort_elev = _layer_gdf_for_resort(
            elev_points_all,
            regional_cache,
            row,
            resort_name,
            filename="ski_area_elevation_points.parquet",
            name_col="name",
            buffer_bounds=buffer_bounds,
        )

        n_trails = (
            len(resort_pistes)
            if pistes_all is not None and name_col in pistes_all.columns
            else -1
        )

        tier_runs: list[tuple[str, str, Optional[str]]]
        # (layout_tier, directory_basename, project_slug or None)
        if all_layout_tiers:
            base_tiers = ("small", "medium", "large", "mega")
            tier_runs = []
            for t in base_tiers:
                tier_runs.append((t, f"{slug}-layout-{t}", f"{slug}-layout-{t}"))
            for t in base_tiers:
                tl = f"{t}_landscape"
                tier_runs.append(
                    (
                        tl,
                        f"{slug}-layout-{t}-landscape",
                        f"{slug}-layout-{t}-landscape",
                    )
                )
        else:
            lt = _resolve_layout_tier(
                resort_name, n_trails, tiers_cfg, layout_tier_override
            )
            tier_runs = _default_tier_runs(slug, lt, config=config)

        ski_north = _ski_north_angle_for_row(input_dir, row, resort_name)

        inset_country_raw: Optional[str] = None
        if "Country" in gdf.columns:
            c = row.get("Country")
            if c is not None:
                cs = str(c).strip()
                if cs and cs.casefold() not in {"nan", "none"}:
                    inset_country_raw = cs

        for layout_tier, dir_basename, project_slug in tier_runs:
            resort_dir = work_dir / dir_basename
            template_path = _template_qgz_for_tier(config, layout_tier)
            if not template_path.exists():
                print(
                    f"  Missing template for layout {layout_tier!r}: {template_path}",
                    file=sys.stderr,
                )
                continue

            try:
                _write_resort_data(
                    resort_name=resort_name,
                    resort_dir=resort_dir,
                    ski_area_gdf=resort_ski_gdf,
                    buffer_gdf=resort_buffer_gdf,
                    contours_gdf=resort_contours,
                    pistes_gdf=resort_pistes,
                    osm_all=osm_all,
                    elev_points_gdf=resort_elev,
                    row=row,
                )
                process_resort(
                    resort_name=resort_name,
                    state_name=state_name,
                    centroid_lon=centroid.x,
                    centroid_lat=centroid.y,
                    resort_dir=resort_dir,
                    template_path=template_path,
                    icon_src=icon_src,
                    work_dir=work_dir,
                    buffer_bounds=buffer_bounds,
                    ski_north_angle=ski_north,
                    inset_country_raw=inset_country_raw,
                    ski_area_gdf=resort_ski_gdf,
                    buffer_gdf=resort_buffer_gdf,
                    contours_gdf=resort_contours,
                    pistes_gdf=resort_pistes,
                    preview=preview,
                    layout_tier=layout_tier,
                    project_slug=project_slug,
                )
                count += 1
                rotation_str = f"  rotation={(360 - ski_north) % 360:.1f}°" if ski_north else ""
                trails_part = f"  trails={n_trails}" if n_trails >= 0 else ""
                print(
                    f"  {dir_basename}  ({resort_name}, {state_name})  "
                    f"layout={layout_tier}{trails_part}{rotation_str}"
                )
                if export_layout:
                    from atlas.map_gen.export_layouts import export_qgz

                    export_qgz(
                        resort_dir / f"{resort_dir.name}_map.qgz",
                        dpi=export_dpi,
                        overwrite=True,
                    )
            except Exception as e:
                print(f"  Error [{resort_name}] ({layout_tier}): {e}", file=sys.stderr)

    return count


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Data-to-map: ski_areas.parquet -> per-resort QGIS projects",
    )
    parser.add_argument("--input-dir", type=Path, default=None)
    parser.add_argument("--work-dir", type=Path, default=None)
    parser.add_argument("--resort", type=str, default=None, help="Filter by winter_sports_id")
    parser.add_argument("--region", type=str, default=None, help="Filter by region (e.g. north-america/us/virginia)")
    parser.add_argument("--all-resorts", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--matplotlib-preview",
        action="store_true",
        help="Also write {slug}_preview.png via matplotlib (not the print layout)",
    )
    parser.add_argument(
        "--no-export-layout",
        action="store_true",
        help="Skip QGIS print layout export ({slug}_export.png); QGZ only",
    )
    parser.add_argument("--dpi", type=int, default=150,
                        help="DPI for layout PNG export (default: 150)")
    parser.add_argument(
        "--qgis-root",
        type=Path,
        default=None,
        help="QGIS install root if not auto-detected (layout export only)",
    )
    parser.add_argument(
        "--layout-tier",
        type=str,
        default=None,
        choices=(
            "small",
            "medium",
            "large",
            "mega",
            "small_medium",
            "small_landscape",
            "medium_landscape",
            "large_landscape",
            "mega_landscape",
        ),
        help="Force print layout tier (default: from pistes count + config/trail_tiers). "
        "Tiers small/medium/large/mega also write the paired landscape export in "
        "{slug}-layout-{tier}-landscape/ (same logical size). "
        "*_landscape tiers write only that orientation.",
    )
    parser.add_argument(
        "--all-layout-tiers",
        action="store_true",
        help="Emit portrait small/medium/large/mega plus landscape variants "
        "({slug}-layout-{tier} and {slug}-layout-{tier}-landscape) per resort "
        "(conflicts with --layout-tier)",
    )
    args = parser.parse_args()

    if args.layout_tier and args.all_layout_tiers:
        parser.error("Use either --layout-tier or --all-layout-tiers, not both.")

    if not args.all_resorts and not args.resort and not args.region:
        parser.print_help()
        return 1

    root = _repo_root()
    config = load_config()
    input_dir = args.input_dir or Path(config.get("input_dir", "output/combined"))
    work_dir = args.work_dir or Path(config.get("work_dir", "atlas_work"))
    if not input_dir.is_absolute():
        input_dir = root / input_dir
    if not work_dir.is_absolute():
        work_dir = root / work_dir

    export_layout = not args.no_export_layout
    if export_layout:
        from atlas.map_gen.export_layouts import ensure_headless_qgis_initialized

        try:
            ensure_headless_qgis_initialized(args.qgis_root)
        except RuntimeError as e:
            print(f"\n{e}", file=sys.stderr)
            print(
                "Tip: use --no-export-layout to build QGZ files without QGIS, "
                "then run atlas\\map_gen\\run_export_layouts.bat",
                file=sys.stderr,
            )
            return 2

    try:
        count = run_resorts(
            input_dir,
            work_dir,
            config,
            resort_id=args.resort,
            region_filter=args.region,
            limit=args.limit,
            preview=args.matplotlib_preview,
            export_layout=export_layout,
            export_dpi=args.dpi,
            layout_tier_override=args.layout_tier,
            all_layout_tiers=args.all_layout_tiers,
        )
    finally:
        if export_layout:
            from atlas.map_gen.export_layouts import shutdown_headless_qgis_if_initialized

            shutdown_headless_qgis_if_initialized()

    print(f"\nProcessed {count} resort(s). Output: {work_dir}/")

    return 0


if __name__ == "__main__":
    sys.exit(main())
