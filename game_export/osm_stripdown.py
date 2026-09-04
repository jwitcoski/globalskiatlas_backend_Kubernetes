"""Export-time OSM keep/drop + piste normalization for clay and game scenes.

Thins nearby parquet rows after ``filter_by_ski`` so scene cakes only carry
ski-relevant geometry: downhill/snowpark lines, lifts, forest polygons, etc.
"""
from __future__ import annotations

import logging
import math
import re
from typing import Any, Literal, Optional

import geopandas as gpd
from shapely.geometry import GeometryCollection
from shapely.ops import linemerge, unary_union

log = logging.getLogger("game_export")

ExportMode = Literal["clay", "game"]

# ~70 m pad so lift terminals / trail ends at the AOI edge survive the clip.
HARD_CLIP_PAD_M = 70.0
# Degrees ≈ meters / (111_320 * cos(lat)); use a conservative mid-latitude floor.
_METERS_PER_DEG_LAT = 111_320.0

KEEP_PISTE_TYPES = frozenset({"downhill", "snowpark", "alpine"})
DROP_PISTE_TYPES = frozenset(
    {
        "nordic",
        "cross_country",
        "cross-country",
        "skitour",
        "ski_tour",
        "sled",
        "sleigh",
        "toboggan",
        "snowshoe",
        "hike",
        "hiking",
        "connection",
        "playground",  # sled/play areas, not alpine runs
    }
)
LIFT_DROP_AERIALWAYS = frozenset({"pylon", "station", "goods"})

# OSM tags parked for a future rock-outcrop layer (do not strip from nearby OSM).
ROCK_NATURALS = frozenset({"bare_rock", "rock", "scree", "cliff", "glacier"})

_CLAY_OSM_KEEP_NATURAL = frozenset({"wood", "forest"}) | ROCK_NATURALS
_CLAY_OSM_KEEP_LANDUSE = frozenset({"forest", "winter_sports"})
_GAME_OSM_KEEP_NATURAL = _CLAY_OSM_KEEP_NATURAL | frozenset({"water"})
_GAME_OSM_KEEP_LANDUSE = _CLAY_OSM_KEEP_LANDUSE


def _pad_deg(polygon, pad_m: float = HARD_CLIP_PAD_M) -> float:
    try:
        lat = float(polygon.centroid.y)
    except Exception:
        lat = 45.0
    lon_m = max(_METERS_PER_DEG_LAT * abs(math.cos(math.radians(lat))), 1.0)
    return float(pad_m) / min(_METERS_PER_DEG_LAT, lon_m)


def _tags_dict(row) -> dict[str, Any]:
    tags: dict[str, Any] = {}
    raw = row.get("tags") if hasattr(row, "get") else None
    if isinstance(raw, dict):
        tags.update(raw)
    elif isinstance(raw, str) and raw.strip():
        import json

        try:
            obj = json.loads(raw)
            if isinstance(obj, dict):
                tags.update(obj)
        except json.JSONDecodeError:
            pass
    other = str(row.get("other_tags") or "") if hasattr(row, "get") else ""
    if other and "=>" in other:
        for m in re.finditer(r'"([^"]+)"\s*=>\s*"([^"]*)"', other):
            tags.setdefault(m.group(1), m.group(2))
    # Flattened ogr columns
    for col in getattr(row, "index", []):
        if col in ("geometry", "tags", "other_tags", "osm_type", "osm_id"):
            continue
        v = row.get(col)
        if v is None:
            continue
        try:
            if v != v:  # NaN
                continue
        except Exception:
            pass
        s = str(v).strip()
        if not s or s.lower() in {"nan", "none"}:
            continue
        tags.setdefault(str(col), s)
    return tags


def _norm_type(val: Any) -> str:
    return str(val or "").strip().lower().replace("-", "_").replace(" ", "_")


def piste_type_of(row_or_props) -> str:
    if hasattr(row_or_props, "get") and not isinstance(row_or_props, dict):
        tags = _tags_dict(row_or_props)
        props = {str(k): row_or_props.get(k) for k in getattr(row_or_props, "index", [])}
    else:
        props = dict(row_or_props or {})
        tags = props.get("tags") if isinstance(props.get("tags"), dict) else {}
        tags = dict(tags or {})
    for key in ("piste:type", "piste_type", "pisteType"):
        v = props.get(key) or tags.get(key)
        if v:
            return _norm_type(v)
    return ""


def _feature_name(row_or_props) -> str:
    if isinstance(row_or_props, dict):
        tags = row_or_props.get("tags") if isinstance(row_or_props.get("tags"), dict) else {}
        return str(row_or_props.get("name") or (tags or {}).get("name") or "").lower()
    tags = _tags_dict(row_or_props)
    return str(row_or_props.get("name") or tags.get("name") or "").lower()


def is_keep_piste(row_or_props) -> bool:
    """True for downhill / snowpark (and alpine alias); false for XC/sled/etc."""
    ptype = piste_type_of(row_or_props)
    name = _feature_name(row_or_props)
    if ptype in DROP_PISTE_TYPES or _xc_or_sled_name(name):
        return False
    if ptype in KEEP_PISTE_TYPES:
        return True
    # Bare "ski" / missing type on dedicated pistes.parquet rows → downhill.
    if ptype in {"", "ski"} or (ptype and "ski" in ptype and "tour" not in ptype):
        return True
    return False


def _xc_or_sled_name(name: str) -> bool:
    n = f" {name} "
    needles = (
        " xc ",
        " nordic",
        " cross country",
        " cross-country",
        " catamount trail",
        " sled",
        " toboggan",
        " snowshoe",
        " ski tour",
        " skitour",
    )
    return any(x in n for x in needles) or name.endswith(" xc")


def is_lift_pylon_or_station(row_or_props) -> bool:
    if isinstance(row_or_props, dict):
        aerial = _norm_type(row_or_props.get("aerialway"))
        tags = row_or_props.get("tags") if isinstance(row_or_props.get("tags"), dict) else {}
        if not aerial:
            aerial = _norm_type((tags or {}).get("aerialway"))
        other = str(row_or_props.get("other_tags") or (tags or {}).get("other_tags") or "")
    else:
        tags = _tags_dict(row_or_props)
        aerial = _norm_type(row_or_props.get("aerialway") or tags.get("aerialway"))
        other = str(row_or_props.get("other_tags") or "")
    if aerial in LIFT_DROP_AERIALWAYS:
        return True
    if '"aerialway"=>"pylon"' in other or '"aerialway"=>"station"' in other:
        return True
    return False


def _is_line(geom) -> bool:
    return geom is not None and (not geom.is_empty) and geom.geom_type in ("LineString", "MultiLineString")


def _is_poly(geom) -> bool:
    return geom is not None and (not geom.is_empty) and geom.geom_type in ("Polygon", "MultiPolygon")


def _is_point(geom) -> bool:
    return geom is not None and (not geom.is_empty) and geom.geom_type in ("Point", "MultiPoint")


def prefer_line_pistes(gdf: Optional[gpd.GeoDataFrame]) -> Optional[gpd.GeoDataFrame]:
    """If a trail exists as both line and polygon, keep the line(s) only."""
    if gdf is None or gdf.empty:
        return gdf
    lines_idx = []
    polys_idx = []
    for i, row in gdf.iterrows():
        g = row.geometry
        if _is_line(g):
            lines_idx.append(i)
        elif _is_poly(g):
            polys_idx.append(i)
        # points / other dropped
    if not lines_idx:
        return gdf.loc[polys_idx].copy() if polys_idx else gdf.iloc[0:0].copy()
    if not polys_idx:
        return gdf.loc[lines_idx].copy()
    line_geoms = [gdf.loc[i].geometry for i in lines_idx]
    line_names = {
        i: str(gdf.loc[i].get("name") or "").strip().casefold()
        for i in lines_idx
    }
    drop_polys: set = set()
    for pi in polys_idx:
        prow = gdf.loc[pi]
        pname = str(prow.get("name") or "").strip().casefold()
        pgeom = prow.geometry
        drop = False
        if pname:
            for li in lines_idx:
                if line_names[li] and line_names[li] == pname:
                    drop = True
                    break
        if not drop:
            try:
                for lg in line_geoms:
                    if pgeom.intersects(lg):
                        drop = True
                        break
            except Exception:
                pass
        if drop:
            drop_polys.add(pi)
    keep_idx = list(lines_idx) + [i for i in polys_idx if i not in drop_polys]
    if len(drop_polys):
        log.info(
            "prefer_line_pistes: dropped %s overlapping polygons (%s lines, %s polys kept)",
            len(drop_polys),
            len(lines_idx),
            len(polys_idx) - len(drop_polys),
        )
    return gdf.loc[keep_idx].copy()


def prefer_line_piste_features(features: list[dict]) -> list[dict]:
    """Feature-list variant (local-meter GeoJSON) of ``prefer_line_pistes``."""
    if not features:
        return features
    from shapely.geometry import shape

    lines: list[tuple[int, Any, str]] = []
    polys: list[tuple[int, Any, str, dict]] = []
    other: list[dict] = []
    for i, feat in enumerate(features):
        props = feat.get("properties") or {}
        try:
            geom = shape(feat["geometry"])
        except Exception:
            other.append(feat)
            continue
        name = str(props.get("name") or "").strip().casefold()
        if _is_line(geom):
            lines.append((i, geom, name))
        elif _is_poly(geom):
            polys.append((i, geom, name, feat))
        else:
            continue  # drop points
    if not polys:
        return [features[i] for i, _, _ in lines] + other if lines else other
    if not lines:
        return [f for _, _, _, f in polys] + other
    drop: set[int] = set()
    for pi, pgeom, pname, _feat in polys:
        if pname and any(pname == ln for _, _, ln in lines if ln):
            drop.add(pi)
            continue
        try:
            if any(pgeom.intersects(lg) for _, lg, _ in lines):
                drop.add(pi)
        except Exception:
            pass
    out = [features[i] for i, _, _ in lines]
    out.extend(f for pi, _, _, f in polys if pi not in drop)
    out.extend(other)
    if drop:
        log.info("prefer_line_piste_features: dropped %s overlapping polygons", len(drop))
    return out


def hard_clip_gdf(
    gdf: Optional[gpd.GeoDataFrame],
    ski_polygon,
    *,
    pad_m: float = HARD_CLIP_PAD_M,
) -> Optional[gpd.GeoDataFrame]:
    """Clip features to the ski AOI (+ meter pad). Drop empty results."""
    if gdf is None or gdf.empty or ski_polygon is None or getattr(ski_polygon, "is_empty", True):
        return gdf
    try:
        clip_poly = ski_polygon.buffer(_pad_deg(ski_polygon, pad_m))
    except Exception:
        clip_poly = ski_polygon
    rows = []
    dropped = 0
    for _, row in gdf.iterrows():
        g = row.geometry
        if g is None or g.is_empty:
            dropped += 1
            continue
        try:
            # Centroid-outside quick reject for large neighbors.
            c = g.centroid
            if c is not None and not c.is_empty and not clip_poly.intersects(c) and not g.intersects(clip_poly):
                dropped += 1
                continue
            clipped = g.intersection(clip_poly)
        except Exception:
            rows.append(row)
            continue
        if clipped is None or clipped.is_empty:
            dropped += 1
            continue
        if isinstance(clipped, GeometryCollection):
            parts = [p for p in clipped.geoms if not p.is_empty and p.geom_type != "Point"]
            if not parts:
                dropped += 1
                continue
            clipped = unary_union(parts)
            if clipped is None or clipped.is_empty:
                dropped += 1
                continue
        # Prefer merged lines after clip.
        if clipped.geom_type == "MultiLineString":
            try:
                merged = linemerge(clipped)
                if merged is not None and not merged.is_empty:
                    clipped = merged
            except Exception:
                pass
        new_row = row.copy()
        new_row.geometry = clipped
        rows.append(new_row)
    if not rows:
        return gdf.iloc[0:0].copy()
    out = gpd.GeoDataFrame(rows, crs=getattr(gdf, "crs", None))
    if dropped:
        log.info("hard_clip: dropped/empty %s features (%s remain)", dropped, len(out))
    return out


def _drop_points(gdf: Optional[gpd.GeoDataFrame]) -> Optional[gpd.GeoDataFrame]:
    if gdf is None or gdf.empty:
        return gdf
    mask = ~gdf.geometry.apply(_is_point)
    before = len(gdf)
    out = gdf.loc[mask].copy()
    if len(out) < before:
        log.info("Dropped %s point geometries (%s remain)", before - len(out), len(out))
    return out


def _filter_pistes(gdf: Optional[gpd.GeoDataFrame]) -> Optional[gpd.GeoDataFrame]:
    if gdf is None or gdf.empty:
        return gdf
    keep = []
    for _, row in gdf.iterrows():
        keep.append(is_keep_piste(row))
    before = len(gdf)
    out = gdf.loc[keep].copy()
    if len(out) < before:
        log.info("piste type filter: %s → %s (downhill/snowpark only)", before, len(out))
    return out


def _filter_lifts(gdf: Optional[gpd.GeoDataFrame]) -> Optional[gpd.GeoDataFrame]:
    if gdf is None or gdf.empty:
        return gdf
    keep = [not is_lift_pylon_or_station(row) for _, row in gdf.iterrows()]
    before = len(gdf)
    out = gdf.loc[keep].copy()
    out = _drop_points(out)
    if len(out) < before:
        log.info("lift stripdown: %s → %s", before, len(out))
    return out


def _osm_row_keep(row, *, mode: ExportMode) -> bool:
    """Decide whether a nearby-OSM row is worth keeping for export."""
    tags = _tags_dict(row)
    geom = row.geometry
    if _is_point(geom):
        # Points never ship; rock/tree POIs included.
        return False

    # Admin / place boundaries
    if tags.get("boundary") or tags.get("admin_level"):
        return False
    if tags.get("place") and not tags.get("piste:type") and not tags.get("aerialway"):
        # place=locality etc. — drop unless also a ski feature
        if not tags.get("landuse") == "winter_sports":
            return False

    # Explicit drops
    if tags.get("highway"):
        return False
    if tags.get("amenity") == "parking":
        return False
    if tags.get("barrier"):
        return False
    natural = _norm_type(tags.get("natural"))
    landuse = _norm_type(tags.get("landuse"))
    if natural in {"grassland", "grass", "scrub", "fell", "heath", "moor"}:
        return False
    if landuse in {"meadow", "grass", "recreation_ground", "residential", "industrial", "farmland"}:
        return False

    # Pistes / lifts in the combined nearby table
    ptype = _norm_type(tags.get("piste:type"))
    if ptype or tags.get("piste:difficulty") or tags.get("piste:name"):
        return is_keep_piste(row)
    if tags.get("aerialway"):
        return not is_lift_pylon_or_station(row)

    keep_natural = _GAME_OSM_KEEP_NATURAL if mode == "game" else _CLAY_OSM_KEEP_NATURAL
    keep_landuse = _GAME_OSM_KEEP_LANDUSE if mode == "game" else _CLAY_OSM_KEEP_LANDUSE

    if natural in keep_natural:
        return True
    if landuse in keep_landuse:
        return True
    if mode == "game":
        if tags.get("building"):
            return True
        if tags.get("waterway") or tags.get("water") or natural == "water":
            return True
        if tags.get("sport") == "skiing":
            return True
    elif landuse == "winter_sports" or tags.get("sport") == "skiing":
        return True
    return False


def _filter_osm(gdf: Optional[gpd.GeoDataFrame], *, mode: ExportMode) -> Optional[gpd.GeoDataFrame]:
    if gdf is None or gdf.empty:
        return gdf
    keep = [_osm_row_keep(row, mode=mode) for _, row in gdf.iterrows()]
    before = len(gdf)
    out = gdf.loc[keep].copy()
    if len(out) < before:
        log.info("osm stripdown (%s): %s → %s", mode, before, len(out))
    return out


def strip_for_export(
    osm: Optional[gpd.GeoDataFrame],
    pistes: Optional[gpd.GeoDataFrame],
    lifts: Optional[gpd.GeoDataFrame],
    ski_polygon,
    *,
    mode: ExportMode = "clay",
) -> tuple[Optional[gpd.GeoDataFrame], Optional[gpd.GeoDataFrame], Optional[gpd.GeoDataFrame]]:
    """Shared keep/drop + hard AOI clip + line-prefer pistes.

    Polygon-only pistes remain for later downhill centerline conversion
    (needs DEM — see ``homepage_vectors.build_piste_trail_features`` / routes).
    """
    pistes = _filter_pistes(pistes)
    pistes = prefer_line_pistes(pistes)
    pistes = _drop_points(pistes)
    pistes = hard_clip_gdf(pistes, ski_polygon)

    lifts = _filter_lifts(lifts)
    lifts = hard_clip_gdf(lifts, ski_polygon)

    osm = _filter_osm(osm, mode=mode)
    osm = hard_clip_gdf(osm, ski_polygon)

    return osm, pistes, lifts


def layer_keep_for_mode(mode: ExportMode) -> frozenset[str]:
    """Which ``collect_layers`` keys to write for each product."""
    if mode == "clay":
        return frozenset({"pistes", "lifts", "forest", "ski_area", "cliffs"})
    return frozenset(
        {
            "pistes",
            "lifts",
            "buildings",
            "water",
            "forest",
            "cliffs",
            "ski_area",
        }
    )
