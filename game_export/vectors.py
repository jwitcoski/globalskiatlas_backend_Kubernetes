"""OSM features → local-meter GeoJSON (not WGS84)."""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Optional

import geopandas as gpd
from shapely.geometry import mapping
from shapely.validation import explain_validity, make_valid

from game_export.config import GameExportConfig
from game_export.coords import LocalCRS, geom_to_local, geom_to_projected
from game_export import jsonutil

log = logging.getLogger("game_export")


def parse_tags(val: Any) -> dict:
    if val is None:
        return {}
    if isinstance(val, dict):
        return val
    s = str(val).strip()
    if not s or s.lower() in {"nan", "none"}:
        return {}
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except json.JSONDecodeError:
        return {}


def tag(tags: dict, *keys: str) -> Optional[str]:
    for k in keys:
        if k in tags and tags[k] not in (None, ""):
            s = str(tags[k])
            if s.lower() in {"nan", "none", "nat"}:
                continue
            return s
    return None


def _clean_name(val: Any) -> Optional[str]:
    if val is None:
        return None
    try:
        if val != val:  # NaN
            return None
    except Exception:
        pass
    s = str(val).strip()
    if not s or s.lower() in {"nan", "none", "nat"}:
        return None
    return s


def _feature_id(osm_type, osm_id, prefix: str, i: int) -> str:
    if osm_type and osm_id not in (None, ""):
        return f"{prefix}:{osm_type}:{osm_id}"
    return f"{prefix}:anon:{i}"


def iter_rows(*gdfs):
    for gdf in gdfs:
        if gdf is None or gdf.empty:
            continue
        for i, row in gdf.iterrows():
            yield i, row


def project_and_local(geom, to_proj, local: LocalCRS, repairs: list):
    if geom is None or geom.is_empty:
        return None
    g = geom
    if not g.is_valid:
        repairs.append(
            {
                "action": "make_valid",
                "reason": explain_validity(g),
                "note": "Shapely make_valid applied; original OSM geometry not silently altered in source files",
            }
        )
        g = make_valid(g)
        if g is None or g.is_empty:
            return None
    gp = geom_to_projected(g, to_proj)
    return geom_to_local(gp, local)


def collect_layers(
    osm,
    pistes,
    lifts,
    to_proj,
    local: LocalCRS,
    cfg: GameExportConfig,
) -> tuple[dict[str, list], list]:
    repairs: list = []
    layers: dict[str, list] = {
        "pistes": [],
        "lifts": [],
        "buildings": [],
        "water": [],
        "forest": [],
        "cliffs": [],
        "roads": [],
        "grassland": [],
        "parking": [],
        "ski_area": [],
        "barriers": [],
    }
    seen: set[tuple] = set()

    def add(layer: str, row, geom_local, extra: dict | None = None):
        tags = parse_tags(row.get("tags") if hasattr(row, "get") else None)
        # ogr geojson may flatten tags onto columns
        for col in getattr(row, "index", []):
            if col in ("geometry", "tags", "osm_type", "osm_id"):
                continue
            v = row.get(col)
            if v is None or str(v).lower() in {"nan", "none", ""}:
                continue
            try:
                if v != v:
                    continue
            except Exception:
                pass
            if col not in tags:
                tags[str(col)] = v if not hasattr(v, "item") else v.item()
        osm_type = row.get("osm_type") or row.get("type")
        osm_id = row.get("osm_id") or row.get("id") or row.get("osm_way_id")
        key = (layer, str(osm_type), str(osm_id), geom_local.wkt[:80])
        if key in seen:
            return
        seen.add(key)
        props = {
            "id": _feature_id(osm_type, osm_id, layer, len(layers[layer])),
            "osm_type": None if osm_type is None else str(osm_type),
            "osm_id": None if osm_id is None else str(osm_id),
            "name": tag(tags, "name") or _clean_name(row.get("name")),
            "tags": {k: str(v) for k, v in tags.items() if k not in ("geometry",)},
            "source": "OpenStreetMap",
            "source_confidence": "osm_as_mapped",
            "coordinate_space": "local_east_m, local_north_m",
        }
        if extra:
            props.update(extra)
        layers[layer].append(
            {
                "type": "Feature",
                "geometry": mapping(geom_local),
                "properties": props,
            }
        )

    # Dedicated pistes / lifts tables
    for i, row in iter_rows(pistes):
        geom = project_and_local(row.geometry, to_proj, local, repairs)
        if geom is None:
            continue
        tags = parse_tags(row.get("tags"))
        ptype = tag(tags, "piste:type") or "downhill"
        add("pistes", row, geom, {"piste_type": ptype})

    for i, row in iter_rows(lifts):
        geom = project_and_local(row.geometry, to_proj, local, repairs)
        if geom is None:
            continue
        tags = parse_tags(row.get("tags"))
        add("lifts", row, geom, {"aerialway": tag(tags, "aerialway")})

    for i, row in iter_rows(osm):
        tags = parse_tags(row.get("tags"))
        # also scan other_tags / columns typical of ogr2ogr
        if not tags:
            for col in row.index:
                if col in ("geometry",):
                    continue
                val = row.get(col)
                if val is not None and str(val).lower() not in {"nan", "none", ""}:
                    tags[str(col)] = str(val)
        geom = None

        def need():
            nonlocal geom
            if geom is None:
                geom = project_and_local(row.geometry, to_proj, local, repairs)
            return geom

        ptype = tag(tags, "piste:type")
        if ptype == "downhill" or (ptype and "ski" in ptype.lower()):
            g = need()
            if g:
                add("pistes", row, g, {"piste_type": ptype})
        elif tag(tags, "aerialway"):
            g = need()
            if g:
                add("lifts", row, g, {"aerialway": tag(tags, "aerialway")})
        elif tag(tags, "building"):
            g = need()
            if g:
                add("buildings", row, g, {"building": tag(tags, "building")})
        elif tag(tags, "natural") == "water" or tag(tags, "waterway") or tag(tags, "water"):
            g = need()
            if g:
                add("water", row, g)
        elif tag(tags, "natural") in {"wood"} or tag(tags, "landuse") == "forest" or tag(tags, "natural") == "tree":
            g = need()
            if g:
                add("forest", row, g)
        elif tag(tags, "natural") == "cliff":
            g = need()
            if g:
                add("cliffs", row, g)
        elif tag(tags, "highway"):
            g = need()
            if g:
                add("roads", row, g, {"highway": tag(tags, "highway")})
        elif tag(tags, "amenity") == "parking":
            g = need()
            if g:
                add("parking", row, g)
        elif tag(tags, "landuse") in {"winter_sports"} or tag(tags, "sport") == "skiing":
            g = need()
            if g:
                add("ski_area", row, g)
        elif tag(tags, "natural") in {"grassland", "grass", "scrub", "fell"} or tag(tags, "landuse") in {
            "meadow",
            "grass",
            "recreation_ground",
        }:
            g = need()
            if g:
                add("grassland", row, g, {"cover": tag(tags, "natural") or tag(tags, "landuse")})
        elif tag(tags, "barrier"):
            g = need()
            if g:
                add("barriers", row, g, {"barrier": tag(tags, "barrier")})

    log.info(
        "OSM layers: pistes=%s lifts=%s buildings=%s water=%s forest=%s cliffs=%s roads=%s "
        "grassland=%s parking=%s ski_area=%s barriers=%s repairs=%s",
        len(layers["pistes"]),
        len(layers["lifts"]),
        len(layers["buildings"]),
        len(layers["water"]),
        len(layers["forest"]),
        len(layers["cliffs"]),
        len(layers["roads"]),
        len(layers["grassland"]),
        len(layers["parking"]),
        len(layers["ski_area"]),
        len(layers["barriers"]),
        len(repairs),
    )
    return layers, repairs


def add_ski_area_polygon(layers: dict, ski_geom, to_proj, local: LocalCRS, repairs: list) -> None:
    """OSM winter_sports AOI used by the pipeline (ski-area boundary)."""
    if ski_geom is None or ski_geom.is_empty:
        return
    g = project_and_local(ski_geom, to_proj, local, repairs)
    if g is None:
        return
    layers.setdefault("ski_area", []).append(
        {
            "type": "Feature",
            "geometry": mapping(g),
            "properties": {
                "id": "ski_area:pipeline_boundary",
                "name": "ski area boundary",
                "source": "OpenStreetMap",
                "source_confidence": "osm_as_mapped",
                "coordinate_space": "local_east_m, local_north_m",
                "kind": "winter_sports_boundary",
            },
        }
    )


def write_local_geojson(path: Path, features: list, local: LocalCRS, layer: str) -> None:
    fc = {
        "type": "FeatureCollection",
        "coordinate_system": {
            "kind": "local_game_meters",
            "not_wgs84": True,
            "axes": {
                "geojson_x": "local_east_m",
                "geojson_y": "local_north_m",
            },
            "game_axes": local.to_dict()["game_axes"],
            "local_origin": local.to_dict()["local_origin"],
            "projected_crs": local.projected_crs,
            "note": "This is not RFC 7946 WGS84 GeoJSON. Convert X,Y to game X,Z via game_axes.",
        },
        "attribution": "© OpenStreetMap contributors (ODbL)",
        "layer": layer,
        "features": features,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(jsonutil.dumps(fc), encoding="utf-8")
