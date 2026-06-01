"""Pick a projected CRS for regional overview maps."""

from __future__ import annotations

from typing import Any

import geopandas as gpd

# Thematic / atlas projections by region
EPSG_CONUS_ALBERS = "EPSG:5070"
EPSG_ALASKA_ALBERS = "EPSG:3338"
EPSG_HAWAII_ALBERS = "EPSG:102007"
EPSG_CANADA_LAMBERT = "EPSG:3979"
EPSG_AUSTRALIA_ALBERS = "EPSG:3577"
EPSG_EUROPE_LAEA = "EPSG:3035"
EPSG_ANTARCTICA = "EPSG:3031"

US_STATE_ALASKA = frozenset({"alaska"})
US_STATE_HAWAII = frozenset({"hawaii"})

COUNTRY_ALIASES = {
    "united states": EPSG_CONUS_ALBERS,
    "united states of america": EPSG_CONUS_ALBERS,
    "usa": EPSG_CONUS_ALBERS,
    "canada": EPSG_CANADA_LAMBERT,
    "australia": EPSG_AUSTRALIA_ALBERS,
    "new zealand": "EPSG:2193",
    "japan": "EPSG:6677",
}


def _norm(s: Any) -> str:
    if s is None:
        return ""
    t = str(s).strip().casefold()
    return "" if t in {"nan", "none"} else t


def _centroid_lonlat(gdf: gpd.GeoDataFrame) -> tuple[float, float]:
    geom = gdf.to_crs("EPSG:4326").geometry.union_all()
    c = geom.centroid
    return float(c.x), float(c.y)


def _aeqd_authid(lat: float, lon: float) -> str:
    """Azimuthal equidistant centered on the unit (good for single countries)."""
    return (
        f"+proj=aeqd +lat_0={lat:.6f} +lon_0={lon:.6f} "
        "+x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    )


def overview_projected_crs(meta: dict[str, Any], boundary: gpd.GeoDataFrame) -> str:
    """
    Return CRS authid or PROJ string for an overview map.

    US states use Conus Albers (or AK/HI variants). Canada, Australia, Europe
    use standard atlas CRS. Others use local azimuthal equidistant.
    """
    country = _norm(meta.get("country"))
    state = _norm(meta.get("state"))
    kind = _norm(meta.get("kind"))

    if state in US_STATE_ALASKA:
        return EPSG_ALASKA_ALBERS
    if state in US_STATE_HAWAII:
        return EPSG_HAWAII_ALBERS

    if country in COUNTRY_ALIASES:
        # US country-level map: Conus Albers still OK for lower 48; AK/HI handled above
        return COUNTRY_ALIASES[country]

    lon, lat = _centroid_lonlat(boundary)

    # Rough regional buckets
    if lat < -60:
        return EPSG_ANTARCTICA
    if -10 <= lat <= 45 and -30 <= lon <= 45:
        return EPSG_EUROPE_LAEA

    return _aeqd_authid(lat, lon)
