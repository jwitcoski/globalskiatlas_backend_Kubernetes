"""Local meter coordinates and Three.js Y-up game axes."""
from __future__ import annotations

import math
from dataclasses import asdict, dataclass

import pyproj
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform


@dataclass(frozen=True)
class LocalCRS:
    """Projected UTM plus a local origin in meters.

    Game axes (Three.js Y-up):
      X = local easting meters (easting - origin_easting)
      Y = elevation meters
      Z = negative local northing meters (-(northing - origin_northing))
    """

    source_crs: str
    projected_crs: str
    origin_easting_m: float
    origin_northing_m: float
    origin_longitude: float
    origin_latitude: float
    units: str = "meters"

    def to_dict(self) -> dict:
        d = asdict(self)
        d["game_axes"] = {
            "x": "east_m",
            "y": "elevation_m",
            "z": "negative_north_m",
        }
        d["local_origin"] = {
            "easting_m": self.origin_easting_m,
            "northing_m": self.origin_northing_m,
            "longitude": self.origin_longitude,
            "latitude": self.origin_latitude,
        }
        return d

    def to_game_xz(self, easting_m: float, northing_m: float) -> tuple[float, float]:
        x = easting_m - self.origin_easting_m
        z = -(northing_m - self.origin_northing_m)
        return x, z

    def from_game_xz(self, x: float, z: float) -> tuple[float, float]:
        easting = x + self.origin_easting_m
        northing = -z + self.origin_northing_m
        return easting, northing


def utm_crs_from_lonlat(lon: float, lat: float) -> str:
    zone = int((lon + 180) / 6) + 1
    epsg = 32600 + zone if lat >= 0 else 32700 + zone
    return f"EPSG:{epsg}"


def make_transformers(projected_crs: str) -> tuple[pyproj.Transformer, pyproj.Transformer]:
    to_proj = pyproj.Transformer.from_crs("EPSG:4326", projected_crs, always_xy=True)
    to_wgs = pyproj.Transformer.from_crs(projected_crs, "EPSG:4326", always_xy=True)
    return to_proj, to_wgs


def geom_to_projected(geom: BaseGeometry, to_proj: pyproj.Transformer) -> BaseGeometry:
    return transform(to_proj.transform, geom)


def geom_to_local(geom: BaseGeometry, local: LocalCRS) -> BaseGeometry:
    """Shift projected meters to local XY (easting, northing relative to origin).

    GeoJSON layers store (x_east_m, z_is_NOT_used_here): we store local easting/northing
    as GeoJSON x/y (not game Z). The client converts with game_axes.
    Local GeoJSON: coordinates are [local_east_m, local_north_m].
    """

    def _shift(x: float, y: float, z: float | None = None):
        return (x - local.origin_easting_m, y - local.origin_northing_m)

    return transform(_shift, geom)


def lonlat_to_projected(
    lon: float, lat: float, to_proj: pyproj.Transformer
) -> tuple[float, float]:
    e, n = to_proj.transform(lon, lat)
    return float(e), float(n)


def projected_to_lonlat(
    easting: float, northing: float, to_wgs: pyproj.Transformer
) -> tuple[float, float]:
    lon, lat = to_wgs.transform(easting, northing)
    return float(lon), float(lat)


def bearing_deg(x0: float, y0: float, x1: float, y1: float) -> float:
    """Bearing in projected meters: 0 = north, 90 = east."""
    dx = x1 - x0
    dy = y1 - y0
    ang = math.degrees(math.atan2(dx, dy))
    return (ang + 360.0) % 360.0
