from __future__ import annotations

from typing import Any

import pandas as pd
from pyproj import CRS, Transformer
from shapely import to_wkb
from shapely.geometry import LineString, Point
from shapely.ops import transform


WGS84_CRS = CRS.from_epsg(4326)


def _is_missing(value: Any) -> bool:
    if isinstance(value, (list, tuple, dict, set)):
        return False
    try:
        return value is None or bool(pd.isna(value))
    except TypeError:
        return value is None


def point_wkb_from_lon_lat(longitude: Any, latitude: Any) -> bytes | None:
    if _is_missing(longitude) or _is_missing(latitude):
        return None
    return to_wkb(Point(float(longitude), float(latitude)), hex=False)


def buffered_point_wkb_from_lon_lat(
    longitude: Any,
    latitude: Any,
    radius_meters: Any,
    *,
    quad_segs: int = 32,
) -> bytes | None:
    if _is_missing(longitude) or _is_missing(latitude) or _is_missing(radius_meters):
        return None

    longitude_value = float(longitude)
    latitude_value = float(latitude)
    radius_value = float(radius_meters)
    if radius_value <= 0:
        return None

    local_crs = CRS.from_proj4(
        f"+proj=aeqd +lat_0={latitude_value} +lon_0={longitude_value} +datum=WGS84 +units=m +no_defs"
    )
    to_local = Transformer.from_crs(WGS84_CRS, local_crs, always_xy=True).transform
    to_wgs84 = Transformer.from_crs(local_crs, WGS84_CRS, always_xy=True).transform

    point = Point(longitude_value, latitude_value)
    buffered = transform(to_local, point).buffer(radius_value, quad_segs=quad_segs)
    return to_wkb(transform(to_wgs84, buffered), hex=False)


def linestring_wkb_from_coords(coords: Any) -> bytes | None:
    if _is_missing(coords) or not isinstance(coords, (list, tuple)):
        return None

    cleaned_coords: list[tuple[float, float]] = []
    for coord in coords:
        if not isinstance(coord, (list, tuple)) or len(coord) < 2:
            continue
        longitude, latitude = coord[0], coord[1]
        if _is_missing(longitude) or _is_missing(latitude):
            continue
        point = (float(longitude), float(latitude))
        if not cleaned_coords or cleaned_coords[-1] != point:
            cleaned_coords.append(point)

    if len(cleaned_coords) < 2:
        return None
    return to_wkb(LineString(cleaned_coords), hex=False)
