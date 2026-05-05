from __future__ import annotations

import math
from typing import Any

import pandas as pd
import searoute as sr

from ingest.comtrade.routing.constants import (
    EAST_OF_SUEZ_BASINS,
    PREFERRED_REPORTER_BASIN,
    SUEZ_ORIGIN_BASINS,
)


def apply_scenario_weights(edges: pd.DataFrame, route_scenario: str) -> pd.DataFrame:
    weighted_edges = edges.copy()
    weighted_edges["scenario_cost"] = weighted_edges["base_cost"]

    if route_scenario == "suez_disrupted":
        weighted_edges.loc[
            weighted_edges["chokepoint_name"].isin(["Suez Canal", "Bab el-Mandeb"]),
            "scenario_cost",
        ] += 9.0
        weighted_edges.loc[weighted_edges["chokepoint_name"].eq("Cape of Good Hope"), "scenario_cost"] -= 2.0
    elif route_scenario == "panama_disrupted":
        weighted_edges.loc[weighted_edges["chokepoint_name"].eq("Panama Canal"), "scenario_cost"] += 9.0
    elif route_scenario == "cape_preferred":
        weighted_edges.loc[
            weighted_edges["chokepoint_name"].isin(["Suez Canal", "Bab el-Mandeb"]),
            "scenario_cost",
        ] += 4.0
        weighted_edges.loc[weighted_edges["chokepoint_name"].eq("Cape of Good Hope"), "scenario_cost"] -= 2.5
    elif route_scenario == "risk_avoidance":
        weighted_edges.loc[
            weighted_edges["chokepoint_name"].isin(["Hormuz Strait", "Bab el-Mandeb", "Suez Canal"]),
            "scenario_cost",
        ] += 5.0

    return weighted_edges


def open_sea_group(origin_basin: str, destination_basin: str) -> str:
    if origin_basin == "BLACK_SEA" or destination_basin == "BLACK_SEA":
        return "BLACK_SEA_REGIONAL"
    if origin_basin in {"MEDITERRANEAN", "NORTH_ATLANTIC_EUROPE", "BALTIC"} and destination_basin in {
        "MEDITERRANEAN",
        "NORTH_ATLANTIC_EUROPE",
        "BALTIC",
        "ATLANTIC",
    }:
        return "EUROPEAN_MARITIME"
    return "DIRECT_OR_OPEN_SEA"


def route_group(cp_sequence: list[str], origin_basin: str, destination_basin: str) -> str:
    if not cp_sequence:
        return open_sea_group(origin_basin, destination_basin)
    cp_text = "|".join(cp_sequence).lower()
    if "hormuz" in cp_text:
        return "HORMUZ_EXPOSED"
    if "suez" in cp_text:
        return "SUEZ_EXPOSED"
    if "panama" in cp_text:
        return "PANAMA_EXPOSED"
    if "malacca" in cp_text:
        return "MALACCA_EXPOSED"
    if "gibraltar" in cp_text:
        return "GIBRALTAR_EXPOSED"
    if "turkish" in cp_text:
        return "BLACK_SEA_EXIT_EXPOSED"
    if "cape of good" in cp_text:
        return "CAPE_REROUTED"
    return "OTHER_ROUTE"


def headline_exposure_group(cp_sequence: list[str], origin_basin: str, destination_basin: str) -> str:
    return route_group(cp_sequence, origin_basin, destination_basin)


def reorder_ports_by_corridor(iso3: str, destination_basin: str | None, ports: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if destination_basin is None:
        return ports
    preferred_basins = PREFERRED_REPORTER_BASIN.get((iso3, destination_basin))
    if not preferred_basins:
        return ports
    preferred_ports = [port for port in ports if port["port_basin"] in preferred_basins]
    other_ports = [port for port in ports if port["port_basin"] not in preferred_basins]
    return preferred_ports + other_ports


def corridor_penalty(
    cp_sequence: list[str],
    origin_basin: str,
    destination_basin: str,
    reporter_iso3: str | None = None,
    partner_iso3: str | None = None,
) -> float:
    del partner_iso3
    penalty = 0.0
    cp_text = "|".join(cp_sequence).lower()

    if origin_basin in SUEZ_ORIGIN_BASINS and destination_basin in EAST_OF_SUEZ_BASINS:
        if "panama" in cp_text:
            penalty += 30.0
        if "cape of good hope" in cp_text:
            penalty += 18.0

    if origin_basin == "MEDITERRANEAN" and destination_basin == "WESTERN_PACIFIC" and "panama" in cp_text:
        penalty += 40.0

    if origin_basin in {"MEDITERRANEAN", "NORTH_ATLANTIC_EUROPE"} and destination_basin in {
        "ARABIAN_SEA",
        "GULF",
        "INDIAN_OCEAN",
    } and "cape of good hope" in cp_text:
        penalty += 22.0

    if reporter_iso3 == "TUR" and origin_basin == "BLACK_SEA" and destination_basin != "BLACK_SEA":
        if "turkish straits" not in cp_text:
            penalty += 25.0

    if reporter_iso3 == "EGY":
        if (
            origin_basin == "MEDITERRANEAN"
            and destination_basin in {"RED_SEA", "ARABIAN_SEA", "INDIAN_OCEAN", "GULF", "WESTERN_PACIFIC", "PACIFIC"}
        ) or (
            origin_basin == "RED_SEA"
            and destination_basin in {"MEDITERRANEAN", "ATLANTIC", "NORTH_ATLANTIC_EUROPE", "CARIBBEAN", "NORTH_AMERICA_ATLANTIC"}
        ):
            if "suez canal" not in cp_text:
                penalty += 22.0

    if reporter_iso3 == "PAN":
        atlantic_side = {"CARIBBEAN", "ATLANTIC", "NORTH_AMERICA_ATLANTIC"}
        pacific_side = {"PACIFIC", "WESTERN_PACIFIC", "SOUTH_AMERICA_PACIFIC"}
        if (
            origin_basin in atlantic_side and destination_basin in pacific_side
        ) or (
            origin_basin in pacific_side and destination_basin in atlantic_side
        ):
            if "panama canal" not in cp_text:
                penalty += 28.0

    return penalty


def great_circle_distance_km(a_lonlat: tuple[float, float], b_lonlat: tuple[float, float]) -> float:
    lon1, lat1 = map(math.radians, a_lonlat)
    lon2, lat2 = map(math.radians, b_lonlat)
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    haversine = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 6371.0088 * (2 * math.asin(math.sqrt(haversine)))


def sea_distance_km(a_lonlat: tuple[float, float], b_lonlat: tuple[float, float]) -> tuple[float, Any]:
    route = sr.searoute(list(a_lonlat), list(b_lonlat), units="km")
    return float(route.properties["length"]), route
