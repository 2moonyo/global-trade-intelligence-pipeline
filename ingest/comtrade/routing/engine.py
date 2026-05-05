from __future__ import annotations

import heapq
from typing import Any

import numpy as np
import pandas as pd

from ingest.comtrade.routing.constants import (
    COUNTRY_INTERNAL_BRIDGES,
    DIRECT_LAND_BORDER_PAIRS,
    EAST_OF_SUEZ_BASINS,
    SUEZ_ORIGIN_BASINS,
)
from ingest.comtrade.routing.helpers import display
from ingest.comtrade.routing.metrics import (
    corridor_penalty,
    great_circle_distance_km,
    headline_exposure_group,
    reorder_ports_by_corridor,
    route_group,
    sea_distance_km,
)


def build_trade_routes(
    *,
    route_candidates: pd.DataFrame,
    existing_keys: set[tuple[Any, Any, Any]],
    dim_country_ports: pd.DataFrame,
    basin_graph_edges: pd.DataFrame,
    dim_chokepoint: pd.DataFrame,
    dim_transshipment_hub: pd.DataFrame,
    bridge_basin_default_hubs: pd.DataFrame,
    country_flags: dict[str, dict[str, Any]],
    landlocked_gateways: dict[str, list[str]],
    route_scenario: str,
) -> pd.DataFrame:
    country_ports_clean = dim_country_ports.dropna(subset=["iso3", "port_name", "latitude", "longitude"]).copy()
    country_ports = (
        country_ports_clean.sort_values(["iso3", "port_rank", "port_name"])
        .groupby("iso3")
        .apply(
            lambda g: [
                {
                    "port_name": row.port_name,
                    "port_name_norm": str(row.port_name).strip().upper() if pd.notna(row.port_name) else "",
                    "lonlat": (float(row.longitude), float(row.latitude)),
                    "port_basin": row.port_basin,
                    "port_score": float(row.port_score) if pd.notna(row.port_score) else 0.0,
                    "world_water_body": row.world_water_body,
                    "fac_container": int(row.fac_container) if pd.notna(row.fac_container) else 0,
                    "fac_solid_bulk": int(row.fac_solid_bulk) if pd.notna(row.fac_solid_bulk) else 0,
                    "fac_liquid_bulk": int(row.fac_liquid_bulk) if pd.notna(row.fac_liquid_bulk) else 0,
                    "fac_oil_terminal": int(row.fac_oil_terminal) if pd.notna(row.fac_oil_terminal) else 0,
                    "fac_lng_terminal": int(row.fac_lng_terminal) if pd.notna(row.fac_lng_terminal) else 0,
                }
                for row in g.itertuples(index=False)
            ]
        )
        .to_dict()
    )

    hub_by_basin = (
        dim_transshipment_hub.sort_values(["hub_basin", "hub_rank"])
        .groupby("hub_basin")
        .apply(lambda g: g.iloc[0].to_dict())
        .to_dict()
    )

    graph: dict[str, list[dict[str, Any]]] = {}
    for row in basin_graph_edges.itertuples(index=False):
        graph.setdefault(row.origin_basin, []).append(
            {
                "destination_basin": row.destination_basin,
                "chokepoint_name": row.chokepoint_name,
                "scenario_cost": float(row.scenario_cost),
            }
        )

    chokepoint_coords = {
        row["chokepoint_name"]: (row["longitude"], row["latitude"])
        for row in dim_chokepoint.dropna(subset=["longitude", "latitude"]).to_dict("records")
    }

    def has_candidate_ports(iso3: str) -> bool:
        return bool(country_ports.get(iso3))

    def shares_direct_land_border(reporter_iso3: str, partner_iso3: str) -> bool:
        return partner_iso3 in DIRECT_LAND_BORDER_PAIRS.get(reporter_iso3, set())

    def should_route_unknown_transport_by_sea(row) -> bool:
        if row.transport_evidence != "transport_unknown":
            return False
        if not has_candidate_ports(row.reporter_iso3):
            return False
        if not has_candidate_ports(row.partner_iso3):
            return False
        if shares_direct_land_border(row.reporter_iso3, row.partner_iso3):
            return False
        return True

    def assign_routing_decision(row) -> str:
        if row.transport_evidence == "sea_observed":
            return "route_by_observed_sea"
        if should_route_unknown_transport_by_sea(row):
            return "route_by_inference"
        return "do_not_route"

    def country_has_internal_bridge(iso3: str) -> bool:
        return iso3 in COUNTRY_INTERNAL_BRIDGES

    def get_ports_for_bridge_side(iso3: str, side: str):
        bridge = COUNTRY_INTERNAL_BRIDGES.get(iso3)
        if not bridge:
            return []
        allowed_basins = bridge["side_a_basins"] if side == "A" else bridge["side_b_basins"]
        return [port for port in country_ports.get(iso3, []) if port["port_basin"] in allowed_basins]

    def shortest_basin_path(origin_basin, destination_basin):
        if pd.isna(origin_basin) or pd.isna(destination_basin):
            return [], np.inf
        if origin_basin == destination_basin:
            return [], 0.0

        pq = [(0.0, origin_basin, [], set())]
        while pq:
            cost, basin, cp_path, seen = heapq.heappop(pq)
            if basin == destination_basin:
                return cp_path, cost
            if basin in seen:
                continue
            next_seen = set(seen)
            next_seen.add(basin)
            for edge in graph.get(basin, []):
                next_basin = edge["destination_basin"]
                next_cost = cost + edge["scenario_cost"]
                next_path = cp_path + ([edge["chokepoint_name"]] if edge["chokepoint_name"] != "Open Sea" else [])
                heapq.heappush(pq, (next_cost, next_basin, next_path, next_seen))
        return [], np.inf

    def choose_gateway_iso3(iso3, partner_iso3=None):
        gateways = landlocked_gateways.get(iso3, [])
        if partner_iso3 in gateways:
            return partner_iso3
        return gateways[0] if gateways else None

    def candidate_ports_for_iso3(iso3, partner_iso3=None, partner_basin=None):
        flags = country_flags.get(iso3, {})
        if flags.get("has_wpi_ports", False):
            ports = country_ports.get(iso3, [])
            if partner_basin:
                ports = reorder_ports_by_corridor(iso3, partner_basin, ports)
            return ports, None, "DIRECT_PORT"
        if flags.get("is_landlocked_assumed", False):
            gateway_iso3 = choose_gateway_iso3(iso3, partner_iso3)
            if gateway_iso3 and gateway_iso3 in country_ports:
                return country_ports[gateway_iso3], gateway_iso3, "SEA_GATEWAY_INFERRED"
            return [], None, "LANDLOCKED_NO_GATEWAY"
        return [], None, "COASTAL_NO_PORT_MATCH"

    def indonesia_malacca_bonus(reporter_iso3, reporter_basin, partner_basin, cp_sequence):
        if reporter_iso3 != "IDN":
            return 0.0
        west_basins = {"INDIAN_OCEAN", "ARABIAN_SEA", "GULF"}
        east_basins = {"WESTERN_PACIFIC", "PACIFIC"}
        crossing = (
            (reporter_basin in west_basins and partner_basin in east_basins)
            or (reporter_basin in east_basins and partner_basin in west_basins)
        )
        cp_text = "|".join(cp_sequence).lower()
        if crossing and "malacca" in cp_text:
            return 8.0
        if crossing and "malacca" not in cp_text:
            return -10.0
        return 0.0

    def score_candidate_pair(
        reporter_iso3,
        partner_iso3,
        reporter_port,
        partner_port,
        cp_sequence,
        basin_cost,
        reporter_gateway_iso3=None,
        partner_gateway_iso3=None,
        reporter_basis="DIRECT_PORT",
        partner_basis="DIRECT_PORT",
        route_basis_detail="DIRECT_PORT_PAIR",
        internal_exit_port=None,
    ):
        gc_km = great_circle_distance_km(reporter_port["lonlat"], partner_port["lonlat"])

        neighbour_penalty = 0.0
        if gc_km < 1500 and len(cp_sequence) >= 2:
            neighbour_penalty += 8.0
        if gc_km < 2500 and any(cp in cp_sequence for cp in ["Suez Canal", "Hormuz Strait", "Bab el-Mandeb"]):
            neighbour_penalty += 8.0

        route_penalty = corridor_penalty(
            cp_sequence,
            reporter_port["port_basin"],
            partner_port["port_basin"],
            reporter_iso3=reporter_iso3,
            partner_iso3=partner_iso3,
        )

        direction_bonus = 0.0
        east_of_suez_basins = {"RED_SEA", "ARABIAN_SEA", "GULF", "INDIAN_OCEAN", "WESTERN_PACIFIC", "PACIFIC"}
        atlantic_side_basins = {
            "ATLANTIC",
            "NORTH_ATLANTIC_EUROPE",
            "NORTH_AMERICA_ATLANTIC",
            "CARIBBEAN",
            "SOUTH_ATLANTIC",
            "WEST_AFRICA_ATLANTIC",
        }

        if partner_port["port_basin"] in east_of_suez_basins:
            direction_bonus += reporter_port.get("eastbound_corridor_fit", 0.0)
            direction_bonus -= partner_port.get("atlantic_corridor_fit", 0.0) * 0.15
        if partner_port["port_basin"] in atlantic_side_basins:
            direction_bonus += reporter_port.get("atlantic_corridor_fit", 0.0)
            direction_bonus -= partner_port.get("eastbound_corridor_fit", 0.0) * 0.15

        if reporter_iso3 == "FRA" and partner_port["port_basin"] in east_of_suez_basins:
            if reporter_port["port_basin"] == "MEDITERRANEAN":
                direction_bonus += 6.0
            elif reporter_port["port_basin"] in {"ATLANTIC", "NORTH_ATLANTIC_EUROPE"}:
                direction_bonus -= 6.0
        if reporter_iso3 == "ESP" and partner_port["port_basin"] in east_of_suez_basins:
            if reporter_port["port_basin"] == "MEDITERRANEAN":
                direction_bonus += 4.0
            elif reporter_port["port_basin"] in {"ATLANTIC", "NORTH_ATLANTIC_EUROPE"}:
                direction_bonus -= 4.0

        direction_bonus += indonesia_malacca_bonus(
            reporter_iso3,
            reporter_port["port_basin"],
            partner_port["port_basin"],
            cp_sequence,
        )

        pair_score = (
            reporter_port["port_score"] * 0.20
            + partner_port["port_score"] * 0.20
            - gc_km * 0.0012
            - basin_cost * 1.30
            - neighbour_penalty
            - route_penalty
            + direction_bonus
        )

        return {
            "reporter_port": reporter_port["port_name"],
            "partner_port": partner_port["port_name"],
            "reporter_lonlat": reporter_port["lonlat"],
            "partner_lonlat": partner_port["lonlat"],
            "reporter_basin": reporter_port["port_basin"],
            "partner_basin": partner_port["port_basin"],
            "reporter_gateway_iso3": reporter_gateway_iso3,
            "partner_gateway_iso3": partner_gateway_iso3,
            "reporter_port_basis": reporter_basis,
            "partner_port_basis": partner_basis,
            "distance_km": gc_km,
            "cp_sequence": cp_sequence,
            "pair_score": pair_score,
            "route_basis_detail": route_basis_detail,
            "internal_exit_port": internal_exit_port,
        }

    def choose_best_port_pair(reporter_iso3, partner_iso3):
        partner_ports_raw, partner_gateway_iso3, partner_basis = candidate_ports_for_iso3(
            partner_iso3, reporter_iso3, partner_basin=None
        )
        if not partner_ports_raw:
            return None

        partner_basin_hint = partner_ports_raw[0]["port_basin"] if partner_ports_raw else None
        reporter_ports, reporter_gateway_iso3, reporter_basis = candidate_ports_for_iso3(
            reporter_iso3, partner_iso3, partner_basin=partner_basin_hint
        )
        if not reporter_ports:
            return None

        candidates = []
        for reporter_port in reporter_ports:
            for partner_port in partner_ports_raw:
                cp_sequence, basin_cost = shortest_basin_path(reporter_port["port_basin"], partner_port["port_basin"])
                candidates.append(
                    score_candidate_pair(
                        reporter_iso3=reporter_iso3,
                        partner_iso3=partner_iso3,
                        reporter_port=reporter_port,
                        partner_port=partner_port,
                        cp_sequence=cp_sequence,
                        basin_cost=basin_cost,
                        reporter_gateway_iso3=reporter_gateway_iso3,
                        partner_gateway_iso3=partner_gateway_iso3,
                        reporter_basis=reporter_basis,
                        partner_basis=partner_basis,
                        route_basis_detail="DIRECT_PORT_PAIR",
                        internal_exit_port=None,
                    )
                )

        if country_has_internal_bridge(reporter_iso3):
            bridge = COUNTRY_INTERNAL_BRIDGES[reporter_iso3]
            a_ports = get_ports_for_bridge_side(reporter_iso3, "A")
            b_ports = get_ports_for_bridge_side(reporter_iso3, "B")

            for reporter_port_in in a_ports:
                for reporter_port_out in b_ports:
                    for partner_port in partner_ports_raw:
                        onward_cp, onward_cost = shortest_basin_path(reporter_port_out["port_basin"], partner_port["port_basin"])
                        cp_sequence = [bridge["bridge_name"]] + onward_cp
                        basin_cost = 1.0 + onward_cost
                        candidates.append(
                            score_candidate_pair(
                                reporter_iso3=reporter_iso3,
                                partner_iso3=partner_iso3,
                                reporter_port=reporter_port_in,
                                partner_port=partner_port,
                                cp_sequence=cp_sequence,
                                basin_cost=basin_cost,
                                reporter_gateway_iso3=reporter_gateway_iso3,
                                partner_gateway_iso3=partner_gateway_iso3,
                                reporter_basis=f"{reporter_basis}_INTERNAL_BRIDGE",
                                partner_basis=partner_basis,
                                route_basis_detail=f"{reporter_iso3}_INTERNAL_BRIDGE_A_TO_B",
                                internal_exit_port=reporter_port_out["port_name"],
                            )
                        )

            for reporter_port_in in b_ports:
                for reporter_port_out in a_ports:
                    for partner_port in partner_ports_raw:
                        onward_cp, onward_cost = shortest_basin_path(reporter_port_out["port_basin"], partner_port["port_basin"])
                        cp_sequence = [bridge["bridge_name"]] + onward_cp
                        basin_cost = 1.0 + onward_cost
                        candidates.append(
                            score_candidate_pair(
                                reporter_iso3=reporter_iso3,
                                partner_iso3=partner_iso3,
                                reporter_port=reporter_port_in,
                                partner_port=partner_port,
                                cp_sequence=cp_sequence,
                                basin_cost=basin_cost,
                                reporter_gateway_iso3=reporter_gateway_iso3,
                                partner_gateway_iso3=partner_gateway_iso3,
                                reporter_basis=f"{reporter_basis}_INTERNAL_BRIDGE",
                                partner_basis=partner_basis,
                                route_basis_detail=f"{reporter_iso3}_INTERNAL_BRIDGE_B_TO_A",
                                internal_exit_port=reporter_port_out["port_name"],
                            )
                        )

        if not candidates:
            return None
        return max(candidates, key=lambda value: value["pair_score"])

    def maybe_assign_hub(origin_basin, destination_basin):
        hits = bridge_basin_default_hubs[
            (bridge_basin_default_hubs["origin_basin"] == origin_basin)
            & (bridge_basin_default_hubs["destination_basin"] == destination_basin)
        ].sort_values("hub_rank")
        if hits.empty:
            return None
        return hub_by_basin.get(hits.iloc[0]["hub_basin"])

    def forced_path_distance(start_lonlat, end_lonlat, chokepoints):
        if not chokepoints:
            dist_km, route = sea_distance_km(start_lonlat, end_lonlat)
            return dist_km, route["geometry"]["coordinates"]

        path_nodes = [start_lonlat] + [chokepoint_coords[cp] for cp in chokepoints if cp in chokepoint_coords] + [end_lonlat]
        total_km = 0.0
        stitched = []
        for index in range(len(path_nodes) - 1):
            leg_km, leg_route = sea_distance_km(path_nodes[index], path_nodes[index + 1])
            total_km += leg_km
            leg_coords = leg_route["geometry"]["coordinates"]
            if index == 0:
                stitched.extend(leg_coords)
            else:
                stitched.extend(leg_coords[1:])
        return total_km, stitched

    def assign_confidence(reporter_iso3, reporter_port_basis, partner_port_basis, reporter_basin, partner_basin, cp_sequence, gc_km):
        score = 2
        if reporter_port_basis != "DIRECT_PORT" or partner_port_basis != "DIRECT_PORT":
            score = min(score, 1)
        if reporter_basin == "UNKNOWN_COASTAL" or partner_basin == "UNKNOWN_COASTAL":
            score = min(score, 0)
        if gc_km < 1200 and any(cp in cp_sequence for cp in ["Suez Canal", "Hormuz Strait", "Bab el-Mandeb", "Panama Canal"]):
            score = min(score, 0)

        if route_scenario == "default_shortest":
            cp_text = "|".join(cp_sequence).lower()
            if reporter_basin in SUEZ_ORIGIN_BASINS and partner_basin in EAST_OF_SUEZ_BASINS:
                if "panama" in cp_text:
                    score = 0
                elif "cape of good hope" in cp_text:
                    score = min(score, 1)

            if reporter_port_basis == "DIRECT_PORT" and reporter_iso3 == "FRA":
                if reporter_basin in {"ATLANTIC", "NORTH_ATLANTIC_EUROPE"} and partner_basin in EAST_OF_SUEZ_BASINS:
                    score = min(score, 1)

        return {0: "very_low", 1: "low", 2: "medium", 3: "high"}[score]

    route_candidates = route_candidates.copy()
    route_candidates["routing_decision"] = route_candidates.apply(assign_routing_decision, axis=1)
    to_route = route_candidates[
        route_candidates["routing_decision"].isin(["route_by_observed_sea", "route_by_inference"])
    ].copy()
    to_route["_key"] = list(
        to_route[["reporter_iso3", "partner_iso3", "partner2_iso3"]]
        .fillna("__NULL__")
        .itertuples(index=False, name=None)
    )
    if existing_keys:
        to_route = to_route[~to_route["_key"].isin(existing_keys)].copy()

    display(
        route_candidates["routing_decision"]
        .value_counts(dropna=False)
        .rename_axis("routing_decision")
        .reset_index(name="route_count")
    )
    display(to_route.head(10))
    print("Keys selected for routing:", len(to_route))

    records = []
    for row in to_route.itertuples(index=False):
        reporter_iso3 = row.reporter_iso3
        partner_iso3 = row.partner_iso3
        partner2_iso3 = row.partner2_iso3 if pd.notna(row.partner2_iso3) else None

        best = choose_best_port_pair(reporter_iso3, partner_iso3)
        if best is None:
            records.append(
                {
                    "reporter_iso3": reporter_iso3,
                    "partner_iso3": partner_iso3,
                    "partner2_iso3": partner2_iso3,
                    "transport_evidence": row.transport_evidence,
                    "routing_decision": row.routing_decision,
                    "route_status": "UNROUTED",
                    "route_basis": "NO_PORT_PATH",
                    "reporter_port": None,
                    "partner_port": None,
                    "reporter_gateway_iso3": None,
                    "partner_gateway_iso3": None,
                    "reporter_basin": None,
                    "partner_basin": None,
                    "distance_km": np.nan,
                    "sea_distance_km": np.nan,
                    "sea_distance_direct_km": np.nan,
                    "sea_distance_forced_km": np.nan,
                    "first_chokepoint": None,
                    "last_chokepoint": None,
                    "chokepoint_sequence": None,
                    "chokepoint_sequence_str": None,
                    "headline_exposure_group": "UNROUTED",
                    "main_chokepoint": None,
                    "route_group": "UNROUTED",
                    "route_mode": "unrouted",
                    "route_confidence": "very_low",
                    "route_applicability_status": row.transport_evidence,
                    "mot_codes_seen": row.mot_codes_seen,
                    "route_scenario": route_scenario,
                    "used_transshipment_hub": False,
                    "hub_port": None,
                    "hub_iso3": None,
                    "hub_basin": None,
                    "route_path_coords": [],
                    "route_basis_detail": None,
                    "internal_exit_port": None,
                }
            )
            continue

        sea_direct_km = np.nan
        direct_coords = []
        try:
            sea_direct_km, direct_route = sea_distance_km(best["reporter_lonlat"], best["partner_lonlat"])
            direct_coords = direct_route["geometry"]["coordinates"]
        except Exception:
            pass

        cp_sequence = best["cp_sequence"]
        sea_forced_km = np.nan
        route_coords = direct_coords
        if cp_sequence:
            try:
                sea_forced_km, route_coords = forced_path_distance(best["reporter_lonlat"], best["partner_lonlat"], cp_sequence)
            except Exception:
                pass

        sea_km = sea_forced_km if np.isfinite(sea_forced_km) else sea_direct_km
        route_mode = "forced_chokepoint" if cp_sequence else "direct"
        first_chokepoint = cp_sequence[0] if cp_sequence else None
        last_chokepoint = cp_sequence[-1] if cp_sequence else None
        chokepoint_sequence_str = " -> ".join(cp_sequence) if cp_sequence else None

        hub_choice = maybe_assign_hub(best["reporter_basin"], best["partner_basin"])
        used_hub = hub_choice is not None and best["distance_km"] > 2000 and best["reporter_basin"] != best["partner_basin"]
        route_confidence = assign_confidence(
            reporter_iso3=reporter_iso3,
            reporter_port_basis=best["reporter_port_basis"],
            partner_port_basis=best["partner_port_basis"],
            reporter_basin=best["reporter_basin"],
            partner_basin=best["partner_basin"],
            cp_sequence=cp_sequence,
            gc_km=best["distance_km"],
        )

        route_basis_parts = []
        if row.routing_decision == "route_by_observed_sea":
            route_basis_parts.append("SEA_OBSERVED_MOTCODE")
        elif row.routing_decision == "route_by_inference":
            route_basis_parts.append("SEA_INFERRED_FROM_UNKNOWN_TRANSPORT")

        route_basis_parts.append(route_scenario)
        if best["reporter_port_basis"] != "DIRECT_PORT":
            route_basis_parts.append(best["reporter_port_basis"])
        if best["partner_port_basis"] != "DIRECT_PORT":
            route_basis_parts.append(best["partner_port_basis"])
        if used_hub:
            route_basis_parts.append("OPTIONAL_HUB_INFERRED")

        records.append(
            {
                "reporter_iso3": reporter_iso3,
                "partner_iso3": partner_iso3,
                "partner2_iso3": partner2_iso3,
                "reporter_port": best["reporter_port"],
                "partner_port": best["partner_port"],
                "reporter_gateway_iso3": best["reporter_gateway_iso3"],
                "partner_gateway_iso3": best["partner_gateway_iso3"],
                "reporter_basin": best["reporter_basin"],
                "partner_basin": best["partner_basin"],
                "distance_km": round(best["distance_km"], 2),
                "sea_distance_km": round(sea_km, 2) if np.isfinite(sea_km) else np.nan,
                "sea_distance_direct_km": round(sea_direct_km, 2) if np.isfinite(sea_direct_km) else np.nan,
                "sea_distance_forced_km": round(sea_forced_km, 2) if np.isfinite(sea_forced_km) else np.nan,
                "first_chokepoint": first_chokepoint,
                "last_chokepoint": last_chokepoint,
                "chokepoint_sequence": cp_sequence,
                "chokepoint_sequence_str": chokepoint_sequence_str,
                "headline_exposure_group": headline_exposure_group(cp_sequence, best["reporter_basin"], best["partner_basin"]),
                "main_chokepoint": first_chokepoint,
                "route_group": route_group(cp_sequence, best["reporter_basin"], best["partner_basin"]),
                "route_mode": route_mode,
                "route_status": "ROUTED",
                "route_basis": "|".join(route_basis_parts),
                "route_basis_detail": best.get("route_basis_detail"),
                "internal_exit_port": best.get("internal_exit_port"),
                "route_confidence": route_confidence,
                "route_applicability_status": row.transport_evidence,
                "transport_evidence": row.transport_evidence,
                "routing_decision": row.routing_decision,
                "mot_codes_seen": row.mot_codes_seen,
                "route_scenario": route_scenario,
                "used_transshipment_hub": used_hub,
                "hub_port": hub_choice.get("hub_port") if used_hub else None,
                "hub_iso3": hub_choice.get("hub_iso3") if used_hub else None,
                "hub_basin": hub_choice.get("hub_basin") if used_hub else None,
                "route_path_coords": route_coords,
            }
        )

    dim_trade_routes_new = pd.DataFrame.from_records(records)
    display(
        dim_trade_routes_new[
            [
                "reporter_iso3",
                "partner_iso3",
                "reporter_port",
                "partner_port",
                "reporter_basin",
                "partner_basin",
                "first_chokepoint",
                "last_chokepoint",
                "chokepoint_sequence",
                "headline_exposure_group",
                "route_confidence",
                "transport_evidence",
                "routing_decision",
                "route_basis",
                "route_basis_detail",
                "internal_exit_port",
            ]
        ].head(30)
    )
    display(
        dim_trade_routes_new["headline_exposure_group"]
        .value_counts(dropna=False)
        .rename_axis("headline_exposure_group")
        .reset_index(name="route_count")
    )
    display(
        dim_trade_routes_new["route_confidence"]
        .value_counts(dropna=False)
        .rename_axis("route_confidence")
        .reset_index(name="route_count")
    )
    display(
        dim_trade_routes_new["routing_decision"]
        .value_counts(dropna=False)
        .rename_axis("routing_decision")
        .reset_index(name="route_count")
    )

    print("\nStrategic chokepoint QA checks")
    for reporter_iso3, chokepoint_name in [
        ("TUR", "Turkish Straits"),
        ("EGY", "Suez Canal"),
        ("PAN", "Panama Canal"),
        ("IDN", "Malacca Strait"),
    ]:
        count = dim_trade_routes_new[
            (dim_trade_routes_new["reporter_iso3"] == reporter_iso3)
            & dim_trade_routes_new["chokepoint_sequence"].map(
                lambda value: isinstance(value, list) and chokepoint_name in value
            )
        ].shape[0]
        print(f"{reporter_iso3} -> {chokepoint_name}: {count}")

    return dim_trade_routes_new
