from __future__ import annotations

from pathlib import Path
from typing import Any

import country_converter as coco
import geopandas as gpd
import numpy as np
import pandas as pd

from ingest.comtrade.routing.constants import (
    BASIN_GRAPH_EDGE_ROWS,
    BASIN_HUB_BRIDGE_ROWS,
    CHOKEPOINT_ROWS,
    CHOKEPOINT_ZONE_OF_INFLUENCE_METERS,
    INLAND_WATER_CODES,
    NON_MARINE_CODES,
    SEA_CODES,
    STRATEGIC_PORT_KEEP,
    TRANSHIPMENT_HUB_ROWS,
    UNKNOWN_CODES,
)
from ingest.comtrade.routing.geometry import (
    buffered_point_wkb_from_lon_lat,
    linestring_wkb_from_coords,
    point_wkb_from_lon_lat,
)
from ingest.comtrade.routing.helpers import display, infer_port_basin_with_override, normalize_port_name
from ingest.comtrade.routing.metrics import apply_scenario_weights


def build_port_dimensions(
    *,
    port_index_path: Path,
    country_port_dim_path: Path,
    port_basin_dim_path: Path,
    write_dataframe_if_changed,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    port_raw = pd.read_csv(port_index_path, low_memory=False)

    port_candidates = port_raw.rename(
        columns={
            "Main Port Name": "main_port_name",
            "Alternate Port Name": "alternate_port_name",
            "Country Code": "country_label",
            "World Water Body": "world_water_body",
            "Harbor Size": "harbor_size",
            "Harbor Type": "harbor_type",
            "Harbor Use": "harbor_use",
            "Facilities - Container": "fac_container",
            "Facilities - Solid Bulk": "fac_solid_bulk",
            "Facilities - Liquid Bulk": "fac_liquid_bulk",
            "Facilities - Oil Terminal": "fac_oil_terminal",
            "Facilities - LNG Terminal": "fac_lng_terminal",
            "Latitude": "latitude",
            "Longitude": "longitude",
        }
    ).copy()

    port_candidates["port_name"] = (
        port_candidates["main_port_name"].astype("string").str.strip()
        .fillna(port_candidates["alternate_port_name"].astype("string").str.strip())
    )

    port_candidates["country_label"] = port_candidates["country_label"].astype("string").str.strip()
    port_candidates["world_water_body"] = port_candidates["world_water_body"].astype("string").str.strip()
    port_candidates["harbor_size"] = port_candidates["harbor_size"].astype("string").str.strip()
    port_candidates["harbor_type"] = port_candidates["harbor_type"].astype("string").str.strip()
    port_candidates["harbor_use"] = port_candidates["harbor_use"].astype("string").str.strip()

    port_candidates["latitude"] = pd.to_numeric(port_candidates["latitude"], errors="coerce")
    port_candidates["longitude"] = pd.to_numeric(port_candidates["longitude"], errors="coerce")

    port_candidates["iso3"] = coco.convert(
        names=port_candidates["country_label"].fillna(""),
        to="ISO3",
        not_found="not found",
    )
    port_candidates["iso3"] = pd.Series(port_candidates["iso3"], index=port_candidates.index).replace("not found", pd.NA)

    port_candidates["port_basin"] = port_candidates.apply(
        lambda row: infer_port_basin_with_override(
            row["iso3"],
            row["port_name"],
            row["world_water_body"],
            row["latitude"],
            row["longitude"],
        ),
        axis=1,
    )

    size_rank = {"Very Small": 1, "Small": 2, "Medium": 3, "Large": 4}
    yes_rank = {"Yes": 1, "No": 0, "Unknown": 0}

    for column in ["fac_container", "fac_solid_bulk", "fac_liquid_bulk", "fac_oil_terminal", "fac_lng_terminal"]:
        port_candidates[column] = port_candidates[column].map(yes_rank).fillna(0).astype(int)

    port_candidates["size_rank"] = port_candidates["harbor_size"].map(size_rank).fillna(0).astype(int)
    port_candidates["port_score"] = (
        port_candidates["size_rank"] * 10
        + port_candidates["fac_container"] * 5
        + port_candidates["fac_solid_bulk"] * 4
        + port_candidates["fac_liquid_bulk"] * 4
        + port_candidates["fac_oil_terminal"] * 6
        + port_candidates["fac_lng_terminal"] * 6
    )

    usable_ports = (
        port_candidates
        .dropna(subset=["iso3", "port_name", "latitude", "longitude"])
        .drop_duplicates(subset=["iso3", "port_name", "latitude", "longitude"])
        .copy()
    )
    usable_ports["port_name_norm"] = usable_ports["port_name"].map(normalize_port_name)

    strategic_keep_ports = usable_ports[
        usable_ports.apply(
            lambda row: row["iso3"] in STRATEGIC_PORT_KEEP and row["port_name_norm"] in STRATEGIC_PORT_KEEP[row["iso3"]],
            axis=1,
        )
    ].copy()

    ranked_by_basin = usable_ports.sort_values(
        ["iso3", "port_basin", "port_score", "size_rank", "port_name"],
        ascending=[True, True, False, False, True],
    ).copy()
    ranked_by_basin["basin_rank"] = ranked_by_basin.groupby(["iso3", "port_basin"]).cumcount() + 1
    diverse_ports = ranked_by_basin[ranked_by_basin["basin_rank"] <= 2].copy()

    ranked_overall = usable_ports.sort_values(
        ["iso3", "port_score", "size_rank", "port_name"],
        ascending=[True, False, False, True],
    ).copy()
    ranked_overall["overall_rank"] = ranked_overall.groupby("iso3").cumcount() + 1
    mega_ports = ranked_overall[ranked_overall["overall_rank"] <= 5].copy()

    combined_ports = pd.concat([diverse_ports, mega_ports, strategic_keep_ports], ignore_index=True).drop_duplicates(
        subset=["iso3", "port_name"]
    )
    combined_ports = combined_ports.sort_values(
        ["iso3", "port_score", "size_rank", "port_name"],
        ascending=[True, False, False, True],
    ).copy()
    combined_ports["port_rank"] = combined_ports.groupby("iso3").cumcount() + 1
    combined_ports = combined_ports.drop(columns=["port_name_norm"], errors="ignore")

    dim_country_ports = combined_ports[
        [
            "iso3",
            "port_name",
            "latitude",
            "longitude",
            "world_water_body",
            "port_basin",
            "harbor_size",
            "harbor_type",
            "harbor_use",
            "fac_container",
            "fac_solid_bulk",
            "fac_liquid_bulk",
            "fac_oil_terminal",
            "fac_lng_terminal",
            "port_score",
            "port_rank",
        ]
    ].copy()
    dim_country_ports["port_point_wkb"] = dim_country_ports.apply(
        lambda row: point_wkb_from_lon_lat(row["longitude"], row["latitude"]),
        axis=1,
    )

    dim_port_basin = (
        dim_country_ports[["port_basin", "world_water_body"]]
        .drop_duplicates()
        .sort_values(["port_basin", "world_water_body"])
        .copy()
    )

    write_dataframe_if_changed(dim_country_ports, country_port_dim_path)
    write_dataframe_if_changed(dim_port_basin, port_basin_dim_path)

    display(dim_country_ports.head(20))
    display(
        dim_country_ports.loc[
            dim_country_ports["iso3"].isin(["USA", "CHN", "RUS", "NLD", "TUR", "EGY", "PAN", "IDN"])
        ].sort_values(["iso3", "port_rank"])
    )
    return dim_country_ports, dim_port_basin


def build_country_gateway_context(
    *,
    dim_country: pd.DataFrame,
    dim_country_ports: pd.DataFrame,
    natural_earth_path: Path,
) -> tuple[pd.DataFrame, dict[str, dict[str, Any]], dict[str, list[str]]]:
    country_port_coverage = (
        dim_country_ports.groupby("iso3", as_index=False)["port_rank"]
        .count()
        .rename(columns={"port_rank": "ports_found"})
    )

    country_geo = dim_country[["iso3", "country_name", "continent", "region", "subregion", "is_eu"]].drop_duplicates().copy()
    country_geo = country_geo.merge(country_port_coverage[["iso3", "ports_found"]], on="iso3", how="left")
    country_geo["ports_found"] = country_geo["ports_found"].fillna(0).astype(int)
    country_geo["has_wpi_ports"] = country_geo["ports_found"] > 0
    country_geo["is_eu"] = country_geo["is_eu"].fillna(False).astype(bool)

    print("Dynamically computing landlocked gateways via spatial topologies...")
    world = gpd.read_file(str(natural_earth_path))

    iso_candidates = ["ADM0_A3", "ISO_A3", "ISO_A3_EH", "adm0_a3", "iso_a3"]
    name_candidates = ["NAME", "ADMIN", "name", "admin"]
    normalized_columns = {str(col).upper(): col for col in world.columns}
    iso_col = next((col for col in iso_candidates if col in world.columns), None)
    name_col = next((col for col in name_candidates if col in world.columns), None)
    if iso_col is None:
        iso_col = normalized_columns.get("ADM0_A3") or normalized_columns.get("ISO_A3") or normalized_columns.get("ISO_A3_EH")
    if name_col is None:
        name_col = normalized_columns.get("NAME") or normalized_columns.get("ADMIN")
    if iso_col is None or name_col is None:
        raise KeyError(f"Unable to determine ISO/name columns from Natural Earth data: {list(world.columns)}")

    world["iso_a3"] = world[iso_col].astype(str)
    world["name"] = world[name_col].astype(str)
    world.loc[world["name"] == "France", "iso_a3"] = "FRA"
    world.loc[world["name"] == "Norway", "iso_a3"] = "NOR"
    world.loc[world["name"] == "Somaliland", "iso_a3"] = "SOM"
    world.loc[world["name"] == "Kosovo", "iso_a3"] = "RKS"
    world.loc[world["name"] == "N. Cyprus", "iso_a3"] = "CYP"

    world = world.merge(country_geo[["iso3", "has_wpi_ports"]], left_on="iso_a3", right_on="iso3", how="left")
    world["has_wpi_ports"] = world["has_wpi_ports"].fillna(False).astype(bool)

    landlocked_gateways: dict[str, list[str]] = {}
    landlocked_iso3s = country_geo[~country_geo["has_wpi_ports"]]["iso3"].tolist()

    for iso3 in landlocked_iso3s:
        country_geom_series = world[world["iso_a3"] == iso3]["geometry"]

        if country_geom_series.empty:
            group_info = country_geo[country_geo["iso3"] == iso3]
            if not group_info.empty:
                info = group_info.iloc[0]
                if info["is_eu"]:
                    gateways = country_geo[(country_geo["is_eu"]) & (country_geo["has_wpi_ports"])]["iso3"].tolist()
                elif pd.notna(info["region"]) and info["region"] not in ["World", "Special"]:
                    gateways = country_geo[
                        (country_geo["region"] == info["region"]) & (country_geo["has_wpi_ports"])
                    ]["iso3"].tolist()
                elif info["region"] == "World":
                    gateways = ["CHN", "USA", "NLD", "SGP", "ARE", "ZAF", "BRA"]
                else:
                    gateways = []

                if gateways:
                    landlocked_gateways[iso3] = gateways
            continue

        geometry = country_geom_series.iloc[0]
        neighbors = world[world.geometry.intersects(geometry) & (world["iso_a3"] != iso3)]
        coastal_neighbors = neighbors[neighbors["has_wpi_ports"]]["iso_a3"].dropna().tolist()

        if coastal_neighbors:
            landlocked_gateways[iso3] = coastal_neighbors
        elif not neighbors.empty:
            hop2_neighbors = world[world.geometry.intersects(neighbors.geometry.unary_union) & (world["iso_a3"] != iso3)]
            hop2_coastal = hop2_neighbors[hop2_neighbors["has_wpi_ports"]]["iso_a3"].dropna().tolist()
            if hop2_coastal:
                landlocked_gateways[iso3] = hop2_coastal

    country_geo["is_landlocked_assumed"] = country_geo["iso3"].isin(landlocked_gateways.keys())
    country_flags = country_geo.set_index("iso3")[
        ["country_name", "is_landlocked_assumed", "has_wpi_ports", "continent", "region", "subregion"]
    ].to_dict("index")

    print(f"Successfully computed coastal gateways for {len(landlocked_gateways)} entities.")
    display(
        country_geo[
            ["iso3", "country_name", "ports_found", "has_wpi_ports", "is_landlocked_assumed"]
        ].sort_values(["is_landlocked_assumed", "ports_found", "iso3"], ascending=[False, True, True]).head(40)
    )
    return country_geo, country_flags, landlocked_gateways


def build_chokepoint_graph(
    *,
    route_scenario: str,
    chokepoint_dim_path: Path,
    basin_edge_path: Path,
    write_dataframe_if_changed,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    dim_chokepoint = pd.DataFrame(CHOKEPOINT_ROWS).copy()
    dim_chokepoint["zone_of_influence_radius_m"] = (
        dim_chokepoint["chokepoint_name"].map(CHOKEPOINT_ZONE_OF_INFLUENCE_METERS).astype("Int64")
    )
    dim_chokepoint["chokepoint_point_wkb"] = dim_chokepoint.apply(
        lambda row: point_wkb_from_lon_lat(row["longitude"], row["latitude"]),
        axis=1,
    )
    dim_chokepoint["zone_of_influence_wkb"] = dim_chokepoint.apply(
        lambda row: buffered_point_wkb_from_lon_lat(
            row["longitude"],
            row["latitude"],
            row["zone_of_influence_radius_m"],
        ),
        axis=1,
    )
    write_dataframe_if_changed(dim_chokepoint, chokepoint_dim_path)

    basin_graph_edges = pd.DataFrame(BASIN_GRAPH_EDGE_ROWS)
    basin_graph_edges = apply_scenario_weights(basin_graph_edges, route_scenario)
    write_dataframe_if_changed(basin_graph_edges, basin_edge_path)

    display(basin_graph_edges)
    return dim_chokepoint, basin_graph_edges


def build_transshipment_hubs(
    *,
    transshipment_hub_path: Path,
    basin_hub_bridge_path: Path,
    write_dataframe_if_changed,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    dim_transshipment_hub = pd.DataFrame(TRANSHIPMENT_HUB_ROWS)
    bridge_basin_default_hubs = pd.DataFrame(BASIN_HUB_BRIDGE_ROWS)

    write_dataframe_if_changed(dim_transshipment_hub, transshipment_hub_path)
    write_dataframe_if_changed(bridge_basin_default_hubs, basin_hub_bridge_path)

    display(dim_transshipment_hub)
    display(bridge_basin_default_hubs)
    return dim_transshipment_hub, bridge_basin_default_hubs


def build_route_candidates(
    *,
    silver_df: pd.DataFrame,
    dim_trade_routes_existing: pd.DataFrame,
    full_rebuild_dim_trade_routes: bool,
    code_to_iso3_map: dict[int, str],
) -> tuple[pd.DataFrame, set[tuple[Any, Any, Any]]]:
    work_df = silver_df.copy()

    if "partner2_iso3" not in work_df.columns:
        if "partner2Code" in work_df.columns:
            work_df["partner2_iso3"] = work_df["partner2Code"].map(code_to_iso3_map)
        else:
            work_df["partner2_iso3"] = pd.NA

    if "trade_value_usd" not in work_df.columns:
        work_df["trade_value_usd"] = 0.0

    work_df["mot_code_norm"] = pd.to_numeric(work_df.get("motCode"), errors="coerce")

    def mot_set(series):
        values = pd.to_numeric(series, errors="coerce").dropna().astype(int).unique().tolist()
        return set(values)

    def codes_seen(series):
        values = sorted(mot_set(series))
        return "|".join(str(value) for value in values)

    route_candidates = (
        work_df
        .dropna(subset=["reporter_iso3", "partner_iso3"])
        .groupby(["reporter_iso3", "partner_iso3", "partner2_iso3"], dropna=False)
        .agg(
            row_count=("partner_iso3", "size"),
            trade_value_usd=("trade_value_usd", "sum"),
            has_sea=("mot_code_norm", lambda s: bool(mot_set(s) & SEA_CODES)),
            has_inland_water=("mot_code_norm", lambda s: bool(mot_set(s) & INLAND_WATER_CODES)),
            has_unknown=("mot_code_norm", lambda s: bool(mot_set(s) & UNKNOWN_CODES)),
            has_non_marine=("mot_code_norm", lambda s: bool(mot_set(s) & NON_MARINE_CODES)),
            mot_codes_seen=("mot_code_norm", codes_seen),
        )
        .reset_index()
    )

    route_candidates["transport_evidence"] = np.select(
        [
            route_candidates["has_sea"],
            route_candidates["has_inland_water"],
            route_candidates["has_unknown"] & ~route_candidates[["has_sea", "has_inland_water", "has_non_marine"]].any(axis=1),
            route_candidates["has_non_marine"] & ~route_candidates[["has_sea", "has_inland_water"]].any(axis=1),
        ],
        ["sea_observed", "inland_water_only", "transport_unknown", "non_maritime_only"],
        default="unclassified",
    )

    existing_keys: set[tuple[Any, Any, Any]] = set()
    if (not full_rebuild_dim_trade_routes) and (not dim_trade_routes_existing.empty):
        existing_keys = set(
            dim_trade_routes_existing[["reporter_iso3", "partner_iso3", "partner2_iso3"]]
            .fillna("__NULL__")
            .itertuples(index=False, name=None)
        )

    display(
        route_candidates["transport_evidence"]
        .value_counts(dropna=False)
        .rename_axis("transport_evidence")
        .reset_index(name="route_count")
    )
    display(route_candidates.head(10))
    print("Route candidate keys prepared:", len(route_candidates))
    return route_candidates, existing_keys


def build_basin_path_audit(
    *,
    dim_trade_routes_new: pd.DataFrame,
    basin_path_rules_path: Path,
    write_dataframe_if_changed,
) -> pd.DataFrame:
    audit_records = []

    for row in dim_trade_routes_new.itertuples(index=False):
        chokepoints = row.chokepoint_sequence if isinstance(row.chokepoint_sequence, list) else []
        if not chokepoints:
            audit_records.append(
                {
                    "reporter_iso3": row.reporter_iso3,
                    "partner_iso3": row.partner_iso3,
                    "origin_basin": row.reporter_basin,
                    "destination_basin": row.partner_basin,
                    "leg_order": 0,
                    "chokepoint_name": "Open Sea / Same Basin",
                    "headline_chokepoint": row.first_chokepoint if hasattr(row, "first_chokepoint") else None,
                    "headline_exposure_group": row.headline_exposure_group,
                    "route_confidence": row.route_confidence,
                }
            )
        else:
            for index, chokepoint in enumerate(chokepoints, start=1):
                audit_records.append(
                    {
                        "reporter_iso3": row.reporter_iso3,
                        "partner_iso3": row.partner_iso3,
                        "origin_basin": row.reporter_basin,
                        "destination_basin": row.partner_basin,
                        "leg_order": index,
                        "chokepoint_name": chokepoint,
                        "headline_chokepoint": row.first_chokepoint if hasattr(row, "first_chokepoint") else chokepoints[0],
                        "headline_exposure_group": row.headline_exposure_group,
                        "route_confidence": row.route_confidence,
                    }
                )

    bridge_port_basin_chokepoints = pd.DataFrame(audit_records).sort_values(
        ["reporter_iso3", "partner_iso3", "origin_basin", "destination_basin", "leg_order"]
    )
    write_dataframe_if_changed(bridge_port_basin_chokepoints, basin_path_rules_path)
    display(bridge_port_basin_chokepoints.head(40))
    return bridge_port_basin_chokepoints


def build_dim_trade_routes(
    *,
    dim_trade_routes_new: pd.DataFrame,
    dim_trade_routes_existing: pd.DataFrame,
    full_rebuild_dim_trade_routes: bool,
    dim_output_path: Path,
    write_dataframe_if_changed,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if full_rebuild_dim_trade_routes or dim_trade_routes_existing.empty:
        dim_trade_routes = dim_trade_routes_new.copy()
    else:
        combined = pd.concat(
            [
                dim_trade_routes_existing.reindex(columns=dim_trade_routes_new.columns),
                dim_trade_routes_new,
            ],
            ignore_index=True,
        )
        dim_trade_routes = combined.drop_duplicates(
            subset=["reporter_iso3", "partner_iso3", "partner2_iso3", "route_scenario"],
            keep="last",
        ).sort_values(
            ["reporter_iso3", "partner_iso3", "partner2_iso3", "route_scenario"],
            na_position="last",
        )

    dim_trade_routes_export = dim_trade_routes.drop(columns=["route_path_coords"], errors="ignore").copy()
    dim_trade_routes_export["route_path_wkb"] = dim_trade_routes["route_path_coords"].map(linestring_wkb_from_coords)
    write_dataframe_if_changed(dim_trade_routes_export, dim_output_path)

    display(dim_trade_routes["route_mode"].value_counts(dropna=False).rename_axis("route_mode").reset_index(name="route_count"))
    display(
        dim_trade_routes["route_confidence"]
        .value_counts(dropna=False)
        .rename_axis("route_confidence")
        .reset_index(name="route_count")
    )
    display(dim_trade_routes.head(20))
    print(dim_output_path)
    return dim_trade_routes, dim_trade_routes_export
