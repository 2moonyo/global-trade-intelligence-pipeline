from __future__ import annotations

import contextlib
import io
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from ingest.common.run_artifacts import (
    append_manifest,
    build_run_id,
    configure_logger,
    duration_seconds,
    json_ready,
)
from ingest.comtrade.comtrade_silver import write_dataframe_if_changed
from ingest.comtrade.routing.builders import (
    build_basin_path_audit,
    build_chokepoint_graph,
    build_country_gateway_context,
    build_dim_trade_routes,
    build_port_dimensions,
    build_route_candidates,
    build_transshipment_hubs,
)
from ingest.comtrade.routing.engine import build_trade_routes
from ingest.comtrade.routing.helpers import suppress_routing_noise
from ingest.comtrade.routing.io import (
    build_output_paths,
    build_route_applicability,
    ensure_local_natural_earth,
    load_routing_inputs,
)
from ingest.comtrade.routing.models import ComtradeRoutingConfig, RoutingArtifacts, RoutingOutputPaths


def run_pipeline(
    *,
    config: ComtradeRoutingConfig,
    natural_earth_path: Path,
    output_paths: RoutingOutputPaths,
    write_dataframe_if_changed,
) -> RoutingArtifacts:
    inputs = load_routing_inputs(config, output_paths)
    dim_country_ports, _ = build_port_dimensions(
        port_index_path=config.port_index_path,
        country_port_dim_path=output_paths.country_port_dim_path,
        port_basin_dim_path=output_paths.port_basin_dim_path,
        write_dataframe_if_changed=write_dataframe_if_changed,
    )
    _, country_flags, landlocked_gateways = build_country_gateway_context(
        dim_country=inputs.dim_country,
        dim_country_ports=dim_country_ports,
        natural_earth_path=natural_earth_path,
    )
    dim_chokepoint, basin_graph_edges = build_chokepoint_graph(
        route_scenario=config.route_scenario,
        chokepoint_dim_path=output_paths.chokepoint_dim_path,
        basin_edge_path=output_paths.basin_edge_path,
        write_dataframe_if_changed=write_dataframe_if_changed,
    )
    dim_transshipment_hub, bridge_basin_default_hubs = build_transshipment_hubs(
        transshipment_hub_path=output_paths.transshipment_hub_path,
        basin_hub_bridge_path=output_paths.basin_hub_bridge_path,
        write_dataframe_if_changed=write_dataframe_if_changed,
    )
    route_candidates, existing_keys = build_route_candidates(
        silver_df=inputs.silver_df,
        dim_trade_routes_existing=inputs.dim_trade_routes_existing,
        full_rebuild_dim_trade_routes=config.full_rebuild_dim_trade_routes,
        code_to_iso3_map=inputs.code_to_iso3_map,
    )
    dim_trade_routes_new = build_trade_routes(
        route_candidates=route_candidates,
        existing_keys=existing_keys,
        dim_country_ports=dim_country_ports,
        basin_graph_edges=basin_graph_edges,
        dim_chokepoint=dim_chokepoint,
        dim_transshipment_hub=dim_transshipment_hub,
        bridge_basin_default_hubs=bridge_basin_default_hubs,
        country_flags=country_flags,
        landlocked_gateways=landlocked_gateways,
        route_scenario=config.route_scenario,
    )
    build_basin_path_audit(
        dim_trade_routes_new=dim_trade_routes_new,
        basin_path_rules_path=output_paths.basin_path_rules_path,
        write_dataframe_if_changed=write_dataframe_if_changed,
    )
    _, dim_trade_routes_export = build_dim_trade_routes(
        dim_trade_routes_new=dim_trade_routes_new,
        dim_trade_routes_existing=inputs.dim_trade_routes_existing,
        full_rebuild_dim_trade_routes=config.full_rebuild_dim_trade_routes,
        dim_output_path=output_paths.dim_output_path,
        write_dataframe_if_changed=write_dataframe_if_changed,
    )
    return RoutingArtifacts(
        route_candidates=route_candidates,
        dim_trade_routes_export=dim_trade_routes_export,
    )


def run(config: ComtradeRoutingConfig) -> dict[str, Any]:
    logger = configure_logger(
        logger_name=config.logger_name,
        log_path=config.log_path,
        log_level=config.log_level,
    )
    run_id = build_run_id(config.run_id_prefix)
    started_at = datetime.now(timezone.utc)
    output_paths = build_output_paths(config.silver_root, config.output_suffix)

    manifest_entry: dict[str, Any] = {
        "run_id": run_id,
        "asset_name": config.asset_name,
        "dataset_name": "comtrade",
        "status": "running",
        "started_at": started_at.isoformat(),
        "finished_at": None,
        "duration_seconds": None,
        "silver_root": str(config.silver_root),
        "natural_earth_path": str(config.natural_earth_path),
        "output_suffix": config.output_suffix,
        "route_scenario": config.route_scenario,
        "full_rebuild_dim_trade_routes": config.full_rebuild_dim_trade_routes,
        "skip_unchanged": config.skip_unchanged,
        "written_outputs": {},
        "route_candidate_count": None,
        "route_fact_count": None,
        "error_summary": None,
    }

    try:
        logger.info("Step 1/4 Resolve Natural Earth reference")
        natural_earth_path = ensure_local_natural_earth(
            natural_earth_path=config.natural_earth_path,
            natural_earth_gcs_uri=config.natural_earth_gcs_uri,
            logger=logger,
        )
        manifest_entry["natural_earth_path"] = str(natural_earth_path)

        logger.info("Step 2/4 Load silver inputs and run standalone routing logic")
        tracked_write_results: dict[str, dict[str, Any]] = {}

        def tracked_write(df: pd.DataFrame, path: Path | str) -> dict[str, Any]:
            result = write_dataframe_if_changed(
                df,
                Path(path),
                skip_unchanged=config.skip_unchanged,
            )
            tracked_write_results[str(Path(path))] = result
            return result

        captured_stdout = io.StringIO()
        with suppress_routing_noise(), contextlib.redirect_stdout(captured_stdout):
            artifacts = run_pipeline(
                config=config,
                natural_earth_path=natural_earth_path,
                output_paths=output_paths,
                write_dataframe_if_changed=tracked_write,
            )

        logger.info("Step 3/4 Persist route applicability and collect %s output metadata", config.version_label)
        route_applicability = build_route_applicability(artifacts.route_candidates)
        route_applicability_result = write_dataframe_if_changed(
            route_applicability,
            output_paths.route_applicability_path,
            skip_unchanged=config.skip_unchanged,
        )

        written_outputs: dict[str, Any] = {"route_applicability": route_applicability_result}
        for key, path in [
            ("COUNTRY_PORT_DIM_PATH", output_paths.country_port_dim_path),
            ("PORT_BASIN_DIM_PATH", output_paths.port_basin_dim_path),
            ("CHOKEPOINT_DIM_PATH", output_paths.chokepoint_dim_path),
            ("BASIN_EDGE_PATH", output_paths.basin_edge_path),
            ("TRANSHIPMENT_HUB_PATH", output_paths.transshipment_hub_path),
            ("BASIN_HUB_BRIDGE_PATH", output_paths.basin_hub_bridge_path),
            ("BASIN_PATH_RULES_PATH", output_paths.basin_path_rules_path),
            ("DIM_OUTPUT_PATH", output_paths.dim_output_path),
        ]:
            written_outputs[key] = tracked_write_results.get(
                str(path),
                {"path": str(path), "exists": path.exists()},
            )

        logger.info("Step 4/4 Write manifest")
        manifest_entry["route_candidate_count"] = int(len(artifacts.route_candidates))
        manifest_entry["route_fact_count"] = int(len(artifacts.dim_trade_routes_export))
        manifest_entry["written_outputs"] = written_outputs
        manifest_entry["status"] = "completed"
        finished_at = datetime.now(timezone.utc)
        manifest_entry["finished_at"] = finished_at.isoformat()
        manifest_entry["duration_seconds"] = duration_seconds(started_at, finished_at)
        append_manifest(config.manifest_path, manifest_entry)
        logger.info(
            "Finished Comtrade routing %s run_id=%s route_candidates=%s routed_pairs=%s duration_s=%.3f",
            config.version_label,
            run_id,
            manifest_entry["route_candidate_count"],
            manifest_entry["route_fact_count"],
            manifest_entry["duration_seconds"],
        )
        return json_ready(manifest_entry)
    except Exception as exc:
        manifest_entry["status"] = "failed"
        finished_at = datetime.now(timezone.utc)
        manifest_entry["finished_at"] = finished_at.isoformat()
        manifest_entry["duration_seconds"] = duration_seconds(started_at, finished_at)
        manifest_entry["error_summary"] = str(exc)
        append_manifest(config.manifest_path, manifest_entry)
        logger.exception("Comtrade routing %s failed run_id=%s", config.version_label, run_id)
        raise
