from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from ingest.comtrade.routing.constants import (
    DEFAULT_ASSET_NAME,
    DEFAULT_LOG_PATH,
    DEFAULT_MANIFEST_PATH,
    DEFAULT_NATURAL_EARTH_PATH,
    DEFAULT_OUTPUT_SUFFIX,
    DEFAULT_PORT_INDEX_PATH,
    DEFAULT_RUN_ID_PREFIX,
    DEFAULT_SILVER_ROOT,
    DEFAULT_VERSION_LABEL,
)


@dataclass(frozen=True)
class RoutingRuntimeDefaults:
    logger_name: str
    log_path: Path
    manifest_path: Path
    output_suffix: str
    asset_name: str
    run_id_prefix: str
    version_label: str


@dataclass(frozen=True)
class ComtradeRoutingConfig:
    silver_root: Path = DEFAULT_SILVER_ROOT
    natural_earth_path: Path = DEFAULT_NATURAL_EARTH_PATH
    natural_earth_gcs_uri: str | None = None
    port_index_path: Path = DEFAULT_PORT_INDEX_PATH
    route_scenario: str = "default_shortest"
    full_rebuild_dim_trade_routes: bool = True
    output_suffix: str = DEFAULT_OUTPUT_SUFFIX
    log_path: Path = DEFAULT_LOG_PATH
    manifest_path: Path = DEFAULT_MANIFEST_PATH
    log_level: str = "INFO"
    skip_unchanged: bool = True
    logger_name: str = "comtrade.routing_v3"
    asset_name: str = DEFAULT_ASSET_NAME
    run_id_prefix: str = DEFAULT_RUN_ID_PREFIX
    version_label: str = DEFAULT_VERSION_LABEL


@dataclass(frozen=True)
class RoutingOutputPaths:
    dim_output_path: Path
    country_port_dim_path: Path
    port_basin_dim_path: Path
    route_applicability_path: Path
    chokepoint_dim_path: Path
    basin_edge_path: Path
    basin_path_rules_path: Path
    transshipment_hub_path: Path
    basin_hub_bridge_path: Path


@dataclass(frozen=True)
class RoutingInputs:
    silver_df: pd.DataFrame
    dim_country: pd.DataFrame
    dim_trade_routes_existing: pd.DataFrame
    code_to_iso3_map: dict[int, str]


@dataclass(frozen=True)
class RoutingArtifacts:
    route_candidates: pd.DataFrame
    dim_trade_routes_export: pd.DataFrame
