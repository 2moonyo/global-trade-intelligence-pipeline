from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from ingest.comtrade.routing.constants import DEFAULT_NATURAL_EARTH_GCS_URI_ENV
from ingest.comtrade.routing.models import ComtradeRoutingConfig, RoutingRuntimeDefaults
from ingest.comtrade.routing.runtime import run


def build_arg_parser(runtime_defaults: RoutingRuntimeDefaults) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=f"Run the standalone {runtime_defaults.version_label} Comtrade routing logic without the notebook dependency."
    )
    parser.add_argument("--silver-root", default=str(ComtradeRoutingConfig.silver_root))
    parser.add_argument("--natural-earth-path", default=str(ComtradeRoutingConfig.natural_earth_path))
    parser.add_argument(
        "--natural-earth-gcs-uri",
        default=None,
        help=(
            "Optional gs:// URI used to populate the local Natural Earth cache when it is missing. "
            f"Can also be set with {DEFAULT_NATURAL_EARTH_GCS_URI_ENV}."
        ),
    )
    parser.add_argument("--port-index-path", default=str(ComtradeRoutingConfig.port_index_path))
    parser.add_argument(
        "--route-scenario",
        default=ComtradeRoutingConfig.route_scenario,
        choices=["default_shortest", "suez_disrupted", "panama_disrupted", "cape_preferred", "risk_avoidance"],
    )
    parser.add_argument("--output-suffix", default=runtime_defaults.output_suffix)
    parser.add_argument(
        "--incremental",
        action="store_true",
        help=f"Append/update {runtime_defaults.version_label} trade routes instead of performing a full rebuild.",
    )
    parser.add_argument(
        "--overwrite-unchanged",
        action="store_true",
        help=f"Rewrite {runtime_defaults.version_label} routing outputs even when parquet content is unchanged.",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser


def build_config_from_args(args: argparse.Namespace, runtime_defaults: RoutingRuntimeDefaults) -> ComtradeRoutingConfig:
    natural_earth_gcs_uri = args.natural_earth_gcs_uri or os.getenv(DEFAULT_NATURAL_EARTH_GCS_URI_ENV)
    return ComtradeRoutingConfig(
        silver_root=Path(args.silver_root),
        natural_earth_path=Path(args.natural_earth_path),
        natural_earth_gcs_uri=natural_earth_gcs_uri,
        port_index_path=Path(args.port_index_path),
        route_scenario=args.route_scenario,
        full_rebuild_dim_trade_routes=not args.incremental,
        output_suffix=args.output_suffix,
        log_path=runtime_defaults.log_path,
        manifest_path=runtime_defaults.manifest_path,
        log_level=args.log_level,
        skip_unchanged=not args.overwrite_unchanged,
        logger_name=runtime_defaults.logger_name,
        asset_name=runtime_defaults.asset_name,
        run_id_prefix=runtime_defaults.run_id_prefix,
        version_label=runtime_defaults.version_label,
    )


def main(runtime_defaults: RoutingRuntimeDefaults) -> None:
    parser = build_arg_parser(runtime_defaults)
    args = parser.parse_args()
    config = build_config_from_args(args, runtime_defaults)
    summary = run(config)
    print(json.dumps(summary, indent=2))
