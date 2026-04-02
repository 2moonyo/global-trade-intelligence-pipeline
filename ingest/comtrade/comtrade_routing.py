from __future__ import annotations

import argparse
import contextlib
import io
import importlib.util
import json
import logging
import os
import sys
import warnings
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingest.common.cloud_config import GcpCloudConfig
from ingest.common.gcs_io import download_gcs_uri
from ingest.common.run_artifacts import (
    append_manifest,
    build_run_id,
    configure_logger,
    duration_seconds,
    json_ready,
)
from ingest.comtrade.comtrade_silver import write_dataframe_if_changed


LOGGER_NAME = "comtrade.routing"
LOG_DIR = PROJECT_ROOT / "logs" / "comtrade"
LOG_PATH = LOG_DIR / "comtrade_routing.log"
MANIFEST_PATH = LOG_DIR / "comtrade_routing_manifest.jsonl"
DEFAULT_NOTEBOOK_PATH = (
    PROJECT_ROOT / "notebooks" / "comtrade" / "05_comtrade_silver_enrichment_scenario_graph_routing_v4.ipynb"
)
DEFAULT_NATURAL_EARTH_PATH = (
    PROJECT_ROOT / "data" / "reference" / "geography" / "ne_110m_admin_0_countries.zip"
)
DEFAULT_NATURAL_EARTH_GCS_URI_ENV = "COMTRADE_NATURAL_EARTH_GCS_URI"


@dataclass(frozen=True)
class ComtradeRoutingConfig:
    silver_root: Path = PROJECT_ROOT / "data" / "silver" / "comtrade"
    notebook_path: Path = DEFAULT_NOTEBOOK_PATH
    natural_earth_path: Path = DEFAULT_NATURAL_EARTH_PATH
    natural_earth_gcs_uri: str | None = None
    log_path: Path = LOG_PATH
    manifest_path: Path = MANIFEST_PATH
    log_level: str = "INFO"
    skip_unchanged: bool = True


def _packaged_natural_earth_fallbacks() -> list[Path]:
    candidates: list[Path] = []

    pyogrio_spec = importlib.util.find_spec("pyogrio")
    if pyogrio_spec and pyogrio_spec.origin:
        pyogrio_root = Path(pyogrio_spec.origin).resolve().parent
        candidates.append(
            pyogrio_root / "tests" / "fixtures" / "naturalearth_lowres" / "naturalearth_lowres.shp"
        )

    geopandas_spec = importlib.util.find_spec("geopandas")
    if geopandas_spec and geopandas_spec.origin:
        geopandas_root = Path(geopandas_spec.origin).resolve().parent
        candidates.append(
            geopandas_root / "datasets" / "naturalearth_lowres" / "naturalearth_lowres.shp"
        )

    return [path for path in candidates if path.exists()]


def ensure_local_natural_earth(
    *,
    natural_earth_path: Path,
    natural_earth_gcs_uri: str | None,
    logger,
) -> Path:
    if natural_earth_path.exists():
        return natural_earth_path

    for fallback_path in _packaged_natural_earth_fallbacks():
        logger.warning(
            "Natural Earth cache missing at %s; using packaged fallback at %s",
            natural_earth_path,
            fallback_path,
        )
        return fallback_path

    if not natural_earth_gcs_uri:
        raise FileNotFoundError(
            f"Missing Natural Earth reference file at {natural_earth_path}. "
            f"Provide a cached local zip there, rely on an installed packaged fallback, "
            f"or set {DEFAULT_NATURAL_EARTH_GCS_URI_ENV} / --natural-earth-gcs-uri."
        )

    config = GcpCloudConfig.from_env()
    logger.info("Downloading Natural Earth reference from %s to %s", natural_earth_gcs_uri, natural_earth_path)
    download_gcs_uri(
        uri=natural_earth_gcs_uri,
        destination_path=natural_earth_path,
        project_id=config.gcp_project_id,
    )
    return natural_earth_path


def load_notebook_source(notebook_path: Path) -> str:
    payload = json.loads(notebook_path.read_text(encoding="utf-8"))
    code_chunks: list[str] = []
    for cell in payload.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        source = "".join(cell.get("source", []))
        if source.strip():
            code_chunks.append(source)
    return "\n\n".join(code_chunks)


def patch_notebook_source(
    source: str,
    *,
    project_root: Path,
    silver_root: Path,
    natural_earth_path: Path,
) -> str:
    project_root_block = """PROJECT_ROOT = Path.cwd()
while not (PROJECT_ROOT / "pyproject.toml").exists() and PROJECT_ROOT.parent != PROJECT_ROOT:
    PROJECT_ROOT = PROJECT_ROOT.parent
"""
    source = source.replace(
        project_root_block,
        f'PROJECT_ROOT = Path(r"{project_root}")\n',
        1,
    )
    source = source.replace(
        'FACT_ROOT = PROJECT_ROOT / "data" / "silver" / "comtrade" / "comtrade_fact"',
        f'FACT_ROOT = Path(r"{silver_root / "comtrade_fact"}")',
        1,
    )
    source = source.replace(
        'FACT_FILES = sorted(FACT_ROOT.rglob("ref_year=*/reporter_iso3=*/*.parquet"))',
        'FACT_FILES = sorted(FACT_ROOT.glob("year=*/month=*/reporter_iso3=*/cmd_code=*/flow_code=*/comtrade_fact.parquet"))\nif not FACT_FILES:\n    FACT_FILES = sorted(FACT_ROOT.rglob("ref_year=*/reporter_iso3=*/*.parquet"))',
        1,
    )
    source = source.replace(
        'DIMENSIONS_ROOT = PROJECT_ROOT / "data" / "silver" / "comtrade" / "dimensions"',
        f'DIMENSIONS_ROOT = Path(r"{silver_root / "dimensions"}")',
        1,
    )
    source = source.replace(
        'DIM_OUTPUT_PATH = PROJECT_ROOT / "data" / "silver" / "comtrade" / "dim_trade_routes.parquet"',
        f'DIM_OUTPUT_PATH = Path(r"{silver_root / "dim_trade_routes.parquet"}")',
        1,
    )

    source = source.replace(
        "import folium",
        "try:\n    import folium\nexcept ModuleNotFoundError:\n    folium = None",
        1,
    )
    source = source.replace(
        "from IPython.display import display",
        "try:\n    from IPython.display import display\nexcept ModuleNotFoundError:\n    def display(*args, **kwargs):\n        return None",
        1,
    )

    natural_earth_block = """# Load standard low-resolution world map directly from Natural Earth's cloud storage
url = "https://naturalearth.s3.amazonaws.com/110m_cultural/ne_110m_admin_0_countries.zip"
world = gpd.read_file(url)
"""
    source = source.replace(
        natural_earth_block,
        f'# Load Natural Earth geography from the local reference cache\nworld = gpd.read_file(r"{natural_earth_path}")\n',
        1,
    )
    schema_drift_block = """# Bulletproof column extraction (handles Natural Earth schema drift)
iso_col = 'ADM0_A3' if 'ADM0_A3' in world.columns else 'ISO_A3' if 'ISO_A3' in world.columns else 'ISO_A3_EH'
name_col = 'NAME' if 'NAME' in world.columns else 'ADMIN'

world['iso_a3'] = world[iso_col].astype(str)
world['name'] = world[name_col].astype(str)
"""
    source = source.replace(
        schema_drift_block,
        """# Bulletproof column extraction (handles Natural Earth schema drift)
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
""",
        1,
    )

    replacements = {
        "dim_country_ports.to_parquet(COUNTRY_PORT_DIM_PATH, index=False)": "_write_parquet_if_changed(dim_country_ports, COUNTRY_PORT_DIM_PATH)",
        "dim_port_basin.to_parquet(PORT_BASIN_DIM_PATH, index=False)": "_write_parquet_if_changed(dim_port_basin, PORT_BASIN_DIM_PATH)",
        "dim_chokepoint.to_parquet(CHOKEPOINT_DIM_PATH, index=False)": "_write_parquet_if_changed(dim_chokepoint, CHOKEPOINT_DIM_PATH)",
        "basin_graph_edges.to_parquet(BASIN_EDGE_PATH, index=False)": "_write_parquet_if_changed(basin_graph_edges, BASIN_EDGE_PATH)",
        "dim_transshipment_hub.to_parquet(TRANSHIPMENT_HUB_PATH, index=False)": "_write_parquet_if_changed(dim_transshipment_hub, TRANSHIPMENT_HUB_PATH)",
        "bridge_basin_default_hubs.to_parquet(BASIN_HUB_BRIDGE_PATH, index=False)": "_write_parquet_if_changed(bridge_basin_default_hubs, BASIN_HUB_BRIDGE_PATH)",
        "bridge_port_basin_chokepoints.to_parquet(BASIN_PATH_RULES_PATH, index=False)": "_write_parquet_if_changed(bridge_port_basin_chokepoints, BASIN_PATH_RULES_PATH)",
        "dim_trade_routes_export.to_parquet(DIM_OUTPUT_PATH, index=False)": "_write_parquet_if_changed(dim_trade_routes_export, DIM_OUTPUT_PATH)",
    }
    for old, new in replacements.items():
        source = source.replace(old, new)

    if "print(DIM_OUTPUT_PATH)" in source:
        source = source.split("print(DIM_OUTPUT_PATH)", 1)[0] + 'print(DIM_OUTPUT_PATH)\n'

    return source


def build_route_applicability(route_candidates: pd.DataFrame) -> pd.DataFrame:
    applicability = route_candidates.copy()
    applicability["route_applicability_status"] = pd.Series("NO_MOT_DATA", index=applicability.index)
    maritime_mask = applicability["has_sea"] | applicability["has_inland_water"]
    non_maritime_mask = applicability["has_non_marine"] & ~maritime_mask
    unknown_mask = applicability["has_unknown"] & ~maritime_mask & ~non_maritime_mask

    applicability.loc[maritime_mask, "route_applicability_status"] = "MARITIME_ELIGIBLE"
    applicability.loc[non_maritime_mask, "route_applicability_status"] = "NON_MARITIME_ONLY"
    applicability.loc[unknown_mask, "route_applicability_status"] = "UNKNOWN_MOT"

    return applicability[
        [
            "reporter_iso3",
            "partner_iso3",
            "partner2_iso3",
            "row_count",
            "trade_value_usd",
            "has_sea",
            "has_inland_water",
            "has_unknown",
            "has_non_marine",
            "mot_codes_seen",
            "route_applicability_status",
        ]
    ].sort_values(["reporter_iso3", "partner_iso3", "partner2_iso3"], na_position="last").reset_index(drop=True)


def build_code_to_iso3(project_root: Path) -> dict[int, str]:
    metadata_root = project_root / "data" / "metadata" / "comtrade"
    partners_meta = pd.read_csv(metadata_root / "partners.csv")
    mapping = {}
    for row in partners_meta.itertuples(index=False):
        try:
            code = int(getattr(row, "PartnerCode"))
        except Exception:
            continue
        iso3 = str(getattr(row, "PartnerCodeIsoAlpha3", "")).strip().upper()
        if iso3 and iso3 != "NAN":
            mapping[code] = iso3
    return mapping


@contextlib.contextmanager
def suppress_notebook_noise():
    country_converter_loggers = [
        logging.getLogger("country_converter"),
        logging.getLogger("country_converter.country_converter"),
    ]
    previous_levels = [logger.level for logger in country_converter_loggers]
    for logger in country_converter_loggers:
        logger.setLevel(logging.ERROR)

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=r"The 'unary_union' attribute is deprecated, use the 'union_all\(\)' method instead\.",
            category=DeprecationWarning,
        )
        try:
            yield
        finally:
            for logger, level in zip(country_converter_loggers, previous_levels):
                logger.setLevel(level)


def run(config: ComtradeRoutingConfig) -> dict[str, Any]:
    logger = configure_logger(
        logger_name=LOGGER_NAME,
        log_path=config.log_path,
        log_level=config.log_level,
    )
    run_id = build_run_id("comtrade_routing")
    started_at = datetime.now(timezone.utc)

    manifest_entry: dict[str, Any] = {
        "run_id": run_id,
        "asset_name": "comtrade_routing",
        "dataset_name": "comtrade",
        "status": "running",
        "started_at": started_at.isoformat(),
        "finished_at": None,
        "duration_seconds": None,
        "notebook_path": str(config.notebook_path),
        "silver_root": str(config.silver_root),
        "natural_earth_path": str(config.natural_earth_path),
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
        logger.info("Step 2/4 Load and patch routing notebook source")
        source = load_notebook_source(config.notebook_path)
        patched_source = patch_notebook_source(
            source,
            project_root=PROJECT_ROOT,
            silver_root=config.silver_root,
            natural_earth_path=natural_earth_path,
        )
        tracked_write_results: dict[str, dict[str, Any]] = {}

        def tracked_write(df, path):
            result = write_dataframe_if_changed(
                df,
                Path(path),
                skip_unchanged=config.skip_unchanged,
            )
            tracked_write_results[str(Path(path))] = result
            return result

        namespace: dict[str, Any] = {
            "__name__": "__main__",
            "_write_parquet_if_changed": tracked_write,
            "code_to_iso3": build_code_to_iso3(PROJECT_ROOT),
        }

        logger.info("Step 3/4 Execute routing notebook logic")
        logger.info("Executing routing logic from %s", config.notebook_path)
        captured_stdout = io.StringIO()
        with suppress_notebook_noise(), contextlib.redirect_stdout(captured_stdout):
            exec(compile(patched_source, str(config.notebook_path), "exec"), namespace)

        logger.info("Step 4/4 Persist routing outputs and manifest")
        route_candidates = namespace.get("route_candidates")
        dim_trade_routes = namespace.get("dim_trade_routes_export")
        if dim_trade_routes is None:
            dim_trade_routes = namespace.get("dim_trade_routes")
        route_applicability_path = namespace.get("ROUTE_APPLICABILITY_PATH")
        if route_candidates is None or dim_trade_routes is None or route_applicability_path is None:
            raise RuntimeError("Routing notebook execution did not produce the expected route_candidates / dim_trade_routes outputs.")

        route_applicability = build_route_applicability(route_candidates)
        route_applicability_result = write_dataframe_if_changed(
            route_applicability,
            Path(route_applicability_path),
            skip_unchanged=config.skip_unchanged,
        )

        output_vars = [
            "COUNTRY_PORT_DIM_PATH",
            "PORT_BASIN_DIM_PATH",
            "CHOKEPOINT_DIM_PATH",
            "BASIN_EDGE_PATH",
            "TRANSHIPMENT_HUB_PATH",
            "BASIN_HUB_BRIDGE_PATH",
            "BASIN_PATH_RULES_PATH",
            "DIM_OUTPUT_PATH",
        ]
        written_outputs: dict[str, Any] = {"route_applicability": route_applicability_result}
        for key in output_vars:
            output_path = namespace.get(key)
            if output_path is not None:
                written_outputs[key] = tracked_write_results.get(
                    str(Path(output_path)),
                    {
                        "path": str(output_path),
                        "exists": Path(output_path).exists(),
                    },
                )

        manifest_entry["route_candidate_count"] = int(len(route_candidates))
        manifest_entry["route_fact_count"] = int(len(dim_trade_routes))
        manifest_entry["written_outputs"] = written_outputs
        manifest_entry["status"] = "completed"
        finished_at = datetime.now(timezone.utc)
        manifest_entry["finished_at"] = finished_at.isoformat()
        manifest_entry["duration_seconds"] = duration_seconds(started_at, finished_at)
        append_manifest(config.manifest_path, manifest_entry)
        logger.info(
            "Finished Comtrade routing run_id=%s route_candidates=%s routed_pairs=%s duration_s=%.3f",
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
        logger.exception("Comtrade routing failed run_id=%s", run_id)
        raise


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the authoritative v4 Comtrade routing logic with a local Natural Earth cache."
    )
    parser.add_argument("--silver-root", default=str(ComtradeRoutingConfig.silver_root))
    parser.add_argument("--notebook-path", default=str(DEFAULT_NOTEBOOK_PATH))
    parser.add_argument("--natural-earth-path", default=str(DEFAULT_NATURAL_EARTH_PATH))
    parser.add_argument(
        "--natural-earth-gcs-uri",
        default=None,
        help=f"Optional gs:// URI used to populate the local Natural Earth cache when it is missing. Can also be set with {DEFAULT_NATURAL_EARTH_GCS_URI_ENV}.",
    )
    parser.add_argument("--overwrite-unchanged", action="store_true", help="Rewrite routing outputs even when the parquet content is unchanged.")
    parser.add_argument("--log-level", default="INFO")
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()
    natural_earth_gcs_uri = args.natural_earth_gcs_uri or os.getenv(DEFAULT_NATURAL_EARTH_GCS_URI_ENV)

    config = ComtradeRoutingConfig(
        silver_root=Path(args.silver_root),
        notebook_path=Path(args.notebook_path),
        natural_earth_path=Path(args.natural_earth_path),
        natural_earth_gcs_uri=natural_earth_gcs_uri,
        log_level=args.log_level,
        skip_unchanged=not args.overwrite_unchanged,
    )
    summary = run(config)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
