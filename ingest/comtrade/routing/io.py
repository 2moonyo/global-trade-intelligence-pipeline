from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd

from ingest.common.cloud_config import GcpCloudConfig
from ingest.common.gcs_io import download_gcs_uri
from ingest.comtrade.routing.constants import DEFAULT_NATURAL_EARTH_GCS_URI_ENV, PROJECT_ROOT
from ingest.comtrade.routing.helpers import build_code_to_iso3
from ingest.comtrade.routing.models import ComtradeRoutingConfig, RoutingInputs, RoutingOutputPaths


def suffix_path(path: Path, suffix: str) -> Path:
    if not suffix:
        return path
    return path.with_name(f"{path.stem}{suffix}{path.suffix}")


def build_output_paths(silver_root: Path, suffix: str) -> RoutingOutputPaths:
    dimensions_root = silver_root / "dimensions"
    return RoutingOutputPaths(
        dim_output_path=suffix_path(silver_root / "dim_trade_routes.parquet", suffix),
        country_port_dim_path=suffix_path(dimensions_root / "dim_country_ports.parquet", suffix),
        port_basin_dim_path=suffix_path(dimensions_root / "dim_port_basin.parquet", suffix),
        route_applicability_path=suffix_path(dimensions_root / "bridge_country_route_applicability.parquet", suffix),
        chokepoint_dim_path=suffix_path(dimensions_root / "dim_chokepoint.parquet", suffix),
        basin_edge_path=suffix_path(dimensions_root / "bridge_basin_graph_edges.parquet", suffix),
        basin_path_rules_path=suffix_path(dimensions_root / "bridge_port_basin_chokepoints.parquet", suffix),
        transshipment_hub_path=suffix_path(dimensions_root / "dim_transshipment_hub.parquet", suffix),
        basin_hub_bridge_path=suffix_path(dimensions_root / "bridge_basin_default_hubs.parquet", suffix),
    )


def packaged_natural_earth_fallbacks() -> list[Path]:
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


def ensure_local_natural_earth(*, natural_earth_path: Path, natural_earth_gcs_uri: str | None, logger) -> Path:
    if natural_earth_path.exists():
        return natural_earth_path

    for fallback_path in packaged_natural_earth_fallbacks():
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


def load_routing_inputs(config: ComtradeRoutingConfig, output_paths: RoutingOutputPaths) -> RoutingInputs:
    fact_root = config.silver_root / "comtrade_fact"
    fact_files = sorted(
        fact_root.glob("year=*/month=*/reporter_iso3=*/cmd_code=*/flow_code=*/comtrade_fact.parquet")
    )
    if not fact_files:
        fact_files = sorted(fact_root.rglob("ref_year=*/reporter_iso3=*/*.parquet"))
    if not fact_files:
        raise FileNotFoundError(f"No parquet fact files found under: {fact_root}")

    silver_df = pd.concat([pd.read_parquet(path) for path in fact_files], ignore_index=True)
    silver_grain = [
        "period",
        "reporter_iso3",
        "partner_iso3",
        "flowCode",
        "cmdCode",
        "classification_version",
        "customsCode",
        "motCode",
        "partner2Code",
    ]
    silver_df = silver_df.drop_duplicates(subset=silver_grain).copy()

    dimensions_root = config.silver_root / "dimensions"
    dim_country = pd.read_parquet(dimensions_root / "dim_country.parquet")
    if output_paths.dim_output_path.exists():
        dim_trade_routes_existing = pd.read_parquet(output_paths.dim_output_path)
    else:
        dim_trade_routes_existing = pd.DataFrame()

    return RoutingInputs(
        silver_df=silver_df,
        dim_country=dim_country,
        dim_trade_routes_existing=dim_trade_routes_existing,
        code_to_iso3_map=build_code_to_iso3(PROJECT_ROOT),
    )
