from __future__ import annotations

import contextlib
import logging
import warnings
from pathlib import Path
from typing import Any

import pandas as pd

from ingest.comtrade.routing.constants import PORT_BASIN_OVERRIDES, PROJECT_ROOT


def display(*args, **kwargs):
    return None


def normalize_port_name(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().upper()


def text_or_empty(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


def infer_port_basin(world_water_body: str, latitude: float = float("nan"), longitude: float = float("nan")) -> str:
    text = text_or_empty(world_water_body)

    if any(key in text for key in ["black sea", "sea of azov"]):
        return "BLACK_SEA"
    if any(key in text for key in ["mediterranean", "aegean", "adriatic", "ionian", "tyrrhenian", "ligurian"]):
        return "MEDITERRANEAN"
    if any(key in text for key in ["north sea", "english channel", "bay of biscay"]):
        return "NORTH_ATLANTIC_EUROPE"
    if "baltic" in text:
        return "BALTIC"
    if any(key in text for key in ["persian gulf", "arabian gulf"]):
        return "GULF"
    if "red sea" in text:
        return "RED_SEA"
    if "arabian sea" in text:
        return "ARABIAN_SEA"
    if "indian ocean" in text:
        return "INDIAN_OCEAN"
    if any(key in text for key in ["south china sea", "east china sea", "yellow sea", "sea of japan"]):
        return "WESTERN_PACIFIC"
    if any(key in text for key in ["north pacific ocean", "bering sea", "sea of okhotsk", "tatar strait"]):
        return "PACIFIC"
    if "gulf of mexico" in text:
        return "NORTH_AMERICA_ATLANTIC"
    if any(key in text for key in ["caribbean", "west indies"]):
        return "CARIBBEAN"
    if "south atlantic ocean" in text:
        return "SOUTH_ATLANTIC"
    if "north atlantic ocean" in text:
        if pd.notna(longitude) and longitude < -30:
            return "NORTH_AMERICA_ATLANTIC"
        return "ATLANTIC"
    if "atlantic ocean" in text:
        if pd.notna(latitude) and latitude < 0:
            return "SOUTH_ATLANTIC"
        if pd.notna(longitude) and longitude < -30:
            return "NORTH_AMERICA_ATLANTIC"
        return "ATLANTIC"
    if any(key in text for key in ["gulf of guinea", "bight of benin", "bight of bonny"]):
        return "WEST_AFRICA_ATLANTIC"
    if any(key in text for key in ["mozambique channel", "somali basin"]):
        return "EAST_AFRICA_INDIAN"
    if any(key in text for key in ["south pacific ocean", "humboldt current"]):
        return "SOUTH_AMERICA_PACIFIC"
    if any(key in text for key in ["great lakes", "danube", "river", "lake "]):
        return "INLAND_WATERWAY"
    return "UNKNOWN_COASTAL"


def infer_port_basin_with_override(
    iso3: str,
    port_name: str,
    world_water_body: str,
    latitude: float = float("nan"),
    longitude: float = float("nan"),
) -> str:
    override_key = (str(iso3).strip().upper(), normalize_port_name(port_name))
    if override_key in PORT_BASIN_OVERRIDES:
        return PORT_BASIN_OVERRIDES[override_key]
    return infer_port_basin(world_water_body, latitude, longitude)


def build_code_to_iso3(project_root: Path = PROJECT_ROOT) -> dict[int, str]:
    metadata_root = project_root / "data" / "metadata" / "comtrade"
    partners_meta = pd.read_csv(metadata_root / "partners.csv")
    mapping: dict[int, str] = {}
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
def suppress_routing_noise():
    country_converter_loggers = [
        logging.getLogger("country_converter"),
        logging.getLogger("country_converter.country_converter"),
    ]
    previous_levels = [logger.level for logger in country_converter_loggers]
    for logger in country_converter_loggers:
        logger.setLevel(logging.ERROR)

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
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
