from __future__ import annotations

from pathlib import Path

import numpy as np


PROJECT_ROOT = Path(__file__).resolve().parents[3]

LOG_DIR = PROJECT_ROOT / "logs" / "comtrade"
DEFAULT_SILVER_ROOT = PROJECT_ROOT / "data" / "silver" / "comtrade"
DEFAULT_LOG_PATH = LOG_DIR / "comtrade_routing_v3.log"
DEFAULT_MANIFEST_PATH = LOG_DIR / "comtrade_routing_v3_manifest.jsonl"
DEFAULT_NATURAL_EARTH_PATH = (
    PROJECT_ROOT / "data" / "reference" / "geography" / "ne_110m_admin_0_countries.zip"
)
DEFAULT_NATURAL_EARTH_GCS_URI_ENV = "COMTRADE_NATURAL_EARTH_GCS_URI"
DEFAULT_PORT_INDEX_PATH = PROJECT_ROOT / "ingest" / "Kaggle" / "world_port_index" / "UpdatedPub150.csv"
DEFAULT_OUTPUT_SUFFIX = "_v3"
DEFAULT_ASSET_NAME = "comtrade_routing_v3"
DEFAULT_RUN_ID_PREFIX = "comtrade_routing_v3"
DEFAULT_VERSION_LABEL = "V3"

SEA_CODES = {2100}
INLAND_WATER_CODES = {2200}
UNKNOWN_CODES = {0, 9000, 9900}
NON_MARINE_CODES = {1000, 3100, 3200, 9100, 9110, 9200, 9300}

PORT_BASIN_OVERRIDES = {
    ("TUR", "ISTANBUL"): "BLACK_SEA",
    ("TUR", "HAYDARPASA"): "BLACK_SEA",
    ("TUR", "SAMSUN"): "BLACK_SEA",
    ("TUR", "TRABZON"): "BLACK_SEA",
    ("TUR", "IZMIR"): "MEDITERRANEAN",
    ("TUR", "MERSIN"): "MEDITERRANEAN",
    ("TUR", "ISKENDERUN"): "MEDITERRANEAN",
    ("EGY", "ALEXANDRIA"): "MEDITERRANEAN",
    ("EGY", "DAMIETTA"): "MEDITERRANEAN",
    ("EGY", "PORT SAID"): "MEDITERRANEAN",
    ("EGY", "SUEZ"): "RED_SEA",
    ("EGY", "AIN SUKHNA"): "RED_SEA",
    ("EGY", "SAFAGA"): "RED_SEA",
    ("PAN", "BALBOA"): "PACIFIC",
    ("PAN", "RODMAN"): "PACIFIC",
    ("PAN", "PUERTO CRISTOBAL"): "CARIBBEAN",
    ("PAN", "COLON"): "CARIBBEAN",
    ("PAN", "MANZANILLO"): "CARIBBEAN",
    ("IDN", "JAKARTA"): "WESTERN_PACIFIC",
    ("IDN", "TANJUNG PRIOK"): "WESTERN_PACIFIC",
    ("IDN", "SURABAYA"): "WESTERN_PACIFIC",
    ("IDN", "BELAWAN"): "WESTERN_PACIFIC",
    ("IDN", "DUMAI"): "WESTERN_PACIFIC",
    ("IDN", "BALIKPAPAN"): "WESTERN_PACIFIC",
}

STRATEGIC_PORT_KEEP = {
    "TUR": {"ISTANBUL", "HAYDARPASA", "SAMSUN", "TRABZON", "IZMIR", "MERSIN", "ISKENDERUN"},
    "EGY": {"ALEXANDRIA", "DAMIETTA", "PORT SAID", "SUEZ", "AIN SUKHNA", "SAFAGA"},
    "PAN": {"BALBOA", "RODMAN", "PUERTO CRISTOBAL", "COLON", "MANZANILLO"},
    "IDN": {"JAKARTA", "TANJUNG PRIOK", "SURABAYA", "BELAWAN", "DUMAI", "BALIKPAPAN"},
}

CHOKEPOINT_ROWS = [
    {"chokepoint_name": "Turkish Straits", "longitude": 29.0, "latitude": 41.0, "kind": "strait"},
    {"chokepoint_name": "Suez Canal", "longitude": 32.5498, "latitude": 30.8167, "kind": "canal"},
    {"chokepoint_name": "Hormuz Strait", "longitude": 56.25, "latitude": 26.57, "kind": "strait"},
    {"chokepoint_name": "Bab el-Mandeb", "longitude": 43.33, "latitude": 12.58, "kind": "strait"},
    {"chokepoint_name": "Panama Canal", "longitude": -79.58, "latitude": 9.08, "kind": "canal"},
    {"chokepoint_name": "Malacca Strait", "longitude": 99.8, "latitude": 2.5, "kind": "strait"},
    {"chokepoint_name": "Gibraltar Strait", "longitude": -5.6, "latitude": 35.95, "kind": "strait"},
    {"chokepoint_name": "Cape of Good Hope", "longitude": 18.47, "latitude": -34.36, "kind": "cape"},
    {"chokepoint_name": "Open Sea", "longitude": np.nan, "latitude": np.nan, "kind": "open_sea"},
]

BASIN_GRAPH_EDGE_ROWS = [
    {"origin_basin": "BLACK_SEA", "destination_basin": "MEDITERRANEAN", "chokepoint_name": "Turkish Straits", "base_cost": 2.0},
    {"origin_basin": "MEDITERRANEAN", "destination_basin": "BLACK_SEA", "chokepoint_name": "Turkish Straits", "base_cost": 2.0},
    {"origin_basin": "MEDITERRANEAN", "destination_basin": "NORTH_ATLANTIC_EUROPE", "chokepoint_name": "Gibraltar Strait", "base_cost": 2.0},
    {"origin_basin": "NORTH_ATLANTIC_EUROPE", "destination_basin": "MEDITERRANEAN", "chokepoint_name": "Gibraltar Strait", "base_cost": 2.0},
    {"origin_basin": "MEDITERRANEAN", "destination_basin": "ATLANTIC", "chokepoint_name": "Gibraltar Strait", "base_cost": 2.0},
    {"origin_basin": "ATLANTIC", "destination_basin": "MEDITERRANEAN", "chokepoint_name": "Gibraltar Strait", "base_cost": 2.0},
    {"origin_basin": "NORTH_ATLANTIC_EUROPE", "destination_basin": "ATLANTIC", "chokepoint_name": "Open Sea", "base_cost": 1.0},
    {"origin_basin": "ATLANTIC", "destination_basin": "NORTH_ATLANTIC_EUROPE", "chokepoint_name": "Open Sea", "base_cost": 1.0},
    {"origin_basin": "MEDITERRANEAN", "destination_basin": "RED_SEA", "chokepoint_name": "Suez Canal", "base_cost": 3.0},
    {"origin_basin": "RED_SEA", "destination_basin": "MEDITERRANEAN", "chokepoint_name": "Suez Canal", "base_cost": 3.0},
    {"origin_basin": "RED_SEA", "destination_basin": "INDIAN_OCEAN", "chokepoint_name": "Bab el-Mandeb", "base_cost": 2.0},
    {"origin_basin": "INDIAN_OCEAN", "destination_basin": "RED_SEA", "chokepoint_name": "Bab el-Mandeb", "base_cost": 2.0},
    {"origin_basin": "ARABIAN_SEA", "destination_basin": "GULF", "chokepoint_name": "Hormuz Strait", "base_cost": 3.0},
    {"origin_basin": "GULF", "destination_basin": "ARABIAN_SEA", "chokepoint_name": "Hormuz Strait", "base_cost": 3.0},
    {"origin_basin": "INDIAN_OCEAN", "destination_basin": "ARABIAN_SEA", "chokepoint_name": "Open Sea", "base_cost": 1.0},
    {"origin_basin": "ARABIAN_SEA", "destination_basin": "INDIAN_OCEAN", "chokepoint_name": "Open Sea", "base_cost": 1.0},
    {"origin_basin": "INDIAN_OCEAN", "destination_basin": "WESTERN_PACIFIC", "chokepoint_name": "Malacca Strait", "base_cost": 3.0},
    {"origin_basin": "WESTERN_PACIFIC", "destination_basin": "INDIAN_OCEAN", "chokepoint_name": "Malacca Strait", "base_cost": 3.0},
    {"origin_basin": "ATLANTIC", "destination_basin": "PACIFIC", "chokepoint_name": "Panama Canal", "base_cost": 8.0},
    {"origin_basin": "PACIFIC", "destination_basin": "ATLANTIC", "chokepoint_name": "Panama Canal", "base_cost": 8.0},
    {"origin_basin": "NORTH_AMERICA_ATLANTIC", "destination_basin": "PACIFIC", "chokepoint_name": "Panama Canal", "base_cost": 8.0},
    {"origin_basin": "PACIFIC", "destination_basin": "NORTH_AMERICA_ATLANTIC", "chokepoint_name": "Panama Canal", "base_cost": 8.0},
    {"origin_basin": "NORTH_AMERICA_ATLANTIC", "destination_basin": "ATLANTIC", "chokepoint_name": "Open Sea", "base_cost": 1.0},
    {"origin_basin": "ATLANTIC", "destination_basin": "NORTH_AMERICA_ATLANTIC", "chokepoint_name": "Open Sea", "base_cost": 1.0},
    {"origin_basin": "CARIBBEAN", "destination_basin": "ATLANTIC", "chokepoint_name": "Open Sea", "base_cost": 1.0},
    {"origin_basin": "ATLANTIC", "destination_basin": "CARIBBEAN", "chokepoint_name": "Open Sea", "base_cost": 1.0},
    {"origin_basin": "CARIBBEAN", "destination_basin": "PACIFIC", "chokepoint_name": "Panama Canal", "base_cost": 8.0},
    {"origin_basin": "PACIFIC", "destination_basin": "CARIBBEAN", "chokepoint_name": "Panama Canal", "base_cost": 8.0},
    {"origin_basin": "INDIAN_OCEAN", "destination_basin": "SOUTH_ATLANTIC", "chokepoint_name": "Cape of Good Hope", "base_cost": 7.0},
    {"origin_basin": "SOUTH_ATLANTIC", "destination_basin": "INDIAN_OCEAN", "chokepoint_name": "Cape of Good Hope", "base_cost": 7.0},
    {"origin_basin": "SOUTH_ATLANTIC", "destination_basin": "ATLANTIC", "chokepoint_name": "Open Sea", "base_cost": 1.0},
    {"origin_basin": "ATLANTIC", "destination_basin": "SOUTH_ATLANTIC", "chokepoint_name": "Open Sea", "base_cost": 1.0},
    {"origin_basin": "WEST_AFRICA_ATLANTIC", "destination_basin": "ATLANTIC", "chokepoint_name": "Open Sea", "base_cost": 1.0},
    {"origin_basin": "ATLANTIC", "destination_basin": "WEST_AFRICA_ATLANTIC", "chokepoint_name": "Open Sea", "base_cost": 1.0},
    {"origin_basin": "EAST_AFRICA_INDIAN", "destination_basin": "INDIAN_OCEAN", "chokepoint_name": "Open Sea", "base_cost": 1.0},
    {"origin_basin": "INDIAN_OCEAN", "destination_basin": "EAST_AFRICA_INDIAN", "chokepoint_name": "Open Sea", "base_cost": 1.0},
    {"origin_basin": "WESTERN_PACIFIC", "destination_basin": "PACIFIC", "chokepoint_name": "Open Sea", "base_cost": 1.0},
    {"origin_basin": "PACIFIC", "destination_basin": "WESTERN_PACIFIC", "chokepoint_name": "Open Sea", "base_cost": 1.0},
    {"origin_basin": "SOUTH_AMERICA_PACIFIC", "destination_basin": "PACIFIC", "chokepoint_name": "Open Sea", "base_cost": 1.0},
    {"origin_basin": "PACIFIC", "destination_basin": "SOUTH_AMERICA_PACIFIC", "chokepoint_name": "Open Sea", "base_cost": 1.0},
    {"origin_basin": "BALTIC", "destination_basin": "NORTH_ATLANTIC_EUROPE", "chokepoint_name": "Open Sea", "base_cost": 1.0},
    {"origin_basin": "NORTH_ATLANTIC_EUROPE", "destination_basin": "BALTIC", "chokepoint_name": "Open Sea", "base_cost": 1.0},
]

TRANSHIPMENT_HUB_ROWS = [
    {"hub_port": "Rotterdam", "hub_iso3": "NLD", "hub_basin": "NORTH_ATLANTIC_EUROPE", "hub_rank": 1},
    {"hub_port": "Piraeus", "hub_iso3": "GRC", "hub_basin": "MEDITERRANEAN", "hub_rank": 1},
    {"hub_port": "Algeciras", "hub_iso3": "ESP", "hub_basin": "MEDITERRANEAN", "hub_rank": 2},
    {"hub_port": "Valencia", "hub_iso3": "ESP", "hub_basin": "MEDITERRANEAN", "hub_rank": 3},
    {"hub_port": "Singapore", "hub_iso3": "SGP", "hub_basin": "WESTERN_PACIFIC", "hub_rank": 1},
    {"hub_port": "Jebel Ali", "hub_iso3": "ARE", "hub_basin": "GULF", "hub_rank": 1},
]

BASIN_HUB_BRIDGE_ROWS = [
    {"origin_basin": "BLACK_SEA", "destination_basin": "ATLANTIC", "hub_basin": "MEDITERRANEAN", "hub_rank": 1},
    {"origin_basin": "BLACK_SEA", "destination_basin": "NORTH_AMERICA_ATLANTIC", "hub_basin": "MEDITERRANEAN", "hub_rank": 1},
    {"origin_basin": "BLACK_SEA", "destination_basin": "WESTERN_PACIFIC", "hub_basin": "MEDITERRANEAN", "hub_rank": 1},
    {"origin_basin": "WEST_AFRICA_ATLANTIC", "destination_basin": "WESTERN_PACIFIC", "hub_basin": "ATLANTIC", "hub_rank": 1},
    {"origin_basin": "MEDITERRANEAN", "destination_basin": "WESTERN_PACIFIC", "hub_basin": "MEDITERRANEAN", "hub_rank": 1},
]

DIRECT_LAND_BORDER_PAIRS = {
    "FRA": {"ESP", "DEU", "BEL", "LUX", "ITA", "CHE"},
    "DEU": {"FRA", "BEL", "NLD", "LUX", "CHE", "AUT", "CZE", "POL", "DNK"},
    "ESP": {"FRA", "PRT"},
    "USA": {"CAN", "MEX"},
    "CHN": {"RUS", "MNG", "PRK", "VNM", "LAO", "MMR", "IND", "BTN", "NPL", "PAK", "AFG", "TJK", "KGZ", "KAZ"},
    "NLD": {"BEL", "DEU"},
    "BEL": {"FRA", "DEU", "NLD", "LUX"},
    "ROU": {"BGR", "HUN", "SRB", "UKR", "MDA"},
    "BGR": {"ROU", "SRB", "MKD", "GRC", "TUR"},
}

COUNTRY_INTERNAL_BRIDGES = {
    "TUR": {
        "bridge_name": "Turkish Straits",
        "side_a_basins": {"BLACK_SEA"},
        "side_b_basins": {"MEDITERRANEAN"},
    },
    "EGY": {
        "bridge_name": "Suez Canal",
        "side_a_basins": {"MEDITERRANEAN"},
        "side_b_basins": {"RED_SEA"},
    },
    "PAN": {
        "bridge_name": "Panama Canal",
        "side_a_basins": {"CARIBBEAN", "ATLANTIC", "NORTH_AMERICA_ATLANTIC"},
        "side_b_basins": {"PACIFIC"},
    },
}

EAST_OF_SUEZ_BASINS = {
    "RED_SEA",
    "INDIAN_OCEAN",
    "ARABIAN_SEA",
    "GULF",
    "EAST_AFRICA_INDIAN",
    "WESTERN_PACIFIC",
}

SUEZ_ORIGIN_BASINS = {
    "MEDITERRANEAN",
    "NORTH_ATLANTIC_EUROPE",
    "BALTIC",
    "BLACK_SEA",
}

PREFERRED_REPORTER_BASIN = {
    ("FRA", "MEDITERRANEAN"): ["MEDITERRANEAN"],
    ("FRA", "RED_SEA"): ["MEDITERRANEAN"],
    ("FRA", "INDIAN_OCEAN"): ["MEDITERRANEAN"],
    ("FRA", "ARABIAN_SEA"): ["MEDITERRANEAN"],
    ("FRA", "GULF"): ["MEDITERRANEAN"],
    ("FRA", "WESTERN_PACIFIC"): ["MEDITERRANEAN"],
    ("FRA", "EAST_AFRICA_INDIAN"): ["MEDITERRANEAN"],
    ("FRA", "ATLANTIC"): ["NORTH_ATLANTIC_EUROPE"],
    ("FRA", "NORTH_AMERICA_ATLANTIC"): ["NORTH_ATLANTIC_EUROPE"],
    ("FRA", "CARIBBEAN"): ["NORTH_ATLANTIC_EUROPE"],
    ("FRA", "SOUTH_ATLANTIC"): ["NORTH_ATLANTIC_EUROPE"],
    ("FRA", "WEST_AFRICA_ATLANTIC"): ["NORTH_ATLANTIC_EUROPE"],
    ("FRA", "PACIFIC"): ["NORTH_ATLANTIC_EUROPE"],
    ("ESP", "MEDITERRANEAN"): ["MEDITERRANEAN"],
    ("ESP", "RED_SEA"): ["MEDITERRANEAN"],
    ("ESP", "INDIAN_OCEAN"): ["MEDITERRANEAN"],
    ("ESP", "ARABIAN_SEA"): ["MEDITERRANEAN"],
    ("ESP", "GULF"): ["MEDITERRANEAN"],
    ("ESP", "WESTERN_PACIFIC"): ["MEDITERRANEAN"],
    ("ESP", "EAST_AFRICA_INDIAN"): ["MEDITERRANEAN"],
    ("ESP", "ATLANTIC"): ["NORTH_ATLANTIC_EUROPE", "ATLANTIC"],
    ("ESP", "NORTH_AMERICA_ATLANTIC"): ["NORTH_ATLANTIC_EUROPE", "ATLANTIC"],
    ("ESP", "CARIBBEAN"): ["NORTH_ATLANTIC_EUROPE", "ATLANTIC"],
    ("ESP", "SOUTH_ATLANTIC"): ["NORTH_ATLANTIC_EUROPE", "ATLANTIC"],
    ("ESP", "WEST_AFRICA_ATLANTIC"): ["NORTH_ATLANTIC_EUROPE", "ATLANTIC"],
    ("ESP", "PACIFIC"): ["NORTH_ATLANTIC_EUROPE", "ATLANTIC"],
    ("ESP", "SOUTH_AMERICA_PACIFIC"): ["NORTH_ATLANTIC_EUROPE", "ATLANTIC"],
    ("ITA", "ATLANTIC"): ["MEDITERRANEAN"],
    ("ITA", "NORTH_AMERICA_ATLANTIC"): ["MEDITERRANEAN"],
    ("GRC", "ATLANTIC"): ["MEDITERRANEAN"],
    ("GRC", "NORTH_AMERICA_ATLANTIC"): ["MEDITERRANEAN"],
}
