from __future__ import annotations

import os
from pathlib import Path


APP_TITLE = "Macro-Political Trade Intelligence"
APP_SUBTITLE = "Trade scale, bilateral dependence, chokepoint risk, event disruption, and energy context"
DEFAULT_DB_RELATIVE_PATH = Path("warehouse") / "analytics.duckdb"

MARTS_SCHEMA = "analytics_marts"
DISPLAY_TABLE_LIMIT = int(os.getenv("STREAMLIT_DISPLAY_TABLE_LIMIT", "250"))
EXPORT_MAX_ROWS = int(os.getenv("STREAMLIT_EXPORT_MAX_ROWS", "5000"))
TOP_N_DEFAULT = int(os.getenv("STREAMLIT_TOP_N_DEFAULT", "10"))
TOP_N_MIN = 5
TOP_N_MAX = 25

DEFAULT_ENERGY_INDICATOR = "dependency_on_imported_energy"
DEFAULT_CHART_HEIGHT = 340
ACCENT_COLOR = "#1f4b7a"
CAUTION_COLOR = "#a04b1a"
MUTED_COLOR = "#8c99a8"


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def resolve_db_path() -> Path:
    configured = os.getenv("TRADE_DUCKDB_PATH")
    if configured:
        return Path(configured).expanduser()
    return project_root() / DEFAULT_DB_RELATIVE_PATH
