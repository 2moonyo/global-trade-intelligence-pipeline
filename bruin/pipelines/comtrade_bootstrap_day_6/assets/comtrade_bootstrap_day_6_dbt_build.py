""" @bruin

name: capstone.comtrade_bootstrap_day_6_dbt_build
image: python:3.12

depends:
  - capstone.comtrade_bootstrap_day_6_load_bigquery

description: |
  Run dbt build after the Comtrade day 6 BigQuery load completes.

@bruin """

from __future__ import annotations

import subprocess
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[4]
DBT_SCRIPT = PROJECT_ROOT / "scripts" / "run_dbt.sh"


if __name__ == "__main__":
    subprocess.run(
        [
            str(DBT_SCRIPT),
            "build",
        ],
        check=True,
        cwd=PROJECT_ROOT,
    )
