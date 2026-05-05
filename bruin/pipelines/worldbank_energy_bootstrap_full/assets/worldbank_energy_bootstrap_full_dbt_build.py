""" @bruin

name: capstone.worldbank_energy_bootstrap_full_dbt_build
uri: capstone://batch/worldbank_energy_bootstrap_full/dbt_build
image: python:3.12

depends:
  - capstone.worldbank_energy_bootstrap_full_load_bigquery

description: |
  Run the dbt build step after the World Bank energy full bootstrap BigQuery load completes.

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
