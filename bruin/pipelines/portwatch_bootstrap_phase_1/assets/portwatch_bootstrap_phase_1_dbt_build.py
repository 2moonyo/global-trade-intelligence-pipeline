""" @bruin

name: capstone.portwatch_bootstrap_phase_1_dbt_build
image: python:3.12

depends:
  - capstone.portwatch_bootstrap_phase_1_load_bigquery

description: |
  Run the dbt build step after the PortWatch phase 1 BigQuery load completes.

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
