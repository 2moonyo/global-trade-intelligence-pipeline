from __future__ import annotations

from ingest.comtrade.routing.cli import main as cli_main
from ingest.comtrade.routing.constants import LOG_DIR
from ingest.comtrade.routing.models import RoutingRuntimeDefaults


RUNTIME_DEFAULTS = RoutingRuntimeDefaults(
    logger_name="comtrade.routing",
    log_path=LOG_DIR / "comtrade_routing.log",
    manifest_path=LOG_DIR / "comtrade_routing_manifest.jsonl",
    output_suffix="",
    asset_name="comtrade_routing",
    run_id_prefix="comtrade_routing",
    version_label="Main",
)


def main() -> None:
    cli_main(RUNTIME_DEFAULTS)


if __name__ == "__main__":
    main()
