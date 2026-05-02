from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent
PIPELINE_SCRIPT = PROJECT_ROOT / "scripts" / "run_pipeline.sh"
DBT_SCRIPT = PROJECT_ROOT / "scripts" / "run_dbt.sh"
SUMMARY_DIR = PROJECT_ROOT / "logs" / "bruin"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_bruin_vars() -> dict[str, Any]:
    raw = os.getenv("BRUIN_VARS", "{}").strip() or "{}"
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid BRUIN_VARS JSON payload: {exc}") from exc

    if not isinstance(data, dict):
        raise RuntimeError("BRUIN_VARS must decode to a JSON object.")

    return data


def resolve_string(
    env_name: str,
    var_name: str,
    *,
    default: str | None = None,
    required: bool = False,
) -> str | None:
    env_value = os.getenv(env_name)
    if env_value:
        return env_value

    value = _load_bruin_vars().get(var_name, default)
    if value in (None, ""):
        if required:
            raise RuntimeError(
                f"Missing required runtime value for {env_name} / var.{var_name}."
            )
        return None

    if not isinstance(value, str):
        value = str(value)

    return value


def resolve_int(
    env_name: str,
    var_name: str,
    *,
    default: int | None = None,
) -> int | None:
    env_value = os.getenv(env_name)
    if env_value:
        return int(env_value)

    value = _load_bruin_vars().get(var_name, default)
    if value in (None, ""):
        return None

    return int(value)


def resolve_list(
    env_name: str,
    var_name: str,
    *,
    default: list[str] | None = None,
) -> list[str]:
    env_value = os.getenv(env_name)
    if env_value:
        return [item for item in env_value.split(" ") if item]

    value = _load_bruin_vars().get(var_name, default or [])
    if value in (None, ""):
        return []

    if not isinstance(value, list):
        raise RuntimeError(f"Expected BRUIN_VARS.{var_name} to be a JSON array.")

    return [str(item) for item in value]


def _tracked_path_snapshot(path_like: str | Path) -> dict[str, Any]:
    path = Path(path_like)
    if not path.is_absolute():
        path = PROJECT_ROOT / path

    return {
        "path": str(path.relative_to(PROJECT_ROOT)),
        "exists": path.exists(),
        "size_bytes": path.stat().st_size if path.exists() and path.is_file() else None,
    }


def _write_summary(summary_name: str, payload: dict[str, Any]) -> None:
    SUMMARY_DIR.mkdir(parents=True, exist_ok=True)
    summary_path = SUMMARY_DIR / f"{summary_name}.json"
    summary_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def run_command(
    command: list[str | Path],
    *,
    summary_name: str,
    tracked_paths: list[str | Path] | None = None,
    context: dict[str, Any] | None = None,
) -> None:
    payload: dict[str, Any] = {
        "asset": os.getenv("BRUIN_ASSET"),
        "command": [str(part) for part in command],
        "context": context or {},
        "pipeline": os.getenv("BRUIN_PIPELINE"),
        "run_id": os.getenv("BRUIN_RUN_ID"),
        "started_at": _utc_now(),
        "status": "running",
    }

    try:
        subprocess.run(payload["command"], check=True, cwd=PROJECT_ROOT)
    except subprocess.CalledProcessError as exc:
        payload["status"] = "failed"
        payload["return_code"] = exc.returncode
        raise
    else:
        payload["status"] = "succeeded"
    finally:
        payload["finished_at"] = _utc_now()
        payload["tracked_paths"] = [
            _tracked_path_snapshot(path_like) for path_like in (tracked_paths or [])
        ]
        _write_summary(summary_name, payload)


def run_pipeline_script(
    *args: str,
    summary_name: str,
    tracked_paths: list[str | Path] | None = None,
    context: dict[str, Any] | None = None,
) -> None:
    run_command(
        [PIPELINE_SCRIPT, *args],
        summary_name=summary_name,
        tracked_paths=tracked_paths,
        context=context,
    )


def run_dbt_command(
    command_name: str,
    *,
    extra_args: list[str] | None = None,
    summary_name: str,
    tracked_paths: list[str | Path] | None = None,
    context: dict[str, Any] | None = None,
) -> None:
    run_command(
        [DBT_SCRIPT, command_name, *(extra_args or [])],
        summary_name=summary_name,
        tracked_paths=tracked_paths,
        context=context,
    )
