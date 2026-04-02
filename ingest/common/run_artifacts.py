from __future__ import annotations

import json
import logging
import sys
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


def build_run_id(prefix: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}_{timestamp}_{uuid.uuid4().hex[:8]}"


def configure_logger(
    *,
    logger_name: str,
    log_path: Path,
    log_level: str = "INFO",
    log_to_stdout: bool = True,
) -> logging.Logger:
    logger = logging.getLogger(logger_name)
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    logger.propagate = False

    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

    formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")

    if log_to_stdout:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    log_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


def duration_seconds(started_at: datetime, finished_at: datetime | None = None) -> float:
    finished_at = finished_at or datetime.now(timezone.utc)
    return round((finished_at - started_at).total_seconds(), 3)


def iter_progress(
    iterable,
    *,
    desc: str,
    total: int | None = None,
    unit: str = "item",
    disable: bool | None = None,
):
    try:
        from tqdm.auto import tqdm
    except ModuleNotFoundError:
        return iterable

    resolved_disable = disable if disable is not None else not sys.stderr.isatty()
    return tqdm(
        iterable,
        total=total,
        desc=desc,
        unit=unit,
        dynamic_ncols=True,
        leave=False,
        disable=resolved_disable,
    )


def append_manifest(path: Path, entry: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(json_ready(entry), ensure_ascii=False) + "\n")


def json_ready(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, "item") and callable(value.item):
        try:
            return json_ready(value.item())
        except Exception:
            pass
    if isinstance(value, list):
        return [json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [json_ready(item) for item in value]
    if isinstance(value, dict):
        return {key: json_ready(item) for key, item in value.items()}
    return value
