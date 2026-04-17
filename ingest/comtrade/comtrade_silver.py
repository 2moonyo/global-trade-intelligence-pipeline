from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

import country_converter as coco
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingest.common.run_artifacts import (
    append_manifest,
    build_run_id,
    configure_logger,
    duration_seconds,
    iter_progress,
    json_ready,
)


LOGGER_NAME = "comtrade.silver"
LOG_DIR = PROJECT_ROOT / "logs" / "comtrade"
LOG_PATH = LOG_DIR / "comtrade_silver.log"
MANIFEST_PATH = LOG_DIR / "comtrade_silver_manifest.jsonl"
SILVER_GRAIN = [
    "period",
    "reporter_iso3",
    "partner_iso3",
    "flowCode",
    "cmdCode",
    "customsCode",
    "motCode",
    "partner2Code",
]
SILVER_COLUMNS = [
    "load_batch_id",
    "source_file",
    "source_year_partition",
    "bronze_extracted_at",
    "ref_date",
    "year_month",
    "quarter",
    "ref_year",
    "refMonth",
    "period",
    "reporterCode",
    "reporter_iso3",
    "reporter_name_clean",
    "partnerCode",
    "partner_iso3",
    "partner_name_clean",
    "is_world_partner",
    "is_special_partner_bucket",
    "is_country_like_partner",
    "is_bilateral_usable",
    "flowCode",
    "trade_flow",
    "flow_sign",
    "classificationCode",
    "classification_version",
    "classificationSearchCode",
    "isOriginalClassification",
    "cmdCode",
    "cmdDesc",
    "aggrLevel",
    "isLeaf",
    "isAggregate",
    "is_commodity_rollup_safe",
    "analysis_grain",
    "qtyUnitCode",
    "qtyUnitAbbr",
    "qty",
    "altQtyUnitCode",
    "altQtyUnitAbbr",
    "altQty",
    "netWgt",
    "grossWgt",
    "has_qty",
    "has_alt_qty",
    "has_net_wgt",
    "has_gross_wgt",
    "is_weight_usable",
    "is_qty_usable",
    "isQtyEstimated",
    "isAltQtyEstimated",
    "isNetWgtEstimated",
    "isGrossWgtEstimated",
    "legacyEstimationFlag",
    "isReported",
    "cifvalue",
    "fobvalue",
    "primaryValue",
    "trade_value_usd",
    "value_basis",
    "value_per_net_kg_usd",
    "value_per_gross_kg_usd",
    "value_per_qty_unit",
    "customsCode",
    "motCode",
    "partner2Code",
]

REQUIRED_METADATA_FILENAMES = (
    "reporters.csv",
    "partners.csv",
    "flows.csv",
)

COMTRADE_API_KEY_ALIASES = (
    "COMTRADE_API_KEY_DATA",
    "COMTRADE_API_KEY_DATA_A",
    "COMTRADE_API_KEY_DATA_B",
    "COMTRADE_API_KEY",
)


@dataclass(frozen=True)
class ComtradeSilverConfig:
    bronze_root: Path = PROJECT_ROOT / "data" / "bronze" / "comtrade" / "monthly_history"
    silver_root: Path = PROJECT_ROOT / "data" / "silver" / "comtrade"
    metadata_root: Path = PROJECT_ROOT / "data" / "metadata" / "comtrade"
    audit_root: Path = PROJECT_ROOT / "data" / "metadata" / "comtrade" / "ingest_reports"
    log_path: Path = LOG_PATH
    manifest_path: Path = MANIFEST_PATH
    log_level: str = "INFO"
    skip_unchanged: bool = True

    @property
    def fact_root(self) -> Path:
        return self.silver_root / "comtrade_fact"

    @property
    def dimensions_root(self) -> Path:
        return self.silver_root / "dimensions"


def _parse_csv_option(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _parse_period(value: str) -> str:
    if not re.fullmatch(r"\d{6}", value):
        raise argparse.ArgumentTypeError(f"Expected YYYYMM period, got {value}")
    return value


def _resolve_comtrade_api_key() -> tuple[str | None, str | None]:
    for alias in COMTRADE_API_KEY_ALIASES:
        candidate = os.getenv(alias)
        if candidate:
            return candidate, alias
    return None, None


def _missing_required_metadata_files(metadata_root: Path) -> list[str]:
    missing: list[str] = []
    for filename in REQUIRED_METADATA_FILENAMES:
        candidate = metadata_root / filename
        if not candidate.exists() or candidate.stat().st_size == 0:
            missing.append(filename)
    return missing


def _ensure_required_metadata(metadata_root: Path, logger) -> dict[str, Any]:
    metadata_root.mkdir(parents=True, exist_ok=True)
    missing_before = _missing_required_metadata_files(metadata_root)
    if not missing_before:
        return {
            "status": "already_present",
            "missing_before": [],
            "missing_after": [],
            "refreshed": False,
        }

    api_key, api_key_alias = _resolve_comtrade_api_key()
    if not api_key:
        raise RuntimeError(
            "Missing required Comtrade metadata files "
            f"{missing_before} under {metadata_root} and no API key is configured. "
            "Set one of: "
            + ", ".join(COMTRADE_API_KEY_ALIASES)
        )

    logger.warning(
        "Missing required Comtrade metadata files under %s: %s. Running metadata bootstrap.",
        metadata_root,
        ", ".join(missing_before),
    )

    from ingest.comtrade.un_comtrade_tools_metadata import ComtradeMetadataExtractor

    summary = ComtradeMetadataExtractor(
        api_key=api_key,
        output_dir=str(metadata_root),
    ).extract_all_metadata()

    missing_after = _missing_required_metadata_files(metadata_root)
    if missing_after:
        raise RuntimeError(
            "Comtrade metadata bootstrap completed but required files are still missing: "
            + ", ".join(missing_after)
        )

    extractions = summary.get("extractions") or []
    success_count = sum(1 for item in extractions if item.get("status") == "success")
    return {
        "status": "refreshed",
        "missing_before": missing_before,
        "missing_after": [],
        "refreshed": True,
        "api_key_alias": api_key_alias,
        "successful_extractions": int(success_count),
        "total_extractions": int(len(extractions)),
    }


def _parse_filename_metadata(file_path: Path) -> dict[str, Any]:
    name = file_path.name
    year_match = re.search(r"year=(\d{4})", str(file_path))
    reporter_match = re.search(r"reps_([0-9\-]+)", name)
    flow_match = re.search(r"flow_([A-Z])", name)
    periods_match = re.search(r"periods_(\d{6})_to_(\d{6})", name)
    cmd_match = re.search(r"cmd_([0-9\-]+)", name)

    return {
        "source_file": name,
        "source_path": str(file_path),
        "source_year_partition": int(year_match.group(1)) if year_match else None,
        "requested_reporter_code": reporter_match.group(1) if reporter_match else None,
        "requested_flow_code": flow_match.group(1) if flow_match else None,
        "requested_period_start": periods_match.group(1) if periods_match else None,
        "requested_period_end": periods_match.group(2) if periods_match else None,
        "requested_cmd_codes": cmd_match.group(1).split("-") if cmd_match else None,
    }


def _as_frame_scalar(value: Any) -> Any:
    if isinstance(value, (list, dict)):
        return json.dumps(value, sort_keys=True)
    return value


def _source_batch_id(file_meta: dict[str, Any], metadata: dict[str, Any]) -> str:
    extracted_at = metadata.get("extracted_at_utc")
    source_file = file_meta.get("source_file") or "unknown_source_file"
    stem = Path(source_file).stem
    if extracted_at:
        digest = hashlib.sha1(f"{stem}|{extracted_at}".encode("utf-8")).hexdigest()[:12]
        return f"{stem}_{digest}"
    return stem


def _load_payload_rows(json_path: Path, run_id: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    with json_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    rows = payload.get("data") or []
    metadata = payload.get("_metadata") or {}
    request = metadata.get("request") or {}
    file_meta = _parse_filename_metadata(json_path)

    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame, {
            **file_meta,
            "record_count": int(metadata.get("record_count") or 0),
            "bronze_extracted_at": metadata.get("extracted_at_utc"),
            "load_batch_id": _source_batch_id(file_meta, metadata),
        }

    for key, value in file_meta.items():
        frame[key] = _as_frame_scalar(value)
    frame["bronze_extracted_at"] = metadata.get("extracted_at_utc")
    frame["load_batch_id"] = _source_batch_id(file_meta, metadata)
    frame["request_reporter_code"] = _as_frame_scalar(request.get("reporterCode"))
    frame["request_periods"] = _as_frame_scalar(request.get("period"))
    frame["request_flow_code"] = _as_frame_scalar(request.get("flowCode"))
    frame["request_cmd_code"] = _as_frame_scalar(request.get("cmdCode"))
    frame["request_partner_code"] = _as_frame_scalar(request.get("partnerCode"))

    return frame, {
        **file_meta,
        "record_count": int(metadata.get("record_count") or len(rows)),
        "bronze_extracted_at": metadata.get("extracted_at_utc"),
        "load_batch_id": _source_batch_id(file_meta, metadata),
    }


def load_bronze_frames(
    bronze_root: Path,
    *,
    run_id: str,
    logger,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    json_files = sorted(
        path
        for path in bronze_root.rglob("*.json")
        if "__MACOSX" not in str(path) and not path.name.startswith("._")
    )
    if not json_files:
        raise FileNotFoundError(f"No Comtrade JSON files found under {bronze_root}")

    year_groups: dict[str, list[Path]] = {}
    for path in json_files:
        year_label = next(
            (part.split("=", 1)[1] for part in path.parts if part.startswith("year=")),
            "unknown",
        )
        year_groups.setdefault(year_label, []).append(path)

    logger.info(
        "Loading %s Comtrade bronze JSON files from %s across %s year partitions",
        len(json_files),
        bronze_root,
        len(year_groups),
    )

    frames: list[pd.DataFrame] = []
    file_rows: list[dict[str, Any]] = []
    cumulative_rows = 0
    for year_label, year_files in iter_progress(
        sorted(year_groups.items()),
        desc="Comtrade bronze years",
        total=len(year_groups),
        unit="year",
    ):
        logger.info("Loading bronze year=%s files=%s", year_label, len(year_files))
        year_rows = 0
        for path in year_files:
            frame, file_row = _load_payload_rows(path, run_id)
            file_rows.append(file_row)
            if not frame.empty:
                frames.append(frame)
                year_rows += len(frame)
        cumulative_rows += year_rows
        logger.info(
            "Loaded bronze year=%s files=%s rows=%s cumulative_rows=%s",
            year_label,
            len(year_files),
            year_rows,
            cumulative_rows,
        )

    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    file_inventory = pd.DataFrame(file_rows)
    logger.info("Loaded %s bronze rows across %s files", len(combined), len(file_inventory))
    return combined, file_inventory


def apply_filters(
    df: pd.DataFrame,
    *,
    periods: set[str],
    since_period: str | None,
    until_period: str | None,
    reporters: set[str],
    cmd_codes: set[str],
    flow_codes: set[str],
) -> pd.DataFrame:
    if df.empty:
        return df

    working = df.copy()
    if "period" in working.columns:
        period_text = working["period"].astype("string").str.strip()
        if periods:
            working = working[period_text.isin(periods)].copy()
        if since_period:
            working = working[period_text >= since_period].copy()
        if until_period:
            working = working[period_text <= until_period].copy()

    if reporters and "reporterISO" in working.columns:
        working = working[
            working["reporterISO"].astype("string").str.strip().str.upper().isin(reporters)
        ].copy()

    if cmd_codes and "cmdCode" in working.columns:
        working = working[
            working["cmdCode"].astype("string").str.strip().isin(cmd_codes)
        ].copy()

    if flow_codes and "flowCode" in working.columns:
        working = working[
            working["flowCode"].astype("string").str.strip().str.upper().isin(flow_codes)
        ].copy()

    return working.reset_index(drop=True)


def prepare_silver(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    if df.empty:
        raise RuntimeError("No Comtrade bronze rows remain after applying the requested filters.")

    working = df.copy()

    int_cols = ["refYear", "refMonth", "period", "aggrLevel", "reporterCode", "partnerCode"]
    float_cols = ["qty", "altQty", "netWgt", "grossWgt", "cifvalue", "fobvalue", "primaryValue"]
    bool_cols = [
        "isOriginalClassification",
        "isLeaf",
        "isQtyEstimated",
        "isAltQtyEstimated",
        "isNetWgtEstimated",
        "isGrossWgtEstimated",
        "isReported",
        "isAggregate",
    ]

    for col in int_cols:
        if col in working.columns:
            working[col] = pd.to_numeric(working[col], errors="coerce").astype("Int64")

    for col in float_cols:
        if col in working.columns:
            working[col] = pd.to_numeric(working[col], errors="coerce")

    for col in bool_cols:
        if col in working.columns:
            working[col] = working[col].astype("boolean")

    working["bronze_extracted_at"] = pd.to_datetime(
        working.get("bronze_extracted_at"),
        errors="coerce",
        utc=True,
    )
    working["load_batch_id"] = working.get("load_batch_id", "").astype("string")

    working["period_str"] = working["period"].astype("string")
    working["ref_date"] = pd.to_datetime(working["period_str"] + "01", format="%Y%m%d", errors="coerce")
    working["year_month"] = working["ref_date"].dt.strftime("%Y-%m")
    working["quarter"] = working["ref_date"].dt.to_period("Q").astype("string")
    working["ref_year"] = working["ref_date"].dt.year.astype("Int64")

    working["reporter_iso3"] = working["reporterISO"].astype("string").str.strip().str.upper()
    working["partner_iso3"] = working["partnerISO"].astype("string").str.strip().str.upper()
    working["reporter_name_clean"] = working["reporterDesc"].astype("string").str.strip()
    working["partner_name_clean"] = working["partnerDesc"].astype("string").str.strip()

    working["flowCode"] = working["flowCode"].astype("string").str.strip().str.upper()
    working["trade_flow"] = working["flowCode"].map({"M": "import", "X": "export"})
    working["flow_sign"] = working["flowCode"].map({"M": 1, "X": -1}).astype("Int64")

    working["classification_version"] = (
        working["classificationCode"].astype("string").str.strip().str.upper()
    )

    working["cmdCode"] = working["cmdCode"].astype("string").str.strip()
    working["cmdDesc"] = working["cmdDesc"].astype("string").str.strip()
    working["customsCode"] = working["customsCode"].astype("string").str.strip()
    working["motCode"] = pd.to_numeric(working["motCode"], errors="coerce").astype("Int64")
    working["partner2Code"] = pd.to_numeric(working["partner2Code"], errors="coerce").astype("Int64")

    special_partner_codes = {"W00"}
    working["is_world_partner"] = working["partner_iso3"].eq("W00")
    working["is_special_partner_bucket"] = working["partner_iso3"].isin(special_partner_codes)
    working["is_country_like_partner"] = ~working["is_special_partner_bucket"]
    working["is_bilateral_usable"] = ~working["is_world_partner"]

    working["trade_value_usd"] = working["primaryValue"]
    working["value_basis"] = "primary"
    import_mask = working["trade_flow"].eq("import") & working["trade_value_usd"].isna()
    export_mask = working["trade_flow"].eq("export") & working["trade_value_usd"].isna()
    working.loc[import_mask, "trade_value_usd"] = working.loc[import_mask, "cifvalue"]
    working.loc[import_mask, "value_basis"] = "cif_fallback"
    working.loc[export_mask, "trade_value_usd"] = working.loc[export_mask, "fobvalue"]
    working.loc[export_mask, "value_basis"] = "fob_fallback"

    working["has_qty"] = working["qty"].fillna(0).gt(0)
    working["has_alt_qty"] = working["altQty"].fillna(0).gt(0)
    working["has_net_wgt"] = working["netWgt"].fillna(0).gt(0)
    working["has_gross_wgt"] = working["grossWgt"].fillna(0).gt(0)
    working["is_weight_usable"] = working["has_net_wgt"] | working["has_gross_wgt"]
    working["is_qty_usable"] = (
        working["has_qty"]
        & working["qtyUnitAbbr"].astype("string").notna()
        & working["qtyUnitAbbr"].astype("string").ne("N/A")
    )

    working["value_per_net_kg_usd"] = working["trade_value_usd"] / working["netWgt"]
    working.loc[working["netWgt"].fillna(0).le(0), "value_per_net_kg_usd"] = pd.NA
    working["value_per_gross_kg_usd"] = working["trade_value_usd"] / working["grossWgt"]
    working.loc[working["grossWgt"].fillna(0).le(0), "value_per_gross_kg_usd"] = pd.NA
    working["value_per_qty_unit"] = working["trade_value_usd"] / working["qty"]
    working.loc[~working["is_qty_usable"] | working["qty"].fillna(0).le(0), "value_per_qty_unit"] = pd.NA

    working["is_commodity_rollup_safe"] = ~working["isAggregate"].fillna(False)
    working["analysis_grain"] = (
        working["period"].astype("string").fillna("")
        + "|"
        + working["reporter_iso3"].fillna("")
        + "|"
        + working["partner_iso3"].fillna("")
        + "|"
        + working["flowCode"].fillna("")
        + "|"
        + working["cmdCode"].fillna("")
        + "|"
        + working["customsCode"].fillna("")
        + "|"
        + working["motCode"].astype("string").fillna("")
        + "|"
        + working["partner2Code"].astype("string").fillna("")
    )

    working = working.sort_values(
        [
            "bronze_extracted_at",
            "source_file",
            "load_batch_id",
        ],
        ascending=[False, False, False],
        na_position="last",
    ).reset_index(drop=True)

    dedupe_frame = pd.DataFrame(
        {
            "period": working["period"].astype("string"),
            "reporter_iso3": working["reporter_iso3"].fillna("__NULL__"),
            "partner_iso3": working["partner_iso3"].fillna("__NULL__"),
            "flowCode": working["flowCode"].fillna("__NULL__"),
            "cmdCode": working["cmdCode"].fillna("__NULL__"),
            "customsCode": working["customsCode"].fillna("__NULL__"),
            "motCode": working["motCode"].astype("string").fillna("__NULL__"),
            "partner2Code": working["partner2Code"].astype("string").fillna("__NULL__"),
        }
    )
    duplicate_mask = dedupe_frame.duplicated(keep=False)
    duplicate_collisions = working.loc[duplicate_mask].copy()
    dedupe_subset = dedupe_frame.columns.tolist()
    deduped = working.loc[
        ~dedupe_frame.duplicated(subset=dedupe_subset, keep="first")
    ].copy()

    deduped = deduped.dropna(
        subset=[
            "ref_date",
            "period",
            "reporter_iso3",
            "partner_iso3",
            "flowCode",
            "trade_flow",
            "cmdCode",
        ]
    ).copy()

    missing_columns = [column for column in SILVER_COLUMNS if column not in deduped.columns]
    for column in missing_columns:
        deduped[column] = pd.NA
    deduped = deduped[SILVER_COLUMNS].copy()

    summary = {
        "source_row_count": int(len(df)),
        "rows_after_required_field_filter": int(len(deduped)),
        "duplicate_collision_row_count": int(len(duplicate_collisions)),
        "distinct_period_count": int(deduped["period"].nunique(dropna=True)),
        "distinct_reporter_count": int(deduped["reporter_iso3"].nunique(dropna=True)),
        "distinct_cmd_code_count": int(deduped["cmdCode"].nunique(dropna=True)),
        "distinct_flow_code_count": int(deduped["flowCode"].nunique(dropna=True)),
    }
    return deduped, duplicate_collisions, summary


def _normalize_for_hash(series: pd.Series) -> pd.Series:
    def normalize_nested(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, dict):
            return {str(key): normalize_nested(inner) for key, inner in sorted(value.items(), key=lambda item: str(item[0]))}
        if isinstance(value, set):
            return [normalize_nested(inner) for inner in sorted(value, key=lambda item: str(item))]
        if isinstance(value, (list, tuple)):
            return [normalize_nested(inner) for inner in value]
        if hasattr(value, "tolist") and not isinstance(value, (str, bytes)):
            converted = value.tolist()
            if converted is not value:
                return normalize_nested(converted)
        return value

    def normalize_object_value(value: Any) -> str:
        if isinstance(value, (dict, set, list, tuple)):
            normalized = normalize_nested(value)
            return json.dumps(normalized, sort_keys=True, ensure_ascii=True)
        if hasattr(value, "tolist") and not isinstance(value, (str, bytes)):
            normalized = normalize_nested(value)
            if isinstance(normalized, (dict, list)):
                return json.dumps(normalized, sort_keys=True, ensure_ascii=True)
            return str(normalized)
        try:
            if pd.isna(value):
                return "<NA>"
        except TypeError:
            pass
        normalized = normalize_nested(value)
        if normalized is None:
            return "<NA>"
        if isinstance(normalized, (dict, list)):
            return json.dumps(normalized, sort_keys=True, ensure_ascii=True)
        return str(normalized)

    if pd.api.types.is_datetime64_any_dtype(series):
        return series.dt.strftime("%Y-%m-%dT%H:%M:%S%z").fillna("<NA>")
    if pd.api.types.is_bool_dtype(series):
        return series.astype("string").fillna("<NA>")
    if pd.api.types.is_integer_dtype(series):
        return series.astype("Int64").astype("string").fillna("<NA>")
    if pd.api.types.is_float_dtype(series):
        return series.astype("Float64").round(12).astype("string").fillna("<NA>")
    if pd.api.types.is_object_dtype(series):
        return series.map(normalize_object_value).astype("string")
    return series.astype("string").fillna("<NA>")


def dataframe_fingerprint(df: pd.DataFrame) -> str:
    if df.empty:
        return "empty"

    canonical = df.copy()
    for column in canonical.columns:
        canonical[column] = _normalize_for_hash(canonical[column])
    canonical = canonical.sort_values(by=canonical.columns.tolist(), na_position="last").reset_index(drop=True)

    row_hashes = pd.util.hash_pandas_object(canonical, index=False).to_numpy()
    digest = hashlib.sha256()
    digest.update("||".join(canonical.columns.tolist()).encode("utf-8"))
    digest.update(row_hashes.tobytes())
    digest.update(str(len(canonical)).encode("utf-8"))
    return digest.hexdigest()


def _write_parquet_atomic(df: pd.DataFrame, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(suffix=".parquet", dir=str(destination.parent), delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        df.to_parquet(tmp_path, index=False)
        tmp_path.replace(destination)
    finally:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)


def write_dataframe_if_changed(
    df: pd.DataFrame,
    destination: Path,
    *,
    skip_unchanged: bool,
) -> dict[str, Any]:
    new_fingerprint = dataframe_fingerprint(df)
    existing_fingerprint = None
    if destination.exists():
        existing = pd.read_parquet(destination)
        existing_fingerprint = dataframe_fingerprint(existing)
        if skip_unchanged and existing_fingerprint == new_fingerprint:
            return {
                "status": "skipped_unchanged",
                "path": str(destination),
                "row_count": int(len(df)),
                "fingerprint": new_fingerprint,
            }

    _write_parquet_atomic(df, destination)
    return {
        "status": "written",
        "path": str(destination),
        "row_count": int(len(df)),
        "fingerprint": new_fingerprint,
        "previous_fingerprint": existing_fingerprint,
    }


def write_fact_slices(
    silver_df: pd.DataFrame,
    *,
    fact_root: Path,
    skip_unchanged: bool,
    logger=None,
) -> tuple[list[dict[str, Any]], list[str]]:
    write_results: list[dict[str, Any]] = []
    touched_year_months: set[str] = set()
    year_labels = silver_df["period"].astype("string").str[:4].fillna("unknown")
    for year_text in iter_progress(
        sorted(year_labels.dropna().unique().tolist()),
        desc="Comtrade fact years",
        total=year_labels.nunique(dropna=True),
        unit="year",
    ):
        year_mask = year_labels.eq(year_text)
        year_df = silver_df.loc[year_mask].copy()
        grouped = year_df.groupby(["period", "reporter_iso3", "cmdCode", "flowCode"], dropna=False)
        year_slice_count = 0
        year_written = 0
        year_skipped = 0
        for (period, reporter_iso3, cmd_code, flow_code), part in grouped:
            period_text = str(int(period)) if pd.notna(period) else ""
            month_text = period_text[4:6]
            path = (
                fact_root
                / f"year={year_text}"
                / f"month={month_text}"
                / f"reporter_iso3={reporter_iso3}"
                / f"cmd_code={cmd_code}"
                / f"flow_code={flow_code}"
                / "comtrade_fact.parquet"
            )
            ordered = part.sort_values(
                ["period", "partner_iso3", "customsCode", "motCode", "partner2Code"],
                na_position="last",
            ).reset_index(drop=True)
            result = write_dataframe_if_changed(ordered, path, skip_unchanged=skip_unchanged)
            result["period"] = period_text
            result["reporter_iso3"] = reporter_iso3
            result["cmd_code"] = cmd_code
            result["flow_code"] = flow_code
            write_results.append(result)
            touched_year_months.add(f"{year_text}-{month_text}")
            year_slice_count += 1
            if result["status"] == "written":
                year_written += 1
            elif result["status"] == "skipped_unchanged":
                year_skipped += 1

        if logger is not None:
            logger.info(
                "Fact year=%s slices=%s written=%s skipped_unchanged=%s",
                year_text,
                year_slice_count,
                year_written,
                year_skipped,
            )

    return write_results, sorted(touched_year_months)


def load_full_fact_dataset(fact_root: Path) -> pd.DataFrame:
    fact_files = sorted(
        fact_root.glob("year=*/month=*/reporter_iso3=*/cmd_code=*/flow_code=*/comtrade_fact.parquet")
    )
    if not fact_files:
        raise FileNotFoundError(f"No Comtrade fact parquet files found under {fact_root}")
    return pd.concat((pd.read_parquet(path) for path in fact_files), ignore_index=True)


def build_dim_country(fact_df: pd.DataFrame, metadata_root: Path) -> pd.DataFrame:
    reporters_meta = pd.read_csv(metadata_root / "reporters.csv")
    partners_meta = pd.read_csv(metadata_root / "partners.csv")

    def normalize_iso3(series: pd.Series) -> pd.Series:
        return series.astype("string").str.strip().str.upper().replace({"": pd.NA, "NAN": pd.NA})

    country_meta = pd.concat(
        [
            reporters_meta[["reporterCode", "reporterCodeIsoAlpha3", "text"]].rename(
                columns={
                    "reporterCode": "country_code",
                    "reporterCodeIsoAlpha3": "iso3",
                    "text": "country_name",
                }
            ),
            partners_meta[["PartnerCode", "PartnerCodeIsoAlpha3", "text"]].rename(
                columns={
                    "PartnerCode": "country_code",
                    "PartnerCodeIsoAlpha3": "iso3",
                    "text": "country_name",
                }
            ),
        ],
        ignore_index=True,
    )

    country_meta["iso3"] = normalize_iso3(country_meta["iso3"])
    country_meta = country_meta.dropna(subset=["iso3", "country_name"]).copy()
    country_meta["country_name"] = country_meta["country_name"].astype("string").str.strip()

    iso3_universe = (
        pd.concat([fact_df["reporter_iso3"], fact_df["partner_iso3"]], ignore_index=True)
        .dropna()
        .astype("string")
        .str.strip()
        .str.upper()
        .drop_duplicates()
    )

    dim_country = (
        country_meta.loc[country_meta["iso3"].isin(iso3_universe)]
        .sort_values(["iso3", "country_code"])
        .drop_duplicates(subset=["iso3"], keep="first")
        .reset_index(drop=True)
    )

    name_fallback = pd.concat(
        [
            fact_df[["reporter_iso3", "reporter_name_clean"]].rename(
                columns={"reporter_iso3": "iso3", "reporter_name_clean": "country_name"}
            ),
            fact_df[["partner_iso3", "partner_name_clean"]].rename(
                columns={"partner_iso3": "iso3", "partner_name_clean": "country_name"}
            ),
        ],
        ignore_index=True,
    ).dropna()
    name_fallback["iso3"] = normalize_iso3(name_fallback["iso3"])
    name_fallback = name_fallback.dropna(subset=["iso3"]).drop_duplicates(subset=["iso3"], keep="first")

    missing_iso3 = sorted(set(iso3_universe) - set(dim_country["iso3"]))
    if missing_iso3:
        missing_df = name_fallback.loc[name_fallback["iso3"].isin(missing_iso3), ["iso3", "country_name"]].copy()
        missing_df["country_code"] = pd.NA
        dim_country = pd.concat(
            [dim_country, missing_df[["country_code", "iso3", "country_name"]]],
            ignore_index=True,
        )

    cc = coco.CountryConverter()
    custom_group_codes = {"A79", "E19", "F19", "S19", "W00", "X1", "XX", "_X", "EUR"}
    valid_iso3_mask = dim_country["iso3"].str.fullmatch(r"[A-Z]{3}") & ~dim_country["iso3"].isin(custom_group_codes)

    for column_name in ["subregion", "continent", "region"]:
        dim_country[column_name] = pd.NA

    dim_country.loc[valid_iso3_mask, "subregion"] = cc.convert(
        names=dim_country.loc[valid_iso3_mask, "iso3"].tolist(),
        src="ISO3",
        to="UNregion",
    )
    dim_country.loc[valid_iso3_mask, "continent"] = cc.convert(
        names=dim_country.loc[valid_iso3_mask, "iso3"].tolist(),
        src="ISO3",
        to="Continent_7",
    )
    dim_country.loc[valid_iso3_mask, "region"] = dim_country.loc[valid_iso3_mask, "continent"].replace(
        {"North America": "Americas", "South America": "Americas", "America": "Americas"}
    )

    cc_membership = cc.data[["ISO3", "EU", "OECD"]].copy()
    cc_membership["ISO3"] = cc_membership["ISO3"].astype("string").str.strip().str.upper()
    membership_map = cc_membership.rename(columns={"ISO3": "iso3", "EU": "_cc_eu", "OECD": "_cc_oecd"})
    dim_country = dim_country.merge(membership_map, on="iso3", how="left")
    dim_country["is_eu"] = dim_country["_cc_eu"].astype("string").str.strip().str.upper().eq("EU")
    dim_country["is_oecd"] = dim_country["_cc_oecd"].notna()

    group_overrides = {
        "A79": {"region": "Americas", "subregion": "Latin America and the Caribbean", "continent": "Americas"},
        "E19": {"region": "Europe", "subregion": "Europe (other/nes)", "continent": "Europe"},
        "F19": {"region": "Africa", "subregion": "Africa (other/nes)", "continent": "Africa"},
        "S19": {"region": "Asia", "subregion": "Asia (other/nes)", "continent": "Asia"},
        "W00": {"region": "World", "subregion": "World", "continent": "World"},
        "X1": {"region": "Special", "subregion": "Bunkers", "continent": "Special"},
        "XX": {"region": "Special", "subregion": "Special Categories", "continent": "Special"},
        "_X": {"region": "Special", "subregion": "Areas, nes", "continent": "Special"},
        "EUR": {"region": "Europe", "subregion": "European Union", "continent": "Europe"},
    }
    for code, values in group_overrides.items():
        mask = dim_country["iso3"].eq(code)
        if mask.any():
            for column_name, value in values.items():
                dim_country.loc[mask, column_name] = value

    dim_country.loc[dim_country["iso3"].eq("EUR"), "is_eu"] = True
    dim_country.loc[dim_country["iso3"].isin(custom_group_codes), "is_oecd"] = False

    return dim_country[
        ["country_code", "iso3", "country_name", "region", "subregion", "continent", "is_eu", "is_oecd"]
    ].sort_values("iso3").reset_index(drop=True)


def build_dim_commodity(fact_df: pd.DataFrame) -> pd.DataFrame:
    commodity_base = fact_df[["cmdCode", "cmdDesc"]].dropna(subset=["cmdCode"]).drop_duplicates().copy()
    commodity_base["cmdCode"] = commodity_base["cmdCode"].astype("string").str.strip()
    commodity_base["hs6"] = commodity_base["cmdCode"].str.pad(6, side="right", fillchar="0").str[:6]
    commodity_base["hs4"] = commodity_base["hs6"].str[:4]
    commodity_base["hs2"] = commodity_base["hs6"].str[:2]
    hs2_num = pd.to_numeric(commodity_base["hs2"], errors="coerce")

    commodity_base["commodity_group"] = pd.Series("Other", index=commodity_base.index)
    commodity_base.loc[hs2_num.between(1, 24, inclusive="both"), "commodity_group"] = "Food"
    commodity_base.loc[hs2_num.between(25, 27, inclusive="both"), "commodity_group"] = "Energy / raw materials"
    commodity_base.loc[hs2_num.between(28, 38, inclusive="both"), "commodity_group"] = "Chemicals"
    commodity_base.loc[hs2_num.between(39, 40, inclusive="both"), "commodity_group"] = "Plastics"

    commodity_base["food_flag"] = commodity_base["commodity_group"].eq("Food")
    commodity_base["energy_flag"] = commodity_base["commodity_group"].eq("Energy / raw materials")
    commodity_base["industrial_flag"] = hs2_num.between(28, 40, inclusive="both")

    return (
        commodity_base.rename(columns={"cmdDesc": "commodity_name"})[
            [
                "cmdCode",
                "hs2",
                "hs4",
                "hs6",
                "commodity_name",
                "commodity_group",
                "food_flag",
                "energy_flag",
                "industrial_flag",
            ]
        ]
        .sort_values(["cmdCode", "commodity_name"])
        .reset_index(drop=True)
    )


def build_dim_trade_flow(fact_df: pd.DataFrame, metadata_root: Path) -> pd.DataFrame:
    flows_meta = pd.read_csv(metadata_root / "flows.csv")
    dim_trade_flow = (
        flows_meta.rename(columns={"id": "flowCode", "text": "flowDesc"})[["flowCode", "flowDesc"]]
        .drop_duplicates()
        .copy()
    )
    dim_trade_flow["flowCode"] = dim_trade_flow["flowCode"].astype("string").str.strip().str.upper()
    dim_trade_flow["flow_group"] = dim_trade_flow["flowCode"].map({"M": "Import", "X": "Export"}).fillna("Other")
    flow_in_fact = set(fact_df["flowCode"].dropna().astype("string").str.strip().str.upper())
    return dim_trade_flow.loc[dim_trade_flow["flowCode"].isin(flow_in_fact)].sort_values("flowCode").reset_index(drop=True)


def build_dim_time(fact_df: pd.DataFrame) -> pd.DataFrame:
    dim_time = pd.DataFrame({"period": pd.Series(fact_df["period"].dropna().unique(), dtype="Int64")})
    dim_time = dim_time.sort_values("period").reset_index(drop=True)
    dim_time["year"] = dim_time["period"] // 100
    dim_time["month"] = dim_time["period"] % 100
    dim_time["quarter"] = ((dim_time["month"] - 1) // 3 + 1).astype("Int64")
    dim_time["year_month"] = dim_time["year"].astype("string") + "-" + dim_time["month"].astype(int).astype(str).str.zfill(2)
    dim_time["month_start_date"] = pd.to_datetime(dim_time["year_month"] + "-01", format="%Y-%m-%d", errors="coerce")
    return dim_time[["period", "year", "month", "quarter", "year_month", "month_start_date"]]


def build_helper_rollups(fact_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    partner_month = (
        fact_df[fact_df["is_bilateral_usable"]]
        .groupby(["ref_date", "period", "year_month", "ref_year", "reporter_iso3", "partner_iso3", "trade_flow"], as_index=False)
        .agg(
            trade_value_usd=("trade_value_usd", "sum"),
            netWgt=("netWgt", "sum"),
            grossWgt=("grossWgt", "sum"),
            row_count=("cmdCode", "count"),
        )
    )

    cmd_month = (
        fact_df.groupby(["ref_date", "period", "year_month", "ref_year", "reporter_iso3", "cmdCode", "cmdDesc", "trade_flow"], as_index=False)
        .agg(
            trade_value_usd=("trade_value_usd", "sum"),
            netWgt=("netWgt", "sum"),
            grossWgt=("grossWgt", "sum"),
            row_count=("partner_iso3", "count"),
        )
    )

    reporter_month = (
        fact_df.groupby(["ref_date", "period", "year_month", "ref_year", "reporter_iso3", "trade_flow"], as_index=False)
        .agg(
            trade_value_usd=("trade_value_usd", "sum"),
            netWgt=("netWgt", "sum"),
            grossWgt=("grossWgt", "sum"),
            row_count=("cmdCode", "count"),
        )
    )

    return {
        "partner_month": partner_month,
        "cmd_month": cmd_month,
        "reporter_month": reporter_month,
    }


def run(
    *,
    config: ComtradeSilverConfig,
    periods: set[str],
    since_period: str | None,
    until_period: str | None,
    reporters: set[str],
    cmd_codes: set[str],
    flow_codes: set[str],
) -> dict[str, Any]:
    logger = configure_logger(
        logger_name=LOGGER_NAME,
        log_path=config.log_path,
        log_level=config.log_level,
    )
    run_id = build_run_id("comtrade_silver")
    started_at = datetime.now(timezone.utc)
    run_audit_dir = config.audit_root / f"run_id={run_id}"

    manifest_entry: dict[str, Any] = {
        "run_id": run_id,
        "asset_name": "comtrade_silver",
        "dataset_name": "comtrade",
        "status": "running",
        "started_at": started_at.isoformat(),
        "finished_at": None,
        "duration_seconds": None,
        "bronze_root": str(config.bronze_root),
        "fact_root": str(config.fact_root),
        "dimensions_root": str(config.dimensions_root),
        "requested_periods": sorted(periods),
        "since_period": since_period,
        "until_period": until_period,
        "requested_reporters": sorted(reporters),
        "requested_cmd_codes": sorted(cmd_codes),
        "requested_flow_codes": sorted(flow_codes),
        "source_row_count": None,
        "rows_after_required_field_filter": None,
        "duplicate_collision_row_count": None,
        "fact_slice_count": None,
        "fact_slices_written": 0,
        "fact_slices_skipped_unchanged": 0,
        "touched_year_months": [],
        "metadata_precondition": None,
        "dimension_results": {},
        "audit_dir": str(run_audit_dir),
        "error_summary": None,
    }

    try:
        config.fact_root.mkdir(parents=True, exist_ok=True)
        config.dimensions_root.mkdir(parents=True, exist_ok=True)
        run_audit_dir.mkdir(parents=True, exist_ok=True)

        logger.info("Step 1/5 Load bronze history")
        bronze_df, bronze_file_inventory = load_bronze_frames(config.bronze_root, run_id=run_id, logger=logger)
        logger.info("Step 2/5 Filter bronze rows and build canonical silver rows")
        filtered_df = apply_filters(
            bronze_df,
            periods=periods,
            since_period=since_period,
            until_period=until_period,
            reporters=reporters,
            cmd_codes=cmd_codes,
            flow_codes=flow_codes,
        )

        silver_df, duplicate_collisions, prep_summary = prepare_silver(filtered_df)
        manifest_entry.update(prep_summary)

        logger.info("Step 3/5 Write canonical fact slices under %s", config.fact_root)
        fact_write_results, touched_year_months = write_fact_slices(
            silver_df,
            fact_root=config.fact_root,
            skip_unchanged=config.skip_unchanged,
            logger=logger,
        )

        manifest_entry["fact_slice_count"] = len(fact_write_results)
        manifest_entry["fact_slices_written"] = sum(1 for item in fact_write_results if item["status"] == "written")
        manifest_entry["fact_slices_skipped_unchanged"] = sum(
            1 for item in fact_write_results if item["status"] == "skipped_unchanged"
        )
        manifest_entry["touched_year_months"] = touched_year_months

        logger.info("Step 4/6 Ensure required Comtrade metadata files")
        metadata_precondition = _ensure_required_metadata(config.metadata_root, logger)
        manifest_entry["metadata_precondition"] = metadata_precondition

        logger.info("Step 5/6 Rebuild dimensions from canonical silver store")
        full_fact_df = load_full_fact_dataset(config.fact_root)
        dim_country = build_dim_country(full_fact_df, config.metadata_root)
        dim_commodity = build_dim_commodity(full_fact_df)
        dim_trade_flow = build_dim_trade_flow(full_fact_df, config.metadata_root)
        dim_time = build_dim_time(full_fact_df)

        dimension_results = {
            "dim_country": write_dataframe_if_changed(
                dim_country,
                config.dimensions_root / "dim_country.parquet",
                skip_unchanged=config.skip_unchanged,
            ),
            "dim_commodity": write_dataframe_if_changed(
                dim_commodity,
                config.dimensions_root / "dim_commodity.parquet",
                skip_unchanged=config.skip_unchanged,
            ),
            "dim_trade_flow": write_dataframe_if_changed(
                dim_trade_flow,
                config.dimensions_root / "dim_trade_flow.parquet",
                skip_unchanged=config.skip_unchanged,
            ),
            "dim_time": write_dataframe_if_changed(
                dim_time,
                config.dimensions_root / "dim_time.parquet",
                skip_unchanged=config.skip_unchanged,
            ),
        }
        manifest_entry["dimension_results"] = dimension_results

        logger.info("Step 6/6 Write audit artifacts to %s", run_audit_dir)
        bronze_file_inventory.to_parquet(run_audit_dir / "bronze_file_inventory.parquet", index=False)
        if not duplicate_collisions.empty:
            duplicate_collisions.to_parquet(run_audit_dir / "duplicate_collisions.parquet", index=False)

        pd.DataFrame(fact_write_results).to_parquet(run_audit_dir / "fact_slice_results.parquet", index=False)
        helper_rollups = build_helper_rollups(silver_df)
        for name, frame in helper_rollups.items():
            frame.to_parquet(run_audit_dir / f"{name}.parquet", index=False)

        summary_payload = {
            "run_id": run_id,
            "prep_summary": prep_summary,
            "metadata_precondition": metadata_precondition,
            "touched_year_months": touched_year_months,
            "fact_write_results": fact_write_results,
            "dimension_results": dimension_results,
        }
        (run_audit_dir / "silver_quality_summary.json").write_text(
            json.dumps(json_ready(summary_payload), indent=2),
            encoding="utf-8",
        )

        finished_at = datetime.now(timezone.utc)
        manifest_entry["status"] = "completed"
        manifest_entry["finished_at"] = finished_at.isoformat()
        manifest_entry["duration_seconds"] = duration_seconds(started_at, finished_at)
        append_manifest(config.manifest_path, manifest_entry)

        logger.info(
            "Finished Comtrade silver run_id=%s fact_slices=%s written=%s skipped_unchanged=%s duration_s=%.3f",
            run_id,
            len(fact_write_results),
            manifest_entry["fact_slices_written"],
            manifest_entry["fact_slices_skipped_unchanged"],
            manifest_entry["duration_seconds"],
        )
        return json_ready(manifest_entry)
    except Exception as exc:
        manifest_entry["status"] = "failed"
        finished_at = datetime.now(timezone.utc)
        manifest_entry["finished_at"] = finished_at.isoformat()
        manifest_entry["duration_seconds"] = duration_seconds(started_at, finished_at)
        manifest_entry["error_summary"] = str(exc)
        append_manifest(config.manifest_path, manifest_entry)
        logger.exception("Comtrade silver failed run_id=%s", run_id)
        raise


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build canonical Comtrade silver fact and dimensions from bronze JSON history."
    )
    parser.add_argument("--bronze-root", default=str(ComtradeSilverConfig.bronze_root))
    parser.add_argument("--silver-root", default=str(ComtradeSilverConfig.silver_root))
    parser.add_argument("--metadata-root", default=str(ComtradeSilverConfig.metadata_root))
    parser.add_argument("--audit-root", default=str(ComtradeSilverConfig.audit_root))
    parser.add_argument("--period", action="append", type=_parse_period, default=None)
    parser.add_argument("--since-period", type=_parse_period, default=None)
    parser.add_argument("--until-period", type=_parse_period, default=None)
    parser.add_argument("--reporters", default=None, help="Comma-separated reporter ISO3 filters.")
    parser.add_argument("--cmd-codes", default=None, help="Comma-separated commodity code filters.")
    parser.add_argument("--flow-codes", default=None, help="Comma-separated flow-code filters, e.g. M,X.")
    parser.add_argument("--overwrite-unchanged", action="store_true", help="Rewrite fact and dimension files even when the slice fingerprint is unchanged.")
    parser.add_argument("--log-level", default="INFO")
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    config = ComtradeSilverConfig(
        bronze_root=Path(args.bronze_root),
        silver_root=Path(args.silver_root),
        metadata_root=Path(args.metadata_root),
        audit_root=Path(args.audit_root),
        log_level=args.log_level,
        skip_unchanged=not args.overwrite_unchanged,
    )
    summary = run(
        config=config,
        periods=set(args.period or []),
        since_period=args.since_period,
        until_period=args.until_period,
        reporters={item.upper() for item in _parse_csv_option(args.reporters)},
        cmd_codes=set(_parse_csv_option(args.cmd_codes)),
        flow_codes={item.upper() for item in _parse_csv_option(args.flow_codes)},
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
