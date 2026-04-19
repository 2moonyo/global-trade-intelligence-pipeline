from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingest.common.cloud_config import GcpCloudConfig
from ingest.common.run_artifacts import json_ready


VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
DEFAULT_POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA", "ops")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _env_flag(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}


def postgres_ops_enabled() -> bool:
    return _env_flag("OPS_POSTGRES_ENABLED", True)


def _validate_identifier(value: str, *, label: str) -> str:
    if not VALID_IDENTIFIER.fullmatch(value):
        raise ValueError(f"Invalid {label}: {value}")
    return value


def _json_text(value: Any) -> str:
    return json.dumps(json_ready(value), ensure_ascii=False, sort_keys=True)


def _pg_jsonb(value: Any) -> Any:
    from psycopg.types.json import Jsonb

    return Jsonb(json_ready(value))


def _normalize_bigquery_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in json_ready(row).items():
        if isinstance(value, (dict, list)):
            normalized[key] = _json_text(value)
        elif isinstance(value, datetime):
            normalized[key] = value.isoformat()
        else:
            normalized[key] = value
    normalized["recorded_at"] = _utc_now().isoformat()
    return normalized


def _postgres_dsn_from_env() -> str:
    direct = os.getenv("POSTGRES_DSN")
    if direct:
        return direct

    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "capstone")
    password = os.getenv("POSTGRES_PASSWORD", "capstone")
    database = os.getenv("POSTGRES_DB", "capstone")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


PIPELINE_RUN_COLUMNS = (
    "pipeline_run_id",
    "dataset_name",
    "batch_id",
    "phase",
    "schedule_lane",
    "bruin_pipeline_name",
    "trigger_type",
    "attempt_number",
    "max_attempts",
    "planned_partition_count",
    "planned_reporter_count",
    "planned_cmd_code_count",
    "planned_window_start",
    "planned_window_end",
    "status",
    "started_at",
    "finished_at",
    "queue_drained",
    "should_retry",
    "next_retry_at",
    "retry_backoff_seconds",
    "log_path",
    "gcs_log_uri",
    "error_summary",
    "run_args_json",
    "metrics_json",
)

TASK_RUN_COLUMNS = (
    "task_run_id",
    "pipeline_run_id",
    "dataset_name",
    "batch_id",
    "task_name",
    "step_order",
    "status",
    "attempt_number",
    "started_at",
    "finished_at",
    "duration_seconds",
    "local_manifest_path",
    "gcs_manifest_uri",
    "log_path",
    "error_summary",
    "command_json",
    "metrics_json",
)

TASK_ARTIFACT_COLUMNS = (
    "task_artifact_id",
    "pipeline_run_id",
    "task_run_id",
    "dataset_name",
    "batch_id",
    "artifact_type",
    "direction",
    "local_path",
    "gcs_uri",
    "load_batch_id",
    "source_file",
    "partition_key",
    "checksum",
    "record_count",
    "payload_json",
)

PARTITION_CHECKPOINT_COLUMNS = (
    "checkpoint_key",
    "pipeline_run_id",
    "dataset_name",
    "batch_id",
    "partition_type",
    "partition_key",
    "last_task_name",
    "status",
    "checkpoint_value",
    "retryable",
    "error_summary",
    "metrics_json",
)

RETRY_REGISTRY_COLUMNS = (
    "retry_id",
    "pipeline_run_id",
    "task_run_id",
    "dataset_name",
    "batch_id",
    "task_name",
    "attempt_number",
    "max_attempts",
    "status",
    "failure_type",
    "http_status",
    "retryable",
    "next_retry_at",
    "resolved_at",
    "error_summary",
    "payload_json",
)


POSTGRES_DDL = (
    """
    CREATE TABLE IF NOT EXISTS {schema}.pipeline_run (
      pipeline_run_id TEXT PRIMARY KEY,
      dataset_name TEXT NOT NULL,
      batch_id TEXT NOT NULL,
      phase TEXT,
      schedule_lane TEXT,
      bruin_pipeline_name TEXT,
      trigger_type TEXT,
      attempt_number INTEGER NOT NULL DEFAULT 1,
      max_attempts INTEGER NOT NULL DEFAULT 3,
      planned_partition_count INTEGER,
      planned_reporter_count INTEGER,
      planned_cmd_code_count INTEGER,
      planned_window_start TEXT,
      planned_window_end TEXT,
      status TEXT NOT NULL,
      started_at TIMESTAMPTZ NOT NULL,
      finished_at TIMESTAMPTZ,
      queue_drained BOOLEAN,
      should_retry BOOLEAN,
      next_retry_at TIMESTAMPTZ,
      retry_backoff_seconds INTEGER,
      log_path TEXT,
      gcs_log_uri TEXT,
      error_summary TEXT,
      run_args_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
      metrics_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS {schema}.task_run (
      task_run_id TEXT PRIMARY KEY,
      pipeline_run_id TEXT NOT NULL REFERENCES {schema}.pipeline_run(pipeline_run_id) ON DELETE CASCADE,
      dataset_name TEXT NOT NULL,
      batch_id TEXT NOT NULL,
      task_name TEXT NOT NULL,
      step_order INTEGER,
      status TEXT NOT NULL,
      attempt_number INTEGER NOT NULL DEFAULT 1,
      started_at TIMESTAMPTZ NOT NULL,
      finished_at TIMESTAMPTZ,
      duration_seconds DOUBLE PRECISION,
      local_manifest_path TEXT,
      gcs_manifest_uri TEXT,
      log_path TEXT,
      error_summary TEXT,
      command_json JSONB NOT NULL DEFAULT '[]'::jsonb,
      metrics_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (pipeline_run_id, task_name, attempt_number)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS {schema}.task_artifact (
      task_artifact_id TEXT PRIMARY KEY,
      pipeline_run_id TEXT NOT NULL REFERENCES {schema}.pipeline_run(pipeline_run_id) ON DELETE CASCADE,
      task_run_id TEXT NOT NULL REFERENCES {schema}.task_run(task_run_id) ON DELETE CASCADE,
      dataset_name TEXT NOT NULL,
      batch_id TEXT NOT NULL,
      artifact_type TEXT NOT NULL,
      direction TEXT,
      local_path TEXT,
      gcs_uri TEXT,
      load_batch_id TEXT,
      source_file TEXT,
      partition_key TEXT,
      checksum TEXT,
      record_count BIGINT,
      payload_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS {schema}.partition_checkpoint (
      checkpoint_key TEXT PRIMARY KEY,
      pipeline_run_id TEXT NOT NULL REFERENCES {schema}.pipeline_run(pipeline_run_id) ON DELETE CASCADE,
      dataset_name TEXT NOT NULL,
      batch_id TEXT NOT NULL,
      partition_type TEXT NOT NULL,
      partition_key TEXT NOT NULL,
      last_task_name TEXT,
      status TEXT NOT NULL,
      checkpoint_value TEXT,
      retryable BOOLEAN,
      error_summary TEXT,
      metrics_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (dataset_name, batch_id, partition_type, partition_key)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS {schema}.retry_registry (
      retry_id TEXT PRIMARY KEY,
      pipeline_run_id TEXT REFERENCES {schema}.pipeline_run(pipeline_run_id) ON DELETE SET NULL,
      task_run_id TEXT REFERENCES {schema}.task_run(task_run_id) ON DELETE SET NULL,
      dataset_name TEXT NOT NULL,
      batch_id TEXT NOT NULL,
      task_name TEXT,
      attempt_number INTEGER NOT NULL,
      max_attempts INTEGER NOT NULL,
      status TEXT NOT NULL,
      failure_type TEXT,
      http_status INTEGER,
      retryable BOOLEAN NOT NULL DEFAULT TRUE,
      next_retry_at TIMESTAMPTZ,
      resolved_at TIMESTAMPTZ,
      error_summary TEXT,
      payload_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """,
)


BIGQUERY_SCHEMAS = {
    "ops_pipeline_run": (
        ("pipeline_run_id", "STRING", "REQUIRED"),
        ("dataset_name", "STRING", "REQUIRED"),
        ("batch_id", "STRING", "REQUIRED"),
        ("phase", "STRING", "NULLABLE"),
        ("schedule_lane", "STRING", "NULLABLE"),
        ("bruin_pipeline_name", "STRING", "NULLABLE"),
        ("trigger_type", "STRING", "NULLABLE"),
        ("attempt_number", "INT64", "NULLABLE"),
        ("max_attempts", "INT64", "NULLABLE"),
        ("planned_partition_count", "INT64", "NULLABLE"),
        ("planned_reporter_count", "INT64", "NULLABLE"),
        ("planned_cmd_code_count", "INT64", "NULLABLE"),
        ("planned_window_start", "STRING", "NULLABLE"),
        ("planned_window_end", "STRING", "NULLABLE"),
        ("status", "STRING", "NULLABLE"),
        ("started_at", "TIMESTAMP", "NULLABLE"),
        ("finished_at", "TIMESTAMP", "NULLABLE"),
        ("queue_drained", "BOOL", "NULLABLE"),
        ("should_retry", "BOOL", "NULLABLE"),
        ("next_retry_at", "TIMESTAMP", "NULLABLE"),
        ("retry_backoff_seconds", "INT64", "NULLABLE"),
        ("log_path", "STRING", "NULLABLE"),
        ("gcs_log_uri", "STRING", "NULLABLE"),
        ("error_summary", "STRING", "NULLABLE"),
        ("run_args_json", "STRING", "NULLABLE"),
        ("metrics_json", "STRING", "NULLABLE"),
        ("recorded_at", "TIMESTAMP", "NULLABLE"),
    ),
    "ops_task_run": (
        ("task_run_id", "STRING", "REQUIRED"),
        ("pipeline_run_id", "STRING", "REQUIRED"),
        ("dataset_name", "STRING", "REQUIRED"),
        ("batch_id", "STRING", "REQUIRED"),
        ("task_name", "STRING", "REQUIRED"),
        ("step_order", "INT64", "NULLABLE"),
        ("status", "STRING", "NULLABLE"),
        ("attempt_number", "INT64", "NULLABLE"),
        ("started_at", "TIMESTAMP", "NULLABLE"),
        ("finished_at", "TIMESTAMP", "NULLABLE"),
        ("duration_seconds", "FLOAT64", "NULLABLE"),
        ("local_manifest_path", "STRING", "NULLABLE"),
        ("gcs_manifest_uri", "STRING", "NULLABLE"),
        ("log_path", "STRING", "NULLABLE"),
        ("error_summary", "STRING", "NULLABLE"),
        ("command_json", "STRING", "NULLABLE"),
        ("metrics_json", "STRING", "NULLABLE"),
        ("recorded_at", "TIMESTAMP", "NULLABLE"),
    ),
    "ops_task_artifact": (
        ("task_artifact_id", "STRING", "REQUIRED"),
        ("pipeline_run_id", "STRING", "REQUIRED"),
        ("task_run_id", "STRING", "REQUIRED"),
        ("dataset_name", "STRING", "REQUIRED"),
        ("batch_id", "STRING", "REQUIRED"),
        ("artifact_type", "STRING", "REQUIRED"),
        ("direction", "STRING", "NULLABLE"),
        ("local_path", "STRING", "NULLABLE"),
        ("gcs_uri", "STRING", "NULLABLE"),
        ("load_batch_id", "STRING", "NULLABLE"),
        ("source_file", "STRING", "NULLABLE"),
        ("partition_key", "STRING", "NULLABLE"),
        ("checksum", "STRING", "NULLABLE"),
        ("record_count", "INT64", "NULLABLE"),
        ("payload_json", "STRING", "NULLABLE"),
        ("recorded_at", "TIMESTAMP", "NULLABLE"),
    ),
    "ops_partition_checkpoint": (
        ("checkpoint_key", "STRING", "REQUIRED"),
        ("pipeline_run_id", "STRING", "REQUIRED"),
        ("dataset_name", "STRING", "REQUIRED"),
        ("batch_id", "STRING", "REQUIRED"),
        ("partition_type", "STRING", "REQUIRED"),
        ("partition_key", "STRING", "REQUIRED"),
        ("last_task_name", "STRING", "NULLABLE"),
        ("status", "STRING", "NULLABLE"),
        ("checkpoint_value", "STRING", "NULLABLE"),
        ("retryable", "BOOL", "NULLABLE"),
        ("error_summary", "STRING", "NULLABLE"),
        ("metrics_json", "STRING", "NULLABLE"),
        ("recorded_at", "TIMESTAMP", "NULLABLE"),
    ),
    "ops_retry_registry": (
        ("retry_id", "STRING", "REQUIRED"),
        ("pipeline_run_id", "STRING", "NULLABLE"),
        ("task_run_id", "STRING", "NULLABLE"),
        ("dataset_name", "STRING", "REQUIRED"),
        ("batch_id", "STRING", "REQUIRED"),
        ("task_name", "STRING", "NULLABLE"),
        ("attempt_number", "INT64", "NULLABLE"),
        ("max_attempts", "INT64", "NULLABLE"),
        ("status", "STRING", "NULLABLE"),
        ("failure_type", "STRING", "NULLABLE"),
        ("http_status", "INT64", "NULLABLE"),
        ("retryable", "BOOL", "NULLABLE"),
        ("next_retry_at", "TIMESTAMP", "NULLABLE"),
        ("resolved_at", "TIMESTAMP", "NULLABLE"),
        ("error_summary", "STRING", "NULLABLE"),
        ("payload_json", "STRING", "NULLABLE"),
        ("recorded_at", "TIMESTAMP", "NULLABLE"),
    ),
}


def _bigquery_imports():
    try:
        from google.cloud import bigquery
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "google-cloud-bigquery is required for ops BigQuery mirroring."
        ) from exc
    return bigquery


@dataclass
class PostgresOpsStore:
    dsn: str
    schema: str = DEFAULT_POSTGRES_SCHEMA

    @classmethod
    def from_env(cls) -> "PostgresOpsStore":
        return cls(
            dsn=_postgres_dsn_from_env(),
            schema=_validate_identifier(os.getenv("POSTGRES_SCHEMA", DEFAULT_POSTGRES_SCHEMA), label="Postgres schema"),
        )

    def _connect(self):
        import psycopg
        from psycopg.rows import dict_row

        return psycopg.connect(self.dsn, autocommit=True, row_factory=dict_row)

    def ensure_schema(self) -> None:
        schema = _validate_identifier(self.schema, label="Postgres schema")
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            for ddl in POSTGRES_DDL:
                cur.execute(ddl.format(schema=schema))

    def _qualified(self, table_name: str) -> str:
        return f"{_validate_identifier(self.schema, label='Postgres schema')}.{_validate_identifier(table_name, label='Postgres table')}"

    def _prepare_values(self, row: dict[str, Any], *, json_columns: set[str]) -> list[Any]:
        values: list[Any] = []
        for key in row:
            value = row[key]
            if key in json_columns:
                values.append(_pg_jsonb(value))
            else:
                values.append(json_ready(value))
        return values

    def _upsert(
        self,
        table_name: str,
        row: dict[str, Any],
        *,
        conflict_columns: tuple[str, ...],
        json_columns: set[str],
    ) -> None:
        columns = list(row.keys())
        placeholders = ", ".join(["%s"] * len(columns))
        update_columns = [column for column in columns if column not in conflict_columns]
        update_clause = ", ".join(f"{column} = EXCLUDED.{column}" for column in update_columns)
        sql = (
            f"INSERT INTO {self._qualified(table_name)} ({', '.join(columns)}) "
            f"VALUES ({placeholders}) "
            f"ON CONFLICT ({', '.join(conflict_columns)}) DO UPDATE SET {update_clause}"
        )
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(sql, self._prepare_values(row, json_columns=json_columns))

    def insert_pipeline_run(self, row: dict[str, Any]) -> None:
        payload = {key: row.get(key) for key in PIPELINE_RUN_COLUMNS}
        self._upsert(
            "pipeline_run",
            payload,
            conflict_columns=("pipeline_run_id",),
            json_columns={"run_args_json", "metrics_json"},
        )

    def insert_task_run(self, row: dict[str, Any]) -> None:
        payload = {key: row.get(key) for key in TASK_RUN_COLUMNS}
        self._upsert(
            "task_run",
            payload,
            conflict_columns=("task_run_id",),
            json_columns={"command_json", "metrics_json"},
        )

    def insert_task_artifacts(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        with self._connect() as conn, conn.cursor() as cur:
            sql = (
                f"INSERT INTO {self._qualified('task_artifact')} ({', '.join(TASK_ARTIFACT_COLUMNS)}) "
                f"VALUES ({', '.join(['%s'] * len(TASK_ARTIFACT_COLUMNS))}) "
                f"ON CONFLICT (task_artifact_id) DO NOTHING"
            )
            for row in rows:
                payload = {key: row.get(key) for key in TASK_ARTIFACT_COLUMNS}
                cur.execute(sql, self._prepare_values(payload, json_columns={"payload_json"}))

    def upsert_partition_checkpoints(self, rows: list[dict[str, Any]]) -> None:
        for row in rows:
            payload = {key: row.get(key) for key in PARTITION_CHECKPOINT_COLUMNS}
            self._upsert(
                "partition_checkpoint",
                payload,
                conflict_columns=("checkpoint_key",),
                json_columns={"metrics_json"},
            )

    def upsert_retry_registry(self, row: dict[str, Any]) -> None:
        payload = {key: row.get(key) for key in RETRY_REGISTRY_COLUMNS}
        self._upsert(
            "retry_registry",
            payload,
            conflict_columns=("retry_id",),
            json_columns={"payload_json"},
        )

    def fetch_latest_batch_statuses(self) -> dict[str, dict[str, Any]]:
        query = f"""
            SELECT DISTINCT ON (batch_id)
              batch_id,
              dataset_name,
              status,
              attempt_number,
              finished_at,
              pipeline_run_id
            FROM {self._qualified('pipeline_run')}
            ORDER BY batch_id, COALESCE(finished_at, started_at) DESC, pipeline_run_id DESC
        """
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
        return {row["batch_id"]: row for row in rows}


@dataclass
class NoOpPostgresOpsStore:
    """Drop-in store used by stateless runtimes that mirror ops records to BigQuery only."""

    schema: str = DEFAULT_POSTGRES_SCHEMA

    @classmethod
    def from_env(cls) -> "NoOpPostgresOpsStore":
        return cls(schema=os.getenv("POSTGRES_SCHEMA", DEFAULT_POSTGRES_SCHEMA))

    def ensure_schema(self) -> None:
        return None

    def insert_pipeline_run(self, row: dict[str, Any]) -> None:
        return None

    def insert_task_run(self, row: dict[str, Any]) -> None:
        return None

    def insert_task_artifacts(self, rows: list[dict[str, Any]]) -> None:
        return None

    def upsert_partition_checkpoints(self, rows: list[dict[str, Any]]) -> None:
        return None

    def upsert_retry_registry(self, row: dict[str, Any]) -> None:
        return None

    def fetch_latest_batch_statuses(self) -> dict[str, dict[str, Any]]:
        return {}


@dataclass
class BigQueryOpsMirror:
    config: GcpCloudConfig

    @classmethod
    def from_env(cls) -> "BigQueryOpsMirror":
        return cls(config=GcpCloudConfig.from_env())

    def ensure_tables(self) -> None:
        bigquery = _bigquery_imports()
        client = bigquery.Client(project=self.config.gcp_project_id)
        dataset = bigquery.Dataset(f"{self.config.gcp_project_id}.{self.config.bq_raw_dataset}")
        dataset.location = self.config.gcp_location
        client.create_dataset(dataset, exists_ok=True)

        for table_name, schema_definition in BIGQUERY_SCHEMAS.items():
            schema = [
                bigquery.SchemaField(field_name, field_type, mode=field_mode)
                for field_name, field_type, field_mode in schema_definition
            ]
            table = bigquery.Table(
                f"{self.config.gcp_project_id}.{self.config.bq_raw_dataset}.{table_name}",
                schema=schema,
            )
            client.create_table(table, exists_ok=True)

    def append_snapshot(self, table_name: str, row: dict[str, Any]) -> None:
        bigquery = _bigquery_imports()
        client = bigquery.Client(project=self.config.gcp_project_id)
        table_id = f"{self.config.gcp_project_id}.{self.config.bq_raw_dataset}.{table_name}"
        errors = client.insert_rows_json(table_id, [_normalize_bigquery_row(row)])
        if errors:
            raise RuntimeError(f"BigQuery insert into {table_id} returned errors: {errors}")

    def append_pipeline_run(self, row: dict[str, Any]) -> None:
        self.append_snapshot("ops_pipeline_run", row)

    def append_task_run(self, row: dict[str, Any]) -> None:
        self.append_snapshot("ops_task_run", row)

    def append_task_artifacts(self, rows: list[dict[str, Any]]) -> None:
        for row in rows:
            self.append_snapshot("ops_task_artifact", row)

    def append_partition_checkpoints(self, rows: list[dict[str, Any]]) -> None:
        for row in rows:
            self.append_snapshot("ops_partition_checkpoint", row)

    def append_retry_registry(self, row: dict[str, Any]) -> None:
        self.append_snapshot("ops_retry_registry", row)


def bigquery_mirror_enabled() -> bool:
    return os.getenv("ENABLE_BIGQUERY_OPS_MIRROR", "true").strip().lower() not in {"0", "false", "no"}


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Initialize and validate the ops ledger tables.")
    parser.add_argument(
        "command",
        choices=("ensure-postgres", "ensure-bigquery", "ensure-all"),
        help="Ledger bootstrap action to run.",
    )
    return parser


def main() -> None:
    args = _build_arg_parser().parse_args()
    if args.command in {"ensure-postgres", "ensure-all"}:
        if postgres_ops_enabled():
            store = PostgresOpsStore.from_env()
            store.ensure_schema()
            print(f"Ensured Postgres ops schema {store.schema}.")
        else:
            print("Postgres ops store disabled by OPS_POSTGRES_ENABLED=false.")

    if args.command in {"ensure-bigquery", "ensure-all"}:
        if not bigquery_mirror_enabled():
            print("BigQuery ops mirror disabled by ENABLE_BIGQUERY_OPS_MIRROR.")
            return
        mirror = BigQueryOpsMirror.from_env()
        mirror.ensure_tables()
        print(
            f"Ensured BigQuery ops tables in {mirror.config.gcp_project_id}.{mirror.config.bq_raw_dataset}."
        )


if __name__ == "__main__":
    main()
