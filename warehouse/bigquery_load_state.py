from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlparse


@dataclass(frozen=True)
class LoadStateRecord:
    entity_type: str
    entity_key: str
    source_checksum: str
    checksum_kind: str
    source_uris: tuple[str, ...]
    target_table_id: str


def blob_name_from_gcs_uri(uri: str) -> str:
    parsed = urlparse(uri)
    if parsed.scheme != "gs" or not parsed.netloc or not parsed.path:
        raise ValueError(f"Expected a gs:// URI, got {uri!r}")
    return parsed.path.lstrip("/")


def composite_checksum(entries: list[tuple[str, str]]) -> str:
    digest = hashlib.sha256()
    for uri, checksum in sorted(entries):
        digest.update(uri.encode("utf-8"))
        digest.update(b"\x00")
        digest.update(checksum.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def ensure_load_state_table(client, *, bigquery, project_id: str, dataset_name: str, table_name: str) -> str:
    table_id = f"{project_id}.{dataset_name}.{table_name}"
    schema = [
        bigquery.SchemaField("entity_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("entity_key", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("target_table_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("source_checksum", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("checksum_kind", "STRING"),
        bigquery.SchemaField("source_uri_count", "INT64"),
        bigquery.SchemaField("source_uris_json", "STRING"),
        bigquery.SchemaField("run_id", "STRING"),
        bigquery.SchemaField("source_mode", "STRING"),
        bigquery.SchemaField("loaded_at", "TIMESTAMP"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table, exists_ok=True)
    return table_id


def fetch_load_state_checksums(
    client,
    *,
    bigquery,
    state_table_id: str,
    entity_type: str,
    entity_keys: list[str],
    location: str,
) -> dict[str, str]:
    if not entity_keys:
        return {}

    query = f"""
    select entity_key, source_checksum
    from `{state_table_id}`
    where entity_type = @entity_type
      and entity_key in unnest(@entity_keys)
    """
    job = client.query(
        query,
        location=location,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("entity_type", "STRING", entity_type),
                bigquery.ArrayQueryParameter("entity_keys", "STRING", entity_keys),
            ]
        ),
    )
    return {row["entity_key"]: row["source_checksum"] for row in job.result()}


def replace_load_state_rows(
    client,
    *,
    bigquery,
    state_table_id: str,
    rows: list[LoadStateRecord],
    run_id: str,
    source_mode: str,
    location: str,
) -> None:
    if not rows:
        return

    rows_by_type: dict[str, list[LoadStateRecord]] = {}
    for row in rows:
        rows_by_type.setdefault(row.entity_type, []).append(row)

    for entity_type, grouped_rows in rows_by_type.items():
        entity_keys = sorted({row.entity_key for row in grouped_rows})
        delete_sql = f"""
        delete from `{state_table_id}`
        where entity_type = @entity_type
          and entity_key in unnest(@entity_keys)
        """
        delete_job = client.query(
            delete_sql,
            location=location,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("entity_type", "STRING", entity_type),
                    bigquery.ArrayQueryParameter("entity_keys", "STRING", entity_keys),
                ]
            ),
        )
        delete_job.result()

    loaded_at = datetime.now(timezone.utc).isoformat()
    payload = [
        {
            "entity_type": row.entity_type,
            "entity_key": row.entity_key,
            "target_table_id": row.target_table_id,
            "source_checksum": row.source_checksum,
            "checksum_kind": row.checksum_kind,
            "source_uri_count": len(row.source_uris),
            "source_uris_json": json.dumps(list(row.source_uris)),
            "run_id": run_id,
            "source_mode": source_mode,
            "loaded_at": loaded_at,
        }
        for row in rows
    ]
    errors = client.insert_rows_json(state_table_id, payload)
    if errors:
        raise RuntimeError(f"Failed to update BigQuery load state table {state_table_id}: {errors}")
