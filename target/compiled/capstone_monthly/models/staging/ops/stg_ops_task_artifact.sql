with ranked as (
  select
    cast(task_artifact_id as string) as task_artifact_id,
    cast(pipeline_run_id as string) as pipeline_run_id,
    cast(task_run_id as string) as task_run_id,
    cast(dataset_name as string) as dataset_name,
    cast(batch_id as string) as batch_id,
    cast(artifact_type as string) as artifact_type,
    cast(direction as string) as direction,
    cast(local_path as string) as local_path,
    cast(gcs_uri as string) as gcs_uri,
    cast(load_batch_id as string) as load_batch_id,
    cast(source_file as string) as source_file,
    cast(partition_key as string) as partition_key,
    cast(checksum as string) as checksum,
    
    safe_cast(record_count as INT64)
   as record_count,
    cast(payload_json as string) as payload_json,
    cast(recorded_at as timestamp) as recorded_at,
    row_number() over (
      partition by cast(task_artifact_id as string)
      order by cast(recorded_at as timestamp) desc
    ) as row_num
  from `fullcap-10111`.`raw`.`ops_task_artifact`
)

select
  task_artifact_id,
  pipeline_run_id,
  task_run_id,
  dataset_name,
  batch_id,
  artifact_type,
  direction,
  local_path,
  gcs_uri,
  load_batch_id,
  source_file,
  partition_key,
  checksum,
  record_count,
  payload_json,
  recorded_at
from ranked
where row_num = 1