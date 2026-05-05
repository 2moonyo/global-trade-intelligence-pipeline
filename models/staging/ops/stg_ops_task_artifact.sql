with ranked as (
  select
    {{ cast_string('task_artifact_id') }} as task_artifact_id,
    {{ cast_string('pipeline_run_id') }} as pipeline_run_id,
    {{ cast_string('task_run_id') }} as task_run_id,
    {{ cast_string('dataset_name') }} as dataset_name,
    {{ cast_string('batch_id') }} as batch_id,
    {{ cast_string('artifact_type') }} as artifact_type,
    {{ cast_string('direction') }} as direction,
    {{ cast_string('local_path') }} as local_path,
    {{ cast_string('gcs_uri') }} as gcs_uri,
    {{ cast_string('load_batch_id') }} as load_batch_id,
    {{ cast_string('source_file') }} as source_file,
    {{ cast_string('partition_key') }} as partition_key,
    {{ cast_string('checksum') }} as checksum,
    {{ safe_cast('record_count', dbt.type_int()) }} as record_count,
    {{ cast_string('payload_json') }} as payload_json,
    cast(recorded_at as timestamp) as recorded_at,
    row_number() over (
      partition by {{ cast_string('task_artifact_id') }}
      order by cast(recorded_at as timestamp) desc
    ) as row_num
  from {{ source('raw', 'ops_task_artifact') }}
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
