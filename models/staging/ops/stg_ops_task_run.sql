with ranked as (
  select
    {{ cast_string('task_run_id') }} as task_run_id,
    {{ cast_string('pipeline_run_id') }} as pipeline_run_id,
    {{ cast_string('dataset_name') }} as dataset_name,
    {{ cast_string('batch_id') }} as batch_id,
    {{ cast_string('task_name') }} as task_name,
    {{ safe_cast('step_order', dbt.type_int()) }} as step_order,
    {{ cast_string('status') }} as status,
    {{ safe_cast('attempt_number', dbt.type_int()) }} as attempt_number,
    cast(started_at as timestamp) as started_at,
    cast(finished_at as timestamp) as finished_at,
    {{ safe_cast('duration_seconds', dbt.type_float()) }} as duration_seconds,
    {{ cast_string('local_manifest_path') }} as local_manifest_path,
    {{ cast_string('gcs_manifest_uri') }} as gcs_manifest_uri,
    {{ cast_string('log_path') }} as log_path,
    {{ cast_string('error_summary') }} as error_summary,
    {{ cast_string('command_json') }} as command_json,
    {{ cast_string('metrics_json') }} as metrics_json,
    cast(recorded_at as timestamp) as recorded_at,
    row_number() over (
      partition by {{ cast_string('task_run_id') }}
      order by cast(recorded_at as timestamp) desc
    ) as row_num
  from {{ source('raw', 'ops_task_run') }}
)

select
  task_run_id,
  pipeline_run_id,
  dataset_name,
  batch_id,
  task_name,
  step_order,
  status,
  attempt_number,
  started_at,
  finished_at,
  duration_seconds,
  local_manifest_path,
  gcs_manifest_uri,
  log_path,
  error_summary,
  command_json,
  metrics_json,
  recorded_at
from ranked
where row_num = 1
