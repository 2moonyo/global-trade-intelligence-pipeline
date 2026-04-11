with ranked as (
  select
    {{ cast_string('pipeline_run_id') }} as pipeline_run_id,
    {{ cast_string('dataset_name') }} as dataset_name,
    {{ cast_string('batch_id') }} as batch_id,
    {{ cast_string('phase') }} as phase,
    {{ cast_string('schedule_lane') }} as schedule_lane,
    {{ cast_string('bruin_pipeline_name') }} as bruin_pipeline_name,
    {{ cast_string('trigger_type') }} as trigger_type,
    {{ safe_cast('attempt_number', dbt.type_int()) }} as attempt_number,
    {{ safe_cast('max_attempts', dbt.type_int()) }} as max_attempts,
    {{ safe_cast('planned_partition_count', dbt.type_int()) }} as planned_partition_count,
    {{ safe_cast('planned_reporter_count', dbt.type_int()) }} as planned_reporter_count,
    {{ safe_cast('planned_cmd_code_count', dbt.type_int()) }} as planned_cmd_code_count,
    {{ cast_string('planned_window_start') }} as planned_window_start,
    {{ cast_string('planned_window_end') }} as planned_window_end,
    {{ cast_string('status') }} as status,
    cast(started_at as timestamp) as started_at,
    cast(finished_at as timestamp) as finished_at,
    cast(queue_drained as boolean) as queue_drained,
    cast(should_retry as boolean) as should_retry,
    cast(next_retry_at as timestamp) as next_retry_at,
    {{ safe_cast('retry_backoff_seconds', dbt.type_int()) }} as retry_backoff_seconds,
    {{ cast_string('log_path') }} as log_path,
    {{ cast_string('gcs_log_uri') }} as gcs_log_uri,
    {{ cast_string('error_summary') }} as error_summary,
    {{ cast_string('run_args_json') }} as run_args_json,
    {{ cast_string('metrics_json') }} as metrics_json,
    cast(recorded_at as timestamp) as recorded_at,
    row_number() over (
      partition by {{ cast_string('pipeline_run_id') }}
      order by cast(recorded_at as timestamp) desc
    ) as row_num
  from {{ source('raw', 'ops_pipeline_run') }}
)

select
  pipeline_run_id,
  dataset_name,
  batch_id,
  phase,
  schedule_lane,
  bruin_pipeline_name,
  trigger_type,
  attempt_number,
  max_attempts,
  planned_partition_count,
  planned_reporter_count,
  planned_cmd_code_count,
  planned_window_start,
  planned_window_end,
  status,
  started_at,
  finished_at,
  queue_drained,
  should_retry,
  next_retry_at,
  retry_backoff_seconds,
  log_path,
  gcs_log_uri,
  error_summary,
  run_args_json,
  metrics_json,
  recorded_at
from ranked
where row_num = 1
