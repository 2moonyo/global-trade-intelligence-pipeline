with ranked as (
  select
    {{ cast_string('retry_id') }} as retry_id,
    {{ cast_string('pipeline_run_id') }} as pipeline_run_id,
    {{ cast_string('task_run_id') }} as task_run_id,
    {{ cast_string('dataset_name') }} as dataset_name,
    {{ cast_string('batch_id') }} as batch_id,
    {{ cast_string('task_name') }} as task_name,
    {{ safe_cast('attempt_number', dbt.type_int()) }} as attempt_number,
    {{ safe_cast('max_attempts', dbt.type_int()) }} as max_attempts,
    {{ cast_string('status') }} as status,
    {{ cast_string('failure_type') }} as failure_type,
    {{ safe_cast('http_status', dbt.type_int()) }} as http_status,
    cast(retryable as boolean) as retryable,
    cast(next_retry_at as timestamp) as next_retry_at,
    cast(resolved_at as timestamp) as resolved_at,
    {{ cast_string('error_summary') }} as error_summary,
    {{ cast_string('payload_json') }} as payload_json,
    cast(recorded_at as timestamp) as recorded_at,
    row_number() over (
      partition by {{ cast_string('retry_id') }}
      order by cast(recorded_at as timestamp) desc
    ) as row_num
  from {{ source('raw', 'ops_retry_registry') }}
)

select
  retry_id,
  pipeline_run_id,
  task_run_id,
  dataset_name,
  batch_id,
  task_name,
  attempt_number,
  max_attempts,
  status,
  failure_type,
  http_status,
  retryable,
  next_retry_at,
  resolved_at,
  error_summary,
  payload_json,
  recorded_at
from ranked
where row_num = 1
