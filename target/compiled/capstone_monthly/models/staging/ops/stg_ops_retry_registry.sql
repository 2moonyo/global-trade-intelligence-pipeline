with ranked as (
  select
    cast(retry_id as string) as retry_id,
    cast(pipeline_run_id as string) as pipeline_run_id,
    cast(task_run_id as string) as task_run_id,
    cast(dataset_name as string) as dataset_name,
    cast(batch_id as string) as batch_id,
    cast(task_name as string) as task_name,
    
    safe_cast(attempt_number as INT64)
   as attempt_number,
    
    safe_cast(max_attempts as INT64)
   as max_attempts,
    cast(status as string) as status,
    cast(failure_type as string) as failure_type,
    
    safe_cast(http_status as INT64)
   as http_status,
    cast(retryable as boolean) as retryable,
    cast(next_retry_at as timestamp) as next_retry_at,
    cast(resolved_at as timestamp) as resolved_at,
    cast(error_summary as string) as error_summary,
    cast(payload_json as string) as payload_json,
    cast(recorded_at as timestamp) as recorded_at,
    row_number() over (
      partition by cast(retry_id as string)
      order by cast(recorded_at as timestamp) desc
    ) as row_num
  from `fullcap-10111`.`raw`.`ops_retry_registry`
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