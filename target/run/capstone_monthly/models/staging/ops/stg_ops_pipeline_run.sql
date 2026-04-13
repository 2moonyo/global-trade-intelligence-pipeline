

  create or replace view `fullcap-10111`.`analytics_staging`.`stg_ops_pipeline_run`
  OPTIONS()
  as with ranked as (
  select
    cast(pipeline_run_id as string) as pipeline_run_id,
    cast(dataset_name as string) as dataset_name,
    cast(batch_id as string) as batch_id,
    cast(phase as string) as phase,
    cast(schedule_lane as string) as schedule_lane,
    cast(bruin_pipeline_name as string) as bruin_pipeline_name,
    cast(trigger_type as string) as trigger_type,
    
    safe_cast(attempt_number as INT64)
   as attempt_number,
    
    safe_cast(max_attempts as INT64)
   as max_attempts,
    
    safe_cast(planned_partition_count as INT64)
   as planned_partition_count,
    
    safe_cast(planned_reporter_count as INT64)
   as planned_reporter_count,
    
    safe_cast(planned_cmd_code_count as INT64)
   as planned_cmd_code_count,
    cast(planned_window_start as string) as planned_window_start,
    cast(planned_window_end as string) as planned_window_end,
    cast(status as string) as status,
    cast(started_at as timestamp) as started_at,
    cast(finished_at as timestamp) as finished_at,
    cast(queue_drained as boolean) as queue_drained,
    cast(should_retry as boolean) as should_retry,
    cast(next_retry_at as timestamp) as next_retry_at,
    
    safe_cast(retry_backoff_seconds as INT64)
   as retry_backoff_seconds,
    cast(log_path as string) as log_path,
    cast(gcs_log_uri as string) as gcs_log_uri,
    cast(error_summary as string) as error_summary,
    cast(run_args_json as string) as run_args_json,
    cast(metrics_json as string) as metrics_json,
    cast(recorded_at as timestamp) as recorded_at,
    row_number() over (
      partition by cast(pipeline_run_id as string)
      order by cast(recorded_at as timestamp) desc
    ) as row_num
  from `fullcap-10111`.`raw`.`ops_pipeline_run`
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
where row_num = 1;

