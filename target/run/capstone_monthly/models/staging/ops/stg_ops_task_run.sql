

  create or replace view `fullcap-10111`.`analytics_staging`.`stg_ops_task_run`
  OPTIONS()
  as with ranked as (
  select
    cast(task_run_id as string) as task_run_id,
    cast(pipeline_run_id as string) as pipeline_run_id,
    cast(dataset_name as string) as dataset_name,
    cast(batch_id as string) as batch_id,
    cast(task_name as string) as task_name,
    
    safe_cast(step_order as INT64)
   as step_order,
    cast(status as string) as status,
    
    safe_cast(attempt_number as INT64)
   as attempt_number,
    cast(started_at as timestamp) as started_at,
    cast(finished_at as timestamp) as finished_at,
    
    safe_cast(duration_seconds as FLOAT64)
   as duration_seconds,
    cast(local_manifest_path as string) as local_manifest_path,
    cast(gcs_manifest_uri as string) as gcs_manifest_uri,
    cast(log_path as string) as log_path,
    cast(error_summary as string) as error_summary,
    cast(command_json as string) as command_json,
    cast(metrics_json as string) as metrics_json,
    cast(recorded_at as timestamp) as recorded_at,
    row_number() over (
      partition by cast(task_run_id as string)
      order by cast(recorded_at as timestamp) desc
    ) as row_num
  from `fullcap-10111`.`raw`.`ops_task_run`
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
where row_num = 1;

