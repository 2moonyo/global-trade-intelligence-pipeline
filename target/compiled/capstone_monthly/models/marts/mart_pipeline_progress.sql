with latest_batch_run as (
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
    status as latest_status,
    started_at as latest_started_at,
    finished_at as latest_finished_at,
    queue_drained,
    should_retry,
    next_retry_at,
    retry_backoff_seconds,
    log_path as latest_log_path,
    gcs_log_uri as latest_gcs_log_uri,
    error_summary as latest_error_summary
  from (
    select
      *,
      row_number() over (
        partition by batch_id
        order by coalesce(finished_at, started_at) desc, attempt_number desc, recorded_at desc
      ) as row_num
    from `fullcap-10111`.`analytics_staging`.`stg_ops_pipeline_run`
  )
  where row_num = 1
),

last_success as (
  select
    batch_id,
    max(finished_at) as last_success_at
  from `fullcap-10111`.`analytics_staging`.`stg_ops_pipeline_run`
  where status = 'completed'
  group by 1
),

task_summary as (
  select
    batch_id,
    count(*) as total_task_runs,
    sum(case when status = 'completed' then 1 else 0 end) as completed_task_runs,
    max(case when status <> 'completed' then task_name else null end) as latest_incomplete_task_name
  from `fullcap-10111`.`analytics_staging`.`stg_ops_task_run`
  group by 1
),

checkpoint_summary as (
  select
    batch_id,
    count(*) as tracked_partition_count,
    sum(case when status = 'completed' then 1 else 0 end) as completed_partition_count,
    sum(case when status = 'failed' then 1 else 0 end) as failed_partition_count
  from `fullcap-10111`.`analytics_staging`.`stg_ops_partition_checkpoint`
  where partition_type <> 'batch'
  group by 1
),

artifact_summary as (
  select
    batch_id,
    count(*) as artifact_count,
    sum(case when load_batch_id is not null then 1 else 0 end) as lineage_artifact_count
  from `fullcap-10111`.`analytics_staging`.`stg_ops_task_artifact`
  group by 1
),

retry_summary as (
  select
    batch_id,
    count(*) as retry_event_count,
    sum(case when status = 'scheduled' then 1 else 0 end) as open_retry_event_count,
    max(next_retry_at) as next_retry_at
  from `fullcap-10111`.`analytics_staging`.`stg_ops_retry_registry`
  group by 1
)

select
  l.dataset_name,
  l.batch_id,
  l.phase,
  l.schedule_lane,
  l.bruin_pipeline_name,
  l.trigger_type,
  l.latest_status,
  l.attempt_number as latest_attempt_number,
  l.max_attempts,
  l.latest_started_at,
  l.latest_finished_at,
  s.last_success_at,
  l.planned_partition_count,
  l.planned_reporter_count,
  l.planned_cmd_code_count,
  l.planned_window_start,
  l.planned_window_end,
  c.tracked_partition_count,
  c.completed_partition_count,
  c.failed_partition_count,
  case
    when l.planned_partition_count is null or l.planned_partition_count = 0 then null
    else cast(c.completed_partition_count as FLOAT64) / cast(l.planned_partition_count as FLOAT64)
  end as planned_partition_completion_ratio,
  t.total_task_runs,
  t.completed_task_runs,
  t.latest_incomplete_task_name,
  a.artifact_count,
  a.lineage_artifact_count,
  r.retry_event_count,
  r.open_retry_event_count,
  r.next_retry_at,
  l.queue_drained,
  l.should_retry,
  l.retry_backoff_seconds,
  l.latest_log_path,
  l.latest_gcs_log_uri,
  l.latest_error_summary
from latest_batch_run as l
left join last_success as s
  on l.batch_id = s.batch_id
left join task_summary as t
  on l.batch_id = t.batch_id
left join checkpoint_summary as c
  on l.batch_id = c.batch_id
left join artifact_summary as a
  on l.batch_id = a.batch_id
left join retry_summary as r
  on l.batch_id = r.batch_id