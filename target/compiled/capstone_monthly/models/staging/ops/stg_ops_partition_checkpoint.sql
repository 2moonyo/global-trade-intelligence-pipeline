with ranked as (
  select
    cast(checkpoint_key as string) as checkpoint_key,
    cast(pipeline_run_id as string) as pipeline_run_id,
    cast(dataset_name as string) as dataset_name,
    cast(batch_id as string) as batch_id,
    cast(partition_type as string) as partition_type,
    cast(partition_key as string) as partition_key,
    cast(last_task_name as string) as last_task_name,
    cast(status as string) as status,
    cast(checkpoint_value as string) as checkpoint_value,
    cast(retryable as boolean) as retryable,
    cast(error_summary as string) as error_summary,
    cast(metrics_json as string) as metrics_json,
    cast(recorded_at as timestamp) as recorded_at,
    row_number() over (
      partition by cast(checkpoint_key as string)
      order by cast(recorded_at as timestamp) desc
    ) as row_num
  from `fullcap-10111`.`raw`.`ops_partition_checkpoint`
)

select
  checkpoint_key,
  pipeline_run_id,
  dataset_name,
  batch_id,
  partition_type,
  partition_key,
  last_task_name,
  status,
  checkpoint_value,
  retryable,
  error_summary,
  metrics_json,
  recorded_at
from ranked
where row_num = 1