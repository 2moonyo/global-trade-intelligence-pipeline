with ranked as (
  select
    {{ cast_string('checkpoint_key') }} as checkpoint_key,
    {{ cast_string('pipeline_run_id') }} as pipeline_run_id,
    {{ cast_string('dataset_name') }} as dataset_name,
    {{ cast_string('batch_id') }} as batch_id,
    {{ cast_string('partition_type') }} as partition_type,
    {{ cast_string('partition_key') }} as partition_key,
    {{ cast_string('last_task_name') }} as last_task_name,
    {{ cast_string('status') }} as status,
    {{ cast_string('checkpoint_value') }} as checkpoint_value,
    cast(retryable as boolean) as retryable,
    {{ cast_string('error_summary') }} as error_summary,
    {{ cast_string('metrics_json') }} as metrics_json,
    cast(recorded_at as timestamp) as recorded_at,
    row_number() over (
      partition by {{ cast_string('checkpoint_key') }}
      order by cast(recorded_at as timestamp) desc
    ) as row_num
  from {{ source('raw', 'ops_partition_checkpoint') }}
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
