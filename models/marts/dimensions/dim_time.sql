with base as (
  select
    period as month_key,
    period,
    year,
    month,
    quarter,
    year_month,
    month_start_date
  from {{ ref('stg_dim_time') }}
)

select
  month_key,
  period,
  year,
  month,
  quarter,
  year_month,
  month_start_date
from base
