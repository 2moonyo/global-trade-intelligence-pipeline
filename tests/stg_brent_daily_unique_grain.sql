-- Fails when stg_brent_daily has duplicate rows at its declared grain.
with duplicate_grain as (
  select
    date_day,
    benchmark_code,
    count(*) as row_count
  from {{ ref('stg_brent_daily') }}
  group by 1, 2
  having count(*) > 1
)

select *
from duplicate_grain
