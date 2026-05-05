-- Fails when comtrade months cannot be resolved in dim_time.
with comtrade_months as (
  select distinct
    coalesce(
      case
        when year_month is not null
          and {{ regex_full_match('year_month', '^\\d{4}-\\d{2}$') }}
          then {{ month_start_from_year_month('year_month') }}
        else null
      end,
      case
        when period is not null
          and {{ regex_full_match('period', '^\\d{4}(0[1-9]|1[0-2])$') }}
          then {{ month_start_from_year_month("substr(" ~ cast_string('period') ~ ", 1, 4) || '-' || substr(" ~ cast_string('period') ~ ", 5, 2)") }}
        else null
      end
    ) as month_start_date
  from {{ source('raw', 'comtrade_fact') }}
),
missing_in_dim_time as (
  select
    cm.month_start_date
  from comtrade_months as cm
  left join {{ ref('dim_time') }} as dt
    on cm.month_start_date = dt.month_start_date
  where cm.month_start_date is not null
    and dt.month_start_date is null
)

select *
from missing_in_dim_time
