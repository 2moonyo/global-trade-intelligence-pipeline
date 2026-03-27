
  
  create view "analytics"."analytics_staging"."stg_dim_time__dbt_tmp" as (
    


with observed_months as (

  select distinct
    cast(strptime(year_month || '-01', '%Y-%m-%d') as date) as month_start_date
  from "analytics"."raw"."comtrade_fact"
  where year_month is not null
    and regexp_full_match(year_month, '^\\d{4}-\\d{2}$')

  union

  select distinct
    cast(strptime(year_month || '-01', '%Y-%m-%d') as date) as month_start_date
  from "analytics"."raw"."portwatch_monthly"
  where year_month is not null
    and regexp_full_match(year_month, '^\\d{4}-\\d{2}$')

  union

  select distinct
    cast(month_start_date as date) as month_start_date
  from "analytics"."raw"."brent_monthly"
  where month_start_date is not null

  union

  select distinct
    cast(date_trunc('month', cast(date as date)) as date) as month_start_date
  from "analytics"."raw"."ecb_fx_eu_daily"
  where date is not null

  union

  select distinct
    cast(strptime(year_month || '-01', '%Y-%m-%d') as date) as month_start_date
  from "analytics"."raw"."bridge_event_month_chokepoint_core"
  where year_month is not null
    and regexp_full_match(year_month, '^\\d{4}-\\d{2}$')

  union

  select distinct
    cast(strptime(year_month || '-01', '%Y-%m-%d') as date) as month_start_date
  from "analytics"."raw"."bridge_event_month_maritime_region"
  where year_month is not null
    and regexp_full_match(year_month, '^\\d{4}-\\d{2}$')

  union

  select distinct
    cast(strptime(cast(year as varchar) || '-01-01', '%Y-%m-%d') as date) as month_start_date
  from "analytics"."raw"."energy_vulnerability"
  where year is not null

),

bounds as (

  select
    min(month_start_date) as min_month_start,
    max(month_start_date) as max_month_start
  from observed_months

),

calendar as (

  select
    cast(generate_series as date) as month_start_date
  from bounds,
  generate_series(
    min_month_start - (12 * interval 1 month),
    max_month_start + (12 * interval 1 month),
    interval 1 month
  )

)

select
  cast(strftime(month_start_date, '%Y%m') as integer) as period,
  cast(extract(year from month_start_date) as integer) as year,
  cast(extract(month from month_start_date) as integer) as month,
  cast(((extract(month from month_start_date) - 1) / 3) + 1 as integer) as quarter,
  strftime(month_start_date, '%Y-%m') as year_month,
  month_start_date
from calendar
  );
