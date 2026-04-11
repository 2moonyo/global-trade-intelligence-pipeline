
  
    

    create or replace table `chokepoint-capfractal`.`analytics_marts`.`mart_global_daily_market_signal`
      
    
    

    
    OPTIONS()
    as (
      -- Daily Looker Studio scorecard mart for Page 2.
-- Grain: one row per date_day.

with portwatch_daily as (
  select
    date_day,
    month_start_date,
    year_month,
    chokepoint_id,
    has_portwatch_daily_data_flag,
    signal_index_rolling_30d
  from `chokepoint-capfractal`.`analytics_marts`.`mart_chokepoint_daily_signal`
),
expected_chokepoints as (
  select count(distinct chokepoint_id) as expected_chokepoint_count
  from portwatch_daily
),
portwatch_agg as (
  select
    date_day,
    max(month_start_date) as month_start_date,
    max(year_month) as year_month_label,
    sum(case when has_portwatch_daily_data_flag = 1 then 1 else 0 end) as observed_chokepoint_count,
    avg(case when has_portwatch_daily_data_flag = 1 then signal_index_rolling_30d end) as avg_chokepoint_signal_index,
    avg(case when has_portwatch_daily_data_flag = 1 then abs(signal_index_rolling_30d) end) as avg_abs_chokepoint_signal_index,
    max(case when has_portwatch_daily_data_flag = 1 then abs(signal_index_rolling_30d) end) as max_abs_chokepoint_signal_index,
    sum(
      case
        when has_portwatch_daily_data_flag = 1
          and signal_index_rolling_30d is not null
          and abs(signal_index_rolling_30d) >= 1 then 1
        else 0
      end
    ) as stressed_chokepoint_count
  from portwatch_daily
  group by 1
),
brent_base as (
  select
    date_day,
    price_usd_per_bbl as brent_price_usd
  from `chokepoint-capfractal`.`analytics_staging`.`stg_brent_daily`
  where benchmark_code = 'BRENT_EU'
),
brent_features as (
  select
    date_day,
    brent_price_usd,
    lag(brent_price_usd, 1) over (
      order by date_day
    ) as brent_price_usd_1_observed_day_ago,
    lag(brent_price_usd, 7) over (
      order by date_day
    ) as brent_price_usd_7_observed_days_ago,
    avg(brent_price_usd) over trailing_7d as brent_rolling_mean_7_observed_days,
    avg(brent_price_usd) over trailing_30d as brent_rolling_mean_30_observed_days,
    stddev_pop(brent_price_usd) over trailing_30d as brent_rolling_stddev_30_observed_days
  from brent_base
  window
    trailing_7d as (
      order by date_day
      rows between 7 preceding and 1 preceding
    ),
    trailing_30d as (
      order by date_day
      rows between 30 preceding and 1 preceding
    )
),
joined as (
  select
    p.date_day,
    p.month_start_date,
    p.year_month_label,
    p.observed_chokepoint_count,
    ec.expected_chokepoint_count,
    case
    when ec.expected_chokepoint_count is null or ec.expected_chokepoint_count = 0 then null
    else (p.observed_chokepoint_count) / (ec.expected_chokepoint_count)
  end as daily_coverage_ratio,
    p.avg_chokepoint_signal_index,
    p.avg_abs_chokepoint_signal_index,
    p.max_abs_chokepoint_signal_index,
    p.stressed_chokepoint_count,
    b.brent_price_usd,
    case
      when b.brent_price_usd is null or b.brent_price_usd_1_observed_day_ago is null then null
      else b.brent_price_usd - b.brent_price_usd_1_observed_day_ago
    end as brent_price_change_usd_1_observed_day,
    case
      when b.brent_price_usd is null
        or b.brent_price_usd_1_observed_day_ago is null
        or b.brent_price_usd_1_observed_day_ago = 0 then null
      else (b.brent_price_usd - b.brent_price_usd_1_observed_day_ago) / b.brent_price_usd_1_observed_day_ago
    end as brent_return_pct_1_observed_day,
    case
      when b.brent_price_usd is null or b.brent_price_usd_7_observed_days_ago is null then null
      else b.brent_price_usd - b.brent_price_usd_7_observed_days_ago
    end as brent_price_change_usd_7_observed_days,
    case
      when b.brent_price_usd is null
        or b.brent_price_usd_7_observed_days_ago is null
        or b.brent_price_usd_7_observed_days_ago = 0 then null
      else (b.brent_price_usd - b.brent_price_usd_7_observed_days_ago) / b.brent_price_usd_7_observed_days_ago
    end as brent_return_pct_7_observed_days,
    b.brent_rolling_mean_7_observed_days,
    b.brent_rolling_mean_30_observed_days,
    b.brent_rolling_stddev_30_observed_days,
    case
      when b.brent_price_usd is null
        or b.brent_rolling_stddev_30_observed_days is null
        or b.brent_rolling_stddev_30_observed_days = 0 then null
      else (b.brent_price_usd - b.brent_rolling_mean_30_observed_days) / b.brent_rolling_stddev_30_observed_days
    end as brent_z_score_rolling_30_observed_days,
    case when b.brent_price_usd is not null then true else false end as has_brent_price_data_flag
  from portwatch_agg as p
  cross join expected_chokepoints as ec
  left join brent_features as b
    on p.date_day = b.date_day
),
latest_portwatch_day as (
  select max(date_day) as latest_date_day
  from portwatch_agg
  where observed_chokepoint_count > 0
)

select
  j.date_day,
  j.month_start_date,
  j.year_month_label,
  j.observed_chokepoint_count,
  j.expected_chokepoint_count,
  j.daily_coverage_ratio,
  case
    when j.observed_chokepoint_count = 0 then 'NO_PORTWATCH_DATA'
    when j.observed_chokepoint_count = j.expected_chokepoint_count then 'FULL_COVERAGE'
    else 'PARTIAL_COVERAGE'
  end as daily_source_coverage_status,
  j.avg_chokepoint_signal_index,
  j.avg_abs_chokepoint_signal_index,
  j.max_abs_chokepoint_signal_index,
  j.stressed_chokepoint_count,
  case
    when j.observed_chokepoint_count = 0 then 'NO_PORTWATCH_DATA'
    when j.max_abs_chokepoint_signal_index is null then 'INSUFFICIENT_BASELINE'
    when j.max_abs_chokepoint_signal_index >= 2 then 'SEVERE'
    when j.max_abs_chokepoint_signal_index >= 1 then 'ELEVATED'
    else 'NORMAL'
  end as system_stress_level,
  j.brent_price_usd,
  j.brent_price_change_usd_1_observed_day,
  j.brent_return_pct_1_observed_day,
  j.brent_price_change_usd_7_observed_days,
  j.brent_return_pct_7_observed_days,
  j.brent_rolling_mean_7_observed_days,
  j.brent_rolling_mean_30_observed_days,
  j.brent_rolling_stddev_30_observed_days,
  j.brent_z_score_rolling_30_observed_days,
  j.has_brent_price_data_flag,
  case when j.date_day = l.latest_date_day then true else false end as latest_day_flag
from joined as j
cross join latest_portwatch_day as l
    );
  