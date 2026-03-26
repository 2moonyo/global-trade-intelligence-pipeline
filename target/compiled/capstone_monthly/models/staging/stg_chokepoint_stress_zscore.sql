-- Monthly chokepoint z-score built from PortWatch throughput proxy.
-- z-score interpretation: statistical deviation from each chokepoint's baseline norm.
-- This is different from stress_index, which is an upstream composite indicator.
-- Capacity signal (avg_capacity) captures weighted traffic intensity.
-- Count signal (avg_n_total) captures movement frequency intensity.
-- vessel_size_index ~= capacity per movement; higher values imply larger average vessel mix.

with raw_portwatch as (
  select
    md5(lower(trim(chokepoint_name))) as chokepoint_id,
    year_month,
    cast(avg_capacity as double) as throughput,
    cast(avg_n_total as double) as traffic_count
  from "analytics"."raw"."portwatch_monthly"
  where year_month is not null
    and chokepoint_name is not null
    and avg_capacity is not null
    and avg_n_total is not null
),
monthly_signals as (
  select
    chokepoint_id,
    year_month,
    avg(throughput) as throughput,
    avg(traffic_count) as traffic_count,
    case
      when avg(traffic_count) = 0 then null
      else avg(throughput) / avg(traffic_count)
    end as vessel_size_index
  from raw_portwatch
  group by 1, 2
),
baseline_window as (
  -- Fixed baseline window: 2020-01 through 2023-12.
  -- This prevents the baseline from drifting as new months arrive.
  select
    chokepoint_id,
    throughput,
    traffic_count,
    vessel_size_index
  from monthly_signals
  where year_month between '2020-01' and '2023-12'
),
baseline_stats as (
  select
    chokepoint_id,
    avg(throughput) as mean_throughput,
    stddev_samp(throughput) as stddev_throughput,
    avg(traffic_count) as mean_traffic_count,
    stddev_samp(traffic_count) as stddev_traffic_count,
    avg(vessel_size_index) as mean_vessel_size_index,
    stddev_samp(vessel_size_index) as stddev_vessel_size_index
  from baseline_window
  group by 1
)

select
  m.chokepoint_id,
  m.year_month,
  m.throughput,
  m.traffic_count as avg_n_total,
  b.mean_throughput,
  b.stddev_throughput,
  case
    when b.stddev_throughput is null or b.stddev_throughput = 0 then null
    else (m.throughput - b.mean_throughput) / b.stddev_throughput
  end as z_score,
  -- Alias retained for explicit capacity naming while keeping existing z_score unchanged.
  case
    when b.stddev_throughput is null or b.stddev_throughput = 0 then null
    else (m.throughput - b.mean_throughput) / b.stddev_throughput
  end as z_score_capacity,
  case
    when b.stddev_traffic_count is null or b.stddev_traffic_count = 0 then null
    else (m.traffic_count - b.mean_traffic_count) / b.stddev_traffic_count
  end as z_score_count,
  m.vessel_size_index,
  case
    when b.stddev_vessel_size_index is null or b.stddev_vessel_size_index = 0 then null
    else (m.vessel_size_index - b.mean_vessel_size_index) / b.stddev_vessel_size_index
  end as z_score_vessel_size
from monthly_signals as m
left join baseline_stats as b
  on m.chokepoint_id = b.chokepoint_id