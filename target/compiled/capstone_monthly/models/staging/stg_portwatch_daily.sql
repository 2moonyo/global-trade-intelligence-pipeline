-- Grain: one row per date_day + chokepoint_id.
-- Builds a scaffolded PortWatch daily calendar so missing or stale chokepoint
-- coverage remains visible to downstream daily marts.

with raw_portwatch as (
  select
    cast(date_day as date) as date_day,
    cast(chokepoint_id as string) as portwatch_source_chokepoint_id,
    
    to_hex(md5(cast(lower(trim(cast(chokepoint_name as string))) as string)))
   as chokepoint_id,
    cast(chokepoint_name as string) as chokepoint_name,
    cast(n_total as FLOAT64) as n_total,
    cast(capacity as FLOAT64) as capacity,
    cast(n_tanker as FLOAT64) as n_tanker,
    cast(n_container as FLOAT64) as n_container,
    cast(n_dry_bulk as FLOAT64) as n_dry_bulk,
    cast(capacity_tanker as FLOAT64) as capacity_tanker,
    cast(capacity_container as FLOAT64) as capacity_container,
    cast(capacity_dry_bulk as FLOAT64) as capacity_dry_bulk
  from `capfractal`.`raw`.`portwatch_daily`
  where date_day is not null
    and chokepoint_id is not null
    and chokepoint_name is not null
),
observed_daily as (
  select
    date_day,
    portwatch_source_chokepoint_id,
    chokepoint_id,
    max(chokepoint_name) as chokepoint_name,
    max(n_total) as n_total,
    max(capacity) as capacity,
    max(n_tanker) as n_tanker,
    max(n_container) as n_container,
    max(n_dry_bulk) as n_dry_bulk,
    max(capacity_tanker) as capacity_tanker,
    max(capacity_container) as capacity_container,
    max(capacity_dry_bulk) as capacity_dry_bulk
  from raw_portwatch
  group by 1, 2, 3
),
chokepoints as (
  select distinct
    portwatch_source_chokepoint_id,
    chokepoint_id,
    chokepoint_name
  from observed_daily
),
bounds as (
  select
    min(date_day) as min_date_day,
    max(date_day) as max_date_day
  from observed_daily
),
calendar as (
  select date_day
  from bounds,
  
    unnest(generate_date_array(cast(bounds.min_date_day as date), cast(bounds.max_date_day as date), interval 1 day)) as date_day
  
),
scaffolded as (
  select
    c.date_day,
    cp.portwatch_source_chokepoint_id,
    cp.chokepoint_id,
    cp.chokepoint_name,
    od.n_total,
    od.capacity,
    od.n_tanker,
    od.n_container,
    od.n_dry_bulk,
    od.capacity_tanker,
    od.capacity_container,
    od.capacity_dry_bulk,
    case when od.date_day is null then 0 else 1 end as has_portwatch_daily_data_flag
  from calendar as c
  cross join chokepoints as cp
  left join observed_daily as od
    on c.date_day = od.date_day
   and cp.chokepoint_id = od.chokepoint_id
),
dated as (
  select
    date_day,
    portwatch_source_chokepoint_id,
    chokepoint_id,
    chokepoint_name,
    
    format_date('%Y-%m', cast(date_day as date))
   as year_month,
    cast(extract(year from cast(date_day as date)) as INT64) as year,
    cast(extract(month from cast(date_day as date)) as INT64) as month,
    cast(extract(day from date_day) as INT64) as day,
    n_total,
    capacity,
    n_tanker,
    n_container,
    n_dry_bulk,
    capacity_tanker,
    capacity_container,
    capacity_dry_bulk,
    has_portwatch_daily_data_flag
  from scaffolded
),
signals as (
  select
    date_day,
    portwatch_source_chokepoint_id,
    chokepoint_id,
    chokepoint_name,
    year_month,
    
    safe_cast(concat(cast(year_month as string), '-01') as date)
   as month_start_date,
    year,
    month,
    day,
    n_total,
    capacity,
    n_tanker,
    n_container,
    n_dry_bulk,
    capacity_tanker,
    capacity_container,
    capacity_dry_bulk,
    case
      when n_total is null or n_total = 0 then null
      else capacity / n_total
    end as vessel_size_index,
    case
      when coalesce(n_tanker, 0) + coalesce(n_container, 0) + coalesce(n_dry_bulk, 0) = 0 then null
      else case
    when coalesce(n_tanker, 0) + coalesce(n_container, 0) + coalesce(n_dry_bulk, 0) is null or coalesce(n_tanker, 0) + coalesce(n_container, 0) + coalesce(n_dry_bulk, 0) = 0 then null
    else (n_tanker) / (coalesce(n_tanker, 0) + coalesce(n_container, 0) + coalesce(n_dry_bulk, 0))
  end
    end as tanker_share,
    case
      when coalesce(n_tanker, 0) + coalesce(n_container, 0) + coalesce(n_dry_bulk, 0) = 0 then null
      else case
    when coalesce(n_tanker, 0) + coalesce(n_container, 0) + coalesce(n_dry_bulk, 0) is null or coalesce(n_tanker, 0) + coalesce(n_container, 0) + coalesce(n_dry_bulk, 0) = 0 then null
    else (n_container) / (coalesce(n_tanker, 0) + coalesce(n_container, 0) + coalesce(n_dry_bulk, 0))
  end
    end as container_share,
    case
      when coalesce(n_tanker, 0) + coalesce(n_container, 0) + coalesce(n_dry_bulk, 0) = 0 then null
      else case
    when coalesce(n_tanker, 0) + coalesce(n_container, 0) + coalesce(n_dry_bulk, 0) is null or coalesce(n_tanker, 0) + coalesce(n_container, 0) + coalesce(n_dry_bulk, 0) = 0 then null
    else (n_dry_bulk) / (coalesce(n_tanker, 0) + coalesce(n_container, 0) + coalesce(n_dry_bulk, 0))
  end
    end as dry_bulk_share,
    has_portwatch_daily_data_flag
  from dated
)

select
  date_day,
  portwatch_source_chokepoint_id,
  chokepoint_id,
  chokepoint_name,
  year_month,
  month_start_date,
  year,
  month,
  day,
  n_total,
  capacity,
  n_tanker,
  n_container,
  n_dry_bulk,
  capacity_tanker,
  capacity_container,
  capacity_dry_bulk,
  vessel_size_index,
  tanker_share,
  container_share,
  dry_bulk_share,
  case
    when chokepoint_name = 'Strait of Hormuz' then tanker_share
    when chokepoint_name in ('Suez Canal', 'Panama Canal') then container_share
    else dry_bulk_share
  end as priority_vessel_share,
  has_portwatch_daily_data_flag
from signals