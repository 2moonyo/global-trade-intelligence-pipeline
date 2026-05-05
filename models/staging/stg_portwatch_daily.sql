-- Grain: one row per date_day + chokepoint_id.
-- Builds a scaffolded PortWatch daily calendar so missing or stale chokepoint
-- coverage remains visible to downstream daily marts.

with raw_portwatch as (
  select
    cast(date_day as date) as date_day,
    {{ cast_string('chokepoint_id') }} as portwatch_source_chokepoint_id,
    {{ canonical_chokepoint_id('chokepoint_name') }} as chokepoint_id,
    {{ canonicalize_chokepoint_name('chokepoint_name') }} as chokepoint_name,
    {{ cast_float('n_total') }} as n_total,
    {{ cast_float('capacity') }} as capacity,
    {{ cast_float('n_tanker') }} as n_tanker,
    {{ cast_float('n_container') }} as n_container,
    {{ cast_float('n_dry_bulk') }} as n_dry_bulk,
    {{ cast_float('capacity_tanker') }} as capacity_tanker,
    {{ cast_float('capacity_container') }} as capacity_container,
    {{ cast_float('capacity_dry_bulk') }} as capacity_dry_bulk
  from {{ source('raw', 'portwatch_daily') }}
  where date_day is not null
    and chokepoint_id is not null
    and {{ clean_label_text('chokepoint_name') }} is not null
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
  {{ date_series('bounds.min_date_day', 'bounds.max_date_day', 'date_day') }}
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
    {{ year_month_from_date('date_day') }} as year_month,
    {{ year_int_from_date('date_day') }} as year,
    {{ month_int_from_date('date_day') }} as month,
    cast(extract(day from date_day) as {{ dbt.type_int() }}) as day,
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
    {{ month_start_from_year_month('year_month') }} as month_start_date,
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
      else {{ safe_divide('n_tanker', 'coalesce(n_tanker, 0) + coalesce(n_container, 0) + coalesce(n_dry_bulk, 0)') }}
    end as tanker_share,
    case
      when coalesce(n_tanker, 0) + coalesce(n_container, 0) + coalesce(n_dry_bulk, 0) = 0 then null
      else {{ safe_divide('n_container', 'coalesce(n_tanker, 0) + coalesce(n_container, 0) + coalesce(n_dry_bulk, 0)') }}
    end as container_share,
    case
      when coalesce(n_tanker, 0) + coalesce(n_container, 0) + coalesce(n_dry_bulk, 0) = 0 then null
      else {{ safe_divide('n_dry_bulk', 'coalesce(n_tanker, 0) + coalesce(n_container, 0) + coalesce(n_dry_bulk, 0)') }}
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
