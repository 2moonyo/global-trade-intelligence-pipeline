with partner_month as (
  select
    upper(trim(reporter_iso3)) as reporter_iso3,
    upper(trim(partner_iso3)) as partner_iso3,
    period,
    year_month,
    sum(trade_value_usd) as partner_trade_value_usd
  from "analytics"."raw"."comtrade_partner_month"
  group by 1, 2, 3, 4
),
route_map as (
  select distinct
    upper(trim(reporter_iso3)) as reporter_iso3,
    upper(trim(partner_iso3)) as partner_iso3,
    trim(main_chokepoint) as chokepoint_name,
    route_applicability_status
  from "analytics"."raw"."dim_trade_routes"
  where main_chokepoint is not null
),
reporter_month_total as (
  select
    reporter_iso3,
    period,
    year_month,
    sum(partner_trade_value_usd) as reporter_month_trade_value_usd
  from partner_month
  group by 1, 2, 3
),
reporter_month_chokepoint as (
  select
    pm.reporter_iso3,
    pm.period,
    pm.year_month,
    rm.chokepoint_name,
    sum(pm.partner_trade_value_usd) as chokepoint_trade_value_usd,
    count(*) as route_pair_count
  from partner_month as pm
  inner join route_map as rm
    on pm.reporter_iso3 = rm.reporter_iso3
   and pm.partner_iso3 = rm.partner_iso3
  where upper(rm.route_applicability_status) = 'MARITIME_ELIGIBLE'
  group by 1, 2, 3, 4
),
active_events as (
  select
    year_month,
    chokepoint_name,
    count(distinct event_id) filter (where is_event_active) as active_event_count,
    max(severity_weight) filter (where is_event_active) as max_active_event_severity,
    avg(severity_weight) filter (where is_event_active) as avg_active_event_severity
  from "analytics"."analytics_staging"."stg_chokepoint_bridge"
  group by 1, 2
),
portwatch as (
  select
    year_month,
    chokepoint_name,
    stress_index,
    stress_index_weighted,
    avg_n_total,
    avg_capacity
  from "analytics"."raw"."portwatch_monthly"
)

select
  rmc.reporter_iso3,
  c.country_name as reporter_country_name,
  rmc.period,
  rmc.year_month,
  t.month_start_date,
  rmc.chokepoint_name,
  rmc.route_pair_count,
  rmc.chokepoint_trade_value_usd,
  rmt.reporter_month_trade_value_usd,
  case
    when rmt.reporter_month_trade_value_usd = 0 then null
    else rmc.chokepoint_trade_value_usd / rmt.reporter_month_trade_value_usd
  end as chokepoint_trade_exposure_ratio,
  p.stress_index,
  p.stress_index_weighted,
  p.avg_n_total,
  p.avg_capacity,
  coalesce(a.active_event_count, 0) as active_event_count,
  a.max_active_event_severity,
  a.avg_active_event_severity
from reporter_month_chokepoint as rmc
inner join reporter_month_total as rmt
  on rmc.reporter_iso3 = rmt.reporter_iso3
 and rmc.period = rmt.period
 and rmc.year_month = rmt.year_month
left join "analytics"."analytics_staging"."stg_dim_country" as c
  on rmc.reporter_iso3 = c.iso3
left join "analytics"."analytics_staging"."stg_dim_time" as t
  on rmc.period = t.period
left join portwatch as p
  on rmc.year_month = p.year_month
 and rmc.chokepoint_name = p.chokepoint_name
left join active_events as a
  on rmc.year_month = a.year_month
 and rmc.chokepoint_name = a.chokepoint_name