
  
    
    

    create  table
      "analytics"."analytics_marts"."mart_reporter_month_chokepoint_exposure__dbt_tmp"
  
    as (
      with route_fact as (
  select
    reporter_iso3,
    partner_iso3,
    cmd_code,
    trade_flow,
    period,
    year_month,
    trade_value_usd,
    main_chokepoint as chokepoint_name,
    route_applicability_status,
    is_maritime_routed
  from "analytics"."analytics_marts"."fct_reporter_partner_commodity_route_month"
),
reporter_month_total as (
  select
    reporter_iso3,
    period,
    year_month,
    sum(trade_value_usd) as reporter_month_trade_value_usd
  from "analytics"."analytics_marts"."fct_reporter_partner_commodity_month"
  group by 1, 2, 3
),
reporter_month_chokepoint as (
  select
    rf.reporter_iso3,
    rf.period,
    rf.year_month,
    rf.chokepoint_name,
    sum(rf.trade_value_usd) as chokepoint_trade_value_usd,
    count(distinct rf.partner_iso3) as route_pair_count
  from route_fact as rf
  where rf.chokepoint_name is not null
    and coalesce(rf.is_maritime_routed, false)
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
    stress_index_rolling_6m,
    stress_index_weighted_rolling_6m,
    avg_n_total,
    avg_capacity
  from "analytics"."analytics_staging"."stg_portwatch_stress_metrics"
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
  p.stress_index_rolling_6m,
  p.stress_index_weighted_rolling_6m,
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
    );
  
  