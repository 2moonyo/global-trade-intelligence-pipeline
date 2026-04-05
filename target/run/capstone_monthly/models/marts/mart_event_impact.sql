
  
    

    create or replace table `capfractal`.`analytics_marts`.`mart_event_impact`
      
    
    

    
    OPTIONS()
    as (
      -- Final grain: one row per event_id.
-- Join order is staged to prevent grain explosion:
-- 1) event-chokepoint-month base
-- 2) attach z-scores
-- 3) aggregate stress metrics to event
-- 4) aggregate structural/disruption metrics to event
-- 5) separately aggregate trade exposure countries to event
-- 6) join event-level aggregates and dim_event metadata

with event_chokepoint_month as (
  -- Step 1: event_id + chokepoint_id + year_month base bridge.
  select distinct
    bem.event_id,
    bec.chokepoint_id,
    bem.year_month
  from `capfractal`.`analytics_analytics_marts`.`bridge_event_month` as bem
  inner join `capfractal`.`analytics_analytics_marts`.`bridge_event_chokepoint` as bec
    on bem.event_id = bec.event_id
),
event_chokepoint_month_z as (
  -- Step 2: attach monthly chokepoint z-score signals.
  select
    ecm.event_id,
    ecm.chokepoint_id,
    ecm.year_month,
    z.throughput,
    z.mean_throughput,
    z.z_score_capacity,
    z.z_score_count,
    z.z_score_vessel_size
  from event_chokepoint_month as ecm
  left join `capfractal`.`analytics_staging`.`stg_chokepoint_stress_zscore` as z
    on ecm.chokepoint_id = z.chokepoint_id
   and ecm.year_month = z.year_month
),
event_stress_metrics as (
  -- Step 3: aggregate event-window stress to event grain.
  select
    event_id,
    count(distinct chokepoint_id) as affected_chokepoint_count,
    avg(z_score_capacity) as mean_event_window_zscore_capacity,
    max(z_score_capacity) as max_event_window_zscore_capacity,
    avg(z_score_count) as mean_event_window_zscore_count,
    avg(z_score_vessel_size) as mean_event_window_zscore_vessel_size
  from event_chokepoint_month_z
  group by 1
),
event_structural_metrics as (
  -- Step 4: structural chokepoint importance from baseline throughput.
  select
    event_id,
    avg(mean_throughput) as mean_baseline_throughput_of_affected_chokepoints
  from (
    select distinct
      event_id,
      chokepoint_id,
      mean_throughput
    from event_chokepoint_month_z
    where mean_throughput is not null
  ) as d
  group by 1
),
event_disruption_metrics as (
  -- Step 5: realized disruption vs baseline at event level.
  select
    event_id,
    avg(
      case
        when mean_throughput is null or mean_throughput = 0 then null
        else (throughput - mean_throughput) / mean_throughput
      end
    ) as mean_throughput_pct_change_vs_baseline
  from event_chokepoint_month_z
  group by 1
),
event_exposure_countries as (
  -- Step 6: aggregate country exposure separately before final join.
  select
    ecm.event_id,
    count(distinct te.reporter_iso3) as affected_country_count
  from event_chokepoint_month as ecm
  inner join `capfractal`.`analytics_marts`.`mart_trade_exposure` as te
    on ecm.chokepoint_id = te.chokepoint_id
   and ecm.year_month = te.year_month
  where te.route_confidence_score in ('HIGH', 'MEDIUM')
  group by 1
)

-- Step 7: final event-level output with conformed event attributes.
select
  d.event_id,
  d.event_name,
  d.event_type,
  d.severity_level,
  coalesce(ec.affected_country_count, 0) as affected_country_count,
  coalesce(esm.affected_chokepoint_count, 0) as affected_chokepoint_count,
  esm.mean_event_window_zscore_capacity,
  esm.max_event_window_zscore_capacity,
  esm.mean_event_window_zscore_count,
  esm.mean_event_window_zscore_vessel_size,
  est.mean_baseline_throughput_of_affected_chokepoints,
  edm.mean_throughput_pct_change_vs_baseline
from `capfractal`.`analytics_analytics_marts`.`dim_event` as d
left join event_stress_metrics as esm
  on d.event_id = esm.event_id
left join event_structural_metrics as est
  on d.event_id = est.event_id
left join event_disruption_metrics as edm
  on d.event_id = edm.event_id
left join event_exposure_countries as ec
  on d.event_id = ec.event_id
    );
  