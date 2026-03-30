

  create or replace view `capfractal`.`analytics_staging`.`stg_chokepoint_bridge`
  OPTIONS()
  as select
  event_id,
  event_name,
  year_month,
  cast(strptime(year_month || '-01', '%Y-%m-%d') as date) as month_start_date,
  chokepoint_name,
  event_phase,
  cast(event_active_flag as boolean) as is_event_active,
  cast(lead_flag as boolean) as is_lead_period,
  cast(lag_flag as boolean) as is_lag_period,
  cast(severity_weight as double) as severity_weight,
  cast(global_event_flag as boolean) as is_global_event,
  event_type,
  event_scope,
  link_role
from `capfractal`.`raw`.`chokepoint_bridge`;

