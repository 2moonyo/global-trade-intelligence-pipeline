

  create or replace view `chokepoint-capfractal`.`analytics_analytics_staging`.`stg_event_month_chokepoint`
  OPTIONS()
  as 


select
    trim(event_id) as event_id,
    trim(event_name) as event_name,
    trim(year_month) as year_month,
    
    safe_cast(concat(cast(trim(year_month) as string), '-01') as date)
   as month_start_date,

    trim(chokepoint_name) as chokepoint_name,
    trim(chokepoint_name) as location_name,
    'chokepoint' as location_type,
    'core_chokepoint' as location_layer,
    true as is_core_chokepoint,
    trim(event_phase) as event_phase,

    cast(event_active_flag as boolean) as is_event_active,
    cast(lead_flag as boolean) as is_lead_period,
    cast(lag_flag as boolean) as is_lag_period,

    cast(severity_weight as FLOAT64) as severity_weight,
    cast(global_event_flag as boolean) as is_global_event,

    trim(event_type) as event_type,
    trim(event_scope) as raw_event_scope,
    trim(link_role) as link_role
from raw.bridge_event_month_chokepoint_core;

