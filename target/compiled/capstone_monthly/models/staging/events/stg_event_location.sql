

select
    event_id,
    event_name,
    year_month,
    month_start_date,
    location_name,
    location_type,
    location_layer,
    is_core_chokepoint,
    event_phase,
    is_event_active,
    is_lead_period,
    is_lag_period,
    severity_weight,
    is_global_event,
    event_type,
    raw_event_scope,
    link_role
from "analytics"."analytics_analytics_staging"."stg_event_month_chokepoint"

union all

select
    event_id,
    event_name,
    year_month,
    month_start_date,
    location_name,
    location_type,
    location_layer,
    is_core_chokepoint,
    event_phase,
    is_event_active,
    is_lead_period,
    is_lag_period,
    severity_weight,
    is_global_event,
    event_type,
    raw_event_scope,
    link_role
from "analytics"."analytics_analytics_staging"."stg_event_month_region"