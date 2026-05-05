{{ config(
    materialized='view',
    schema='analytics_staging'
) }}


select
    trim(event_id) as event_id,
    trim(event_name) as event_name,
    trim(year_month) as year_month,
    {{ month_start_from_year_month('trim(year_month)') }} as month_start_date,

    {{ canonicalize_chokepoint_name('chokepoint_name') }} as chokepoint_name,
    {{ canonicalize_chokepoint_name('chokepoint_name') }} as location_name,
    'chokepoint' as location_type,
    'core_chokepoint' as location_layer,
    true as is_core_chokepoint,
    trim(event_phase) as event_phase,

    cast(event_active_flag as boolean) as is_event_active,
    cast(lead_flag as boolean) as is_lead_period,
    cast(lag_flag as boolean) as is_lag_period,

    {{ cast_float('severity_weight') }} as severity_weight,
    cast(global_event_flag as boolean) as is_global_event,

    trim(event_type) as event_type,
    trim(event_scope) as raw_event_scope,
    trim(link_role) as link_role
from raw.bridge_event_month_chokepoint_core
