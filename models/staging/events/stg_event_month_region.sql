{{ config(
    materialized='view',
    schema='analytics_staging'
) }}


select
    trim(event_id) as event_id,
    trim(event_name) as event_name,
    trim(year_month) as year_month,
    cast(trim(year_month) || '-01' as date) as month_start_date,

    trim(chokepoint_name) as region_name,
    trim(chokepoint_name) as location_name,
    case
        when lower(trim(chokepoint_name)) = 'port of baltimore' then 'port'
        when lower(trim(chokepoint_name)) = 'us east coast' then 'coastal_region'
        when lower(trim(chokepoint_name)) = 'turkish straits' then 'maritime_passage'
        else 'maritime_region'
    end as location_type,
    'noncore_location' as location_layer,
    false as is_core_chokepoint,
    trim(event_phase) as event_phase,

    cast(event_active_flag as boolean) as is_event_active,
    cast(lead_flag as boolean) as is_lead_period,
    cast(lag_flag as boolean) as is_lag_period,

    cast(severity_weight as double) as severity_weight,
    cast(global_event_flag as boolean) as is_global_event,

    trim(event_type) as event_type,
    trim(event_scope) as raw_event_scope,
    trim(link_role) as link_role
from raw.bridge_event_month_maritime_region
