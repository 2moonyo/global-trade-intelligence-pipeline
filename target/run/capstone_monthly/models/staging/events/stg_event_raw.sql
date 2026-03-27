
  
  create view "analytics"."analytics_analytics_staging"."stg_event_raw__dbt_tmp" as (
    


select
    trim(event_id) as event_id,
    'system' as event_source,
    trim(event_name) as event_name,
    trim(event_type) as event_type,

    cast(start_date as date) as event_start_date,
    cast(end_date as date) as event_end_date,

    cast(lead_months as integer) as lead_months,
    cast(lag_months as integer) as lag_months,
    cast(base_severity as double) as base_severity_score,

    trim(event_scope) as raw_event_scope,
    trim(description) as description,
    trim(source_class) as source_class
from raw.dim_event
  );
