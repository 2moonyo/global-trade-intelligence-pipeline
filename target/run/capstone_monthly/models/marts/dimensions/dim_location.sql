
  
    
    

    create  table
      "analytics"."analytics_analytics_marts"."dim_location__dbt_tmp"
  
    as (
      

with base as (

    select distinct
        location_name,
        location_type,
        location_layer,
        is_core_chokepoint
    from "analytics"."analytics_analytics_staging"."stg_event_location"

)

select
    md5(lower(trim(location_name)) || '|' || lower(trim(location_type))) as location_id,
    location_name,
    location_type,
    location_layer,
    is_core_chokepoint
from base
    );
  
  