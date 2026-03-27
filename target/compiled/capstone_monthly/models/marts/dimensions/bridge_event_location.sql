

with base as (

    select distinct
        event_id,
        location_name,
        location_type,
        location_layer,
        link_role
    from "analytics"."analytics_analytics_staging"."stg_event_location"

)

select
    event_id,
    md5(lower(trim(location_name)) || '|' || lower(trim(location_type))) as location_id,
    location_name,
    location_type,
    location_layer,
    link_role
from base