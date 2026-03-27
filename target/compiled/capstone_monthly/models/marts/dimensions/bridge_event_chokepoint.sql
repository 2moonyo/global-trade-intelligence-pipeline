

with base as (

    select distinct
        event_id,
        location_name as chokepoint_name,
        link_role
    from "analytics"."analytics_analytics_staging"."stg_event_location"
    where location_type = 'chokepoint'

)

select
    event_id,
    md5(lower(trim(chokepoint_name))) as chokepoint_id,
    chokepoint_name,
    link_role
from base