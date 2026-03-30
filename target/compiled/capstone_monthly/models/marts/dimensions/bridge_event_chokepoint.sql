

with base as (

    select distinct
        event_id,
        location_name as chokepoint_name,
        link_role
    from `capfractal`.`analytics_analytics_staging`.`stg_event_location`
    where location_type = 'chokepoint'

)

select
    event_id,
    
    to_hex(md5(cast(lower(trim(chokepoint_name)) as string)))
   as chokepoint_id,
    chokepoint_name,
    link_role
from base