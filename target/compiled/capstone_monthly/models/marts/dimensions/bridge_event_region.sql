

with base as (

    select distinct
        event_id,
        location_name as region_name,
        link_role
    from `chokepoint-capfractal`.`analytics_analytics_staging`.`stg_event_location`
    where location_type <> 'chokepoint'

)

select
    event_id,
    
    to_hex(md5(cast(lower(trim(region_name)) as string)))
   as region_id,
    region_name,
    link_role
from base