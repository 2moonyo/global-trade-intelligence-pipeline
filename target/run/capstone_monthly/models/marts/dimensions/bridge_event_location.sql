
  
    

    create or replace table `capfractal`.`analytics_analytics_marts`.`bridge_event_location`
      
    
    

    
    OPTIONS()
    as (
      

with base as (

    select distinct
        event_id,
        location_name,
        location_type,
        location_layer,
        link_role
    from `capfractal`.`analytics_analytics_staging`.`stg_event_location`

)

select
    event_id,
    
    to_hex(md5(cast(lower(trim(location_name)) || '|' || lower(trim(location_type)) as string)))
   as location_id,
    location_name,
    location_type,
    location_layer,
    link_role
from base
    );
  