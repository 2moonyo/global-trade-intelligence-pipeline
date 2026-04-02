
  
    

    create or replace table `capfractal`.`analytics_analytics_marts`.`dim_location`
      
    
    

    
    OPTIONS()
    as (
      

with base as (

    select distinct
        location_name,
        location_type,
        location_layer,
        is_core_chokepoint
    from `capfractal`.`analytics_analytics_staging`.`stg_event_location`

)

select
    
    to_hex(md5(cast(lower(trim(location_name)) || '|' || lower(trim(location_type)) as string)))
   as location_id,
    location_name,
    location_type,
    location_layer,
    is_core_chokepoint
from base
    );
  