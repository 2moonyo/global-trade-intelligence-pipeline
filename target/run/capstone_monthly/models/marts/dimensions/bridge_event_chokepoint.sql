
  
    
    

    create  table
      "analytics"."analytics_analytics_marts"."bridge_event_chokepoint__dbt_tmp"
  
    as (
      

with base as (

    select distinct
        event_id,
        chokepoint_name,
        link_role
    from "analytics"."analytics_analytics_staging"."stg_event_month_chokepoint"

)

select
    event_id,
    md5(lower(trim(chokepoint_name))) as chokepoint_id,
    chokepoint_name,
    link_role
from base
    );
  
  