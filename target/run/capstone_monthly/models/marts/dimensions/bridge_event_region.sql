
  
    
    

    create  table
      "analytics"."analytics_analytics_marts"."bridge_event_region__dbt_tmp"
  
    as (
      

with base as (

    select distinct
        event_id,
        region_name,
        link_role
    from "analytics"."analytics_analytics_staging"."stg_event_month_region"

)

select
    event_id,
    md5(lower(trim(region_name))) as region_id,
    region_name,
    link_role
from base
    );
  
  