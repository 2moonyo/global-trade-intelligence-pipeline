{{ config(
    materialized='table',
    schema='analytics_marts'
) }}

with base as (

    select distinct
        event_id,
        location_name as region_name,
        link_role
    from {{ ref('stg_event_location') }}
    where location_type <> 'chokepoint'

)

select
    event_id,
    md5(lower(trim(region_name))) as region_id,
    region_name,
    link_role
from base
