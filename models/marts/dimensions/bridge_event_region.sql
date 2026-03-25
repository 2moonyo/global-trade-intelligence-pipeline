{{ config(
    materialized='table',
    schema='analytics_marts'
) }}

with base as (

    select distinct
        event_id,
        region_name,
        link_role
    from {{ ref('stg_event_month_region') }}

)

select
    event_id,
    md5(lower(trim(region_name))) as region_id,
    region_name,
    link_role
from base