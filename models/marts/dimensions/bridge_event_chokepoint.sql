{{ config(
    materialized='table',
    schema='analytics_marts'
) }}

with base as (

    select distinct
        event_id,
        chokepoint_name,
        link_role
    from {{ ref('stg_event_month_chokepoint') }}

)

select
    event_id,
    md5(lower(trim(chokepoint_name))) as chokepoint_id,
    chokepoint_name,
    link_role
from base