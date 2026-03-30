{{ config(
    materialized='table',
    schema='analytics_marts'
) }}

with base as (

    select distinct
        event_id,
        location_name as chokepoint_name,
        link_role
    from {{ ref('stg_event_location') }}
    where location_type = 'chokepoint'

)

select
    event_id,
    {{ hash_text('lower(trim(chokepoint_name))') }} as chokepoint_id,
    chokepoint_name,
    link_role
from base
