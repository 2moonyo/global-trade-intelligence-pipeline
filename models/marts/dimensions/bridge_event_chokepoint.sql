{{ config(
    materialized='table',
    schema='analytics_marts'
) }}

with base as (

    select distinct
        event_id,
        {{ canonicalize_chokepoint_name('location_name') }} as chokepoint_name,
        link_role
    from {{ ref('stg_event_location') }}
    where location_type = 'chokepoint'

)

select
    event_id,
    {{ canonical_chokepoint_id('chokepoint_name') }} as chokepoint_id,
    chokepoint_name,
    link_role
from base
