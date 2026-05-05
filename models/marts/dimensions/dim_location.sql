{{ config(
    materialized='table',
    schema='analytics_marts'
) }}

with base as (

    select distinct
        location_name,
        location_type,
        location_layer,
        is_core_chokepoint
    from {{ ref('stg_event_location') }}

)

select
    {{ hash_text("lower(trim(location_name)) || '|' || lower(trim(location_type))") }} as location_id,
    location_name,
    location_type,
    location_layer,
    is_core_chokepoint
from base
