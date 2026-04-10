
    
    

with child as (
    select event_id as from_field
    from "analytics"."analytics_marts"."mart_trade_route_geography"
    where event_id is not null
),

parent as (
    select event_id as to_field
    from "analytics"."analytics_analytics_marts"."dim_event"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


