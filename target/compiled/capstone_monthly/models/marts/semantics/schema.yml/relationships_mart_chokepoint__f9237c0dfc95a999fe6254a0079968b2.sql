
    
    

with child as (
    select chokepoint_id as from_field
    from `capfractal`.`analytics_marts`.`mart_chokepoint_monthly_hotspot_map`
    where chokepoint_id is not null
),

parent as (
    select chokepoint_id as to_field
    from `capfractal`.`analytics_marts`.`dim_chokepoint`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


