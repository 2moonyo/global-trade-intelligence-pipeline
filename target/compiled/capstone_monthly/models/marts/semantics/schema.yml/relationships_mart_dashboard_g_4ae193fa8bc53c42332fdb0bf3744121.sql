
    
    

with child as (
    select reporter_country_code as from_field
    from `chokepoint-capfractal`.`analytics_marts`.`mart_dashboard_global_trade_overview`
    where reporter_country_code is not null
),

parent as (
    select iso3 as to_field
    from `chokepoint-capfractal`.`analytics_marts`.`dim_country`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


