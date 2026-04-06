
    
    

with child as (
    select commodity_code as from_field
    from `capfractal`.`analytics_marts`.`mart_country_commodity_dependency`
    where commodity_code is not null
),

parent as (
    select cmd_code as to_field
    from `capfractal`.`analytics_marts`.`dim_commodity`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


