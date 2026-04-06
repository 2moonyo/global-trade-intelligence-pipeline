
    
    

with child as (
    select partner_country_code as from_field
    from `capfractal`.`analytics_marts`.`mart_country_partner_dependency`
    where partner_country_code is not null
),

parent as (
    select iso3 as to_field
    from `capfractal`.`analytics_marts`.`dim_country`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


