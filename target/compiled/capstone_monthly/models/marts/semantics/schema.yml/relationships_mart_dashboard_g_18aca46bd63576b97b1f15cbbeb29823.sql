
    
    

with child as (
    select year_month_key as from_field
    from `chokepoint-capfractal`.`analytics_marts`.`mart_dashboard_global_trade_overview`
    where year_month_key is not null
),

parent as (
    select period as to_field
    from `chokepoint-capfractal`.`analytics_marts`.`dim_time`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


