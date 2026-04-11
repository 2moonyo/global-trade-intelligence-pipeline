
    
    

with all_values as (

    select
        route_confidence_score as value_field,
        count(*) as n_records

    from `chokepoint-capfractal`.`analytics_marts`.`mart_reporter_partner_commodity_month_enriched`
    group by route_confidence_score

)

select *
from all_values
where value_field not in (
    'HIGH','MEDIUM','LOW','VERY_LOW'
)


