
    
    

with all_values as (

    select
        risk_level as value_field,
        count(*) as n_records

    from `capfractal`.`analytics_marts`.`mart_country_partner_dependency`
    group by risk_level

)

select *
from all_values
where value_field not in (
    'high','medium','low'
)


