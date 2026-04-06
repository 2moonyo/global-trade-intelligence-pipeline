
    
    

with all_values as (

    select
        severity_level as value_field,
        count(*) as n_records

    from `capfractal`.`analytics_marts`.`mart_country_event_impact`
    group by severity_level

)

select *
from all_values
where value_field not in (
    'low','medium','high','critical'
)


