
    
    

with all_values as (

    select
        dependency_level as value_field,
        count(*) as n_records

    from `capfractal`.`analytics_marts`.`mart_country_event_impact`
    group by dependency_level

)

select *
from all_values
where value_field not in (
    'very_high','high','moderate','low','very_low'
)


