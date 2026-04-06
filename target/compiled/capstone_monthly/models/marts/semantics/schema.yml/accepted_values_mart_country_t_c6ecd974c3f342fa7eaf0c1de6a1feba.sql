
    
    

with all_values as (

    select
        trade_orientation as value_field,
        count(*) as n_records

    from `capfractal`.`analytics_marts`.`mart_country_trade_profile`
    group by trade_orientation

)

select *
from all_values
where value_field not in (
    'export_oriented','import_oriented','mixed'
)


