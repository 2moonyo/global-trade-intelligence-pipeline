
    
    

with all_values as (

    select
        trade_reporting_status as value_field,
        count(*) as n_records

    from `capfractal`.`analytics_marts`.`mart_trade_month_coverage_status`
    group by trade_reporting_status

)

select *
from all_values
where value_field not in (
    'NO_TRADE_DATA','FULL_COVERAGE','PARTIAL_COVERAGE'
)


