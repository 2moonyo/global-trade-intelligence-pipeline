
    
    

with all_values as (

    select
        daily_source_coverage_status as value_field,
        count(*) as n_records

    from `chokepoint-capfractal`.`analytics_marts`.`mart_global_daily_market_signal`
    group by daily_source_coverage_status

)

select *
from all_values
where value_field not in (
    'NO_PORTWATCH_DATA','FULL_COVERAGE','PARTIAL_COVERAGE'
)


