
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        coverage_gap_flag as value_field,
        count(*) as n_records

    from `capfractal`.`analytics_marts`.`mart_trade_month_coverage_status`
    group by coverage_gap_flag

)

select *
from all_values
where value_field not in (
    True,False
)



  
  
      
    ) dbt_internal_test