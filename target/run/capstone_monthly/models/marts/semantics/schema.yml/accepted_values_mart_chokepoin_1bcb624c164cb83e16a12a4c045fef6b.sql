
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        latest_observed_month_flag as value_field,
        count(*) as n_records

    from `capfractal`.`analytics_marts`.`mart_chokepoint_monthly_stress`
    group by latest_observed_month_flag

)

select *
from all_values
where value_field not in (
    True,False
)



  
  
      
    ) dbt_internal_test