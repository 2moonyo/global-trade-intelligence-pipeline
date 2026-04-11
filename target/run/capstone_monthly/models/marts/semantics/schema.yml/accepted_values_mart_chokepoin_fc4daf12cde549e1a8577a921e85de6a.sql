
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        direction_of_change as value_field,
        count(*) as n_records

    from `chokepoint-capfractal`.`analytics_marts`.`mart_chokepoint_daily_signal`
    group by direction_of_change

)

select *
from all_values
where value_field not in (
    'NO_DATA','NO_PRIOR_DAY','UP','DOWN','FLAT'
)



  
  
      
    ) dbt_internal_test