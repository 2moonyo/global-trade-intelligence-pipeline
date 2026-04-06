
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        risk_level as value_field,
        count(*) as n_records

    from `capfractal`.`analytics_marts`.`mart_country_chokepoint_exposure`
    group by risk_level

)

select *
from all_values
where value_field not in (
    'high','medium','low'
)



  
  
      
    ) dbt_internal_test