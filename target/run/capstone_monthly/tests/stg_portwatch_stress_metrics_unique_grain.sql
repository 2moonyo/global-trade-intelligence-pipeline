
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select
  chokepoint_id,
  year_month,
  count(*) as row_count
from `capfractal`.`analytics_staging`.`stg_portwatch_stress_metrics`
group by 1, 2
having count(*) > 1
  
  
      
    ) dbt_internal_test