
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select
  month_start_date,
  reporter_iso3,
  count(*) as row_count
from `capfractal`.`analytics_marts`.`mart_reporter_month_exposure_map`
group by 1, 2
having count(*) > 1
  
  
      
    ) dbt_internal_test