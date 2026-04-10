
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select
  month_start_date,
  reporter_iso3,
  partner_iso3,
  cmd_code,
  chokepoint_id,
  count(*) as row_count
from `capfractal`.`analytics_marts`.`mart_reporter_partner_commodity_month_enriched`
group by 1, 2, 3, 4, 5
having count(*) > 1
  
  
      
    ) dbt_internal_test