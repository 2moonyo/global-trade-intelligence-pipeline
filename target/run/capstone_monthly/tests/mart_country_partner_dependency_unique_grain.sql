
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test: mart_country_partner_dependency must be unique at reporter_country_code + partner_country_code + year_month_key + year_month.

select
  reporter_country_code,
  partner_country_code,
  year_month_key,
  year_month,
  count(*) as row_count
from `capfractal`.`analytics_marts`.`mart_country_partner_dependency`
group by 1, 2, 3, 4
having count(*) > 1
  
  
      
    ) dbt_internal_test