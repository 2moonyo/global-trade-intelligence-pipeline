
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select *
from `chokepoint-capfractal`.`analytics_staging`.`stg_brent_monthly`
where benchmark_code = 'BRENT_EU'
  and avg_price_usd_per_bbl < 0
  
  
      
    ) dbt_internal_test