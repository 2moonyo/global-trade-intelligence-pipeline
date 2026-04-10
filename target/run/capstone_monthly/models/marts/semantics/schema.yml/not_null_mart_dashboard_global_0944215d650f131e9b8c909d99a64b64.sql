
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select reporter_rank_by_total_trade_in_month
from `capfractal`.`analytics_marts`.`mart_dashboard_global_trade_overview`
where reporter_rank_by_total_trade_in_month is null



  
  
      
    ) dbt_internal_test