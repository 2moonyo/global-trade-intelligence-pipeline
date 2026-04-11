
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select *
from `chokepoint-capfractal`.`analytics_marts`.`mart_reporter_partner_commodity_month_enriched`
where (
    chokepoint_exposed_trade_share_of_reporter_total is not null
    and (
      chokepoint_exposed_trade_share_of_reporter_total < 0
      or chokepoint_exposed_trade_share_of_reporter_total > 1.000001
    )
  )
  or (
    partner_commodity_trade_share_of_reporter_chokepoint is not null
    and (
      partner_commodity_trade_share_of_reporter_chokepoint < 0
      or partner_commodity_trade_share_of_reporter_chokepoint > 1.000001
    )
  )
  
  
      
    ) dbt_internal_test