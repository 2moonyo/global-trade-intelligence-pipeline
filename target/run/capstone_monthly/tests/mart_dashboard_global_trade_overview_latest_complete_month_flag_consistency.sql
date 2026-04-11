
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test: latest_complete_month_flag must only be true on the maximum complete month.

with latest_complete_month as (
  select
    max(case when complete_month_flag then month_start_date end) as latest_complete_month_start_date
  from `chokepoint-capfractal`.`analytics_marts`.`mart_dashboard_global_trade_overview`
)

select
  mart.*
from `chokepoint-capfractal`.`analytics_marts`.`mart_dashboard_global_trade_overview` as mart
cross join latest_complete_month as lcm
where (
  lcm.latest_complete_month_start_date is null
  and mart.latest_complete_month_flag
)
or (
  lcm.latest_complete_month_start_date is not null
  and mart.month_start_date = lcm.latest_complete_month_start_date
  and not mart.latest_complete_month_flag
)
or (
  lcm.latest_complete_month_start_date is not null
  and mart.month_start_date <> lcm.latest_complete_month_start_date
  and mart.latest_complete_month_flag
)
  
  
      
    ) dbt_internal_test