
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select *
from `capfractal`.`analytics_marts`.`mart_chokepoint_monthly_stress_detail`
where not previous_month_available_flag
  and (
    previous_month_stress_index is not null
    or previous_month_stress_index_weighted is not null
    or previous_month_z_score_historical is not null
    or stress_index_mom_change is not null
    or stress_index_weighted_mom_change is not null
    or abs_z_score_historical_mom_change is not null
  )
  
  
      
    ) dbt_internal_test