
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Fails when 30-day rolling signal fields appear before the model has enough prior observations.
select *
from `capfractal`.`analytics_marts`.`mart_chokepoint_daily_signal`
where (
    observed_days_in_30d_window < 2
    or has_portwatch_daily_data_flag = 0
  )
  and (
    z_score_rolling_30d is not null
    or vessel_count_z_score_rolling_30d is not null
    or signal_index_rolling_30d is not null
  )
  
  
      
    ) dbt_internal_test