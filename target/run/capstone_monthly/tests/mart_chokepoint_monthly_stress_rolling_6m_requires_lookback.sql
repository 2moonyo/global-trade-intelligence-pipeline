
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Fails when rolling 6-month stress fields appear before there are enough prior observed months.
select *
from `chokepoint-capfractal`.`analytics_marts`.`mart_chokepoint_monthly_stress`
where rolling_6m_baseline_observation_count < 2
  and (
    z_score_rolling_6m is not null
    or z_score_count_rolling_6m is not null
    or z_score_vessel_size_rolling_6m is not null
    or stress_index_rolling_6m is not null
    or stress_index_weighted_rolling_6m is not null
  )
  
  
      
    ) dbt_internal_test