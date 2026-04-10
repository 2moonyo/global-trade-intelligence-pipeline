
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Fails when expanding historical stress fields appear before there are enough prior observed months.
select *
from `capfractal`.`analytics_marts`.`mart_chokepoint_monthly_stress`
where historical_baseline_observation_count < 2
  and (
    z_score_historical is not null
    or z_score_count_historical is not null
    or z_score_vessel_size_historical is not null
    or stress_index is not null
    or stress_index_weighted is not null
  )
  
  
      
    ) dbt_internal_test