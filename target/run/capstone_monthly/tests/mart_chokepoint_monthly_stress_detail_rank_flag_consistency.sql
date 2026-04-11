
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select *
from `chokepoint-capfractal`.`analytics_marts`.`mart_chokepoint_monthly_stress_detail`
where (
    stress_rank_in_month is null
    and top_5_stressed_chokepoint_flag
  )
  or (
    stress_rank_in_month is not null
    and stress_rank_in_month <= 5
    and not top_5_stressed_chokepoint_flag
  )
  or (
    stress_rank_in_month is not null
    and stress_rank_in_month > 5
    and top_5_stressed_chokepoint_flag
  )
  
  
      
    ) dbt_internal_test