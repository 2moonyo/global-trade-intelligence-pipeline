
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    year_month as unique_field,
    count(*) as n_records

from "analytics"."analytics_staging"."stg_dim_time"
where year_month is not null
group by year_month
having count(*) > 1



  
  
      
    ) dbt_internal_test