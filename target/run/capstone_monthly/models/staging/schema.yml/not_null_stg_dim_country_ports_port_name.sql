
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select port_name
from `capfractal`.`analytics_staging`.`stg_dim_country_ports`
where port_name is null



  
  
      
    ) dbt_internal_test