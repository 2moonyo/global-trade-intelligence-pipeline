
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        has_portwatch_daily_data_flag as value_field,
        count(*) as n_records

    from `fullcap-10111`.`analytics_staging`.`stg_portwatch_daily`
    group by has_portwatch_daily_data_flag

)

select *
from all_values
where value_field not in (
    0,1
)



  
  
      
    ) dbt_internal_test