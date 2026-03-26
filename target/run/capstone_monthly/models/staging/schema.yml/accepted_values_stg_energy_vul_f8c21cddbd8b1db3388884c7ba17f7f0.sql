
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        indicator_code as value_field,
        count(*) as n_records

    from "analytics"."analytics_staging"."stg_energy_vulnerability"
    group by indicator_code

)

select *
from all_values
where value_field not in (
    'renewables_share','fossil_fuels_share','dependency_on_imported_energy','oil_electricity_share','gas_electricity_share','coal_electricity_share'
)



  
  
      
    ) dbt_internal_test