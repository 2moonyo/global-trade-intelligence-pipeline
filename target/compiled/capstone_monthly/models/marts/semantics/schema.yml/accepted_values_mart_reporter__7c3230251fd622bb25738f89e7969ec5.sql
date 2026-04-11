
    
    

with all_values as (

    select
        used_transshipment_hub_flag as value_field,
        count(*) as n_records

    from `chokepoint-capfractal`.`analytics_marts`.`mart_reporter_partner_commodity_month_enriched`
    group by used_transshipment_hub_flag

)

select *
from all_values
where value_field not in (
    True,False
)


