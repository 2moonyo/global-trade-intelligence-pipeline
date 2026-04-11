
  
    

    create or replace table `chokepoint-capfractal`.`analytics_marts`.`mart_reporter_month_trade_summary`
      
    
    

    
    OPTIONS()
    as (
      with reporter_month as (
  select
    f.reporter_iso3,
    f.period,
    f.year_month,
    f.ref_year,
    sum(f.trade_value_usd) as total_trade_value_usd,
    sum(case when lower(f.trade_flow) like '%export%' then f.trade_value_usd else 0 end) as export_trade_value_usd,
    sum(case when lower(f.trade_flow) like '%import%' then f.trade_value_usd else 0 end) as import_trade_value_usd,
    sum(f.net_weight_kg) as total_net_weight_kg,
    sum(f.gross_weight_kg) as total_gross_weight_kg,
    sum(f.record_count) as source_row_count
  from `chokepoint-capfractal`.`analytics_marts`.`fct_reporter_partner_commodity_month` as f
  group by 1, 2, 3, 4
)

select
  rm.reporter_iso3,
  c.country_name as reporter_country_name,
  c.region as reporter_region,
  c.subregion as reporter_subregion,
  c.continent as reporter_continent,
  c.is_eu as reporter_is_eu,
  c.is_oecd as reporter_is_oecd,
  rm.period,
  rm.year_month,
  t.month_start_date,
  t.year,
  t.month,
  t.quarter,
  rm.total_trade_value_usd,
  rm.export_trade_value_usd,
  rm.import_trade_value_usd,
  rm.total_net_weight_kg,
  rm.total_gross_weight_kg,
  rm.source_row_count
from reporter_month as rm
left join `chokepoint-capfractal`.`analytics_marts`.`dim_country` as c
  on rm.reporter_iso3 = c.iso3
left join `chokepoint-capfractal`.`analytics_marts`.`dim_time` as t
  on rm.period = t.period
    );
  