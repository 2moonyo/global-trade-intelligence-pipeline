with reporter_commodity_month as (
  select
    f.reporter_iso3,
    f.cmd_code,
    f.period,
    f.year_month,
    f.ref_year,
    sum(f.trade_value_usd) as total_trade_value_usd,
    sum(case when lower(f.trade_flow) like '%export%' then f.trade_value_usd else 0 end) as export_trade_value_usd,
    sum(case when lower(f.trade_flow) like '%import%' then f.trade_value_usd else 0 end) as import_trade_value_usd,
    sum(f.net_weight_kg) as total_net_weight_kg,
    sum(f.gross_weight_kg) as total_gross_weight_kg,
    sum(f.record_count) as source_row_count
  from {{ ref('fct_reporter_partner_commodity_month') }} as f
  group by 1, 2, 3, 4, 5
)

select
  rcm.reporter_iso3,
  c.country_name as reporter_country_name,
  rcm.cmd_code,
  co.commodity_name,
  co.commodity_group,
  co.food_flag,
  co.energy_flag,
  co.industrial_flag,
  rcm.period,
  rcm.year_month,
  t.month_start_date,
  t.year,
  t.month,
  t.quarter,
  rcm.total_trade_value_usd,
  rcm.export_trade_value_usd,
  rcm.import_trade_value_usd,
  rcm.total_net_weight_kg,
  rcm.total_gross_weight_kg,
  rcm.source_row_count
from reporter_commodity_month as rcm
left join {{ ref('dim_country') }} as c
  on rcm.reporter_iso3 = c.iso3
left join {{ ref('dim_commodity') }} as co
  on rcm.cmd_code = co.cmd_code
left join {{ ref('dim_time') }} as t
  on rcm.period = t.period
