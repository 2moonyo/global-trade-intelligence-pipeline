select
  cast(ref_date as date) as ref_date,
  cast(period as integer) as period,
  year_month,
  cast(ref_year as integer) as ref_year,
  upper(trim(reporter_iso3)) as reporter_iso3,
  cmdCode as cmd_code,
  cmdDesc as commodity_name_raw,
  trim(trade_flow) as trade_flow,
  cast(trade_value_usd as double) as trade_value_usd,
  cast(netWgt as double) as net_weight_kg,
  cast(grossWgt as double) as gross_weight_kg,
  cast(row_count as bigint) as row_count
from "analytics"."raw"."comtrade_fact"