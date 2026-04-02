create schema if not exists raw;
create schema if not exists analytics;

-- Core Comtrade fact tables
create or replace table raw.comtrade_fact as
select *
from read_parquet('data/silver/comtrade/comtrade_fact/year=*/month=*/reporter_iso3=*/cmd_code=*/flow_code=*/comtrade_fact.parquet');

-- Comtrade dimensions and route bridge
create or replace table raw.dim_country as
select *
from read_parquet('data/silver/comtrade/dimensions/dim_country.parquet');

create or replace table raw.dim_time as
select *
from read_parquet('data/silver/comtrade/dimensions/dim_time.parquet');

create or replace table raw.dim_commodity as
select *
from read_parquet('data/silver/comtrade/dimensions/dim_commodity.parquet');

create or replace table raw.dim_trade_flow as
select *
from read_parquet('data/silver/comtrade/dimensions/dim_trade_flow.parquet');

create or replace table raw.route_applicability as
select *
from read_parquet('data/silver/comtrade/dimensions/bridge_country_route_applicability.parquet');

create or replace table raw.dim_trade_routes as
select *
from read_parquet('data/silver/comtrade/dim_trade_routes.parquet');

-- Event and portwatch bridges used by exposure mart
create or replace table raw.chokepoint_bridge as
select *
from read_parquet('data/silver/events/bridge_event_month_chokepoint_core/*/*.parquet');

create or replace table raw.portwatch_monthly as
select *
from read_parquet('data/silver/portwatch/portwatch_chokepoint_stress_monthly_all.parquet');

create or replace table raw.brent_daily as
select *
from read_parquet('data/silver/brent/brent_daily/year=*/month=*/brent_daily.parquet');

create or replace table raw.brent_monthly as
select *
from read_parquet('data/silver/brent/brent_monthly/year=*/month=*/brent_monthly.parquet');

-- ECB FX daily feed from bronze batch exports.
create or replace table raw.ecb_fx_eu_daily as
select
	cast("date" as date) as date,
	upper(trim(cast(quote_ccy as varchar))) as quote_ccy,
	upper(trim(cast(base_ccy as varchar))) as base_ccy,
	cast(rate as double) as rate,
	cast(load_ts as varchar) as load_ts
from read_csv_auto('data/bronze/ecb_fx_eu/Batch/*.csv', header=true);

-- World Bank energy vulnerability indicators from annual silver parquet outputs.
create or replace table raw.energy_vulnerability as
select
	cast(dt as date) as dt,
	cast(month_start_date as date) as month_start_date,
	cast(year as integer) as year,
	cast(dataset as varchar) as dataset,
	cast(source as varchar) as source,
	cast(ingest_ts as varchar) as ingest_ts,
	cast(indicator_alias as varchar) as indicator_alias,
	cast(indicator_code as varchar) as indicator_code,
	cast(indicator_id as varchar) as indicator_id,
	cast(indicator_name as varchar) as indicator_name,
	cast(metric_name as varchar) as metric_name,
	cast(unit_hint as varchar) as unit_hint,
	cast(country_name as varchar) as country_name,
	cast(country_id as varchar) as country_id,
	upper(trim(cast(country_iso3 as varchar))) as country_iso3,
	cast(value as double) as value,
	cast(wb_unit as varchar) as wb_unit,
	cast(obs_status as varchar) as obs_status,
	cast(decimal_places as integer) as decimal_places,
	cast(grain_key as varchar) as grain_key
from read_parquet('data/silver/worldbank_energy/energy_vulnerability/year=*/*.parquet');

-- Event bridge: chokepoint core (monthly, wide)
create or replace table raw.bridge_event_month_chokepoint_core as
select *
from read_csv_auto('data/silver/events/bridge_event_month_chokepoint_core.csv', header=true);

-- Event bridge: maritime region (monthly, wide)
create or replace table raw.bridge_event_month_maritime_region as
select *
from read_csv_auto('data/silver/events/bridge_event_month_maritime_region.csv', header=true);

-- Event dimension
create or replace table raw.dim_event as
select *
from read_csv_auto('data/silver/events/dim_event.csv', header=true);
