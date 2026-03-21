create schema if not exists raw;
create schema if not exists analytics;

-- Core Comtrade fact tables
create or replace table raw.comtrade_fact as
select *
from read_parquet('data/silver/comtrade/comtrade_fact/cmd_month.parquet');

create or replace table raw.comtrade_partner_month as
select *
from read_parquet('data/silver/comtrade/comtrade_fact/partner_month.parquet');

create or replace table raw.comtrade_reporter_month as
select *
from read_parquet('data/silver/comtrade/comtrade_fact/reporter_month.parquet');

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
from read_parquet('data/silver/brent/brent_daily.parquet');

create or replace table raw.brent_monthly as
select *
from read_parquet('data/silver/brent/brent_monthly.parquet');