# Comtrade Contract

See also:

- [SHARED_WAREHOUSE_AND_SERVING_CONTRACT.md](./SHARED_WAREHOUSE_AND_SERVING_CONTRACT.md)

## Status

- Implemented locally end to end for analytical marts and dashboard use
- Bronze ingest is scripted
- Curated silver assets exist and are authoritative for the local warehouse
- Cloud landing path is not yet provisioned in the repo

## Source Systems

- UN Comtrade Data API
- Comtrade metadata extracts under `data/metadata/comtrade`

## Purpose

- Provide canonical reporter-partner-commodity-month-flow trade facts
- Support dependence analysis, route enrichment, hub dependency, and chokepoint exposure

## Lifecycle By Phase

| Phase | Current implemented asset | Grain | Partitioning | Canonical path or table | Notes |
| --- | --- | --- | --- | --- | --- |
| Bronze annual | Comtrade annual JSON extracts | request-specific | `year=YYYY/reporter=CODE` | `data/bronze/comtrade/year=YYYY/reporter=CODE/*.json` | Primary scripted bronze pattern. |
| Bronze monthly recovery | Comtrade monthly JSON extracts | request-specific | `year=YYYY/monthly/reporter=CODE` | `data/bronze/comtrade/year=YYYY/monthly/reporter=CODE/*.json` | Used to recover annual zero-row or gap cases. |
| Bronze event batch | event-window JSON extracts | batch request | no strict partition | `data/bronze/comtrade/events/*.json` | Specialized batch helper for event windows. |
| Bronze audit metadata | extraction registry and checkpoint | one row per job or checkpoint snapshot | append-only JSONL / JSON | `logs/extraction_registry.jsonl`, `logs/comtrade_checkpoint.json`, plus test2 and monthly-history variants | Operational record of quota hits, completed jobs, and coverage gaps. |
| Silver curated fact | `comtrade_fact` | source-like monthly trade rows | `ref_year=YYYY/reporter_iso3=ISO3` | `data/silver/comtrade/comtrade_fact/ref_year=YYYY/reporter_iso3=ISO3/*.parquet` | Current authoritative analytical source for local warehouse. |
| Silver curated dimensions | country, commodity, time, trade flow, route applicability, route map, ports/basins/chokepoints | dimension-specific | mostly none | `data/silver/comtrade/dimensions/*.parquet`, `data/silver/comtrade/dim_trade_routes.parquet` | These are already analytics-oriented silver assets. |
| DuckDB raw landing | `raw.comtrade_fact`, `raw.dim_country`, `raw.dim_time`, `raw.dim_commodity`, `raw.dim_trade_flow`, `raw.route_applicability`, `raw.dim_trade_routes` | table-specific | DuckDB tables | `raw.*` | Loaded by `warehouse/bootstrap_silver_to_duckdb.sql`. |
| dbt staging | `stg_comtrade_trade_base`, `stg_comtrade_fact`, `stg_dim_country`, `stg_dim_commodity`, `stg_dim_time`, `stg_route_applicability` | model-specific | dbt-managed | analytics schemas | Standardizes codes, time keys, and route evidence fields. |
| dbt facts | `fct_reporter_partner_commodity_month`, `fct_reporter_partner_commodity_month_provenance`, `fct_reporter_partner_commodity_route_month`, `fct_reporter_partner_commodity_hub_month` | fact-specific | dbt-managed | analytics schemas | Canonical fact, provenance, route enrichment, and hub expansion. |
| dbt marts | trade summary, trade exposure, hub dependency, macro features | mart-specific | dbt-managed | analytics schemas | Main analytical outputs consumed by Streamlit. |
| Dashboard | Streamlit pages 1, 2, 3, 4, and 5 | page-specific | query filtered by month, reporter, partner, commodity | `app/` | Comtrade is the core structural backbone of the dashboard. |

## Canonical Business Grain

The canonical analytical trade grain is:

- `reporter_iso3`
- `partner_iso3`
- `cmd_code`
- `period`
- `year_month`
- `trade_flow`

This grain is represented by the stable `canonical_grain_key` in dbt.

## Current Required Fields

At the cleaned staging and canonical fact layer, the required fields are:

- `canonical_grain_key`
- `ref_date`
- `period`
- `year_month`
- `ref_year`
- `reporter_iso3`
- `partner_iso3`
- `cmd_code`
- `trade_flow`
- `trade_value_usd`

Common optional-but-important analytical fields:

- `net_weight_kg`
- `gross_weight_kg`
- `qty`
- `mot_code`
- `partner2_code`
- raw lineage columns retained in silver and provenance-compatible raw inputs

## Canonical Trade Fact Logic

`stg_comtrade_trade_base`:

- normalizes `period`, `year_month`, and `ref_year`
- uppercases country ISO3 values
- maps `flowCode` values `M` and `X` to `Import` and `Export`
- removes structurally unusable rows
- creates `canonical_grain_key` from reporter, partner, commodity, period, and flow

`fct_reporter_partner_commodity_month`:

- keeps the same declared analytical grain
- aggregates `trade_value_usd`, `net_weight_kg`, `gross_weight_kg`, and `qty`
- computes `usd_per_kg`
- counts source records contributing to each canonical row

`fct_reporter_partner_commodity_month_provenance`:

- aggregates raw row lineage to the canonical grain
- preserves batch ids, source files, and bronze extraction timestamps
- exists specifically to keep analytical aggregation auditable

## Routing Logic Contract

Routing is intentionally modeled as a pair-level analytical enrichment, not as commodity-ground-truth shipping telemetry.

Inputs:

- `raw.dim_trade_routes`
- `raw.route_applicability`

The route logic in `fct_reporter_partner_commodity_route_month` works as follows:

1. Select one preferred route candidate per `reporter_iso3 + partner_iso3`.
   - `default_shortest` is prioritized when multiple route scenarios exist.
2. Summarize pair-level transport evidence from `stg_route_applicability`.
   - `has_sea`
   - `has_inland_water`
   - `has_unknown`
   - `has_non_marine`
3. Join the canonical trade fact to the preferred route and applicability evidence.
4. Compute:
   - `route_applicability_status`
   - `mot_code_filter_status`
   - `is_maritime_routed`
   - `route_confidence_score`

Current confidence interpretation:

- `HIGH`
  - routed with high or very high route confidence
  - and maritime evidence exists
- `MEDIUM`
  - maritime-routed with sea or inland-water evidence but weaker route confidence
- `LOW`
  - routed with unknown evidence or weaker inferred support
- `VERY_LOW`
  - explicitly non-maritime-only evidence

Justification:

- pair-level route enrichment preserves the canonical trade row count
- motCode evidence is used as a guardrail against over-claiming maritime exposure
- one preferred route per pair keeps the main exposure mart interpretable and cheap to query
- the model is designed for risk and exposure analytics, not vessel-by-vessel route reconstruction

## Hub Allocation Logic Contract

`fct_reporter_partner_commodity_hub_month` expands the canonical fact to `partner2_iso3` variants while preserving additive integrity.

Allocation method:

1. Aggregate route-applicability evidence by `reporter_iso3 + partner_iso3 + partner2_iso3`
2. Compute `partner2_trade_value_usd` and pair totals
3. Allocate each canonical trade row by:
   - observed `partner2_trade_value_usd / pair_trade_value_usd` when positive totals exist
   - equal shares across variants when totals exist but values are zero
   - a fallback row with `partner2_iso3 = null` and allocation share `1.0` when no hub variants exist

Justification:

- this preserves total trade value across hub expansion
- it makes hub dependency analytically useful without corrupting canonical totals
- it supports a dedicated hub dependency mart without forcing every dashboard view into hub grain

## Exposure Logic Contract

`mart_trade_exposure`:

- grain: `reporter_iso3 + chokepoint_id + year_month + route_confidence_score`
- numerator: maritime-routed trade value assigned to a chokepoint
- denominator: total reporter monthly trade value
- output: `chokepoint_trade_exposure_ratio`

`mart_reporter_month_chokepoint_exposure`:

- joins reporter-chokepoint trade exposure to:
  - PortWatch stress metrics
  - event counts and severity from `stg_chokepoint_bridge`
- provides the main reporter exposure table used in the dashboard

## Downstream Use

- Executive Overview uses `mart_reporter_month_trade_summary` and `mart_reporter_month_chokepoint_exposure`
- Trade Dependence uses `fct_reporter_partner_commodity_month`
- Chokepoint Stress & Exposure uses route facts and exposure marts
- Events & Commodity Impact counts affected countries using `mart_trade_exposure`
- Energy Vulnerability Context uses `mart_reporter_commodity_month_trade_summary` and `mart_trade_exposure`

## Known Gaps

- Silver transformation into the current curated Comtrade parquet assets is not yet represented by one standardized Python silver builder in the repo.
- Route applicability status naming is not fully harmonized between source values and downstream interpretation.
- This is an analytical routing model, not a ground-truth shipping route registry.
