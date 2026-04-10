# Shared Warehouse And Serving Contract

## Scope And Current Reality

This contract describes the shared behavior of the repository that applies across datasets.

It intentionally distinguishes between:

- the implemented local stack:
  - source files and APIs
  - `data/bronze/*` and `data/silver/*`
  - DuckDB `raw.*`
  - dbt staging and marts
  - Streamlit dashboard under `app/`
- the first cloud slice:
  - implemented for PortWatch and extended to additional slices such as Comtrade, Brent, and events ingestion paths
  - bronze and silver published to GCS where those dataset pipelines exist
  - BigQuery landing tables in `raw.*`
  - dbt BigQuery target available as `bigquery_dev`
  - future BI target is Looker Studio

## Active Environment Contract

The current verified dbt runtime environment is:

| Concern | Current contract |
| --- | --- |
| dbt profile | `capstone_monthly` |
| default local target | `duckdb_dev` |
| warehouse validation target | `bigquery_dev` |
| local warehouse file | `warehouse/analytics.duckdb` |
| local DuckDB base schema | `analytics` |
| dbt staging schema | `analytics_staging` |
| dbt marts schema | `analytics_marts` |
| dbt model materialization defaults | staging = `view`, marts = `table` |

Important current truths for environment handling:

- `profiles.yml` sets `duckdb_dev` as the default target for local development.
- `bigquery_dev` is the explicit target for cloud-side dbt validation and dashboard-serving preparation.
- dbt model schemas are currently controlled centrally by `dbt_project.yml`, not by per-folder custom schema macros.
- The canonical semantic layer reset has started in `models/marts/semantics` with `mart_dashboard_global_trade_overview` as the first new dashboard mart.

## Architecture Reality

The live warehouse is not a strict medallion-in-database design. It is:

1. source APIs or curated/manual files
2. bronze and silver files on local disk
3. DuckDB `raw` landing tables loaded from those files
4. dbt staging models
5. dbt marts
6. Streamlit pages reading those marts and selected staging models

Important current truths:

- `raw` is a mixed landing layer, not a pure bronze mirror.
- PortWatch has the most mature cloud slice, but the repo also contains BigQuery-oriented publish/load paths for Comtrade, Brent, and events.
- Local analytics and local app serving still center on DuckDB.
- Events are currently owner-curated data assets in `data/silver/events/*`; there is no standardized bronze ingest job for them yet.
- The current dashboard is Streamlit, not Looker Studio. Looker Studio is the target cloud BI pattern, not the current local serving layer.

## Cross-Source Canonical Rules

- `year_month` in `YYYY-MM` format is the primary cross-source analytical month key.
- `period` in `YYYYMM` integer form is the canonical monthly warehouse key for many trade and time joins.
- `month_start_date` is the preferred physical monthly date key when a real date column exists.
- ISO3 is the preferred country join key wherever a true country grain exists.
- Derived analytical metrics belong in dbt unless there is a strong operational reason to materialize them earlier.
- Re-runnable and idempotent behavior matters more than minimizing file count at this project stage.

## Serving Layer Contract

### Current Local Dashboard

The implemented dashboard contract is the Streamlit app under `app/`, backed by DuckDB `warehouse/analytics.duckdb`.

| Streamlit page | Primary models used | Purpose |
| --- | --- | --- |
| Executive Overview | `analytics_marts.mart_reporter_month_trade_summary`, `analytics_marts.mart_reporter_month_chokepoint_exposure` | High-level trade scale, exposure context, and source freshness. |
| Trade Dependence | `analytics_marts.fct_reporter_partner_commodity_month` | Bilateral corridor, commodity dependence, and partner concentration analysis. |
| Chokepoint Stress & Exposure | `analytics_staging.stg_portwatch_stress_metrics`, `analytics_marts.mart_reporter_month_chokepoint_exposure`, optional `analytics_marts.fct_reporter_partner_commodity_route_month` | Traffic stress, reporter exposure, and route-level evidence. |
| Events & Commodity Impact | `analytics_marts.dim_event`, `analytics_marts.bridge_event_month`, `analytics_marts.bridge_event_chokepoint`, `analytics_marts.mart_event_impact`, `analytics_staging.stg_portwatch_stress_metrics`, `analytics_marts.fct_reporter_partner_commodity_month` | Event windows, affected chokepoints, commodity movement, and impact evidence. |
| Energy Vulnerability Context | `analytics_marts.mart_reporter_energy_vulnerability`, `analytics_marts.mart_reporter_commodity_month_trade_summary`, `analytics_marts.mart_trade_exposure` | Structural energy dependence overlaid with trade scale and chokepoint exposure. |

### Current Semantic Dashboard Contract

The semantic presentation layer is being rebuilt under `models/marts/semantics` for Looker Studio.

Current implemented semantic mart:

| Semantic mart | Grain | Current purpose |
| --- | --- | --- |
| `analytics_marts.mart_dashboard_global_trade_overview` | one row per reporter country per month across a full reporter-month coverage grid | Page 1 overview scorecards, monthly trend line, top reporter ranking, and completeness or missingness messaging |

This semantic layer is intended to be dashboard-facing and business-readable:

- country names should remain standardized and human-readable
- compact labels such as `8.24B`, `645.00M`, and `1.40T` are allowed alongside raw numeric fields
- Looker Studio should require minimal custom SQL
- semantic marts should be updated incrementally, one mart at a time, with matching tests and explicit grain documentation
- completeness fields repeated at month grain are intentional so Looker Studio scorecards can use `MAX` without blending

### Target Cloud Serving Pattern

The intended cloud serving pattern is:

- bronze and silver in GCS
- landing tables in BigQuery
- dbt on BigQuery
- BI in Looker Studio

Today, only PortWatch has concrete repo support for that path.

Current implementation note:

- `bigquery_dev` is a real, usable target for targeted dbt validation.
- Full-project parity across all models is still a migration effort and should not be assumed from a single successful targeted build.

## Cross-Source Computed Models And Justification

## Canonical Time, Country, And Commodity Dimensions

`stg_dim_time`:

- is generated in dbt from observed months across trade, PortWatch, Brent, FX, energy, and event sources
- intentionally acts as a conformed monthly spine
- avoids coupling the warehouse to a single raw time table

Justification:

- no single source covers every month used in the full analytical graph
- event lead and lag windows need time coverage beyond the trade-only range

`stg_dim_country` and `stg_dim_commodity`:

- are currently rooted in the curated Comtrade dimension assets
- act as conformed dimensions reused across trade, macro, energy, and dashboard joins

## Reporter Summary Marts

`mart_reporter_month_trade_summary`:

- grain: one row per `reporter_iso3 + period + year_month`
- aggregates total, import, and export USD values and weights
- joins country and time dimensions

`mart_reporter_commodity_month_trade_summary`:

- grain: one row per `reporter_iso3 + cmd_code + period + year_month`
- provides commodity group context

Justification:

- these marts are cheap to query
- they give the dashboard a stable, interpretable summary layer
- they prevent repeated heavy scans of the canonical fact for common overview questions

## Reporter Chokepoint Exposure Marts

`mart_trade_exposure`:

- keeps route confidence explicit as part of the grain
- is the reusable analytical mart for event exposure country counting

`mart_reporter_month_chokepoint_exposure`:

- joins trade exposure to PortWatch stress and active events
- is the main combined reporter-risk surface

`mart_reporter_month_chokepoint_exposure_with_brent`:

- adds Brent context to the reporter-chokepoint exposure mart

Justification:

- separates structural trade dependence from traffic stress and event overlay
- lets the dashboard mix risk signals without re-deriving them page by page

## Macro And Energy Context Marts

`mart_macro_monthly_features`:

- grain: `year_month + fx_currency_code`
- joins Brent monthly and FX monthly

`mart_reporter_month_macro_features`:

- grain: `reporter_iso3 + period + fx_currency_code`
- joins reporter-month trade summaries to:
  - Brent monthly
  - FX monthly
  - annual energy indicators broadcast by reporter and year

Justification:

- these marts keep macro context separate from core trade exposure logic
- annual energy values are intentionally broadcast to month grain because no true monthly series exists in the source

## Event Impact Mart

`mart_event_impact`:

- final grain: one row per `event_id`
- join order is staged to avoid grain explosion

Current computation pattern:

1. build an `event_id + chokepoint_id + year_month` base
2. attach PortWatch z-scores
3. aggregate stress metrics to event grain
4. aggregate structural baseline throughput to event grain
5. compute realized throughput change vs baseline
6. separately count exposed countries via `mart_trade_exposure`
7. join conformed event attributes

Justification:

- event analytics must combine disruption intensity and structural relevance
- keeping event grain final makes the dashboard page stable and interpretable
- the mart deliberately avoids mixing trade and traffic joins too early, which would create duplicate event rows

## Quality And Governance Expectations

The highest-value blocking checks by layer are:

| Layer | Blocking checks |
| --- | --- |
| Bronze | partition-path consistency, required identifiers, parseable dates, non-empty extracts where a successful job is expected |
| Silver | uniqueness at declared grain, stable typing, join-key preservation, explicit handling of missing coverage |
| Raw landing | expected table existence, expected schema, expected date grain, no silent duplication from repeated loads |
| dbt staging | key not-null checks, unique grain checks, canonical code normalization |
| dbt marts | grain tests, allocation integrity for hub routing, event bridge deduplication, reporter exposure sanity |
| Dashboard | graceful degradation when optional marts are absent, truthful empty states instead of silent fallback distortion |

## Migration Notes

To move the remaining datasets toward the PortWatch pattern, the next desired state is:

1. define a canonical silver contract for each dataset
2. add a repeatable publish-to-GCS step
3. add BigQuery landing tables dataset by dataset
4. keep dbt-derived metrics in dbt unless there is a clear cost or latency reason not to
5. preserve the current Streamlit dashboard until the BigQuery and Looker path is complete enough to replace it

Current migration maturity by dataset:

| Dataset | Local contract maturity | Cloud maturity |
| --- | --- | --- |
| PortWatch | high | high relative to the repo; first working vertical slice |
| Comtrade | high locally | low |
| Brent | medium | low |
| FX | medium | low |
| World Bank energy | medium-high | low |
| Events | medium locally, but manually curated | low |
