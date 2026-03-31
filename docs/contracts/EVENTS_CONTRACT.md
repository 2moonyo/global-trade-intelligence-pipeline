# Events Contract

See also:

- [SHARED_WAREHOUSE_AND_SERVING_CONTRACT.md](./SHARED_WAREHOUSE_AND_SERVING_CONTRACT.md)

## Status

- Implemented locally from curated silver files
- No standardized bronze ingest job exists yet
- No cloud landing path exists yet
- This is the dataset family most likely to evolve toward user-authored input

## Source Systems

- Owner-curated event metadata and event-location bridge files under `data/silver/events/*`

## Purpose

- Represent disruptive events and their affected chokepoints or non-core maritime locations
- Support event-window analytics and the event impact dashboard page
- Create a future path for user-added events and custom analytical scenarios

## Lifecycle By Phase

| Phase | Current implemented asset | Grain | Partitioning | Canonical path or table | Notes |
| --- | --- | --- | --- | --- | --- |
| Curated silver event dimension | `dim_event` | one row per `event_id` | none | `data/silver/events/dim_event.csv` and `data/silver/events/dim_event.parquet` | Owner-maintained event metadata source. |
| Curated silver core bridge | `bridge_event_month_chokepoint_core` | one row per `event_id` x `year_month` x `chokepoint_name` | `year_month=YYYY-MM` parquet plus CSV | `data/silver/events/bridge_event_month_chokepoint_core/*` | Core chokepoint-linked event bridge. |
| Curated silver non-core bridge | `bridge_event_month_maritime_region` | one row per `event_id` x `year_month` x `location_name` | `year_month=YYYY-MM` parquet plus CSV | `data/silver/events/bridge_event_month_maritime_region/*` | Non-core geography bridge still using a legacy source column name. |
| DuckDB raw landing | `raw.dim_event`, `raw.bridge_event_month_chokepoint_core`, `raw.bridge_event_month_maritime_region` | table-specific | DuckDB tables | `raw.*` | Loaded directly from curated silver files. |
| dbt staging | `stg_event_raw`, `stg_event_month_chokepoint`, `stg_event_month_region`, `stg_event_location` | model-specific | dbt-managed | event staging schemas | Normalizes event, core chokepoint, and non-core location interfaces. |
| dbt dimensions and bridges | `dim_event`, `dim_location`, `bridge_event_month`, `bridge_event_chokepoint`, `bridge_event_region`, `bridge_event_location` | dimension or bridge grain | dbt-managed | event marts schemas | Conformed event serving layer. |
| dbt mart | `mart_event_impact` | one row per `event_id` | dbt-managed | analytics marts | Final event-level impact output. |
| Dashboard | Streamlit page 4 | event and event-window grain | page-level filters | `app/pages/4_Events_Commodity_Impact.py` | Current end user event consumer. |

## Contract Decisions

- The event contract currently starts at curated silver, not bronze.
- `event_id` is the stable primary identifier across the entire event subgraph.
- Manual authoring is acceptable now, but event assets should evolve toward a validated user-input contract rather than ad hoc file edits.
- Event geography is normalized downstream through `stg_event_location`, even though the non-core raw bridge still carries the legacy source column name `chokepoint_name`.

## Minimum Required Fields

Event metadata:

- `event_id`
- `event_name`
- `event_type`
- `start_date`
- `end_date`
- `lead_months`
- `lag_months`
- `base_severity`
- `event_scope`

Monthly bridge essentials:

- `event_id`
- `event_name`
- `year_month`
- `event_phase`
- `event_active_flag`
- `lead_flag`
- `lag_flag`
- `severity_weight`
- `global_event_flag`
- `link_role`
- one location field:
  - `chokepoint_name` in current source files

## Event Modelling Logic

`stg_event_location` is the canonical normalization layer:

- core chokepoints become `location_type = 'chokepoint'`
- non-core locations become normalized `location_type` values such as:
  - `port`
  - `coastal_region`
  - `maritime_passage`
  - `maritime_region`

`dim_event` derives:

- `severity_level` from `base_severity_score`
- `event_scope_type` from the observed location coverage

`bridge_event_month`:

- deduplicates event-months
- preserves lead, active, and lag flags
- aligns event windows to the conformed monthly time dimension when possible

`bridge_event_chokepoint`:

- hashes `chokepoint_name` into the canonical analytics `chokepoint_id`
- keeps event and PortWatch joins consistent without depending on raw source ids

## User-Authored Future Contract

This dataset is the most likely candidate for a future user-input workflow.

Future contract requirements should include:

- user-supplied `event_id` or deterministic generated ids
- required date range validation
- required scope typing
- required link-role semantics
- versioned edits rather than silent overwrite
- moderation or validation checks before downstream refresh

Recommended future interface:

- a bronze-style append-only event submission layer
- a validated silver event dimension and bridge builder
- preserved author metadata and submission timestamp

## Dashboard Use

The Events & Commodity Impact page uses:

- event metadata from `dim_event`
- event windows from `bridge_event_month`
- affected chokepoints from `bridge_event_chokepoint`
- event-level metrics from `mart_event_impact`
- fallback commodity and traffic trend logic when event bridge support is incomplete
