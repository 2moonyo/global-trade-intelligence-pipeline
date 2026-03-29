from __future__ import annotations

import sys
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.lib.config import ACCENT_COLOR, CAUTION_COLOR, DEFAULT_CHART_HEIGHT, MUTED_COLOR
from app.lib.db import table_exists
from app.lib.export import render_csv_download
from app.lib.filters import render_sidebar
from app.lib.narrative import event_interpretation, events_subtitle, scope_caption
from app.lib.queries import (
    EVENT_CHOKEPOINT_TABLE,
    EVENT_DIM_TABLE,
    EVENT_IMPACT_TABLE,
    EVENT_MONTH_TABLE,
    FACT_TABLE,
    PORTWATCH_TABLE,
    get_chokepoint_stress_trend,
    get_commodity_trend,
    get_default_commodity_code,
    get_default_event_id,
    get_event_before_after_table,
    get_event_chokepoint_window,
    get_event_options,
    get_event_summary,
    get_event_trade_window,
    get_filter_options,
    get_latest_stress_month,
    get_latest_stress_ranking,
)
from app.lib.ui import (
    format_pct,
    format_usd,
    render_empty_state,
    render_methodology_expander,
    render_missing_table,
    render_page_header,
)


def _phase_bands(window_df: pd.DataFrame) -> pd.DataFrame:
    if window_df.empty:
        return pd.DataFrame(columns=["month_start_date", "phase_end", "phase_label"])
    bands_df = (
        window_df.loc[window_df["phase_label"].isin(["Lead", "Active", "Lag"]), ["month_start_date", "phase_label"]]
        .sort_values("month_start_date")
        .copy()
    )
    if bands_df.empty:
        return bands_df.assign(phase_end=pd.NaT)
    bands_df["phase_end"] = bands_df["month_start_date"].shift(-1)
    bands_df.loc[bands_df.index[-1], "phase_end"] = bands_df.iloc[-1]["month_start_date"] + pd.offsets.MonthBegin(1)
    return bands_df


filters = render_sidebar()
render_page_header("Events & Commodity Impact", events_subtitle())
st.caption(scope_caption(filters))

has_event_support = all(
    table_exists(table_name)
    for table_name in (EVENT_DIM_TABLE, EVENT_MONTH_TABLE, EVENT_CHOKEPOINT_TABLE, EVENT_IMPACT_TABLE, FACT_TABLE, PORTWATCH_TABLE)
)

commodity_options = get_filter_options()["commodities"]
commodity_labels = {option["value"]: option["label"] for option in commodity_options}
commodity_values = [option["value"] for option in commodity_options]

if has_event_support and get_event_options():
    event_options = get_event_options()
    event_labels = {option["value"]: option["label"] for option in event_options}
    event_values = [option["value"] for option in event_options]
    default_event_id = get_default_event_id() or event_values[0]
    default_event_index = event_values.index(default_event_id) if default_event_id in event_values else 0

    control_col, commodity_col = st.columns(2)
    with control_col:
        selected_event_id = st.selectbox(
            "Event scenario",
            options=event_values,
            index=default_event_index,
            format_func=lambda value: event_labels.get(value, value),
        )
    default_commodity_code = get_default_commodity_code(filters, selected_event_id)
    if default_commodity_code and default_commodity_code not in commodity_values:
        commodity_values = [default_commodity_code, *commodity_values]
        commodity_labels[default_commodity_code] = default_commodity_code
    default_commodity_index = commodity_values.index(default_commodity_code) if default_commodity_code in commodity_values else 0
    with commodity_col:
        selected_commodity_code = st.selectbox(
            "Commodity in focus",
            options=commodity_values,
            index=default_commodity_index,
            format_func=lambda value: commodity_labels.get(value, value),
        )

    event_summary_df = get_event_summary(selected_event_id)
    trade_window_df = get_event_trade_window(selected_event_id, filters, selected_commodity_code)
    traffic_window_df = get_event_chokepoint_window(selected_event_id, filters)
    evidence_df = get_event_before_after_table(trade_window_df, traffic_window_df)

    if event_summary_df.empty:
        render_empty_state("The selected event does not have summary metadata.")
        st.stop()

    event_row = event_summary_df.iloc[0]
    affected_chokepoints = (
        int(event_row["affected_chokepoint_count"])
        if pd.notna(event_row["affected_chokepoint_count"])
        else 0
    )
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Event start", str(event_row["event_start_date"]))
    col2.metric("Severity", str(event_row["severity_level"]).title())
    col3.metric("Affected chokepoints", f"{affected_chokepoints}")
    col4.metric("Commodity focus", commodity_labels.get(selected_commodity_code, selected_commodity_code))

    st.markdown(event_interpretation(str(event_row["event_name"]), affected_chokepoints))

    phase_bands_df = _phase_bands(trade_window_df if not trade_window_df.empty else traffic_window_df)
    band_colors = alt.Scale(domain=["Lead", "Active", "Lag"], range=["#d7dfe8", "#f2d8c6", "#e6ebf0"])

    if trade_window_df.empty:
        render_empty_state("No commodity trade series could be built for the selected event window and filters.")
    else:
        st.subheader("Commodity trade moves visibly around the selected disruption window")
        bands = (
            alt.Chart(phase_bands_df)
            .mark_rect(opacity=0.18)
            .encode(
                x=alt.X("month_start_date:T", title=None),
                x2="phase_end:T",
                color=alt.Color("phase_label:N", scale=band_colors, legend=None),
            )
        )
        trade_line = (
            alt.Chart(trade_window_df)
            .mark_line(color=ACCENT_COLOR, point=True, strokeWidth=2.4)
            .encode(
                x=alt.X("month_start_date:T", title=None, axis=alt.Axis(grid=False)),
                y=alt.Y("trade_value_usd:Q", title="Trade value (USD)", axis=alt.Axis(format="$,.2s", grid=False)),
                tooltip=["year_month", "phase_label", "trade_value_usd"],
            )
        )
        st.altair_chart((bands + trade_line).properties(height=DEFAULT_CHART_HEIGHT), use_container_width=True)

    if traffic_window_df.empty:
        render_empty_state("No affected-chokepoint traffic rows could be mapped onto the selected event window.")
    else:
        st.subheader("Affected chokepoint traffic shifts alongside the event phases")
        traffic_bands = (
            alt.Chart(phase_bands_df)
            .mark_rect(opacity=0.14)
            .encode(
                x=alt.X("month_start_date:T", title=None),
                x2="phase_end:T",
                color=alt.Color("phase_label:N", scale=band_colors, legend=None),
            )
        )
        traffic_line = (
            alt.Chart(traffic_window_df)
            .mark_line(point=True, strokeWidth=2.1)
            .encode(
                x=alt.X("month_start_date:T", title=None, axis=alt.Axis(grid=False)),
                y=alt.Y("avg_n_total:Q", title="Average vessel movements", axis=alt.Axis(grid=False)),
                color=alt.Color("chokepoint_name:N", title=None),
                tooltip=["year_month", "phase_label", "chokepoint_name", "avg_n_total", "stress_index_weighted"],
            )
        )
        st.altair_chart((traffic_bands + traffic_line).properties(height=DEFAULT_CHART_HEIGHT), use_container_width=True)

    st.subheader("Before-and-after evidence table keeps the comparison bounded to the event window")
    if evidence_df.empty:
        render_empty_state("No before-and-after metrics could be calculated for the selected event.")
    else:
        evidence_display = evidence_df.copy()
        for column in ("lead_average", "active_average", "lag_average"):
            evidence_display[column] = evidence_display[column].map(lambda value: "n/a" if pd.isna(value) else f"{value:,.2f}")
        evidence_display["active_vs_lead_pct_change"] = evidence_display["active_vs_lead_pct_change"].map(format_pct)
        st.dataframe(
            evidence_display.rename(
                columns={
                    "metric": "Metric",
                    "lead_average": "Lead avg",
                    "active_average": "Active avg",
                    "lag_average": "Lag avg",
                    "active_vs_lead_pct_change": "Active vs lead",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )
        render_csv_download(evidence_df, "Download event evidence table", "event_impact_evidence")

    render_methodology_expander(
        [
            "Event windows come from `analytics_analytics_marts.bridge_event_month`, with event metadata from `analytics_analytics_marts.dim_event`.",
            "Commodity trade is aggregated from `analytics_marts.fct_reporter_partner_commodity_month` inside the selected event window.",
            "Affected chokepoints are mapped through `analytics_analytics_marts.bridge_event_chokepoint` and then aligned to `analytics_staging.stg_portwatch_stress_metrics` through canonical chokepoint ids derived from names.",
            "The page uses truthful event overlays only when the event bridge tables and affected-chokepoint mappings are actually present.",
        ]
    )
else:
    if not table_exists(FACT_TABLE) and not table_exists(PORTWATCH_TABLE):
        render_missing_table("Neither event support nor the fallback commodity and traffic sources are available.")
        st.stop()

    st.warning(
        "Event bridge tables are unavailable or incomplete in the current warehouse scope. The page falls back to commodity trade and chokepoint traffic trends without explicit event overlays."
    )

    default_commodity_code = get_default_commodity_code(filters)
    if default_commodity_code and default_commodity_code not in commodity_values:
        commodity_values = [default_commodity_code, *commodity_values]
        commodity_labels[default_commodity_code] = default_commodity_code
    if not commodity_values:
        render_empty_state("No commodity options are available for the fallback trade trend.")
        st.stop()

    selected_commodity_code = st.selectbox(
        "Commodity in focus",
        options=commodity_values,
        index=commodity_values.index(default_commodity_code) if default_commodity_code in commodity_values else 0,
        format_func=lambda value: commodity_labels.get(value, value),
    )

    commodity_trend_df = get_commodity_trend(filters, selected_commodity_code)
    stress_trend_df = get_chokepoint_stress_trend(filters) if table_exists(PORTWATCH_TABLE) else pd.DataFrame()
    latest_stress_month = get_latest_stress_month(filters) if table_exists(PORTWATCH_TABLE) else None
    stress_ranking_df = get_latest_stress_ranking(filters, latest_stress_month) if latest_stress_month else pd.DataFrame()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Commodity focus", commodity_labels.get(selected_commodity_code, selected_commodity_code))
    col2.metric("Latest commodity month", commodity_trend_df["year_month"].max() if not commodity_trend_df.empty else "n/a")
    col3.metric("Latest traffic month", latest_stress_month or "n/a")
    col4.metric("Chokepoints in traffic view", f"{stress_trend_df['chokepoint_name'].nunique()}" if not stress_trend_df.empty else "0")

    if commodity_trend_df.empty:
        render_empty_state("No commodity trade trend is available for the fallback view.")
    else:
        st.subheader("Commodity trade still shows the broad contour of disruption, even without explicit event scaffolding")
        commodity_chart = (
            alt.Chart(commodity_trend_df)
            .mark_line(color=ACCENT_COLOR, point=True, strokeWidth=2.4)
            .encode(
                x=alt.X("month_start_date:T", title=None, axis=alt.Axis(grid=False)),
                y=alt.Y("trade_value_usd:Q", title="Trade value (USD)", axis=alt.Axis(format="$,.2s", grid=False)),
                tooltip=["year_month", "trade_value_usd"],
            )
            .properties(height=DEFAULT_CHART_HEIGHT)
        )
        st.altair_chart(commodity_chart, use_container_width=True)

    if stress_trend_df.empty:
        render_empty_state("No chokepoint traffic trend is available for the fallback view.")
    else:
        st.subheader("Traffic still provides the operational context even when explicit event joins are absent")
        stress_chart = (
            alt.Chart(stress_trend_df)
            .mark_line(point=True, strokeWidth=2.0)
            .encode(
                x=alt.X("month_start_date:T", title=None, axis=alt.Axis(grid=False)),
                y=alt.Y("stress_index_weighted:Q", title="Weighted stress", axis=alt.Axis(grid=False)),
                color=alt.Color("chokepoint_name:N", title=None),
                tooltip=["year_month", "chokepoint_name", "avg_n_total", "stress_index_weighted"],
            )
            .properties(height=DEFAULT_CHART_HEIGHT)
        )
        st.altair_chart(stress_chart, use_container_width=True)

    st.subheader("Latest traffic ranking becomes the bounded evidence table in fallback mode")
    if stress_ranking_df.empty:
        render_empty_state("No latest traffic ranking is available for export.")
    else:
        stress_display = stress_ranking_df.copy()
        stress_display["stress_index_weighted"] = stress_display["stress_index_weighted"].map(lambda value: f"{value:,.2f}")
        st.dataframe(stress_display, use_container_width=True, hide_index=True)
        render_csv_download(stress_ranking_df, "Download fallback traffic ranking", "events_fallback_traffic_ranking")

    render_methodology_expander(
        [
            "Fallback mode is activated only when the event dimension or bridge tables are missing or incomplete.",
            "Commodity trade still comes from `analytics_marts.fct_reporter_partner_commodity_month` and remains fully filterable.",
            "Traffic context still comes from `analytics_staging.stg_portwatch_stress_metrics`, but without event shading because no trustworthy event scaffold is available.",
        ]
    )
