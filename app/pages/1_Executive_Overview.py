from __future__ import annotations

import sys
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.lib.config import ACCENT_COLOR, MUTED_COLOR
from app.lib.db import table_exists
from app.lib.export import render_csv_download
from app.lib.filters import render_sidebar
from app.lib.narrative import overview_interpretation, overview_subtitle, scope_caption
from app.lib.queries import (
    CHOKEPOINT_EXPOSURE_TABLE,
    TRADE_SUMMARY_TABLE,
    get_data_freshness,
    get_filter_options,
    get_overview_latest_exposure,
    get_overview_latest_exposure_month,
    get_overview_top_reporters,
    get_overview_trade_trend,
    get_trade_flow_label,
)
from app.lib.ui import (
    format_pct,
    format_usd,
    render_empty_state,
    render_methodology_expander,
    render_missing_table,
    render_page_header,
)


filters = render_sidebar()
render_page_header("Executive Overview", overview_subtitle())
st.caption(scope_caption(filters))

if not table_exists(TRADE_SUMMARY_TABLE):
    render_missing_table("`analytics_marts.mart_reporter_month_trade_summary` is required for the executive overview.")
    st.stop()

trade_trend_df = get_overview_trade_trend(filters)
if trade_trend_df.empty:
    render_empty_state("No trade rows matched the current selection.")
    st.stop()

latest_trade_month = str(trade_trend_df["year_month"].max())
top_reporters_df = get_overview_top_reporters(filters, latest_trade_month)
latest_exposure_month = get_overview_latest_exposure_month(filters) if table_exists(CHOKEPOINT_EXPOSURE_TABLE) else None
latest_exposure_df = (
    get_overview_latest_exposure(filters, latest_exposure_month) if latest_exposure_month else pd.DataFrame()
)
freshness_df = get_data_freshness()

latest_trade_value = float(trade_trend_df.iloc[-1]["trade_value_usd"])
selected_total_trade = float(trade_trend_df["trade_value_usd"].sum())
trade_median = float(trade_trend_df["trade_value_usd"].median())
top_reporter_name = top_reporters_df.iloc[0]["reporter_country_name"] if not top_reporters_df.empty else "n/a"
top_reporter_value = float(top_reporters_df.iloc[0]["trade_value_usd"]) if not top_reporters_df.empty else None
dataset_reporter_count = len(get_filter_options()["reporters"])

col1, col2, col3, col4 = st.columns(4)
col1.metric("Latest trade month", latest_trade_month)
col2.metric("Trade in latest month", format_usd(latest_trade_value))
col3.metric("Top latest reporter", top_reporter_name)
col4.metric("Latest exposure month", latest_exposure_month or "n/a")

info_col, freshness_col = st.columns((1.3, 1))
with info_col:
    st.markdown(overview_interpretation(top_reporters_df))
    st.caption(
        f"The selected scope sums to {format_usd(selected_total_trade)} across the visible period and is being compared on a `{get_trade_flow_label(filters)}` basis."
    )
    if not filters.get("reporters") and len(top_reporters_df) < dataset_reporter_count:
        st.caption(
            f"Only {len(top_reporters_df)} of the {dataset_reporter_count} reporter countries in the dataset have rows in the latest trade month ({latest_trade_month})."
        )
with freshness_col:
    st.caption("Coverage and latest input by source")
    st.dataframe(
        freshness_df.rename(
            columns={
                "dataset_name": "Dataset",
                "coverage_start": "From",
                "coverage_end": "To",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )

trend_title = (
    "Trade in the selected scope remains above its period median in the latest observed month"
    if latest_trade_value >= trade_median
    else "Trade in the selected scope sits below its period median in the latest observed month"
)
st.subheader(trend_title)
trade_trend_chart = (
    alt.Chart(trade_trend_df)
    .mark_line(color=ACCENT_COLOR, point=True, strokeWidth=2.5)
    .encode(
        x=alt.X("month_start_date:T", title=None, axis=alt.Axis(grid=False)),
        y=alt.Y("trade_value_usd:Q", title="Trade value (USD)", axis=alt.Axis(format="$,.2s", grid=False)),
        tooltip=["year_month", "trade_value_usd"],
    )
    .properties(height=360)
)
st.altair_chart(trade_trend_chart, use_container_width=True)

if top_reporters_df.empty:
    render_empty_state("The latest-month reporter ranking is unavailable for the current scope.")
else:
    ranking_df = top_reporters_df.copy()
    ranking_df["reporter_label"] = ranking_df["reporter_country_name"] + " (" + ranking_df["reporter_iso3"] + ")"
    st.subheader(f"{top_reporter_name} leads the latest trade month in the selected scope")
    reporter_chart = (
        alt.Chart(ranking_df)
        .mark_bar(color=ACCENT_COLOR, cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
        .encode(
            x=alt.X("trade_value_usd:Q", title="Trade value (USD)", axis=alt.Axis(format="$,.2s", grid=False)),
            y=alt.Y("reporter_label:N", sort="-x", title=None, axis=alt.Axis(labelLimit=240)),
            tooltip=["reporter_country_name", "reporter_iso3", "trade_value_usd", "trade_flow_scope"],
        )
        .properties(height=340)
    )
    st.altair_chart(reporter_chart, use_container_width=True)
    st.caption(
        f"{top_reporter_name} contributes {format_usd(top_reporter_value)} in the latest month shown for this scope."
    )
    render_csv_download(top_reporters_df, "Download latest reporter ranking", "executive_overview_top_reporters")

if filters.get("partners") or filters.get("commodities"):
    st.caption("The exposure summary below stays at reporter-chokepoint grain, so it does not drill down to the selected partners or commodities.")

if latest_exposure_df.empty:
    render_empty_state("No reporter-level chokepoint exposure rows matched the current filters.")
else:
    exposure_headline = latest_exposure_df.iloc[0]
    st.subheader(
        f"{exposure_headline['reporter_country_name']} shows the highest latest mapped exposure to {exposure_headline['chokepoint_name']}"
    )
    exposure_display = latest_exposure_df.copy()
    exposure_display["chokepoint_trade_exposure_ratio"] = exposure_display["chokepoint_trade_exposure_ratio"].map(format_pct)
    exposure_display["chokepoint_trade_value_usd"] = exposure_display["chokepoint_trade_value_usd"].map(format_usd)
    exposure_display["stress_index_weighted"] = exposure_display["stress_index_weighted"].map(
        lambda value: "n/a" if pd.isna(value) else f"{value:,.2f}"
    )
    st.dataframe(
        exposure_display.rename(
            columns={
                "reporter_country_name": "Reporter",
                "reporter_iso3": "ISO3",
                "chokepoint_name": "Chokepoint",
                "chokepoint_trade_exposure_ratio": "Exposure ratio",
                "chokepoint_trade_value_usd": "Trade value",
                "stress_index_weighted": "Weighted stress",
                "active_event_count": "Active events",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )
    render_csv_download(latest_exposure_df, "Download latest exposure summary", "executive_overview_latest_exposure")

render_methodology_expander(
    [
        "Trade scale comes from `analytics_marts.mart_reporter_month_trade_summary`, with flow-aware measure selection across total, import, and export USD values.",
        "Latest reporter rankings are taken from the latest month available inside the selected trade scope.",
        "Exposure context comes from `analytics_marts.mart_reporter_month_chokepoint_exposure` at reporter-chokepoint-month grain.",
        "Coverage dates shown on the page are read directly from the live DuckDB tables and are not hardcoded.",
    ]
)
