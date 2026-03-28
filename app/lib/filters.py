from __future__ import annotations

from collections.abc import Iterable

import streamlit as st

from app.lib.config import TOP_N_DEFAULT, TOP_N_MAX, TOP_N_MIN
from app.lib.queries import get_filter_options


def _normalize_selection(selected: list[str], all_values: Iterable[str]) -> tuple[str, ...]:
    all_tuple = tuple(all_values)
    if not selected or tuple(selected) == all_tuple or len(selected) == len(all_tuple):
        return ()
    return tuple(selected)


def render_sidebar() -> dict[str, object]:
    options = get_filter_options()

    st.sidebar.header("Filters")
    st.sidebar.caption("Shared controls across the full narrative arc. Reporter choices are limited to the countries present in the trade marts.")

    reporter_values = [value["value"] for value in options["reporters"]]
    reporter_labels = {value["value"]: value["label"] for value in options["reporters"]}
    partner_values = [value["value"] for value in options["partners"]]
    partner_labels = {value["value"]: value["label"] for value in options["partners"]}
    commodity_values = [value["value"] for value in options["commodities"]]
    commodity_labels = {value["value"]: value["label"] for value in options["commodities"]}
    chokepoint_values = [value["value"] for value in options["chokepoints"]]
    periods = options["periods"]

    selected_reporters = st.sidebar.multiselect(
        "Reporter countries in dataset",
        options=reporter_values,
        default=reporter_values,
        format_func=lambda value: reporter_labels.get(value, value),
    )

    selected_partners = st.sidebar.multiselect(
        "Partner countries",
        options=partner_values,
        default=[],
        format_func=lambda value: partner_labels.get(value, value),
    )

    trade_flow = st.sidebar.selectbox(
        "Trade direction",
        options=["All trade", "Import", "Export"],
        index=0,
    )

    if periods:
        period_range = st.sidebar.select_slider(
            "Trade and traffic time range",
            options=periods,
            value=(periods[0], periods[-1]),
        )
    else:
        period_range = ("", "")

    top_n = st.sidebar.slider("Top N", min_value=TOP_N_MIN, max_value=TOP_N_MAX, value=TOP_N_DEFAULT)

    with st.sidebar.expander("Commodity focus", expanded=False):
        selected_commodities = st.multiselect(
            "Commodities",
            options=commodity_values,
            default=[],
            format_func=lambda value: commodity_labels.get(value, value),
        )

    with st.sidebar.expander("Chokepoint focus", expanded=False):
        selected_chokepoints = st.multiselect(
            "Chokepoints",
            options=chokepoint_values,
            default=[],
        )

    return {
        "reporters": _normalize_selection(selected_reporters, reporter_values),
        "partners": tuple(selected_partners),
        "commodities": tuple(selected_commodities),
        "trade_flow": trade_flow,
        "start_period": period_range[0],
        "end_period": period_range[1],
        "top_n": int(top_n),
        "chokepoints": tuple(selected_chokepoints),
    }
