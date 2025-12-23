from __future__ import annotations

import streamlit as st

from app.data_access import load_segment_kpis, load_campaign_kpis
from app.ui_utils import fmt_pct, fmt_money, fmt_num

st.title("Segment Analysis")

seg = load_segment_kpis()
camp = load_campaign_kpis()
if seg.empty or camp.empty:
    st.stop()

campaigns = camp["campaign_id"].dropna().unique().tolist()
default = st.session_state.get("selected_campaign") or (campaigns[0] if campaigns else None)
sel = st.selectbox("Select campaign", campaigns, index=campaigns.index(default) if default in campaigns else 0)

d = seg[seg["campaign_id"] == sel].copy()
if d.empty:
    st.warning("No segment KPIs for this campaign.")
    st.stop()

d = d.sort_values("incremental_revenue", ascending=False)

show = d[[
    "segment_name",
    "exposed_n_customers", "holdout_n_customers",
    "CR_uplift", "RPC_uplift", "incremental_revenue",
    "insufficient_sample_flag"
]].copy()

show["CR_uplift"] = show["CR_uplift"].map(fmt_pct)
show["RPC_uplift"] = show["RPC_uplift"].map(fmt_money)
show["incremental_revenue"] = show["incremental_revenue"].map(fmt_money)

st.markdown("### Where is incrementality concentrated?")
st.dataframe(show, use_container_width=True)

st.markdown("### Incremental revenue by segment")
chart = d[["segment_name", "incremental_revenue"]].set_index("segment_name")
st.bar_chart(chart)
