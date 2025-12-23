from __future__ import annotations

import streamlit as st
import pandas as pd

from app.data_access import load_campaign_kpis
from app.ui_utils import fmt_pct, fmt_money, fmt_num, decision_label

st.title("Executive Overview")

df = load_campaign_kpis()
if df.empty:
    st.stop()

# Apply global filters
sel_campaign = st.session_state.get("selected_campaign")
sel_channel = st.session_state.get("selected_channel")

if sel_campaign:
    df = df[df["campaign_id"] == sel_campaign]
if sel_channel:
    df = df[df["channel"] == sel_channel]

if df.empty:
    st.warning("No data after filters.")
    st.stop()

df = df.sort_values("incremental_revenue", ascending=False).copy()
df["decision"] = df.apply(lambda r: decision_label(float(r["incremental_revenue"]), int(r["insufficient_sample_flag"])), axis=1)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Campaigns", fmt_num(len(df)))
c2.metric("Total Incremental Revenue", fmt_money(df["incremental_revenue"].sum()))
c3.metric("Avg RPC Uplift", fmt_money(df["RPC_uplift"].mean()))
c4.metric("Avg CR Uplift", fmt_pct(df["CR_uplift"].mean()))

st.markdown("### Ranked campaigns (decision-first)")
show = df[[
    "campaign_id", "campaign_name", "channel", "target_segment",
    "exposed_n_customers", "holdout_n_customers",
    "CR_uplift", "RPC_uplift", "incremental_revenue",
    "insufficient_sample_flag", "decision"
]].copy()

show["CR_uplift"] = show["CR_uplift"].map(fmt_pct)
show["RPC_uplift"] = show["RPC_uplift"].map(fmt_money)
show["incremental_revenue"] = show["incremental_revenue"].map(fmt_money)

st.dataframe(show, use_container_width=True)

st.markdown("### Incremental revenue by campaign")
chart_df = df[["campaign_id", "incremental_revenue"]].set_index("campaign_id")
st.bar_chart(chart_df)
