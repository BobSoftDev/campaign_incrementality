from __future__ import annotations

import streamlit as st

from app.data_access import load_campaign_kpis, load_outcomes_light
from app.ui_utils import fmt_pct, fmt_money, fmt_num, decision_label

st.title("Campaign Deep Dive")

kpis = load_campaign_kpis()
out = load_outcomes_light()
if kpis.empty or out.empty:
    st.stop()

campaigns = kpis["campaign_id"].dropna().unique().tolist()
default = st.session_state.get("selected_campaign") or (campaigns[0] if campaigns else None)
camp = st.selectbox("Select campaign", campaigns, index=campaigns.index(default) if default in campaigns else 0)

row = kpis[kpis["campaign_id"] == camp].iloc[0]

st.markdown("### Decision summary")
st.write(f"**Decision:** {decision_label(float(row['incremental_revenue']), int(row['insufficient_sample_flag']))}")
st.write(f"**Incremental Revenue:** {fmt_money(float(row['incremental_revenue']))}")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Exposed N", fmt_num(int(row["exposed_n_customers"])))
c2.metric("Holdout N", fmt_num(int(row["holdout_n_customers"])))
c3.metric("CR uplift", fmt_pct(float(row["CR_uplift"])))
c4.metric("RPC uplift", fmt_money(float(row["RPC_uplift"])))

st.markdown("### Exposed vs Holdout (what changed?)")
c5, c6, c7, c8 = st.columns(4)
c5.metric("CR exposed", fmt_pct(float(row["exposed_CR"])))
c6.metric("CR holdout", fmt_pct(float(row["holdout_CR"])))
c7.metric("RPC exposed", fmt_money(float(row["exposed_RPC"])))
c8.metric("RPC holdout", fmt_money(float(row["holdout_RPC"])))

st.markdown("### Customer-level distribution (sanity check)")
d = out[out["campaign_id"] == camp].copy()
if d.empty:
    st.warning("No outcomes for this campaign.")
    st.stop()

st.caption("Revenue per customer in window (includes zeros). Compare exposed vs holdout distributions.")
d["group"] = d["exposed_flag"].map({1: "Exposed", 0: "Holdout"})
pivot = d.groupby("group", as_index=False).agg(
    customers=("customer_id", "count"),
    converters=("converted_flag", "sum"),
    avg_revenue=("revenue_in_window", "mean"),
    p95_revenue=("revenue_in_window", lambda x: x.quantile(0.95)),
)

st.dataframe(pivot, use_container_width=True)

st.markdown("### Top customers (outlier check)")
top = d.sort_values("revenue_in_window", ascending=False).head(25)[
    ["customer_id", "group", "converted_flag", "revenue_in_window", "segment_name", "baseline_buy_prob_daily"]
]
st.dataframe(top, use_container_width=True)
