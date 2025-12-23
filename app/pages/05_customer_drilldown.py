from __future__ import annotations

import streamlit as st

from app.data_access import load_outcomes_light, load_campaign_kpis
from app.ui_utils import fmt_money, fmt_pct, fmt_num

st.title("Customer Drilldown")

out = load_outcomes_light()
camp = load_campaign_kpis()
if out.empty or camp.empty:
    st.stop()

campaigns = camp["campaign_id"].dropna().unique().tolist()
default = st.session_state.get("selected_campaign") or (campaigns[0] if campaigns else None)
sel = st.selectbox("Select campaign", campaigns, index=campaigns.index(default) if default in campaigns else 0)

d = out[out["campaign_id"] == sel].copy()
if d.empty:
    st.warning("No customer outcomes for this campaign.")
    st.stop()

st.markdown("### Distribution checks (is uplift driven by outliers?)")

# Revenue concentration: top X% share
d_sorted = d.sort_values("revenue_in_window", ascending=False).copy()
d_sorted["cum_rev"] = d_sorted["revenue_in_window"].cumsum()
total_rev = d_sorted["revenue_in_window"].sum()
d_sorted["cum_rev_share"] = d_sorted["cum_rev"] / (total_rev if total_rev > 0 else 1.0)
d_sorted["rank_pct"] = (d_sorted.reset_index().index + 1) / len(d_sorted)

# Approx: share of revenue from top 5% customers
top5_cut = max(int(0.05 * len(d_sorted)), 1)
top5_rev_share = d_sorted.head(top5_cut)["revenue_in_window"].sum() / (total_rev if total_rev > 0 else 1.0)

c1, c2, c3 = st.columns(3)
c1.metric("Customers", fmt_num(len(d)))
c2.metric("Converters", fmt_num(int(d["converted_flag"].sum())))
c3.metric("Top 5% revenue share", fmt_pct(float(top5_rev_share)))

st.caption("If top-customer share is extremely high, validate decisions to avoid outlier-driven scaling.")
st.line_chart(d_sorted.set_index("rank_pct")[["cum_rev_share"]])

st.markdown("### Search a customer")
cust_id = st.text_input("Customer ID (e.g., 12345)", value="")
if cust_id.strip():
    try:
        cid = int(cust_id.strip())
        one = d[d["customer_id"] == cid]
        if one.empty:
            st.warning("Customer not found in this campaign outcomes.")
        else:
            st.dataframe(one, use_container_width=True)
    except ValueError:
        st.error("Customer ID must be an integer.")
