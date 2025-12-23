from __future__ import annotations

import streamlit as st

st.title("Home / Context")

st.markdown(
    """
### Why observed campaign performance is misleading
- Customers often purchase **naturally** without any campaign.
- Targeting typically selects **high-intent** customers (recent buyers, engaged members).
- Therefore, exposed-only results can show strong performance even when the campaign adds **little causal value**.

### Why incrementality is required
Incrementality compares:
- **Exposed (delivered)** customers vs.
- **Eligible holdout (not delivered)** customers

This estimates the counterfactual: *what would have happened without the campaign.*

### What decisions this enables
- Scale, optimize, or stop campaigns based on **incremental revenue**, not vanity metrics.
"""
)

st.warning(
    "Portfolio note: If you are using synthetic data, results are illustrative. "
    "The methodology and pipeline design are the deliverable."
)
