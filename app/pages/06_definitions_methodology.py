from __future__ import annotations

import streamlit as st

st.title("Definitions & Methodology")

st.markdown(
    r"""
### Key definitions
- **Exposed** = delivered message (selection is not exposure)
- **Holdout** = eligible but not delivered
- **Conversion** = ≥ 1 transaction in the attribution window
- **RPC (Revenue per Customer)** = total revenue in window / number of customers in group (includes zeros)

### Mandatory formulas
- **CR uplift = CR_exposed − CR_holdout**
- **RPC uplift = RPC_exposed − RPC_holdout**
- **Incremental revenue = RPC uplift × exposed customers**

### When results are not “causal”
Treat as **directional** when:
- Exposed or holdout group sizes are too small
- Holdout integrity is compromised (leakage)
- Campaign overlap/interference is high within the attribution window

### Scope boundaries (governance)
- No ML, no attribution models, no black-box logic.
- This framework supports **scale / optimize / stop** decisions for CRM campaigns using an explainable holdout design.
"""
)

st.info("If you swap synthetic data for real data, keep the same grains and definitions to preserve auditability.")
