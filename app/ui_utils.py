from __future__ import annotations

import numpy as np


def fmt_pct(x: float) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "—"
    return f"{x*100:.2f}%"


def fmt_num(x: float) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "—"
    return f"{x:,.0f}"


def fmt_money(x: float) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "—"
    return f"{x:,.2f}"


def decision_label(incremental_revenue: float, insufficient_flag: int) -> str:
    if insufficient_flag == 1:
        return "INSUFFICIENT EVIDENCE"
    if incremental_revenue > 0:
        return "SCALE / KEEP"
    if incremental_revenue < 0:
        return "STOP / INVESTIGATE"
    return "OPTIMIZE / RE-TEST"
