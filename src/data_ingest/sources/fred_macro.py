"""Macroeconomic series from FRED.

This source exists as much to prove the engine generalises as to be useful:
it has a different domain, a different frequency, an irregular publication
calendar and, unlike prices, its values are *revised*. It is therefore the
first VERSIONED source, and the reason that write mode exists.

FRED restates figures routinely: an unemployment rate published in January is
commonly revised in February and again later. Overwriting the old value would
make it impossible to ask what was actually knowable on a given day, which is
exactly the question a backtest has to answer. Each run therefore records what
FRED reported at that moment, and `macro.fred_series_latest` gives the current
vintage for callers that do not care.
"""

from __future__ import annotations

import os
from typing import List

import pandas as pd

from ._fred_common import fetch_fred_series
from ..core.source import Source, Window
from ..core.spec import Column, TableSpec, WriteMode

#: Reasonable starting set; override with FRED_SERIES as a comma-separated list.
DEFAULT_SERIES = [
    # ── Macro indicators ──────────────────────────────────────────────
    "GS10",       # 10-year Treasury constant maturity rate (monthly)
    "CPIAUCSL",   # CPI, all urban consumers
    "UNRATE",     # Unemployment rate
    "T10Y2Y",     # 10-year minus 2-year Treasury spread
    # ── Treasury CMT par-yield curve (daily), 1M → 30Y ────────────────
    # -- exactly the par-rate quotes bootstrap_curve() (quant-modeling) needs.
    "DGS1MO", "DGS3MO", "DGS6MO",
    "DGS1", "DGS2", "DGS3", "DGS5", "DGS7", "DGS10", "DGS20", "DGS30",
    # ── T-Bill secondary-market discount rates (daily) ────────────────
    # -- the genuine short-end deposit quote for a curve bootstrap's front
    # pillar; convert with t_bill_bond_equivalent_yield() before use, the
    # simple <=182-day formula does not apply to DTB1YR.
    "DTB4WK", "DTB3", "DTB6", "DTB1YR",
    # ── Overnight / short-rate references ─────────────────────────────
    "DFF",              # Effective federal funds rate
    "EFFR",             # Effective federal funds rate (NY Fed vintage)
    "SOFR",             # Secured Overnight Financing Rate
    "SOFR30DAYAVG",     # 30-day average SOFR
    "SOFR90DAYAVG",     # 90-day average SOFR
    "SOFR180DAYAVG",    # 180-day average SOFR
    # ── Volatility and credit, for vol-surface and xVA work ───────────
    "VIXCLS",           # CBOE Volatility Index
    "BAMLC0A0CM",       # ICE BofA US Corporate Index OAS (investment grade)
    "BAMLH0A0HYM2",     # ICE BofA US High Yield Index OAS
]


class FredMacro(Source):
    name = "fred-macro"
    description = "Macroeconomic series from FRED (revisions kept)"
    write_mode = WriteMode.VERSIONED
    # FRED publishes on business days, in the US morning. Once daily is plenty
    # for series that mostly move monthly.
    schedule = "0 23 * * 1-5"
    lookback_days = 400

    table = TableSpec(
        schema="macro",
        name="fred_series",
        columns=(
            Column("series_id", "TEXT", nullable=False),
            Column("date", "DATE", nullable=False),
            Column("value", "DOUBLE PRECISION"),
        ),
        primary_key=("series_id", "date"),
        indexes=(("date",),),
    )

    def series(self) -> List[str]:
        configured = os.getenv("FRED_SERIES", "")
        if configured.strip():
            return [s.strip() for s in configured.split(",") if s.strip()]
        return DEFAULT_SERIES

    def fetch(self, window: Window) -> pd.DataFrame:
        return fetch_fred_series(self.series(), window)
