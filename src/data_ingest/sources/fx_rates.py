"""Daily FX spot rates from FRED (the Fed's H.10 release).

A separate source from fred-macro, in its own `fx` schema, because it is a
different domain (README: "schema is the domain the data belongs to") even
though the fetch mechanics are identical -- see _fred_common.fetch_fred_series.

Unlike macro indicators, these are not later restated: the H.10 release
publishes one noon buying rate per business day and does not revise it, so
this is UPSERT rather than VERSIONED (like prices, not like fred-macro).
"""

from __future__ import annotations

import os
from typing import List

import pandas as pd

from ._fred_common import fetch_fred_series
from ..core.source import Source, Window
from ..core.spec import Column, TableSpec, WriteMode

#: FRED series id -> what it quotes. Some are USD per foreign unit (EUR, GBP,
#: AUD), most are foreign units per USD -- check before dividing.
DEFAULT_SERIES = [
    "DEXUSEU",   # USD per EUR
    "DEXUSUK",   # USD per GBP
    "DEXJPUS",   # JPY per USD
    "DEXCHUS",   # CNY per USD
    "DEXCAUS",   # CAD per USD
    "DEXSZUS",   # CHF per USD
]


class FxRates(Source):
    name = "fx-rates"
    description = "Daily FX spot rates, major USD pairs (FRED H.10)"
    write_mode = WriteMode.UPSERT
    schedule = "Mon..Fri 23:00"
    lookback_days = 10

    table = TableSpec(
        schema="fx",
        name="daily_rates",
        columns=(
            Column("series_id", "TEXT", nullable=False),
            Column("date", "DATE", nullable=False),
            Column("value", "DOUBLE PRECISION"),
        ),
        primary_key=("series_id", "date"),
        indexes=(("date",),),
    )

    def series(self) -> List[str]:
        configured = os.getenv("FX_SERIES", "")
        if configured.strip():
            return [s.strip() for s in configured.split(",") if s.strip()]
        return DEFAULT_SERIES

    def fetch(self, window: Window) -> pd.DataFrame:
        return fetch_fred_series(self.series(), window)
