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

import logging
import os
from typing import List

import pandas as pd
from fredapi import Fred

from ..config import require
from ..core.source import Source, Window
from ..core.spec import Column, TableSpec, WriteMode

logger = logging.getLogger("data_ingest.fred_macro")

#: Reasonable starting set; override with FRED_SERIES as a comma-separated list.
DEFAULT_SERIES = [
    # ── Macro indicators ──────────────────────────────────────────────
    "GS10",       # 10-year Treasury constant maturity rate (monthly)
    "CPIAUCSL",   # CPI, all urban consumers
    "UNRATE",     # Unemployment rate
    "T10Y2Y",     # 10-year minus 2-year Treasury spread
    # ── Treasury CMT par-yield curve (daily), 1M → 30Y ────────────────
    "DGS1MO", "DGS3MO", "DGS6MO",
    "DGS1", "DGS2", "DGS3", "DGS5", "DGS7", "DGS10", "DGS20", "DGS30",
    # ── Overnight / short-rate references ─────────────────────────────
    "DFF",              # Effective federal funds rate
    "EFFR",             # Effective federal funds rate (NY Fed vintage)
    "SOFR",             # Secured Overnight Financing Rate
    "SOFR30DAYAVG",     # 30-day average SOFR
    "SOFR90DAYAVG",     # 90-day average SOFR
    "SOFR180DAYAVG",    # 180-day average SOFR
]


class FredMacro(Source):
    name = "fred-macro"
    description = "Macroeconomic series from FRED (revisions kept)"
    write_mode = WriteMode.VERSIONED
    # FRED publishes on business days, in the US morning. Once daily is plenty
    # for series that mostly move monthly.
    schedule = "Mon..Fri 23:00"
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
        client = Fred(api_key=require("FRED_API_KEY"))
        start = None if window.full else window.start

        frames = []
        for series_id in self.series():
            try:
                observations = client.get_series(series_id, observation_start=start)
            except Exception as exc:
                # One unavailable series must not sink the whole run.
                logger.warning("Skipping %s: %s", series_id, exc)
                continue
            if observations is None or observations.empty:
                logger.info("%s returned no observations", series_id)
                continue
            # Series.reset_index has no `names` argument; name the axis first.
            frame = observations.rename("value").rename_axis("date").reset_index()
            frame["series_id"] = series_id
            frames.append(frame)

        if not frames:
            return pd.DataFrame()

        data = pd.concat(frames, ignore_index=True)
        data["date"] = pd.to_datetime(data["date"]).dt.date
        data["value"] = pd.to_numeric(data["value"], errors="coerce")
        return data[["series_id", "date", "value"]]
