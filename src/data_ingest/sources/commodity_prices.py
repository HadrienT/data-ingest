"""Daily OHLCV for a handful of continuous commodity futures, from yfinance.

Free (delayed) front-month continuous contracts -- the same provider and
license terms as sp500-prices, just a different, small, fixed ticker list
matched to quant-modeling's instruments/commodity/* (forward and option on a
single underlying). No attempt at a full commodity curve: that would need the
individual contract months, not the continuous front-month series yfinance
gives for free.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pandas as pd

from ._yfinance_common import download_daily_ohlcv
from ..core.source import Source, Window
from ..core.spec import Column, TableSpec, WriteMode

logger = logging.getLogger("data_ingest.commodity_prices")

#: yfinance continuous-future ticker -> a stable, readable instrument id.
TICKERS = {
    "CL=F": "wti-crude",
    "BZ=F": "brent-crude",
    "NG=F": "natural-gas",
    "GC=F": "gold",
    "SI=F": "silver",
    "HG=F": "copper",
}

FIELDS = {
    "Date": "date",
    "Ticker": "instrument",
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Volume": "volume",
}


class CommodityPrices(Source):
    name = "commodity-prices"
    description = "Daily OHLCV for continuous commodity futures (yfinance)"
    write_mode = WriteMode.UPSERT
    # Futures trade nearly round the clock across sessions, unlike a single
    # equity exchange's hours, so there is no close-vs-timezone edge case to
    # align to here -- a plain lookback window is enough.
    schedule = "Mon..Fri 23:30"
    lookback_days = 5

    table = TableSpec(
        schema="prices",
        name="commodity_daily",
        columns=(
            Column("date", "DATE", nullable=False),
            Column("instrument", "TEXT", nullable=False),
            Column("open", "DOUBLE PRECISION"),
            Column("high", "DOUBLE PRECISION"),
            Column("low", "DOUBLE PRECISION"),
            Column("close", "DOUBLE PRECISION"),
            Column("volume", "BIGINT"),
        ),
        primary_key=("date", "instrument"),
        indexes=(("instrument", "date"),),
    )

    def fetch(self, window: Window) -> pd.DataFrame:
        tickers = list(TICKERS)
        logger.info("Fetching %s commodity tickers", len(tickers))
        today = datetime.now(timezone.utc).date()

        if window.full:
            start, end = "2000-01-01", today.strftime("%Y-%m-%d")
        else:
            start = (window.start or today).strftime("%Y-%m-%d")
            end = ((window.end or today) + timedelta(days=1)).strftime("%Y-%m-%d")

        raw = download_daily_ohlcv(tickers, start, end)
        if raw.empty:
            return raw
        raw["Ticker"] = raw["Ticker"].map(TICKERS)
        return raw.rename(columns=FIELDS)[list(FIELDS.values())]
